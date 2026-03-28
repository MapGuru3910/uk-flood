# UK Flood Data Pipeline — Extension Plan

**Version:** 1.0.0  
**Date:** 2026-03-28  
**Status:** Draft  
**Purpose:** Detailed plan to extend the `australia-flood` pipeline pattern to ingest UK Environment Agency flood data

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Australia Pipeline — Pattern Summary](#2-australia-pipeline--pattern-summary)
3. [UK Data Sources — Detailed Inventory](#3-uk-data-sources--detailed-inventory)
4. [Gap Analysis — Australia vs UK](#4-gap-analysis--australia-vs-uk)
5. [Architecture Changes](#5-architecture-changes)
6. [CRS / Projection Handling](#6-crs--projection-handling)
7. [Schema Mapping](#7-schema-mapping)
8. [Classification Rules — UK Adaptation](#8-classification-rules--uk-adaptation)
9. [Implementation Steps](#9-implementation-steps)
10. [Gotchas, Risks and Mitigations](#10-gotchas-risks-and-mitigations)

---

## 1. Executive Summary

The `australia-flood` repo implements a production Databricks medallion pipeline (Bronze → Silver → Gold) that scrapes flood study data from two Australian government portals (NSW SES CKAN, Brisbane City Council OpenDataSoft), normalises it, downloads raster/report files, and registers raster metadata via Apache Sedona into shared Gold tables (`au_flood_report_index`, `au_flood_raster_catalog`).

The UK Environment Agency (EA) publishes a **fundamentally different** class of flood data via a REST API and the Defra Data Services Platform:

- **Real-time monitoring data** (stations, water levels, flood warnings) — high-frequency time-series data with no direct analogue in the Australia pipeline.
- **National flood risk products** (Flood Map for Planning zones, Risk of Flooding from Rivers and Sea, flood depth grids, flood extent polygons) — static/semi-static national-scale vector and raster products delivered as bulk downloads (Shapefile, GeoPackage, GeoJSON, GDB ZIP) and WMS/WFS services.
- **Modelled flood depth rasters** — per-OS-grid-square depth GeoTIFF/ASC files at defined AEP scenarios (1%, 0.1%), partitioned by OS grid reference.

The UK extension should target **three pipeline tracks**:

| Track | UK Source | Analogy to Australia | Cadence |
|-------|-----------|---------------------|---------|
| **EA Monitoring** | EA Real-Time Flood Monitoring API | _(new — no AU equivalent)_ | Every 15 min → hourly bronze, daily silver/gold |
| **EA Flood Risk Products** | Defra Data Services Platform (bulk downloads) | Closest to BCC (national downloads, vector + raster ZIP) | Quarterly or on-change |
| **EA Modelled Depths** | Defra Data Services Platform (per-grid-square ZIPs) | Closest to NSW SES (many downloadable raster archives) | On-change (infrequent — `notPlanned` updates) |

---

## 2. Australia Pipeline — Pattern Summary

### Architecture (from `docs/architecture.md`)

```
01_bronze_scrape → 02_silver_normalize → 03_gold_catalog → 04_download → 05_raster_ingest
```

- **Bronze:** Append-only scrape history, `_is_current` flag, `_scraped_at` timestamp
- **Silver:** SCD Type 2 (projects) or MERGE upsert (datasets/resources), classification applied here
- **Gold:** Full overwrite (source-specific catalogs) + source-scoped refresh (shared tables)
- **Download:** Driver-side `ThreadPoolExecutor`, MERGE results back to silver
- **Raster Ingest:** Sedona `RS_*` metadata extraction, batch processing, MERGE to shared `au_flood_raster_catalog`

### Key Files Referenced

| File | Role |
|------|------|
| `notebooks/nsw_ses/nsw_ses_01_bronze_scrape.py` | CKAN API scrape (5-stage parallel) |
| `notebooks/bcc/bcc_01_bronze_scrape.py` | ODS API scrape (parallel pagination) |
| `notebooks/*/02_silver_normalize.py` | Classification, SCD2, MERGE |
| `notebooks/*/03_gold_catalog.py` | Denormalized catalogs, shared report index |
| `notebooks/*/04_download.py` | File download + ZIP extraction to UC Volume |
| `notebooks/*/05_raster_ingest.py` | Sedona raster metadata extraction |
| `jobs/nsw_ses_pipeline_job.json` | Databricks Workflow definition |
| `jobs/bcc_pipeline_job.json` | Databricks Workflow definition |
| `docs/data_dictionary.md` | All table schemas |

### Shared Gold Tables (from `docs/architecture.md` §6)

| Table | Write Strategy | `_source` Key |
|-------|---------------|---------------|
| `au_flood_report_index` | DELETE + INSERT scoped by `_source` | `NSW_SES`, `BCC` |
| `au_flood_raster_catalog` | MERGE on `raster_id`, scoped by `_source` | `NSW_SES`, `BCC` |

Adding UK data means adding new `_source` values (e.g., `EA_MONITORING`, `EA_RISK`, `EA_DEPTH`) to these shared tables — exactly as designed in ADR-005.

---

## 3. UK Data Sources — Detailed Inventory

### 3.1 EA Real-Time Flood Monitoring API

| Property | Value |
|----------|-------|
| **Base URL** | `https://environment.data.gov.uk/flood-monitoring` |
| **Auth** | None — Open Government Licence, no key required |
| **SSL** | Valid cert, standard verification works |
| **Format** | JSON (default), CSV, RDF/XML, Turtle |
| **Update cadence** | Water levels every 15 min; flood warnings every 15 min |

**Key Endpoints:**

| Endpoint | Description | Pagination |
|----------|-------------|------------|
| `GET /id/stations` | All ~4,500 monitoring stations | `_limit` + `_offset` |
| `GET /id/stations/{id}` | Single station detail | N/A |
| `GET /id/stations/{id}/measures` | Measures at a station | N/A |
| `GET /id/measures` | All measures (~12,000) | `_limit` + `_offset` |
| `GET /id/floods` | All current flood warnings/alerts | None (returns all active) |
| `GET /id/floodAreas` | All flood warning/alert areas (~1,300) | `_limit` + `_offset` |
| `GET /id/floodAreas/{id}/polygon` | GeoJSON polygon for flood area | N/A |
| `GET /data/readings?latest` | Latest readings from ALL stations | None (single call) |
| `GET /data/readings?today` | Today's readings (all stations) | `_limit` + `_offset` |
| `GET /id/stations/{id}/readings?since={dt}` | Historical readings since datetime | `_limit` + `_offset` |

**Station fields (confirmed from live API):**

```json
{
  "@id": "http://environment.data.gov.uk/flood-monitoring/id/stations/1029TH",
  "RLOIid": "7041",
  "catchmentName": "Cotswolds",
  "dateOpened": "1994-01-01",
  "easting": 417990,
  "northing": 219610,
  "lat": 51.874767,
  "long": -1.740083,
  "label": "Bourton Dickler",
  "riverName": "River Dikler",
  "stationReference": "1029TH",
  "town": "Little Rissington",
  "status": "statusActive",
  "measures": [ ... ]
}
```

**Flood warning fields (confirmed from live API):**

```json
{
  "@id": "http://...id/floods/061WAF21PangSulm",
  "description": "River Pang from East Ilsley to Pangbourne...",
  "eaAreaName": "Thames",
  "floodArea": {
    "county": "Oxfordshire, West Berkshire",
    "notation": "061WAF21PangSulm",
    "polygon": "http://...id/floodAreas/061WAF21PangSulm/polygon",
    "riverOrSea": "River Pang, Sulham Brook"
  },
  "isTidal": false,
  "message": "...",
  "severity": "Flood alert",
  "severityLevel": 3,
  "timeRaised": "2026-03-27T09:10:18",
  "timeSeverityChanged": "2026-01-21T17:42:00"
}
```

### 3.2 EA Flood Risk Products (Defra Data Services Platform)

These are **national-scale, semi-static** datasets available via bulk download from `environment.data.gov.uk`. They use the Defra Data Services Platform (not CKAN, not ODS — a custom Defra platform).

| Dataset | data.gov.uk ID | Formats | CRS | Update |
|---------|---------------|---------|-----|--------|
| **Flood Map for Planning - Flood Zones** | `104434b0-5263-4c90-9b1e-e43b1d57c750` | SHP ZIP, GPKG ZIP, GeoJSON ZIP, GDB ZIP, WMS | EPSG:27700 (BNG) | Jan 2026 |
| **Flood Map for Planning - Flood Zones + Climate Change** | `77931470-ee6b-4f8e-8868-82842aed2e5d` | SHP ZIP, GPKG ZIP, GeoJSON ZIP, GDB ZIP | EPSG:27700 | Jan 2026 |
| **Risk of Flooding from Rivers and Sea (RoFRS)** | `943d2bbb-aa08-45d1-96cb-42556cd01d94` | WMS, download via Defra platform | EPSG:27700 | Jan 2026 |
| **Risk of Flooding from Surface Water (RoFSW)** | `0d6fa1f4-0c82-4c91-8667-a549e8e3ca2d` | WMS, download via Defra platform | EPSG:27700 | Jan 2026 |
| **Flood Depth Grid 20m** | `ee57d0dd-4e51-42d3-91b1-292b5ec2a2d9` | SHP ZIP, GPKG ZIP, GeoJSON ZIP, GDB ZIP, WMS | EPSG:27700 | Sep 2025 |
| **Historic Flood Map** | `76292bec-7d8b-43e8-9c98-02734fd89c81` | Various via Defra platform | EPSG:27700 | Feb 2026 |
| **Rivers & Sea 3.3% defended extents - present day** | `a87a7b78-75ea-4475-b5d7-cf5445a4765d` | Via Defra platform | EPSG:27700 | Jan 2026 |
| **Rivers & Sea 3.3% defended extents - climate change** | `e4e3c85c-07d4-421e-a8ac-e7aa7f8a526d` | Via Defra platform | EPSG:27700 | Jan 2026 |

**Download URL pattern:**
```
https://environment.data.gov.uk/api/file/download?fileDataSetId={uuid}&fileName={filename}
```

Or the Explore download:
```
https://environment.data.gov.uk/explore/{harvest-guid}?download=true
```

### 3.3 EA Modelled Flood Depth Data (Per-Grid-Square)

These are **partitioned raster datasets** — one ZIP per OS National Grid 100km square per AEP scenario. ~390 datasets on data.gov.uk matching the pattern `"Modelled fluvial flood depth data"`.

| Property | Value |
|----------|-------|
| **Pattern** | One dataset per grid square (SS, TR, SE, etc.) per model vintage/AEP |
| **Format** | ZIP containing ASC (ArcInfo ASCII Grid) rasters |
| **CRS** | EPSG:27700 (British National Grid) |
| **AEP scenarios** | 1% (1 in 100), 0.1% (1 in 1000) |
| **Download URL** | `https://environment.data.gov.uk/api/file/download?fileDataSetId={uuid}&fileName={grid}_Fluvial_Flood_Depth_Data_{pct}percent.zip` |
| **Update frequency** | `notPlanned` (2004 vintage data) |
| **Vintage** | Created 2004, published 2025 |

This is the closest analogue to the NSW SES raster data — many individual ZIP archives containing depth rasters that need downloading, extracting, and ingesting via Sedona.

---

## 4. Gap Analysis — Australia vs UK

### What maps directly from Australia pattern

| Australia Feature | UK Equivalent | Mapping Quality |
|-------------------|--------------|-----------------|
| Bronze scrape from API | EA Monitoring API scrape | ✅ Direct — different API shape but same pattern |
| Classification rules (AEP, product type) | UK AEP bandings (3.3%, 1%, 0.1%) | ✅ Direct — simpler (fewer AEP scenarios) |
| Download to UC Volume | Download EA ZIPs to UC Volume | ✅ Direct — same pattern |
| Raster ingest via Sedona | Ingest UK ASC/TIF rasters via Sedona | ✅ Direct — same `RS_*` functions |
| `au_flood_raster_catalog` shared table | Add `_source = 'EA_DEPTH'` rows | ✅ Designed for this (ADR-005) |
| `au_flood_report_index` shared table | Not applicable (no UK PDF reports in scope) | ⚠️ Could skip or adapt |
| Run ID chaining via `taskValues` | Same pattern | ✅ Direct |
| Pipeline audit log (`pipeline_run_log`) | Same pattern | ✅ Direct |

### What's fundamentally new / different

| UK Requirement | Australia Gap | Impact |
|----------------|-------------|--------|
| **Real-time time-series data** (15-min water levels) | AU pipeline only ingests static portal metadata + files | **Major** — needs new table design for time-series readings |
| **Flood warnings** (transient, severity-changing events) | No AU equivalent — AU portal has static study metadata | **Major** — needs event-tracking tables in silver/gold |
| **Flood area polygons** (via API with polygon endpoint) | AU has study-level bounding boxes only | **Moderate** — new vector ingest via `ST_*` functions |
| **BNG coordinate system** (EPSG:27700) | AU uses GDA94/GDA2020 MGA zones | **Moderate** — reprojection handled by Sedona, but CRS is uniform (single zone for all UK) |
| **National-scale vector products** (Flood Zones, RoFRS) | AU products are per-study/per-catchment | **Moderate** — file sizes much larger (national datasets), may need spatial partitioning |
| **No portal scrape needed for static products** | AU depends on CKAN/ODS discovery | **Simplification** — URLs are known/stable for Defra downloads |
| **EA API follows Linked Data / JSON-LD patterns** | AU uses CKAN v3 and ODS v2.1 | **Minor** — different JSON parsing, but simpler than CKAN |
| **No PDF flood study reports** (EA doesn't publish per-study PDFs like AU) | AU has rich report classification | **Simplification** — `au_flood_report_index` not needed for UK |
| **No per-study metadata hierarchy** (no org → project → dataset) | AU has deep CKAN hierarchy | **Simplification** — flatter data model |

---

## 5. Architecture Changes

### 5.1 Recommended Pipeline Structure

Create three new pipeline tracks under `notebooks/ea/`:

```
notebooks/
  ea/
    ea_monitoring/
      ea_mon_01_bronze_scrape.py       # Scrape stations, warnings, latest readings
      ea_mon_02_silver_normalize.py    # Normalize stations, readings, flood events
      ea_mon_03_gold_catalog.py        # Gold station catalog, flood event history, reading summaries
    ea_risk_products/
      ea_risk_01_bronze_catalog.py     # Catalog known Defra platform datasets (static list)
      ea_risk_02_download.py           # Download national flood zone / RoFRS / depth grid ZIPs
      ea_risk_03_vector_ingest.py      # Ingest vector layers (flood zones) via Sedona ST_*
      ea_risk_04_raster_ingest.py      # Ingest raster products via Sedona RS_*
    ea_depth/
      ea_depth_01_bronze_catalog.py    # Catalog per-grid-square depth datasets from data.gov.uk
      ea_depth_02_download.py          # Download per-grid-square ZIPs to UC Volume
      ea_depth_03_raster_ingest.py     # Sedona RS_* ingest → au_flood_raster_catalog
```

### 5.2 New Unity Catalog Assets

**Volumes:**
```sql
CREATE VOLUME IF NOT EXISTS ceg_delta_bronze_prnd.international_flood.ea_monitoring_raw;
CREATE VOLUME IF NOT EXISTS ceg_delta_bronze_prnd.international_flood.ea_risk_raw;
CREATE VOLUME IF NOT EXISTS ceg_delta_bronze_prnd.international_flood.ea_depth_raw;
```

**New Bronze Tables:**

| Table | Content |
|-------|---------|
| `ea_monitoring_stations_scrape` | All EA stations (append-only, `_is_current`) |
| `ea_monitoring_readings_scrape` | Time-series water level readings (append-only) |
| `ea_monitoring_floods_scrape` | Flood warnings/alerts (append-only, `_is_current`) |
| `ea_monitoring_flood_areas_scrape` | Flood warning area definitions (append-only, `_is_current`) |
| `ea_risk_product_catalog` | Static catalog of known Defra platform download URLs |

**New Silver Tables:**

| Table | Content |
|-------|---------|
| `ea_monitoring_stations` | Normalized station records (MERGE upsert) |
| `ea_monitoring_readings` | Partitioned readings table (append, partitioned by `date`) |
| `ea_monitoring_flood_events` | SCD Type 2 flood warning events |
| `ea_monitoring_flood_areas` | Flood area polygons with Sedona geometry (MERGE) |
| `ea_risk_flood_zones` | Flood Zone 2/3 vector data (MERGE on feature ID) |
| `ea_risk_rofrs` | RoFRS risk band polygons (MERGE) |
| `ea_depth_resources` | Per-grid-square download tracking (MERGE) |

**New Gold Tables:**

| Table | Content |
|-------|---------|
| `ea_monitoring_station_catalog` | Denormalized station + latest reading + status |
| `ea_monitoring_flood_event_history` | Full flood warning history with duration/peak analysis |
| `ea_flood_zone_catalog` | National flood zone spatial catalog |
| `ea_risk_assessment_catalog` | RoFRS probability bands |

**Extended Shared Tables:**

| Table | New `_source` Values |
|-------|---------------------|
| `au_flood_raster_catalog` | `EA_DEPTH`, `EA_RISK` |
| `au_flood_report_index` | _(not used for UK — no PDF reports)_ |

> **Naming decision:** Consider renaming `au_flood_*` → `flood_*` to reflect multi-country scope. This is optional — the `_source` column already provides filtering.

### 5.3 New Job Definitions

| Job | File | Schedule |
|-----|------|----------|
| `ea_flood_monitoring_pipeline` | `jobs/ea_monitoring_pipeline_job.json` | Every 4 hours (or hourly) |
| `ea_flood_risk_products_pipeline` | `jobs/ea_risk_pipeline_job.json` | Quarterly (or on-demand) |
| `ea_flood_depth_pipeline` | `jobs/ea_depth_pipeline_job.json` | On-demand (one-time ingest, then on-change) |

### 5.4 Network Requirements

| Host | Port | Purpose |
|------|------|---------|
| `environment.data.gov.uk` | 443 | EA Real-Time API + Defra download platform |
| `www.data.gov.uk` | 443 | Optional — data.gov.uk catalog queries |

---

## 6. CRS / Projection Handling

### UK Coordinate Systems

| CRS | EPSG | Usage |
|-----|------|-------|
| **British National Grid (BNG)** | 27700 | Default for all EA raster and vector products |
| **WGS84** | 4326 | EA API station lat/long; target for `au_flood_raster_catalog` bbox |
| **OSGB36** | 4277 | Underlying geodetic datum for BNG (rarely seen in data) |

### Key Difference from Australia

Australia has **multiple MGA zones** (49–56) depending on study location, requiring zone detection per-raster. UK uses a **single national grid** (EPSG:27700) for everything. This is a **simplification** — all UK rasters will have `crs_epsg = 27700`.

### Reprojection Strategy

Maintain the existing Sedona pattern from `nsw_ses_05_raster_ingest.py`:

```sql
CASE
    WHEN crs_epsg IS NOT NULL AND crs_epsg != 0
    THEN ST_Transform(
             native_envelope,
             CONCAT('EPSG:', CAST(crs_epsg AS STRING)),
             'EPSG:4326'
         )
    ELSE NULL
END
```

This works unchanged for EPSG:27700 → EPSG:4326. Sedona's GeoTools wrapper handles BNG↔WGS84 transformations natively.

### EA API Dual Coordinates

The EA monitoring API provides both BNG (easting/northing) and WGS84 (lat/long) per station. Store both in silver:

```python
"easting":   station["easting"],    # BNG
"northing":  station["northing"],   # BNG
"lat":       station["lat"],        # WGS84
"long":      station["long"],       # WGS84
```

No reprojection needed for point features — use the WGS84 values directly for the gold spatial catalog.

For flood area polygons (fetched from `{floodArea}/polygon`), the polygons may be in BNG. Apply `ST_Transform` from 27700 → 4326 in the silver normalization step.

---

## 7. Schema Mapping

### 7.1 EA Monitoring Station → `ea_monitoring_stations` (Silver)

| EA API Field | Silver Column | Type | Notes |
|-------------|--------------|------|-------|
| `@id` | `station_uri` | STRING | Full URI identifier |
| `stationReference` | `station_id` | STRING | Natural key (e.g., `1029TH`) |
| `label` | `station_name` | STRING | |
| `RLOIid` | `rloi_id` | STRING | River Levels on the Internet ID |
| `riverName` | `river_name` | STRING | |
| `catchmentName` | `catchment_name` | STRING | Maps to AU `river_basin_name` |
| `town` | `town` | STRING | Maps to AU `place_name` |
| `lat` | `lat` | DOUBLE | WGS84 |
| `long` | `lon` | DOUBLE | WGS84 |
| `easting` | `easting` | INTEGER | BNG |
| `northing` | `northing` | INTEGER | BNG |
| `dateOpened` | `date_opened` | DATE | |
| `status` | `station_status` | STRING | active/closed/suspended |
| _(derived)_ | `county` | STRING | From flood area join or API |
| _(derived)_ | `ea_area_name` | STRING | EA operational area |
| `measures` | `measures_json` | STRING | JSON array of measure metadata |

### 7.2 EA Flood Warning → `ea_monitoring_flood_events` (Silver, SCD Type 2)

| EA API Field | Silver Column | Type | Notes |
|-------------|--------------|------|-------|
| `@id` | `flood_warning_uri` | STRING | |
| `floodAreaID` | `flood_area_id` | STRING | Natural key |
| `description` | `description` | STRING | |
| `severity` | `severity` | STRING | `Severe Flood Warning`, `Flood Warning`, `Flood alert`, `Warning no longer in force` |
| `severityLevel` | `severity_level` | INTEGER | 1 (severe) → 4 (no longer in force) |
| `isTidal` | `is_tidal` | BOOLEAN | |
| `message` | `message` | STRING | Full advisory text |
| `timeRaised` | `time_raised` | TIMESTAMP | |
| `timeSeverityChanged` | `time_severity_changed` | TIMESTAMP | |
| `timeMessageChanged` | `time_message_changed` | TIMESTAMP | |
| `floodArea.county` | `county` | STRING | |
| `floodArea.riverOrSea` | `river_or_sea` | STRING | |
| `eaAreaName` | `ea_area_name` | STRING | |

**SCD Type 2 rationale:** Flood warnings change severity over time (raised → escalated → de-escalated → removed). Tracking these transitions has analytical value — identical to the NSW SES project approval state transitions.

### 7.3 EA Depth Rasters → `au_flood_raster_catalog` (Gold, shared)

| `au_flood_raster_catalog` Column | UK Mapping |
|----------------------------------|-----------|
| `raster_id` | `SHA256(volume_path)[:32]` — same pattern |
| `resource_id` | `SHA256("ea_depth::{grid_ref}::{aep_pct}")[:32]` |
| `volume_path` | `/Volumes/.../ea_depth_raw/{grid_ref}/{aep_pct}/{filename}` |
| `original_filename` | Extracted ZIP member name |
| `file_format` | `ASC` (primary for 2004-vintage data) |
| `source_state` | `'England'` (EA covers England only) |
| `source_agency` | `'Environment Agency'` |
| `study_name` | `'Modelled fluvial flood depth ({grid_ref})'` |
| `project_id` | data.gov.uk dataset UUID |
| `council_lga_primary` | _(NULL — national model, not council-specific)_ |
| `inferred_aep_pct` | `1.0` or `0.1` (from filename/dataset title) |
| `value_type` | `'depth'` |
| `crs_epsg` | `27700` (known; `.asc` files may not embed it) |
| `_source` | `'EA_DEPTH'` |
| `_state` | `'England'` |

### 7.4 AEP Mapping — UK vs Australia

| UK Terminology | AEP % | Return Period | AU Equivalent |
|----------------|-------|---------------|---------------|
| High risk (≥3.3%) | 3.3 | 30 yr | `q020` (5%) or `q050` (2%) — closest |
| Medium risk (1%–3.3%) | 1.0 | 100 yr | `q100` ✅ |
| Low risk (0.1%–1%) | 0.1 | 1000 yr | _(no standard AU equivalent)_ |
| Very Low (<0.1%) | <0.1 | >1000 yr | _(no standard AU equivalent)_ |
| Flood Zone 3 | 1.0 (rivers) / 0.5 (sea) | 100 yr / 200 yr | `q100` / `q200` |
| Flood Zone 2 | 0.1 | 1000 yr | _(no standard AU equivalent)_ |

---

## 8. Classification Rules — UK Adaptation

### 8.1 Product Category (for EA Risk Products)

| Source Dataset | `product_category` |
|---------------|-------------------|
| Flood Map for Planning - Flood Zones | `flood_zone` |
| Flood Zones + Climate Change | `flood_zone_cc` |
| Risk of Flooding from Rivers and Sea (RoFRS) | `rofrs` |
| Risk of Flooding from Surface Water (RoFSW) | `rofsw` |
| Flood Depth Grid 20m | `depth` |
| Historic Flood Map | `historic_extent` |
| Rivers & Sea defended extents | `defended_extent` |

### 8.2 AEP Inference (for depth grid downloads)

Parse from filename pattern `{grid}_Fluvial_Flood_Depth_Data_{n}percent.zip`:

| Filename Pattern | `inferred_aep_pct` | `inferred_return_period_yr` |
|-----------------|-------------------|---------------------------|
| `*_1percent*` | `1.0` | `100` |
| `*_0.1percent*` or `*_01percent*` | `0.1` | `1000` |

### 8.3 Severity Mapping (for flood warnings)

| `severityLevel` | `severity` | Numeric Risk Score |
|-----------------|-----------|-------------------|
| 1 | Severe Flood Warning | 4 (highest) |
| 2 | Flood Warning | 3 |
| 3 | Flood Alert | 2 |
| 4 | Warning no longer in force | 1 (lowest) |

---

## 9. Implementation Steps

### Phase 1: EA Monitoring Pipeline (Highest Value, Quickest Win)

**Duration estimate:** 2–3 weeks

#### Step 1.1: Create `notebooks/ea/ea_monitoring/ea_mon_01_bronze_scrape.py`

- **Model after:** `bcc_01_bronze_scrape.py` (simpler pagination, single API)
- **API calls:**
  1. `GET /id/stations` — full station list (paginate with `_limit=500&_offset=N`)
  2. `GET /id/floods` — all current warnings (no pagination needed — returns all active)
  3. `GET /id/floodAreas` — all flood area definitions (paginate)
  4. `GET /data/readings?latest` — single call for all latest readings
- **Bronze tables:** Write to `ea_monitoring_stations_scrape`, `ea_monitoring_floods_scrape`, `ea_monitoring_flood_areas_scrape`, and `ea_monitoring_readings_scrape`
- **Key difference from AU:** SSL verification **enabled** (EA has valid certs). No `beautifulsoup4` needed — pure JSON API.
- **HTTP config:** Reuse the `http_get()` pattern from `nsw_ses_01` — 30s timeout, 3 retries, exponential backoff

#### Step 1.2: Create `ea_mon_02_silver_normalize.py`

- **Model after:** `nsw_ses_02_silver_normalize.py`
- **Tables:**
  - `ea_monitoring_stations` — MERGE upsert on `station_id`
  - `ea_monitoring_flood_events` — SCD Type 2 on `flood_area_id` (track severity changes)
  - `ea_monitoring_flood_areas` — MERGE upsert on `flood_area_id`, with polygon fetched from `{area}/polygon` endpoint and stored as WKT via `ST_GeomFromGeoJSON` → `ST_AsText`
  - `ea_monitoring_readings` — Append-only, partitioned by `reading_date` (DATE column)
- **Readings handling:** The `?latest` endpoint returns one reading per measure. Append these to a time-partitioned table. Don't use SCD2 or MERGE — readings are immutable time-series data. Partition by `date` for efficient queries.

#### Step 1.3: Create `ea_mon_03_gold_catalog.py`

- **Model after:** `nsw_ses_03_gold_catalog.py`
- **Tables:**
  - `ea_monitoring_station_catalog` — Denormalized station + latest reading + status (full overwrite)
  - `ea_monitoring_flood_event_history` — Full SCD2 history of flood warnings with derived columns (`duration_hours`, `peak_severity`, `area_affected_wkt`)

#### Step 1.4: Create `jobs/ea_monitoring_pipeline_job.json`

- **Model after:** `jobs/bcc_pipeline_job.json`
- **Schedule:** `0 0 */4 ? * *` (every 4 hours) or hourly depending on data freshness needs
- **Cluster:** Same `au_flood_cluster` spec — Sedona is needed for flood area polygon processing

### Phase 2: EA Modelled Depth Pipeline (Raster-Heavy, Direct AU Analogue)

**Duration estimate:** 2–3 weeks

#### Step 2.1: Create `notebooks/ea/ea_depth/ea_depth_01_bronze_catalog.py`

- **Model after:** `bcc_01_bronze_scrape.py` but much simpler
- **Approach:** The modelled depth datasets have known, stable URLs on data.gov.uk. Rather than scraping a portal, maintain a **configuration table** of known datasets:
  - Query `data.gov.uk` CKAN API: `GET https://ckan.publishing.service.gov.uk/api/3/action/package_search?q="modelled fluvial flood depth"&rows=500`
  - Or hardcode the ~40 known grid-square dataset UUIDs as a seed catalog
- **Bronze table:** `ea_depth_catalog_scrape` — one row per grid-square dataset per AEP scenario
- **Fields:** `dataset_id`, `grid_reference`, `aep_pct_label`, `download_url`, `file_name`

#### Step 2.2: Create `ea_depth_02_download.py`

- **Model after:** `nsw_ses_04_download.py`
- **Volume path convention:**
  ```
  /Volumes/.../ea_depth_raw/
    {grid_reference}/
      {aep_pct}/
        {filename}.zip
        extracted/
          {member}.asc
  ```
- **Download tracking:** MERGE to `ea_depth_resources` silver table
- **ZIP extraction:** Identical to AU pattern — one level deep, basename only

#### Step 2.3: Create `ea_depth_03_raster_ingest.py`

- **Model after:** `bcc_05_raster_ingest.py` — closest pattern
- **Key difference:** All rasters are `.asc` files in EPSG:27700. Many `.asc` files will have `crs_epsg = 0` from `RS_SRID()` because ASCII Grid doesn't embed CRS.
- **CRS override:** When `crs_epsg = 0` AND `_source = 'EA_DEPTH'`, **default to EPSG:27700**:
  ```sql
  CASE
      WHEN crs_epsg = 0 AND _source = 'EA_DEPTH' THEN 27700
      ELSE crs_epsg
  END
  ```
  This is a safe assumption because ALL EA modelled depth data is in BNG.
- **MERGE to:** `au_flood_raster_catalog` with `_source = 'EA_DEPTH'`

#### Step 2.4: Create `jobs/ea_depth_pipeline_job.json`

- **Schedule:** On-demand (data doesn't update). Run once for initial ingest, then manually when EA republishes.

### Phase 3: EA Flood Risk Products Pipeline (Vector-Heavy)

**Duration estimate:** 3–4 weeks (includes new vector ingest pattern)

#### Step 3.1: Create `ea_risk_01_bronze_catalog.py`

- **Approach:** Maintain a static configuration of known Defra platform datasets and their download URLs
- **Bronze table:** `ea_risk_product_catalog` — one row per dataset per available format

#### Step 3.2: Create `ea_risk_02_download.py`

- **Model after:** `bcc_04_download.py`
- **Priority format:** GeoPackage (`.gpkg.zip`) — smallest, most efficient, single-file spatial format
- **Volume path:**
  ```
  /Volumes/.../ea_risk_raw/
    {product_name}/
      {filename}.gpkg.zip
      extracted/
        {product}.gpkg
  ```

#### Step 3.3: Create `ea_risk_03_vector_ingest.py`

- **Model after:** `notebooks/ica/ica_04_vector_ingest.py` (existing vector ingest pattern)
- **Use fiona/geopandas** or **Sedona `ST_*` functions** to load vector layers
- **Flood Zones ingestion:**
  ```python
  # Load GeoPackage via Sedona
  sedona.read.format("geopackage").load(gpkg_path)
  # Or via fiona + conversion to Spark DataFrame
  ```
- **Write to:** `ea_risk_flood_zones` (silver), `ea_flood_zone_catalog` (gold)

#### Step 3.4: Create `ea_risk_04_raster_ingest.py`

- For Flood Depth Grid 20m — same Sedona `RS_*` pattern as Phase 2
- MERGE to `au_flood_raster_catalog` with `_source = 'EA_RISK'`

---

## 10. Gotchas, Risks and Mitigations

### 10.1 EA API Rate Limits / Caching

**Risk:** The EA API docs say responses may be cached and recommend limiting calls to common patterns. No explicit rate limit is documented, but aggressive crawling may result in throttling or temporary blocks.

**Mitigation:**
- Use the `?latest` endpoint (single call for all readings) instead of per-station crawling — exactly as EA recommends
- Add a polite `User-Agent` header: `au-flood-pipeline/1.0 (+https://github.com/MapGuru3910/australia-flood; EA monitoring harvest)`
- Respect `Last-Modified` headers and use `If-Modified-Since` for conditional requests
- Keep scrape frequency to every 4 hours (not every 15 minutes) unless real-time requirements demand it

### 10.2 `.asc` Files Without Embedded CRS (EPSG:27700 default)

**Risk:** The `RS_SRID()` function returns `0` for ASCII Grid files because the format doesn't embed CRS metadata. This causes NULL bbox columns in `au_flood_raster_catalog`.

**Mitigation:** Apply a source-specific CRS default in `ea_depth_03_raster_ingest.py`:
```sql
COALESCE(NULLIF(RS_SRID(raster), 0), 27700) AS crs_epsg
```
This is safe because **all** EA modelled depth data is definitively in BNG (confirmed from dataset metadata: `Spatial reference system: EPSG/0/27700`). Document this assumption clearly.

### 10.3 Large National-Scale Downloads

**Risk:** National flood zone datasets (Flood Map for Planning, RoFRS) are very large — potentially multi-GB Shapefiles/GeoPackages covering all of England.

**Mitigation:**
- Download to UC Volume with streaming (1MB chunks, 600s read timeout — same as AU)
- Use GeoPackage format (smaller than Shapefile for vector data)
- Consider spatial partitioning in silver/gold using Sedona's `ST_*` functions
- Process vector layers in Spark (not driver-only) — these are too large for pandas
- Use `binaryFile` → Sedona for rasters; for vectors, consider `spark.read.format("geopackage")` if available, otherwise stage through fiona on driver with batched writes

### 10.4 Transient Flood Warning URIs

**Risk:** Flood warning URIs (e.g., `/id/floods/91436`) are transient — they return 404 after the warning expires. The warnings list endpoint only shows currently active warnings.

**Mitigation:**
- Scrape the full `/id/floods` endpoint on every run and store in bronze (append-only)
- Use SCD Type 2 in silver to track severity transitions
- **Never rely on fetching historical warning URIs** — treat bronze as the system of record
- Consider more frequent scraping (every 1–4 hours) during flood season (Oct–Mar) to capture short-lived warnings

### 10.5 British National Grid Easting/Northing vs WGS84

**Risk:** Mixing BNG (easting, northing) with WGS84 (lat, long) in queries without reprojection.

**Mitigation:**
- Store **both** coordinate systems in silver for stations (BNG from API + WGS84 from API)
- For all spatial queries in gold, standardize on WGS84 (consistent with existing `au_flood_raster_catalog` pattern)
- For UK-specific analyses, provide BNG columns as secondary (useful for OS grid reference lookups)

### 10.6 EA API Schema Stability

**Risk:** The EA API is marked "Beta" (version 0.9 as of March 2026). Schema changes could break the pipeline.

**Mitigation:**
- Store raw JSON responses in bronze (not just extracted fields)
- Use defensive parsing — `dict.get()` with defaults, not direct key access
- Version the scrape notebook and log the API version from the `meta.version` field
- Set up monitoring on the `pipeline_run_log` for schema change errors

### 10.7 England-Only Coverage

**Risk:** The EA Flood Monitoring API covers **England only**. Scotland (SEPA), Wales (NRW), and Northern Ireland (DfI) have separate agencies and APIs.

**Mitigation:**
- Clearly scope the UK extension as "England (EA)" initially
- Use `_state = 'England'` (not `'UK'`) in all tables
- Design the pipeline structure to accommodate future Scotland/Wales sources by using the same source-scoped refresh pattern (new `_source` values like `SEPA`, `NRW`)

### 10.8 Sedona GeoTools Compatibility with BNG

**Risk:** The GeoTools wrapper in Sedona must have BNG (EPSG:27700) projection definitions available for `ST_Transform` to work.

**Mitigation:**
- The `geotools-wrapper:1.8.1-28.2` JAR includes the EPSG database — BNG is a standard projection and should be available
- Test `ST_Transform(ST_Point(530000, 180000), 'EPSG:27700', 'EPSG:4326')` on the cluster before deploying the UK pipeline
- If BNG is not found, the GeoTools EPSG authority database may need to be explicitly loaded via an init script

### 10.9 Readings Table Growth

**Risk:** If scraping all station readings every 4 hours, the readings table will grow very quickly (~4,500 stations × multiple measures × 4 readings/day = potentially 50,000+ rows/day).

**Mitigation:**
- Partition `ea_monitoring_readings` by `reading_date` (DATE type)
- Apply `OPTIMIZE` + `ZORDER BY (station_id, reading_date)` weekly
- Implement a retention policy — `DELETE` readings older than N months to manage storage
- Consider Delta Lake's `VACUUM` on a more aggressive schedule for this table

---

## Appendix A: Repository Structure After UK Extension

```
australia-flood/
│
├── notebooks/
│   ├── bcc/                                   # Existing
│   ├── nsw_ses/                               # Existing
│   ├── ica/                                   # Existing
│   └── ea/                                    # NEW — UK Environment Agency
│       ├── ea_monitoring/
│       │   ├── ea_mon_01_bronze_scrape.py
│       │   ├── ea_mon_02_silver_normalize.py
│       │   └── ea_mon_03_gold_catalog.py
│       ├── ea_depth/
│       │   ├── ea_depth_01_bronze_catalog.py
│       │   ├── ea_depth_02_download.py
│       │   └── ea_depth_03_raster_ingest.py
│       └── ea_risk_products/
│           ├── ea_risk_01_bronze_catalog.py
│           ├── ea_risk_02_download.py
│           ├── ea_risk_03_vector_ingest.py
│           └── ea_risk_04_raster_ingest.py
│
├── jobs/
│   ├── bcc_pipeline_job.json                  # Existing
│   ├── nsw_ses_pipeline_job.json              # Existing
│   ├── ica_pipeline_job.json                  # Existing
│   ├── ea_monitoring_pipeline_job.json        # NEW
│   ├── ea_depth_pipeline_job.json             # NEW
│   └── ea_risk_pipeline_job.json              # NEW
│
├── docs/
│   ├── architecture.md                         # UPDATE — add UK sections
│   ├── data_dictionary.md                      # UPDATE — add UK tables
│   ├── deployment.md                           # UPDATE — add UK network/volume prereqs
│   ├── operations.md                           # UPDATE — add UK monitoring queries
│   ├── ea_monitoring_pipeline.md               # NEW
│   ├── ea_depth_pipeline.md                    # NEW
│   └── ea_risk_products_pipeline.md            # NEW
│
├── src/
│   └── au_flood/
│       ├── wa_dwer/                            # Existing
│       └── ea/                                 # NEW — shared UK utilities
│           ├── __init__.py
│           └── ea_api_client.py                # Typed client for EA API
│
└── README.md                                   # UPDATE — add UK section
```

## Appendix B: Implementation Priority Matrix

| Phase | Pipeline | Effort | Business Value | Dependencies |
|-------|----------|--------|---------------|--------------|
| **Phase 1** | EA Monitoring | 2–3 weeks | High (real-time flood warnings) | None |
| **Phase 2** | EA Modelled Depths | 2–3 weeks | High (raster depth grids → same gold table as AU) | None |
| **Phase 3** | EA Risk Products | 3–4 weeks | Medium (national vector products — useful but less novel) | Phase 1 for Sedona validation |

**Recommended start:** Phase 1 and Phase 2 in parallel (different developers), Phase 3 after Phase 1 validates the EA API integration pattern.

---

## Part 2: Full UK Coverage — SEPA (Scotland) & NRW (Wales)

**Added:** 2026-03-28  
**Purpose:** Extend coverage from England (EA) to Scotland (SEPA) and Wales (NRW) for full Great Britain coverage.

> **Research note:** All endpoints below were tested live on 2026-03-28. URLs, response formats, and field names are verified unless marked "⚠️ unverified".

---

### 11. SEPA (Scotland) Pipeline Track

#### 11.1 Data Sources

##### 11.1.1 SEPA Time Series API (KiWIS)

| Property | Value |
|----------|-------|
| **Base URL** | `https://timeseries.sepa.org.uk/KiWIS/KiWIS` |
| **Documentation** | `https://timeseriesdoc.sepa.org.uk/api-documentation/` |
| **Auth** | None — free access, no API key |
| **Format** | JSON, CSV |
| **Protocol** | KiWIS (Kisters Water Information System) REST — different from EA's Linked Data API |
| **Update cadence** | 15-minute readings for river stage; daily/monthly/annual aggregates |
| **Status** | ✅ LIVE — verified |

**Key Query Functions (all verified):**

| Request | Description | Pagination |
|---------|-------------|------------|
| `getStationList` | All monitoring stations (~500+) with lat/long, catchment, river | No pagination (returns all) |
| `getParameterList` | Parameters at a station: `S` (Stage), `Q` (Flow), `RE` (Rainfall) | Filter by `station_no` |
| `getTimeseriesList` | Available time series at a station, with `ts_id` values | Filter by `station_no` |
| `getTimeseriesValues` | Actual reading values for a `ts_id` with date range | Supports `from`/`to` dates |

**Query anatomy:**
```
https://timeseries.sepa.org.uk/KiWIS/KiWIS
  ?service=kisters
  &type=queryServices
  &datasource=0
  &request=getStationList
  &format=json
  &returnfields=station_no,station_name,station_latitude,station_longitude,catchment_name,river_name
  &station_no=*
```

**Station response format (confirmed — array-of-arrays, NOT JSON objects):**
```json
[
  ["station_no", "station_name", "station_latitude", "station_longitude", "catchment_name", "river_name"],
  ["15018", "Abbey St Bathans", "55.85329633", "-2.387448356", "Tweed", "Whiteadder Water"],
  ["234150", "Aberlour", "57.48021805", "-3.205975095", "Spey", "Spey"]
]
```

**Time series names available per station (example for station 14888 "Anie"):**

| `ts_id` | `ts_name` | `parametertype_name` | Description |
|---------|-----------|---------------------|-------------|
| `54512010` | `15minute` | `S` | 15-min stage readings |
| `54516010` | `Day.Max` | `S` | Daily maximum stage |
| `54517010` | `Day.Mean` | `S` | Daily mean stage |
| `54519010` | `HydrologicalYear.Max` | `S` | Annual maximum |
| `67431010` | `15minute` | `Q` | 15-min flow readings |
| `61917010` | `Day.Mean` | `Q` | Daily mean flow |

**Reading response (confirmed live — ts_id 54512010, 2026-03-27):**
```json
[{
  "ts_id": "54512010",
  "rows": "97",
  "columns": "Timestamp,Value",
  "data": [
    ["2026-03-27T00:00:00.000Z", 0.779],
    ["2026-03-27T00:15:00.000Z", 0.778],
    ...
  ]
}]
```

##### 11.1.2 SEPA Flood Warnings (RSS)

| Property | Value |
|----------|-------|
| **RSS Feed URL** | `https://floodline.sepa.org.uk/floodupdates/feed/` |
| **Format** | RSS 2.0 XML |
| **Status** | ✅ LIVE — verified (empty channel during non-flood period) |

```xml
<rss version="2.0">
<channel>
  <title>SEPA Floodwarning</title>
  <link>https://floodline.sepa.org.uk</link>
  <pubDate>Sat, 28 Mar 2026 09:32:37 +0000</pubDate>
</channel>
</rss>
```

**⚠️ No REST/JSON API.** SEPA does not expose a flood warning REST endpoint. The RSS feed is the only machine-readable source. Items appear during active warnings with severity, area, and message.

##### 11.1.3 SEPA Flood Maps

| Property | Value |
|----------|-------|
| **URL** | `https://map.sepa.org.uk/floodmaps/` |
| **Type** | Web viewer (ArcGIS-based internally) |
| **Bulk download** | ❌ Not available |
| **WMS/WFS** | ❌ Not publicly accessible (tested, 404) |

##### 11.1.4 Data Availability Summary

| Data Type | Available | Access Method |
|-----------|-----------|--------------|
| Real-time station readings | ✅ | KiWIS REST API |
| Station metadata (location, catchment) | ✅ | KiWIS REST API |
| Flood warnings | ⚠️ RSS only | RSS 2.0 XML |
| Flood zone maps (vector) | ❌ | Not available for download |
| Modelled depth grids (raster) | ❌ | Not available |
| Historic flood events | ❌ | Not available |

#### 11.2 Gap Analysis vs EA

| Feature | EA | SEPA | Gap Impact |
|---------|-----|------|-----------|
| Monitoring API | REST/JSON (Linked Data) | REST/JSON (KiWIS) | 🟡 Different API; new client needed |
| Bulk latest readings | `?latest` — one call | Must iterate per `ts_id` | 🟡 More API calls required |
| Flood warning API | REST/JSON | RSS/XML only | 🔴 No structured JSON; XML parsing |
| Warning area polygons | API + GeoJSON | Not available | 🔴 Cannot map warning areas |
| Flood zone downloads | SHP/GPKG/GDB bulk ZIP | Not available | 🔴 No vector products for Scotland |
| Depth grid rasters | Per-grid ASC ZIPs | Not available | 🔴 No raster products for Scotland |

#### 11.3 Notebook Structure

```
notebooks/sepa/
  sepa_monitoring/
    sepa_mon_01_bronze_scrape.py       # KiWIS stations + readings + RSS warnings
    sepa_mon_02_silver_normalize.py    # Normalize, MERGE/SCD2/append
    sepa_mon_03_gold_catalog.py        # Station catalog + warning history
```

**Notebook 01 — Bronze Scrape:**
1. `getStationList` → all stations with coords, catchment, river → `sepa_monitoring_stations_scrape`
2. For active `S`/`Q` stations: batch `getTimeseriesValues` for 15-min readings → `sepa_monitoring_readings_scrape`
3. Parse RSS feed → `sepa_monitoring_warnings_scrape`

**Key implementation notes:**
- KiWIS returns arrays-of-arrays; first row is headers: `data = [dict(zip(headers, row)) for row in response[1:]]`
- No pagination needed for station/parameter lists — KiWIS returns all results
- For readings: batch by comma-separated `ts_id` values
- RSS parsing: use `xml.etree.ElementTree` or `feedparser` library

**Notebook 02 — Silver:**
- `sepa_monitoring_stations` — MERGE on `station_no`
- `sepa_monitoring_readings` — Append, partition by `reading_date`
- `sepa_monitoring_warnings` — SCD Type 2 on warning `guid`

**Notebook 03 — Gold:**
- `sepa_monitoring_station_catalog` — denormalized station + latest reading
- `sepa_monitoring_warning_history` — complete warning event history

#### 11.4 CRS Handling

KiWIS provides **WGS84 lat/long directly** (`station_latitude`, `station_longitude`). No BNG easting/northing available from the API.

No reprojection needed for station points — simpler than EA which provides both BNG and WGS84.

#### 11.5 Schema Mapping

| KiWIS Field | Silver Column | EA Equivalent |
|------------|--------------|---------------|
| `station_no` | `station_id` | `stationReference` |
| `station_name` | `station_name` | `label` |
| `station_latitude` | `lat` | `lat` |
| `station_longitude` | `lon` | `long` |
| `catchment_name` | `catchment_name` | `catchmentName` |
| `river_name` | `river_name` | `riverName` |

#### 11.6 Gotchas and Risks

1. **KiWIS API is fundamentally different from EA API** — new client needed (`sepa_kiwis_client.py`)
2. **No "latest readings" bulk endpoint** — must discover `ts_id` values first, then batch
3. **RSS-only warnings** — must observe feed during active flood to understand XML structure
4. **SEPA 2020 cyber attack aftermath** — data infrastructure still rebuilding; URLs may change
5. **`open.sepa.org.uk` unreachable** — domain not responding (connection refused)
6. **No flood risk spatial downloads** — significant gap vs EA
7. **Readings volume** — ~500 stations × 96 readings/day = 48,000+ rows/day

---

### 12. NRW (Wales) Pipeline Track

#### 12.1 Data Sources

##### 12.1.1 DataMapWales GeoServer — NRW Flood Layers (WFS 2.0.0)

| Property | Value |
|----------|-------|
| **WFS Endpoint** | `https://datamap.gov.wales/geoserver/inspire-nrw/wfs` |
| **Auth** | None |
| **Format** | GML, GeoJSON, Shapefile via WFS |
| **CRS** | EPSG:27700 (BNG) |
| **Status** | ✅ LIVE — verified with GetCapabilities, DescribeFeatureType, and GetFeature |

**20 Confirmed NRW Flood Layers:**

| # | WFS Layer Name | Category | Description |
|---|---------------|----------|-------------|
| 1 | `NRW_FLOOD_WARNING` | Warnings | Flood warning area polygons |
| 2 | `NRW_FLOODZONE_RIVERS` | Zones | Flood Zone — Rivers |
| 3 | `NRW_FLOODZONE_SEAS` | Zones | Flood Zone — Seas |
| 4 | `NRW_FLOODZONE_RIVERS_SEAS_MERGED` | Zones | Combined flood zones |
| 5 | `NRW_FLOODZONE_SURFACE_WATER_AND_SMALL_WATERCOURSES` | Zones | Surface water flood zones |
| 6 | `NRW_FLOOD_RISK_FROM_RIVERS` | Risk | Risk of flooding — rivers |
| 7 | `NRW_FLOOD_RISK_FROM_SEA` | Risk | Risk of flooding — sea |
| 8 | `NRW_FLOOD_RISK_FROM_SURFACE_WATER_SMALL_WATERCOURSES` | Risk | Risk of flooding — surface water |
| 9 | `NRW_AREA_BENEFITING_FROM_FLOOD_DEFENCE` | Defence | Areas behind flood defences |
| 10 | `NRW_FLOOD_RISK_AREAS` | Assessment | Flood Risk Areas (Floods Directive) |
| 11 | `NRW_FLOODMAP_FLOOD_STORAGE` | Storage | Flood storage areas |
| 12 | `NRW_NATIONAL_FLOOD_RISK_RIVER_PEOPLE` | National Risk | River risk — people at risk |
| 13 | `NRW_NATIONAL_FLOOD_RISK_RIVER_ECONOMIC` | National Risk | River risk — economic damage |
| 14 | `NRW_NATIONAL_FLOOD_RISK_RIVER_ENVIRO` | National Risk | River risk — environmental |
| 15 | `NRW_NATIONAL_FLOOD_RISK_SEA_PEOPLE` | National Risk | Sea risk — people at risk |
| 16 | `NRW_NATIONAL_FLOOD_RISK_SEA_ECONOMIC` | National Risk | Sea risk — economic damage |
| 17 | `NRW_NATIONAL_FLOOD_RISK_SEA_ENVIRO` | National Risk | Sea risk — environmental |
| 18 | `NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_PEOPLE` | National Risk | SW risk — people at risk |
| 19 | `NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_ECON` | National Risk | SW risk — economic damage |
| 20 | `NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_ENVIRO` | National Risk | SW risk — environmental |

**Verified schemas (from DescribeFeatureType):**

`NRW_FLOOD_WARNING`:
```
objectid (int), region (string), area (string), fwd_tacode (string),
fwis_code (string), fwa_name (string), descrip (string), geom (MultiPolygon)
```

`NRW_FLOODZONE_RIVERS`:
```
objectid (int), mm_id (string), pub_date (date), risk (string),
risk_cy (string), geom (MultiPolygon)
```

**Sample GetFeature response (confirmed):**
```json
{
  "type": "FeatureCollection",
  "features": [{
    "type": "Feature",
    "id": "NRW_FLOOD_WARNING.1",
    "geometry": {
      "type": "MultiPolygon",
      "coordinates": [[[294580.99, 378709.99], ...]]
    },
    "properties": {"objectid": 1, "region": "...", "fwis_code": "..."}
  }]
}
```

##### 12.1.2 Bilingual Data (Welsh/English)

NRW datasets include bilingual fields per the Welsh Language Measure 2011:
- `risk` (English) / `risk_cy` (Welsh)
- Display labels: `propertyNamesEn` / `propertyNamesCy`

**Pipeline rule:** Store both. Use English as canonical for analytics. Suffix Welsh fields with `_cy`.

##### 12.1.3 NRW Flood Warning Service (Web)

| Property | Value |
|----------|-------|
| **URL** | `https://flood-warning.naturalresources.wales/` |
| **API** | `/api/warnings` returned 404 — no public REST API |
| **Status** | ⚠️ Web-only; no programmatic access to live warnings |

The WFS layer `NRW_FLOOD_WARNING` provides **static warning area definitions** (polygons), not live warning status. To get real-time warning status would require web scraping.

##### 12.1.4 Real-Time River Levels

The previously listed `rloi.naturalresourceswales.gov.wales` is unreachable (HTTP 000). NRW does not expose a public river level monitoring API.

**Partial workaround:** EA monitors some cross-border rivers (Dee, Wye, Severn at Welsh border). These stations appear in the EA API with `stationReference` prefixes.

#### 12.2 Gap Analysis vs EA

| Feature | EA | NRW | Gap Impact |
|---------|-----|------|-----------|
| Real-time stations | ✅ REST API | ❌ No public API | 🔴 No monitoring data for Wales |
| Flood warnings | ✅ REST/JSON | ⚠️ Static WFS polygons only | 🟡 Area definitions but no live status |
| Flood zone maps | ✅ Bulk download (SHP/GPKG) | ✅ WFS (20 layers) | 🟢 **Better** — live WFS > bulk files |
| Risk of flooding | ✅ RoFRS/RoFSW | ✅ 9 risk layers (rivers/sea/SW × people/econ/enviro) | 🟢 **Richer** than EA |
| Flood defences | ✅ Defended extents | ✅ Areas benefiting from defences | 🟢 Direct equivalent |
| Flood storage | ❌ Not separate | ✅ Flood storage areas | 🟢 **Bonus** — not in EA |
| Depth grids | ✅ Per-grid ASC | ❌ Not available | 🔴 No rasters for Wales |
| Bilingual support | N/A | ✅ English + Welsh fields | 🟡 Extra fields to handle |

#### 12.3 Notebook Structure

```
notebooks/nrw/
  nrw_risk_products/
    nrw_risk_01_bronze_scrape.py       # WFS GetFeature for all 20 layers
    nrw_risk_02_silver_normalize.py    # Normalize, classify, bilingual handling
    nrw_risk_03_gold_catalog.py        # Denormalized catalogs
    nrw_risk_04_vector_ingest.py       # Sedona ST_* spatial registration
    nrw_risk_05_monitoring.py          # Change detection + pipeline health
```

**Notebook 01 — Bronze Scrape:**
- For each of 20 layers: WFS `GetFeature` with GeoJSON output, paginated by `count=10000&startIndex=N`
- Write to `nrw_flood_layers_scrape` (append-only, `_is_current`)

**WFS call pattern:**
```
GET https://datamap.gov.wales/geoserver/inspire-nrw/wfs
  ?service=WFS&version=2.0.0&request=GetFeature
  &typeName=inspire-nrw:NRW_FLOODZONE_RIVERS
  &outputFormat=application/json
  &count=10000&startIndex=0
```

**Notebook 02 — Silver:**
- `nrw_flood_zones` — MERGE on `objectid` per source layer
- `nrw_flood_risk_areas` — MERGE on `objectid`
- `nrw_flood_warning_areas` — MERGE on `fwis_code`
- `nrw_flood_defences` — MERGE on `objectid`

**Notebook 03 — Gold:**
- `nrw_flood_zone_catalog` — flood zones with risk classification
- `nrw_flood_risk_catalog` — 9-category risk assessment (rivers/sea/SW × people/econ/enviro)
- `nrw_flood_warning_area_catalog` — warning areas

**Notebook 04 — Vector Ingest:**
```python
# Load WFS GeoJSON features
raw_df = spark.read.json(wfs_json_path)

# Parse geometry (BNG coordinates)
parsed = raw_df.selectExpr(
    "properties.objectid AS feature_id",
    "properties.risk AS risk_category",
    "properties.risk_cy AS risk_category_cy",
    "ST_GeomFromGeoJSON(geometry) AS geom_bng"
)

# Transform to WGS84
gold = parsed.selectExpr(
    "*",
    "ST_Transform(geom_bng, 'EPSG:27700', 'EPSG:4326') AS geom_wgs84",
    "ST_AsText(ST_Transform(geom_bng, 'EPSG:27700', 'EPSG:4326')) AS geometry_wkt_wgs84"
)
```

#### 12.4 CRS Handling

All NRW WFS layers use EPSG:27700 (BNG). Same as EA — use `ST_Transform(geom, 'EPSG:27700', 'EPSG:4326')` in Sedona.

**Important:** WFS GeoJSON output returns coordinates in **native CRS** (BNG), not WGS84. The numbers will be in the 100,000-700,000 range (eastings/northings), not degrees.

#### 12.5 Schema Mapping

**NRW Flood Zone → Unified Gold Table:**

| WFS Field | Gold Column | EA Equivalent |
|-----------|-----------|---------------|
| `objectid` | `feature_id` | Feature ID |
| `mm_id` | `model_id` | _(EA doesn't have)_ |
| `pub_date` | `publication_date` | _(metadata in EA)_ |
| `risk` | `risk_category` | Zone 2/3 |
| `risk_cy` | `risk_category_cy` | _(no Welsh in EA)_ |
| `geom` (BNG) | `geometry_bng` | Same CRS |
| _(transformed)_ | `geometry_wgs84` | Reprojected |
| `'NRW'` | `_source` | `'EA_RISK'` |
| `'Wales'` | `_state` | `'England'` |

#### 12.6 Gotchas and Risks

1. **WFS response size** — national flood zone layers may be multi-GB GeoJSON; use pagination and stream to disk
2. **No real-time data** — biggest gap; no river levels or live warnings for Wales
3. **Bilingual fields** — must store both `risk` and `risk_cy`; analytics on English only
4. **WFS pagination** — use `count`/`startIndex`; some GeoServer implementations have `maxFeatures` limits
5. **No depth grids** — NRW does not publish rasters; pipeline is vector-only
6. **GeoServer rate limits** — implement retry with backoff for 429/503 responses

---

### 13. Full UK Coverage Summary

#### 13.1 Coverage Matrix

| Capability | EA (England) | SEPA (Scotland) | NRW (Wales) |
|-----------|-------------|-----------------|-------------|
| **Real-time stations** | ~4,500 (REST) | ~500 (KiWIS) | ❌ None |
| **Live flood warnings** | ✅ REST/JSON | ⚠️ RSS/XML | ❌ No API |
| **Warning area polygons** | ✅ API GeoJSON | ❌ None | ✅ WFS |
| **Flood zone maps** | ✅ Bulk ZIP | ❌ Web only | ✅ WFS (20 layers) |
| **Risk assessment** | ✅ RoFRS/RoFSW | ❌ None | ✅ 9 categories |
| **Depth grid rasters** | ✅ Per-grid ASC | ❌ None | ❌ None |
| **Flood defences** | ✅ Defended extents | ❌ None | ✅ Benefiting areas |
| **CRS** | EPSG:27700 | WGS84 (API) | EPSG:27700 |

#### 13.2 Build Order (Full UK)

| Phase | Pipeline | Weeks | Priority | Dependency |
|-------|----------|-------|----------|-----------|
| 1 | EA Monitoring | 2–3 | ⭐⭐⭐⭐⭐ | None |
| 2 | EA Modelled Depths | 2–3 | ⭐⭐⭐⭐ | None |
| 3 | EA Risk Products | 3–4 | ⭐⭐⭐ | Phase 1 |
| **4** | **NRW Risk Products (WFS)** | **2–3** | **⭐⭐⭐⭐** | Phase 3 |
| **5** | **SEPA Monitoring (KiWIS)** | **2–3** | **⭐⭐⭐** | Phase 1 |
| **6** | **SEPA Warnings (RSS)** | **1** | **⭐⭐** | Phase 5 |
| Future | SEPA Risk Products | TBD | ⭐ | Blocked on data |

**Phases 4–6 are new additions for full UK coverage.**

#### 13.3 New Repository Assets

```
notebooks/nrw/nrw_risk_products/      # 5 notebooks
notebooks/sepa/sepa_monitoring/        # 3 notebooks
jobs/nrw_risk_pipeline_job.json
jobs/sepa_monitoring_pipeline_job.json
src/au_flood/sepa/sepa_kiwis_client.py
src/au_flood/nrw/nrw_wfs_client.py
src/au_flood/shared/rss_parser.py
docs/nrw_risk_pipeline.md
docs/sepa_monitoring_pipeline.md
```

#### 13.4 Network Requirements (Additions)

| Host | Port | Purpose |
|------|------|---------|
| `timeseries.sepa.org.uk` | 443 | SEPA KiWIS API |
| `floodline.sepa.org.uk` | 443 | SEPA flood warnings RSS |
| `datamap.gov.wales` | 443 | NRW WFS/WMS + GeoNode API |
