# UK Flood Data Pipeline — Architecture

**Version:** 1.0.0  
**Date:** 2026-03-28  
**Status:** Phase 1 (EA England) + Phase 4 (NRW Wales) implemented

---

## 1. Overview

The UK Flood Data Pipeline is a Databricks medallion architecture (Bronze → Silver → Gold) that ingests flood monitoring and risk data from UK government sources into Unity Catalog Delta Lake tables.

The pipeline follows the same patterns established in the `australia-flood` repository and extends them with UK-specific data sources, CRS handling, and bilingual field support.

---

## 2. Pipeline Tracks — Phase 1 & Phase 4

### Track 1: EA Real-Time Flood Monitoring (England)

```
ea_01_bronze_scrape → ea_02_silver_normalize → ea_03_gold_catalog → ea_04_download → ea_05_raster_ingest
```

**Source:** Environment Agency Real-Time Flood Monitoring API  
**Base URL:** `https://environment.data.gov.uk/flood-monitoring`  
**Auth:** None (Open Government Licence)  
**Update cadence:** Every 4 hours  
**Coverage:** England only

**Data ingested:**
- ~4,500 monitoring stations (lat/long + BNG coordinates)
- All currently active flood warnings (transient — SCD2 tracked)
- ~1,300 flood warning area definitions (polygon references)
- Latest readings from all stations (one call per run)

**Key design decisions:**
- `ea_01` uses `GET /data/readings?latest` — single call for all readings (EA-recommended pattern)
- `ea_02` implements SCD2 for flood events (severity transitions are analytically valuable)
- Stations are MERGE upsert; readings are append-only time-series
- `ea_04` and `ea_05` are graceful skips — EA Monitoring is vector/tabular only

### Track 2: NRW Wales Flood Risk Products

```
nrw_01_bronze_scrape → nrw_02_silver_normalize → nrw_03_gold_catalog → nrw_04_download → nrw_05_raster_ingest
```

**Source:** DataMapWales GeoServer WFS  
**Base URL:** `https://datamap.gov.wales/geoserver/inspire-nrw/wfs`  
**Auth:** None (publicly accessible)  
**Update cadence:** Weekly  
**Coverage:** Wales only

**Data ingested (20 confirmed WFS layers):**
- Flood zone polygons (rivers, seas, surface water, combined)
- Flood risk assessment areas (9 categories: rivers/sea/SW × people/economic/environmental)
- Flood warning area definitions (with FWIS codes)
- Areas benefiting from flood defences
- Flood storage areas

**Key design decisions:**
- All 20 layers stored in a single `nrw_flood_layers_scrape` bronze table (partitioned by `layer_name`)
- Bilingual fields stored: `risk` (English) and `risk_cy` (Welsh/Cymraeg)
- EPSG:27700 → EPSG:4326 reprojection via Sedona `ST_Transform` in silver
- `nrw_04` and `nrw_05` are graceful skips — NRW WFS data is fetched inline (no file downloads)

---

## 3. Unity Catalog Structure

### Catalog Layout

```
ceg_delta_bronze_prnd.international_flood.*   — Bronze tables + audit log + volumes
ceg_delta_silver_prnd.international_flood.*   — Silver tables
ceg_delta_gold_prnd.international_flood.*     — Gold tables
```

### Volumes

```
/Volumes/ceg_delta_bronze_prnd/international_flood/ea_monitoring_raw/
  — Reserved for EA monitoring file downloads (future use)

/Volumes/ceg_delta_bronze_prnd/international_flood/nrw_risk_raw/
  — Reserved for NRW bulk downloads (future use if NRW publishes bulk files)
```

### Bronze Tables

| Table | Source | Content |
|-------|--------|---------|
| `ea_monitoring_stations_scrape` | EA API | Station metadata snapshots |
| `ea_monitoring_floods_scrape` | EA API | Active flood warning snapshots |
| `ea_monitoring_flood_areas_scrape` | EA API | Flood area definition snapshots |
| `ea_monitoring_readings_scrape` | EA API | Latest readings (one per measure) |
| `nrw_flood_layers_scrape` | NRW WFS | All 20 WFS layer feature snapshots |
| `pipeline_run_log` | All pipelines | Audit log — one row per notebook per run |

### Silver Tables

| Table | Source | Write Strategy |
|-------|--------|---------------|
| `ea_monitoring_stations` | EA | MERGE upsert on `station_id` |
| `ea_monitoring_flood_events` | EA | SCD Type 2 on `flood_area_id` |
| `ea_monitoring_flood_areas` | EA | MERGE upsert on `notation` |
| `ea_monitoring_readings` | EA | Append-only, partitioned by `reading_date` |
| `nrw_flood_zones` | NRW | MERGE on `(layer_name, objectid)` |
| `nrw_flood_risk_areas` | NRW | MERGE on `(layer_name, objectid)` |
| `nrw_flood_warning_areas` | NRW | MERGE on `fwis_code` |
| `nrw_flood_defences` | NRW | MERGE on `(layer_name, objectid)` |

### Gold Tables

| Table | Source | Write Strategy |
|-------|--------|---------------|
| `ea_monitoring_station_catalog` | EA | Full overwrite (current snapshot) |
| `ea_monitoring_flood_event_history` | EA | Full overwrite (all SCD2 versions) |
| `nrw_flood_zone_catalog` | NRW | Full overwrite |
| `nrw_flood_risk_catalog` | NRW | Full overwrite |
| `nrw_flood_warning_area_catalog` | NRW | Full overwrite |
| `au_flood_raster_catalog` | Shared | Source-scoped MERGE (EA_DEPTH, EA_RISK) |

---

## 4. CRS / Coordinate Systems

| Source | Native CRS | Storage | Gold CRS |
|--------|-----------|---------|---------|
| EA stations | WGS84 (lat/long) + BNG (easting/northing) | Both stored in silver | WGS84 |
| EA flood area polygons | GeoJSON (typically WGS84 from API) | As fetched | WGS84 |
| NRW WFS features | EPSG:27700 (BNG) | BNG WKT in silver | WGS84 via Sedona |
| EA depth rasters | EPSG:27700 (BNG) | BNG native | WGS84 bbox via Sedona |

**NRW reprojection:**  
`ST_Transform(ST_GeomFromGeoJSON(geometry_json), 'EPSG:27700', 'EPSG:4326')`

Both native BNG WKT (`geometry_wkt_bng`) and WGS84 WKT (`geometry_wkt_wgs84`) are stored in silver and carried to gold.

---

## 5. Run ID Chaining

All notebooks in a pipeline track share a single `run_id` (UUID) via Databricks Task Values:

```
notebook_01 → dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
notebook_02 → bronze_run_id = {{tasks.notebook_01.values.run_id}}
notebook_03 → silver_run_id = {{tasks.notebook_01.values.run_id}}
```

The `run_id` is also written to the `pipeline_run_log` audit table by every notebook stage, enabling full pipeline traceability.

---

## 6. Shared Infrastructure

### Shared Gold Tables (from australia-flood)

The `au_flood_raster_catalog` and `au_flood_report_index` tables from the `australia-flood` pipeline are extended to support UK data:

| Table | UK `_source` Values |
|-------|---------------------|
| `au_flood_raster_catalog` | `EA_DEPTH`, `EA_RISK` |
| `au_flood_report_index` | _(not used for UK — no PDF reports)_ |

### Cluster Configuration

All pipelines use the `uk_flood_cluster` (or `au_flood_cluster` from the Australia repo) with:
- Spark 17.3.x (Databricks Runtime)
- Apache Sedona: `org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.8.1`
- GeoTools wrapper: `org.datasyslab:geotools-wrapper:1.8.1-28.2`

Sedona is required for:
- NRW silver: `ST_GeomFromGeoJSON`, `ST_Transform`, `ST_AsText`
- EA depth raster ingest (future): `RS_*` functions

---

## 7. Error Handling

All notebooks follow the pattern:
1. `try` block executes pipeline logic
2. `except` captures errors to `_pipeline_error`
3. `finally` always writes an audit record (success or failure)
4. Audit record includes `error_message`, `status`, and `extra_metadata` JSON

Partial failures (e.g., one WFS layer fails while others succeed) are recorded as `status = "partial"` in the audit log.

---

## 8. Network Requirements

| Host | Port | Purpose |
|------|------|---------|
| `environment.data.gov.uk` | 443 | EA Real-Time Monitoring API |
| `datamap.gov.wales` | 443 | NRW WFS / DataMapWales GeoServer |

---

## 9. Future Phases

| Phase | Pipeline | Status |
|-------|----------|--------|
| 1 | EA Monitoring (England) | ✅ **Implemented** |
| 2 | EA Modelled Depths (England rasters) | 📋 Planned |
| 3 | EA Risk Products (national vector + raster) | 📋 Planned |
| 4 | NRW Wales WFS (Wales) | ✅ **Implemented** |
| 5 | SEPA Monitoring (Scotland, KiWIS API) | 📋 Planned |
| 6 | SEPA Warnings (Scotland, RSS) | 📋 Planned |
