# UK Flood Data Pipeline — SEPA (Scotland) & NRW (Wales) Extension Plan

**Version:** 1.0.0  
**Date:** 2026-03-28  
**Status:** Draft  
**Purpose:** Extend UK flood data pipeline coverage from England (EA) to include Scotland (SEPA) and Wales (NRW) for full Great Britain coverage

---

## 1. SEPA (Scotland) Pipeline Track

### 1.1 Data Sources — Detailed Inventory

#### 1.1.1 SEPA Time Series API (KiWIS)

| Property | Value |
|----------|-------|
| **Base URL** | `https://timeseries.sepa.org.uk/KiWIS/KiWIS` |
| **Documentation** | `https://timeseriesdoc.sepa.org.uk/api-documentation/` |
| **Auth** | None — free access, no API key |
| **SSL** | Valid cert |
| **Format** | JSON (default), CSV, HTML |
| **Update cadence** | Real-time: 15-minute readings for river levels; daily/monthly aggregates |
| **Protocol** | KiWIS (Kisters Water Information System) REST API — NOT the same API pattern as EA |
| **Status** | ✅ LIVE — verified 2026-03-28 |

**Key Endpoints (verified live):**

| Request Function | Description | Example |
|-----------------|-------------|---------|
| `getStationList` | All monitoring stations (~500+) | `?service=kisters&type=queryServices&datasource=0&request=getStationList&format=json&returnfields=station_no,station_name,station_latitude,station_longitude,catchment_name,river_name&station_no=*` |
| `getParameterList` | Parameters at a station (S=Stage, Q=Flow, RE=Rain) | `?...&request=getParameterList&station_no=14888` |
| `getTimeseriesList` | Available time series at a station | `?...&request=getTimeseriesList&station_no=14888` |
| `getTimeseriesValues` | Actual data values for a time series | `?...&request=getTimeseriesValues&ts_id=54512010&from=2026-03-27&to=2026-03-28` |

**Station fields (confirmed from live response):**

```json
["station_no", "station_name", "station_latitude", "station_longitude", "catchment_name", "river_name"]
["15018", "Abbey St Bathans", "55.85329633", "-2.387448356", "Tweed", "Whiteadder Water"]
["234150", "Aberlour", "57.48021805", "-3.205975095", "Spey", "Spey"]
```

**Time series naming pattern:**

| `ts_name` | `parametertype_name` | Meaning |
|-----------|---------------------|---------|
| `15minute` | `S` | 15-min stage (level) readings |
| `Day.Mean` | `S` | Daily mean stage |
| `Day.Max` | `S` | Daily max stage |
| `HydrologicalYear.Max` | `S` | Annual maximum |
| `15minute` | `Q` | 15-min flow readings |
| `Day.Mean` | `Q` | Daily mean flow |
| `HDay.Total` | `RE` | Hydro-day rainfall total |

**Time series data response (confirmed live):**

```json
{
  "ts_id": "54512010",
  "rows": "97",
  "columns": "Timestamp,Value",
  "data": [
    ["2026-03-27T00:00:00.000Z", 0.779],
    ["2026-03-27T00:15:00.000Z", 0.778],
    ...
  ]
}
```

#### 1.1.2 SEPA Flood Warnings

| Property | Value |
|----------|-------|
| **URL** | `https://floodline.sepa.org.uk/floodupdates/` |
| **RSS Feed** | `https://floodline.sepa.org.uk/floodupdates/feed/` |
| **Auth** | None |
| **Format** | RSS/XML (feed), HTML (web pages) |
| **Update cadence** | Real-time during flood events |
| **Status** | ✅ RSS feed LIVE — verified 2026-03-28 (empty during non-flood period) |

**RSS Feed (confirmed live):**
```xml
<rss version="2.0">
<channel>
  <title>SEPA Floodwarning</title>
  <link>https://floodline.sepa.org.uk</link>
  <pubDate>Sat, 28 Mar 2026 09:32:37 +0000</pubDate>
</channel>
</rss>
```

**⚠️ Major difference from EA:** SEPA does **not** expose a REST/JSON flood warning API. The only machine-readable feed is RSS. During flood events, RSS items contain warning details. When no warnings are active, the feed is empty.

**Severity levels (same 3-tier system as EA):**
- Severe Flood Warning
- Flood Warning  
- Flood Alert

#### 1.1.3 SEPA Flood Maps (Static Risk Products)

| Property | Value |
|----------|-------|
| **URL** | `https://map.sepa.org.uk/floodmaps/` |
| **Format** | Web map application (ArcGIS-based internally) |
| **Download** | No direct bulk download found; WMS/WFS endpoints returning 404 |
| **Status** | ⚠️ Web viewer only — no confirmed public WFS/WMS for programmatic access |

**Risk categories shown on SEPA flood map:**
- Flood risk from rivers
- Flood risk from the sea (coastal)
- Flood risk from surface water
- Future flood maps (climate change scenarios)

**Data availability issue:** SEPA suffered a major cyber attack in December 2020 which destroyed significant internal systems. Some services have been rebuilt on new platforms (beta.sepa.scot), and data service URLs may have migrated. The `open.sepa.org.uk` domain does not respond (HTTP 000/connection refused — verified 2026-03-28).

#### 1.1.4 Scotland's Spatial Data Infrastructure (spatialdata.gov.scot)

| Property | Value |
|----------|-------|
| **URL** | `https://spatialdata.gov.scot/geonetwork/` |
| **Format** | GeoNetwork catalog (Elasticsearch-based) |
| **Auth** | None (read), but ES search API returned 403 Forbidden |
| **Status** | ⚠️ Catalog accessible but API requires further investigation |

The Scottish SSDI may host flood risk assessment datasets from the second cycle of Flood Risk Management Plans (2022-2028). These would include:
- Potentially Vulnerable Areas (PVAs)
- Flood Risk Assessment (FRA) outputs
- Flood hazard/risk maps under Floods Directive

#### 1.1.5 Licensing

| Source | Licence |
|--------|---------|
| SEPA Time Series API | Open Government Licence (Scotland) |
| SEPA Flood Warnings RSS | Open Government Licence (Scotland) |
| SEPA Flood Maps | Unknown — no explicit licence on web viewer |
| SpatialData.gov.scot | Open Government Licence (where specified per dataset) |

### 1.2 Gap Analysis — SEPA vs EA Pattern

| EA Pipeline Feature | SEPA Equivalent | Mapping Quality | Notes |
|-------------------|----------------|-----------------|-------|
| EA Real-Time API (REST/JSON) | SEPA KiWIS API (REST/JSON) | ⚠️ **Different API** | KiWIS uses different query structure; returns array-of-arrays, not JSON objects |
| Flood Warning REST API | RSS feed only | ❌ **Major gap** | No JSON API; must parse XML RSS |
| Station list with lat/long | KiWIS `getStationList` with lat/long | ✅ Good | Lat/long provided directly (WGS84) |
| Flood area polygons via API | Not available via API | ❌ **Gap** | No programmatic access to flood warning area polygons |
| National flood risk products (SHP/GPKG downloads) | Not available as bulk download | ❌ **Major gap** | SEPA flood map data not available for download; web viewer only |
| Modelled depth rasters (per-grid ZIP) | No equivalent public data | ❌ **Not available** | SEPA does not publish depth grids for download |
| Historical flood map | May exist in spatialdata.gov.scot | ⚠️ **Uncertain** | Requires further investigation |
| `readings?latest` single-call bulk endpoint | No equivalent; must iterate per ts_id | ⚠️ **Different pattern** | KiWIS requires specifying ts_id; no "get all latest" endpoint |

### 1.3 Notebook Structure — `notebooks/sepa/`

Following the 5-notebook pattern, but adapted for the available data:

```
notebooks/sepa/
  sepa_monitoring/
    sepa_mon_01_bronze_scrape.py       # KiWIS station list + 15-min readings + RSS warnings
    sepa_mon_02_silver_normalize.py    # Normalize stations, readings, warning events
    sepa_mon_03_gold_catalog.py        # Station catalog, reading summaries, warning history
  sepa_risk_products/                  # FUTURE — pending data availability
    sepa_risk_01_bronze_catalog.py     # Placeholder for when SEPA publishes download links
    sepa_risk_02_download.py
    sepa_risk_03_vector_ingest.py
```

#### Notebook 01: Bronze Scrape (`sepa_mon_01_bronze_scrape.py`)

**API calls (sequenced):**

1. `getStationList` — full station list with coordinates, catchment, river
2. For each station with `parametertype_name = 'S'` (stage): `getTimeseriesValues` with `ts_name=15minute` for latest 4h window
3. Parse RSS feed at `https://floodline.sepa.org.uk/floodupdates/feed/`

**Bronze tables:**
- `sepa_monitoring_stations_scrape` — station metadata, append-only with `_is_current`
- `sepa_monitoring_readings_scrape` — 15-min readings, append-only  
- `sepa_monitoring_warnings_scrape` — RSS items parsed to JSON, append-only

**Key implementation difference from EA:**
- KiWIS API returns **arrays of arrays** (first row = header), not JSON objects. Parser must handle this:
  ```python
  headers = response[0]  # ["station_no", "station_name", ...]
  data = [dict(zip(headers, row)) for row in response[1:]]
  ```
- KiWIS has no pagination — returns all results in one call for station/parameter lists
- For readings, must specify `ts_id` (numeric) — no wildcard "get all latest" endpoint
- **Batch strategy:** Get station list → filter for `S`/`Q` parameters → batch `getTimeseriesValues` with multiple `ts_id` values (KiWIS supports comma-separated `ts_id` values)

#### Notebook 02: Silver Normalize (`sepa_mon_02_silver_normalize.py`)

- `sepa_monitoring_stations` — MERGE on `station_no`
- `sepa_monitoring_readings` — Append, partitioned by `reading_date`
- `sepa_monitoring_warnings` — SCD Type 2 on warning area (from RSS `link` or `guid`)

#### Notebook 03: Gold Catalog (`sepa_mon_03_gold_catalog.py`)

- `sepa_monitoring_station_catalog` — station + latest reading + status
- `sepa_monitoring_warning_history` — historical warning events from RSS

### 1.4 CRS Handling

| Source | CRS | Notes |
|--------|-----|-------|
| KiWIS stations | WGS84 (lat/long provided directly) | `station_latitude`, `station_longitude` — use directly |
| KiWIS readings | N/A (scalar values, not spatial) | |
| SEPA Flood Maps (if available) | EPSG:27700 (BNG) | Same as England — standard UK CRS |

**Simplification:** No coordinate conversion needed for station data — WGS84 coordinates come directly from the API. This is simpler than EA which provides both BNG and WGS84.

### 1.5 Schema Mapping to Existing Gold Tables

#### SEPA Station → `sepa_monitoring_stations` (Silver)

| KiWIS Field | Silver Column | Type | EA Equivalent |
|------------|--------------|------|---------------|
| `station_no` | `station_id` | STRING | `stationReference` |
| `station_name` | `station_name` | STRING | `label` |
| `station_latitude` | `lat` | DOUBLE | `lat` |
| `station_longitude` | `lon` | DOUBLE | `long` |
| `catchment_name` | `catchment_name` | STRING | `catchmentName` |
| `river_name` | `river_name` | STRING | `riverName` |
| _(not available)_ | `easting` | NULL | `easting` |
| _(not available)_ | `northing` | NULL | `northing` |

#### SEPA Readings → `sepa_monitoring_readings` (Silver)

| Field | Column | Type | EA Equivalent |
|-------|--------|------|---------------|
| `ts_id` | `ts_id` | STRING | measure `@id` |
| `Timestamp` | `reading_timestamp` | TIMESTAMP | `dateTime` |
| `Value` | `value` | DOUBLE | `value` |
| _(derived)_ | `station_no` | STRING | station `stationReference` |
| _(derived)_ | `parameter_type` | STRING | measure `parameterName` |
| _(derived)_ | `ts_name` | STRING | measure period label |

#### Extension of Shared Tables

| Table | New `_source` Value | Applicable |
|-------|--------------------|-----------|
| `au_flood_raster_catalog` | `SEPA_RISK` | ❌ Not applicable initially (no rasters available) |
| `au_flood_report_index` | N/A | ❌ Not applicable |

### 1.6 Gotchas and Risks

1. **KiWIS API is fundamentally different from EA API:** The query pattern, response format, and pagination model are completely different. Cannot reuse EA API client code — needs a new `sepa_kiwis_client.py`.

2. **No bulk "latest readings" endpoint:** Unlike EA's `?latest` which returns all stations in one call, KiWIS requires specifying `ts_id` values. To get readings for all stations, must first discover ts_ids, then batch requests. Could result in hundreds of API calls per scrape.

3. **RSS-only flood warnings:** The RSS feed is the only machine-readable flood warning source. It is empty when no warnings are active (confirmed 2026-03-28). The feed's XML structure during active warnings needs to be observed during an actual flood event.

4. **Post-cyber-attack data landscape:** SEPA's 2020 cyber attack destroyed many systems. Some data services may be on legacy infrastructure or transitioning. URLs may change. `open.sepa.org.uk` is currently unreachable.

5. **No flood risk spatial downloads:** Unlike EA which publishes national flood zone shapefiles/GeoPackages, SEPA does not provide bulk download of flood risk areas. This is a significant gap for vector ingest.

6. **Station count uncertainty:** KiWIS returned a large station list (500+), but some may be rain gauges, groundwater stations, or inactive. Need to filter by `parametertype_name` and status.

7. **Readings volume:** With ~500 stations × multiple parameters × 96 readings/day (15-min), the readings table could grow by 50,000+ rows per day — same challenge as EA.

---

## 2. NRW (Wales) Pipeline Track

### 2.1 Data Sources — Detailed Inventory

#### 2.1.1 DataMapWales GeoServer — NRW Flood Layers (WFS)

| Property | Value |
|----------|-------|
| **Base URL** | `https://datamap.gov.wales/geoserver/inspire-nrw/wfs` |
| **Auth** | None — publicly accessible |
| **Format** | GML, GeoJSON, Shapefile via WFS |
| **CRS** | EPSG:27700 (BNG) — same as EA |
| **Protocol** | OGC WFS 2.0.0 |
| **Status** | ✅ LIVE — verified 2026-03-28 |

**Confirmed NRW Flood Layers (from WFS GetCapabilities — verified live):**

| WFS Layer Name | Description | Geometry |
|---------------|-------------|----------|
| `inspire-nrw:NRW_FLOOD_WARNING` | Flood warning areas (polygons) | MultiPolygon |
| `inspire-nrw:NRW_FLOODZONE_RIVERS` | Flood Zone 2/3 — Rivers | MultiPolygon |
| `inspire-nrw:NRW_FLOODZONE_SEAS` | Flood Zone 2/3 — Seas | MultiPolygon |
| `inspire-nrw:NRW_FLOODZONE_RIVERS_SEAS_MERGED` | Combined flood zones | MultiPolygon |
| `inspire-nrw:NRW_FLOODZONE_SURFACE_WATER_AND_SMALL_WATERCOURSES` | Surface water flood zones | MultiPolygon |
| `inspire-nrw:NRW_FLOOD_RISK_FROM_RIVERS` | Risk of flooding from rivers | MultiPolygon |
| `inspire-nrw:NRW_FLOOD_RISK_FROM_SEA` | Risk of flooding from sea | MultiPolygon |
| `inspire-nrw:NRW_FLOOD_RISK_FROM_SURFACE_WATER_SMALL_WATERCOURSES` | Risk from surface water | MultiPolygon |
| `inspire-nrw:NRW_AREA_BENEFITING_FROM_FLOOD_DEFENCE` | Areas behind flood defences | MultiPolygon |
| `inspire-nrw:NRW_FLOOD_RISK_AREAS` | Flood Risk Areas (Floods Directive) | MultiPolygon |
| `inspire-nrw:NRW_FLOODMAP_FLOOD_STORAGE` | Flood storage areas | MultiPolygon |
| `inspire-nrw:NRW_NATIONAL_FLOOD_RISK_RIVER_PEOPLE` | National flood risk — river — people | MultiPolygon |
| `inspire-nrw:NRW_NATIONAL_FLOOD_RISK_RIVER_ECONOMIC` | National flood risk — river — economic | MultiPolygon |
| `inspire-nrw:NRW_NATIONAL_FLOOD_RISK_RIVER_ENVIRO` | National flood risk — river — environmental | MultiPolygon |
| `inspire-nrw:NRW_NATIONAL_FLOOD_RISK_SEA_PEOPLE` | National flood risk — sea — people | MultiPolygon |
| `inspire-nrw:NRW_NATIONAL_FLOOD_RISK_SEA_ECONOMIC` | National flood risk — sea — economic | MultiPolygon |
| `inspire-nrw:NRW_NATIONAL_FLOOD_RISK_SEA_ENVIRO` | National flood risk — sea — environmental | MultiPolygon |
| `inspire-nrw:NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_PEOPLE` | National flood risk — SW — people | MultiPolygon |
| `inspire-nrw:NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_ECON` | National flood risk — SW — economic | MultiPolygon |
| `inspire-nrw:NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_ENVIRO` | National flood risk — SW — environmental | MultiPolygon |

**NRW Flood Warning schema (confirmed from DescribeFeatureType):**

| Field | Type | Description |
|-------|------|-------------|
| `objectid` | int | Primary key |
| `region` | string | NRW region |
| `area` | string | Warning area name |
| `fwd_tacode` | string | Flood Warning Direct TA code |
| `fwis_code` | string | Flood Warning Information Service code |
| `fwa_name` | string | Flood Warning Area name |
| `descrip` | string | Description |
| `geom` | MultiPolygon | Area geometry (EPSG:27700) |

**NRW Flood Zone Rivers schema (confirmed from DescribeFeatureType):**

| Field | Type | Description |
|-------|------|-------------|
| `objectid` | int | Primary key |
| `mm_id` | string | Map model ID |
| `pub_date` | date | Publication date |
| `risk` | string | Risk level (English) |
| `risk_cy` | string | Risk level (Welsh / Cymraeg) |
| `geom` | MultiPolygon | Zone geometry (EPSG:27700) |

**NRW Flood Warning sample feature (confirmed from GetFeature):**
```json
{
  "type": "Feature",
  "id": "NRW_FLOOD_WARNING.1",
  "geometry": {
    "type": "MultiPolygon",
    "coordinates": [[[294580.99, 378709.99], ...]]  // BNG coordinates
  },
  "properties": {
    "objectid": 1,
    "region": "...",
    "fwis_code": "...",
    "fwa_name": "..."
  }
}
```

#### 2.1.2 DataMapWales GeoNode REST API

| Property | Value |
|----------|-------|
| **Base URL** | `https://datamap.gov.wales/api/v2/layers/` |
| **Auth** | None — publicly accessible |
| **Format** | JSON |
| **Status** | ✅ LIVE — verified 2026-03-28 (1836 total layers) |
| **Purpose** | Layer catalog and metadata discovery |

The GeoNode API can be used to discover flood layers and their metadata. It supports search, but the full-text search is not specific to the keyword — filtering by workspace `inspire-nrw` and title keywords is more reliable.

#### 2.1.3 NRW Flood Warning Service (Web)

| Property | Value |
|----------|-------|
| **URL** | `https://flood-warning.naturalresources.wales/` |
| **API** | Not publicly documented; `/api/warnings` returned 404 |
| **Format** | HTML (web viewer) |
| **Status** | ⚠️ Web viewer only — no confirmed public REST API for live warnings |

NRW's live flood warning page at `https://naturalresources.wales/flooding/check-flood-warnings/` links to `https://flood-warning.naturalresources.wales/?culture=en-GB` for active warning searches. The API endpoint `/api/warnings` returned `{"success":false}` with 404. Live warning data may be consumed internally but not exposed publicly.

**Workaround:** The `NRW_FLOOD_WARNING` WFS layer contains the **flood warning area definitions** (static polygons), not live warnings. For live warning status, would need to scrape the web page or find an undocumented API.

#### 2.1.4 NRW WWNP (Working with Natural Processes)

The WFS also includes `inspire-nrw:NRW_WWNP_RIPERIAN_WOODLAND_POTENTIAL` — relevant for flood-related natural flood management data.

#### 2.1.5 Bilingual Considerations

NRW data is bilingual (Welsh/English) under the Welsh Language (Wales) Measure 2011. This manifests in the data as:

| Pattern | Example |
|---------|---------|
| Dual field names | `risk` (English), `risk_cy` (Welsh/Cymraeg) |
| Dual display labels | `propertyNamesEn` / `propertyNamesCy` in DataMapWales config |
| Dual property visibility | Some fields `visible: "en"`, others `visible: "cy"`, others `visible: "always"` |

**Pipeline impact:** Store both English and Welsh values. Use English (`risk`) as the canonical field for analytics, preserve Welsh (`risk_cy`) as an auxiliary column. Field name convention: `{field}` for English, `{field}_cy` for Welsh.

#### 2.1.6 Licensing

| Source | Licence |
|--------|---------|
| DataMapWales NRW layers | Contains NRW information © Natural Resources Wales and Database Right. All rights Reserved. |
| NRW flood maps | Open Government Licence — Wales (OGL-W) where specified |
| DataMapWales GeoNode | Mixed — check per layer; most NRW layers are OGL |

### 2.2 Gap Analysis — NRW vs EA Pattern

| EA Pipeline Feature | NRW Equivalent | Mapping Quality | Notes |
|-------------------|---------------|-----------------|-------|
| EA Real-Time Monitoring API | No direct equivalent | ❌ **Major gap** | NRW does not expose a time-series monitoring API like EA or SEPA KiWIS |
| Flood Warning REST API | No REST API; WFS has static warning areas | ⚠️ **Partial** | WFS provides warning area polygons, not live warning status |
| National flood zone products (SHP/GPKG bulk) | WFS download via GeoServer | ✅ **Better than bulk** | Can query features directly via WFS; no need for bulk ZIP downloads |
| Flood zone polygons | `NRW_FLOODZONE_RIVERS`, `NRW_FLOODZONE_SEAS`, `NRW_FLOODZONE_RIVERS_SEAS_MERGED` | ✅ **Direct** | Excellent coverage via WFS |
| Risk of flooding categories | `NRW_FLOOD_RISK_FROM_RIVERS`, `_SEA`, `_SURFACE_WATER` | ✅ **Direct** | Maps directly to EA's RoFRS/RoFSW |
| National flood risk assessment | 9 layers: rivers/sea/SW × people/economic/environmental | ✅ **Richer than EA** | More granular than EA's single RoFRS dataset |
| Areas behind defences | `NRW_AREA_BENEFITING_FROM_FLOOD_DEFENCE` | ✅ Direct | Maps to EA's defended extent products |
| Flood storage areas | `NRW_FLOODMAP_FLOOD_STORAGE` | ✅ Direct | No EA equivalent — bonus dataset |
| Modelled depth rasters | No equivalent public data | ❌ **Not available** | NRW does not publish depth grid rasters |
| Flood warning area polygons | `NRW_FLOOD_WARNING` WFS layer | ✅ **Direct** | 267+ warning areas with FWIS codes and polygons |

### 2.3 Notebook Structure — `notebooks/nrw/`

```
notebooks/nrw/
  nrw_risk_products/
    nrw_risk_01_bronze_scrape.py       # WFS GetFeature for all 20 flood layers
    nrw_risk_02_silver_normalize.py    # Normalize, classify, add bilingual columns
    nrw_risk_03_gold_catalog.py        # Denormalized flood zone/risk catalogs
    nrw_risk_04_vector_ingest.py       # Sedona ST_* spatial registration
    nrw_risk_05_monitoring.py          # Pipeline health + change detection
```

#### Notebook 01: Bronze Scrape (`nrw_risk_01_bronze_scrape.py`)

**Approach:** Use WFS `GetFeature` with `outputFormat=application/json` to download all features from each layer.

**WFS calls (one per layer):**
```
GET https://datamap.gov.wales/geoserver/inspire-nrw/wfs
  ?service=WFS
  &version=2.0.0
  &request=GetFeature
  &typeName=inspire-nrw:NRW_FLOODZONE_RIVERS
  &outputFormat=application/json
```

**Bronze tables:**
- `nrw_flood_layers_scrape` — raw GeoJSON features per layer, append-only with `_is_current`

**Pagination:** WFS 2.0.0 supports `count` and `startIndex` for large layers. Use `count=10000` batches.

**⚠️ Large layer handling:** The flood zone and risk layers may contain tens of thousands of features. Stream responses and write in batches. Expect multi-MB responses.

#### Notebook 02: Silver Normalize (`nrw_risk_02_silver_normalize.py`)

- `nrw_flood_zones` — MERGE on `objectid` per layer
- `nrw_flood_risk_areas` — MERGE on `objectid`  
- `nrw_flood_warning_areas` — MERGE on `fwis_code`
- `nrw_flood_defences` — MERGE on `objectid`

**Bilingual field handling:**
```python
# Store both English and Welsh values
"risk": feature["properties"]["risk"],
"risk_cy": feature["properties"].get("risk_cy"),
```

#### Notebook 03: Gold Catalog (`nrw_risk_03_gold_catalog.py`)

- `nrw_flood_zone_catalog` — Denormalized flood zone polygons with risk classification
- `nrw_flood_risk_catalog` — Risk assessment areas (rivers/sea/SW × people/econ/enviro)
- `nrw_flood_warning_area_catalog` — Warning areas with FWIS codes for potential integration

#### Notebook 04: Vector Ingest (`nrw_risk_04_vector_ingest.py`)

- Load WFS GeoJSON into Spark via Sedona
- `ST_GeomFromGeoJSON` to parse geometries
- `ST_Transform` from EPSG:27700 → EPSG:4326 for gold tables
- Store in unified format compatible with EA flood zone tables

### 2.4 CRS Handling

| Source | Native CRS | Gold CRS |
|--------|-----------|----------|
| All NRW WFS layers | EPSG:27700 (BNG) | EPSG:4326 (WGS84) via `ST_Transform` |

**Identical to EA:** All NRW layers use EPSG:27700. The same `ST_Transform(geom, 'EPSG:27700', 'EPSG:4326')` call used for EA vector data works for NRW.

**Note:** WFS `GetFeature` with GeoJSON output returns coordinates in the layer's native CRS (EPSG:27700), NOT WGS84. Must transform on ingest.

### 2.5 Schema Mapping to Existing Gold Tables

#### NRW Flood Zone → `nrw_flood_zones` (Silver)

| WFS Field | Silver Column | Type | EA Equivalent |
|-----------|--------------|------|---------------|
| `objectid` | `feature_id` | INTEGER | Feature ID |
| `mm_id` | `model_id` | STRING | _(EA doesn't have this)_ |
| `pub_date` | `publication_date` | DATE | _(metadata only in EA)_ |
| `risk` | `risk_category` | STRING | Zone 2/Zone 3 |
| `risk_cy` | `risk_category_cy` | STRING | _(no Welsh equiv in EA)_ |
| `geom` | `geometry_wkt` | STRING (WKT) | Polygon geometry |
| _(derived)_ | `geometry_wgs84` | STRING (WKT) | Reprojected to 4326 |
| _(constant)_ | `_source` | STRING | `'NRW'` |
| _(constant)_ | `_state` | STRING | `'Wales'` |
| _(constant)_ | `source_layer` | STRING | Layer name |

#### NRW Warning Areas → `nrw_flood_warning_areas` (Silver)

| WFS Field | Silver Column | Type | EA Equivalent |
|-----------|--------------|------|---------------|
| `objectid` | `warning_area_id` | INTEGER | floodArea notation |
| `region` | `region` | STRING | `eaAreaName` |
| `area` | `area_name` | STRING | floodArea county |
| `fwd_tacode` | `ta_code` | STRING | _(EA uses notation)_ |
| `fwis_code` | `fwis_code` | STRING | floodArea `notation` |
| `fwa_name` | `warning_area_name` | STRING | floodArea `description` |
| `descrip` | `description` | STRING | _(EA uses `description`)_ |
| `geom` | `geometry_wkt` | STRING (WKT) | polygon GeoJSON |

#### Extension of Shared Tables

| Table | New `_source` Value | Applicable |
|-------|--------------------|-----------|
| `au_flood_raster_catalog` | N/A | ❌ Not applicable (NRW has no rasters for download) |
| _(new unified)_ `uk_flood_zone_catalog` | `NRW` | ✅ NRW flood zones can join with EA flood zones |

### 2.6 Gotchas and Risks

1. **WFS response size:** National flood zone layers may contain millions of vertices. The GeoJSON response for `NRW_FLOODZONE_RIVERS_SEAS_MERGED` could be multi-GB. Use WFS pagination (`count`/`startIndex`) and stream to disk.

2. **No real-time monitoring data:** NRW does not expose river level or flow data via a public API. The previously listed `rloi.naturalresourceswales.gov.wales` domain is unreachable (verified 2026-03-28). For real-time river levels in Wales, the EA API may cover some cross-border stations, but there is a coverage gap.

3. **Bilingual field names:** Some layers have dual `_cy` suffixed fields. Pipeline must handle both and store both. Analytics should default to English fields.

4. **WFS rate limiting:** GeoServer at `datamap.gov.wales` may have rate limits or timeouts on large feature requests. Implement retry logic and respect any `429` responses.

5. **No flood depth data:** Unlike EA, NRW does not publish modelled flood depth grids for public download. The pipeline will be vector-only for Wales.

6. **Feature coordinate precision:** WFS features come in BNG with very high precision (e.g., `294580.99999997`). This is normal for GeoServer and should be preserved.

7. **WMS alternative:** If WFS performance is poor for large layers, DataMapWales also exposes WMS:
   ```
   https://datamap.gov.wales/geoserver/inspire-nrw/wms?service=WMS&request=GetCapabilities
   ```
   But WMS returns images, not features — only useful for visualization, not data ingest.

---

## 3. Full UK Coverage Summary

### 3.1 Unified View — EA + SEPA + NRW

| Capability | EA (England) | SEPA (Scotland) | NRW (Wales) | UK Total |
|-----------|-------------|-----------------|-------------|----------|
| **Real-time stations** | ~4,500 (REST API) | ~500 (KiWIS API) | ❌ None public | ~5,000 |
| **Live flood warnings** | ✅ REST/JSON | ⚠️ RSS only | ❌ No API | Partial |
| **Flood warning areas** | ✅ GeoJSON via API | ❌ Not available | ✅ WFS polygons | EA + NRW |
| **Flood zone maps** | ✅ Bulk SHP/GPKG download | ❌ Web viewer only | ✅ WFS (20 layers) | EA + NRW |
| **Risk of flooding** | ✅ RoFRS/RoFSW | ❌ Not available | ✅ 9 risk layers | EA + NRW |
| **Modelled depth grids** | ✅ Per-grid-square ASC | ❌ Not available | ❌ Not available | EA only |
| **Historic flood map** | ✅ Via Defra | ❌ Unknown | ❌ Unknown | EA only |
| **Flood defences** | ✅ Defended extents | ❌ Not available | ✅ Areas benefiting | EA + NRW |

### 3.2 Overlaps and Gaps

**Border regions:** Some rivers cross England/Scotland (e.g., River Tweed) or England/Wales (e.g., River Dee, River Severn) borders. Both agencies may monitor the same river at different stations. The `_source` column in shared tables handles this — no deduplication needed.

**Monitoring gap — Wales:** NRW is the most significant gap. There is no public real-time monitoring API for Welsh rivers. Options:
- Check if EA monitors some Welsh border rivers (River Dee, River Wye)
- Monitor NRW for future API releases
- Consider third-party data (e.g., NRFA — National River Flow Archive at nrfa.ceh.ac.uk)

**Flood warning gap — Scotland:** SEPA's RSS-only approach means warnings are less structured than EA's JSON API. RSS items during events will need custom XML parsing.

**Static product gap — Scotland:** SEPA is the biggest gap for static products. The 2020 cyber attack may have delayed data publication. The Scottish Government's second cycle Flood Risk Management Plans (2022-2028) may eventually be published on spatialdata.gov.scot.

### 3.3 Shared Infrastructure Opportunities

| Component | Shareable? | Notes |
|-----------|-----------|-------|
| Bronze append-only pattern | ✅ Yes | Same `_is_current`, `_scraped_at` pattern for all three |
| Silver MERGE/SCD2 pattern | ✅ Yes | Same patterns, different source schemas |
| Gold denormalization | ✅ Yes | Same catalog pattern |
| CRS handling (EPSG:27700 → 4326) | ✅ Yes | All three use BNG; same Sedona `ST_Transform` call |
| Databricks cluster config | ✅ Yes | Same `au_flood_cluster` — Sedona runtime needed for vector ingest |
| Unity Catalog structure | ✅ Yes | `ceg_delta_*.international_flood.{sepa,nrw}_*` tables |
| Pipeline audit log | ✅ Yes | Same `pipeline_run_log` table, different `pipeline_name` values |
| HTTP retry/backoff logic | ✅ Yes | Same `http_get()` pattern (different base URLs) |
| `_source` column in shared tables | ✅ Yes | `SEPA`, `NRW` join `EA_MONITORING`, `EA_DEPTH`, `EA_RISK` |

**New shared code:**
```
src/au_flood/
  ea/ea_api_client.py          # Existing EA client
  sepa/sepa_kiwis_client.py    # NEW — KiWIS API client
  nrw/nrw_wfs_client.py        # NEW — WFS 2.0.0 client (paginated GetFeature)
  shared/rss_parser.py         # NEW — RSS feed parser (for SEPA warnings)
```

### 3.4 Recommended Build Order

| Phase | Pipeline | Effort | Value | Dependencies |
|-------|----------|--------|-------|-------------|
| **Phase 1** | EA Monitoring | 2-3 weeks | ⭐⭐⭐⭐⭐ | None |
| **Phase 2** | EA Modelled Depths | 2-3 weeks | ⭐⭐⭐⭐ | None |
| **Phase 3** | EA Risk Products | 3-4 weeks | ⭐⭐⭐ | Phase 1 |
| **Phase 4** | NRW Risk Products (WFS) | 2-3 weeks | ⭐⭐⭐⭐ | Phase 3 (reuse vector ingest pattern) |
| **Phase 5** | SEPA Monitoring (KiWIS) | 2-3 weeks | ⭐⭐⭐ | Phase 1 (similar but different API) |
| **Phase 6** | SEPA Warnings (RSS) | 1 week | ⭐⭐ | Phase 5 |
| **Future** | SEPA Risk Products | TBD | ⭐ | Blocked on data availability |

**Rationale:**
- **NRW before SEPA:** NRW has rich, well-structured WFS data covering 20 flood layers — high data quality, standard OGC protocol, and fills the England+Wales coverage gap immediately.
- **SEPA monitoring before SEPA risk:** SEPA's KiWIS API provides valuable real-time data even without risk map downloads.
- **SEPA risk products deferred:** Blocked on data availability — monitor for publication on spatialdata.gov.scot or SEPA's rebuilt services.

### 3.5 Repository Structure After Full UK Extension

```
notebooks/
  bcc/                          # Existing — Australia Brisbane
  nsw_ses/                      # Existing — Australia NSW
  ica/                          # Existing — Australia ICA
  ea/                           # Phase 1-3 — England EA
    ea_monitoring/
    ea_depth/
    ea_risk_products/
  nrw/                          # Phase 4 — Wales NRW
    nrw_risk_products/
      nrw_risk_01_bronze_scrape.py
      nrw_risk_02_silver_normalize.py
      nrw_risk_03_gold_catalog.py
      nrw_risk_04_vector_ingest.py
      nrw_risk_05_monitoring.py
  sepa/                         # Phase 5-6 — Scotland SEPA
    sepa_monitoring/
      sepa_mon_01_bronze_scrape.py
      sepa_mon_02_silver_normalize.py
      sepa_mon_03_gold_catalog.py
    sepa_risk_products/         # Future — pending data availability
      README.md                 # Placeholder documenting the gap

jobs/
  ea_monitoring_pipeline_job.json
  ea_depth_pipeline_job.json
  ea_risk_pipeline_job.json
  nrw_risk_pipeline_job.json    # NEW
  sepa_monitoring_pipeline_job.json  # NEW

src/au_flood/
  ea/ea_api_client.py
  sepa/                         # NEW
    __init__.py
    sepa_kiwis_client.py
  nrw/                          # NEW
    __init__.py
    nrw_wfs_client.py
  shared/                       # NEW
    rss_parser.py

docs/
  ea_monitoring_pipeline.md
  ea_depth_pipeline.md
  ea_risk_products_pipeline.md
  nrw_risk_pipeline.md          # NEW
  sepa_monitoring_pipeline.md   # NEW
```

### 3.6 New Unity Catalog Assets (SEPA + NRW)

**Volumes:**
```sql
CREATE VOLUME IF NOT EXISTS ceg_delta_bronze_prnd.international_flood.sepa_monitoring_raw;
CREATE VOLUME IF NOT EXISTS ceg_delta_bronze_prnd.international_flood.nrw_risk_raw;
```

**New Tables — SEPA:**

| Table | Tier | Content |
|-------|------|---------|
| `sepa_monitoring_stations_scrape` | Bronze | KiWIS station list snapshots |
| `sepa_monitoring_readings_scrape` | Bronze | 15-min readings from KiWIS |
| `sepa_monitoring_warnings_scrape` | Bronze | RSS warning items |
| `sepa_monitoring_stations` | Silver | Normalized stations (MERGE) |
| `sepa_monitoring_readings` | Silver | Partitioned readings (append) |
| `sepa_monitoring_warnings` | Silver | Warning events (SCD2) |
| `sepa_monitoring_station_catalog` | Gold | Station + latest reading |
| `sepa_monitoring_warning_history` | Gold | Warning event history |

**New Tables — NRW:**

| Table | Tier | Content |
|-------|------|---------|
| `nrw_flood_layers_scrape` | Bronze | WFS feature snapshots per layer |
| `nrw_flood_zones` | Silver | Flood zone polygons (MERGE) |
| `nrw_flood_risk_areas` | Silver | Risk assessment areas (MERGE) |
| `nrw_flood_warning_areas` | Silver | Warning area definitions (MERGE) |
| `nrw_flood_defences` | Silver | Areas benefiting from defences (MERGE) |
| `nrw_flood_zone_catalog` | Gold | Denormalized flood zones with risk |
| `nrw_flood_risk_catalog` | Gold | Risk assessment catalog (9 categories) |
| `nrw_flood_warning_area_catalog` | Gold | Warning areas with FWIS codes |

### 3.7 Network Requirements (Additional)

| Host | Port | Purpose |
|------|------|---------|
| `timeseries.sepa.org.uk` | 443 | SEPA KiWIS time series API |
| `floodline.sepa.org.uk` | 443 | SEPA flood warning RSS feed |
| `datamap.gov.wales` | 443 | NRW WFS/WMS + GeoNode API |
| `naturalresources.wales` | 443 | NRW web (reference only) |
