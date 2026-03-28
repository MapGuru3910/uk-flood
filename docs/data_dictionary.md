# UK Flood Data Pipeline — Data Dictionary

**Version:** 1.0.0  
**Date:** 2026-03-28  
**Covers:** Phase 1 (EA Monitoring) + Phase 4 (NRW Wales)

---

## Bronze Tables

### `ea_monitoring_stations_scrape`

Append-only scrape of EA monitoring stations. Use `_is_current = TRUE` for current state queries.

| Column | Type | Description |
|--------|------|-------------|
| `station_uri` | STRING | Full EA Linked Data URI (`@id` field) |
| `station_id` | STRING | Station reference code — natural key (e.g. `1029TH`) |
| `station_name` | STRING | Human-readable label from EA API |
| `rloi_id` | STRING | River Levels on the Internet ID |
| `river_name` | STRING | River name |
| `catchment_name` | STRING | Catchment/river basin name |
| `town` | STRING | Associated settlement |
| `lat` | DOUBLE | WGS84 latitude |
| `lon` | DOUBLE | WGS84 longitude |
| `easting` | INT | BNG easting (EPSG:27700) |
| `northing` | INT | BNG northing (EPSG:27700) |
| `date_opened` | STRING | Date station opened (raw ISO string) |
| `station_status` | STRING | Status URI from EA API |
| `ea_area_name` | STRING | EA operational area name |
| `measures_json` | STRING | JSON array of measure definitions at this station |
| `raw_json` | STRING | Full raw JSON from EA API |
| `_run_id` | STRING | UUID identifying the scrape run |
| `_ingested_at` | TIMESTAMP | UTC timestamp when row was ingested |
| `_scrape_version` | STRING | Notebook version |
| `_is_current` | BOOLEAN | TRUE for most recent record per `station_id` |
| `_source` | STRING | Always `EA_MONITORING` |
| `_state` | STRING | Always `England` |

---

### `ea_monitoring_floods_scrape`

Append-only scrape of currently active EA flood warnings. Only active warnings are returned by the API; append + `_is_current` preserves history.

| Column | Type | Description |
|--------|------|-------------|
| `flood_warning_uri` | STRING | Full EA URI (`@id`) |
| `flood_area_id` | STRING | Flood area notation — natural key |
| `description` | STRING | Plain-text description of the affected area |
| `severity` | STRING | Severity label: `Severe Flood Warning` \| `Flood Warning` \| `Flood alert` \| `Warning no longer in force` |
| `severity_level` | INT | 1=Severe, 2=Warning, 3=Alert, 4=No longer in force |
| `is_tidal` | BOOLEAN | Whether the warning is tidal |
| `message` | STRING | Full advisory message text |
| `time_raised` | STRING | ISO timestamp when warning was raised |
| `time_severity_changed` | STRING | ISO timestamp of last severity change |
| `time_message_changed` | STRING | ISO timestamp of last message change |
| `county` | STRING | County name from `floodArea` |
| `river_or_sea` | STRING | Named water body |
| `ea_area_name` | STRING | EA operational area name |
| `flood_area_polygon` | STRING | URL to fetch GeoJSON polygon for this flood area |
| `raw_json` | STRING | Full raw JSON from EA API |

---

### `ea_monitoring_flood_areas_scrape`

Append-only scrape of EA flood warning area definitions (~1,300 areas).

| Column | Type | Description |
|--------|------|-------------|
| `flood_area_uri` | STRING | Full EA Linked Data URI |
| `notation` | STRING | Unique area code — natural key |
| `area_description` | STRING | Description of the flood area |
| `county` | STRING | County name |
| `ea_area_name` | STRING | EA operational area |
| `river_or_sea` | STRING | Named water body |
| `polygon_url` | STRING | URL to fetch GeoJSON polygon |
| `label` | STRING | Display label |
| `raw_json` | STRING | Full raw JSON |

---

### `ea_monitoring_readings_scrape`

Append-only scrape of latest EA readings (one reading per measure per scrape run). From `GET /data/readings?latest`.

| Column | Type | Description |
|--------|------|-------------|
| `measure_uri` | STRING | Full URI of the measure |
| `station_id` | STRING | Station reference code (extracted from measure URI) |
| `parameter` | STRING | Parameter type: `level` \| `flow` \| `rainfall` \| `groundWaterLevel` |
| `period` | INT | Measurement period in seconds (e.g. 900 = 15 min) |
| `qualifier` | STRING | Qualifier label (e.g. `Stage`, `Downstream Stage`) |
| `unit_name` | STRING | Unit of measurement (e.g. `m`, `m3/s`) |
| `reading_datetime` | STRING | ISO timestamp of the reading |
| `value` | DOUBLE | Observed value in the specified unit |
| `raw_json` | STRING | Full raw JSON of the reading record |

---

### `nrw_flood_layers_scrape`

Append-only scrape of NRW Wales WFS flood layers. All 20 layers stored in one table, distinguished by `layer_name`. GeoJSON geometry is in native EPSG:27700 (BNG) coordinates.

| Column | Type | Description |
|--------|------|-------------|
| `layer_name` | STRING | WFS layer name (e.g. `NRW_FLOODZONE_RIVERS`) |
| `feature_id` | STRING | WFS feature `@id` (e.g. `NRW_FLOODZONE_RIVERS.1`) |
| `objectid` | INT | GeoServer integer primary key |
| `properties_json` | STRING | All feature properties as JSON (preserves bilingual fields) |
| `geometry_json` | STRING | GeoJSON geometry in native EPSG:27700 (BNG) — values are eastings/northings, not degrees |
| `geometry_type` | STRING | GeoJSON geometry type: `MultiPolygon` \| `Polygon` |
| `feature_count` | LONG | Total feature count for this layer (from WFS response metadata) |
| `_run_id` | STRING | Scrape run UUID |
| `_ingested_at` | TIMESTAMP | UTC ingestion timestamp |
| `_is_current` | BOOLEAN | TRUE for most recent record per `(layer_name, objectid)` |
| `_source` | STRING | Always `NRW_WALES` |
| `_state` | STRING | Always `Wales` |

---

## Silver Tables

### `ea_monitoring_stations`

Normalized EA monitoring stations — MERGE upsert on `station_id`.

| Column | Type | Description |
|--------|------|-------------|
| `station_id` | STRING | Natural key — EA `stationReference` |
| `station_uri` | STRING | Full EA Linked Data URI |
| `station_name` | STRING | Human-readable label |
| `rloi_id` | STRING | River Levels on the Internet ID |
| `river_name` | STRING | River name |
| `catchment_name` | STRING | Catchment/river basin name |
| `town` | STRING | Associated settlement |
| `lat` | DOUBLE | WGS84 latitude |
| `lon` | DOUBLE | WGS84 longitude |
| `easting` | INT | BNG easting (EPSG:27700) |
| `northing` | INT | BNG northing (EPSG:27700) |
| `date_opened` | DATE | Date station opened |
| `station_status` | STRING | `Active` \| `Closed` \| `Suspended` |
| `ea_area_name` | STRING | EA operational area |
| `measures_json` | STRING | JSON array of measure definitions |

---

### `ea_monitoring_flood_events`

SCD Type 2 flood warning events. Each severity transition creates a new row.

| Column | Type | Description |
|--------|------|-------------|
| `flood_area_id` | STRING | Natural key — flood area notation |
| `flood_warning_uri` | STRING | EA Linked Data URI |
| `description` | STRING | Description of the area |
| `severity` | STRING | Severity label |
| `severity_level` | INT | 1=Severe, 2=Warning, 3=Alert, 4=No longer in force |
| `is_tidal` | BOOLEAN | Whether warning is tidal |
| `message` | STRING | Advisory message |
| `time_raised` | TIMESTAMP | Warning raised timestamp |
| `time_severity_changed` | TIMESTAMP | Last severity change |
| `county` | STRING | County name |
| `river_or_sea` | STRING | Named water body |
| `ea_area_name` | STRING | EA operational area |
| `_content_hash` | STRING | MD5 of key fields — used for SCD2 change detection |
| `_effective_from` | TIMESTAMP | UTC when this version became current |
| `_effective_to` | TIMESTAMP | UTC when superseded (NULL = current) |
| `_is_current` | BOOLEAN | TRUE for the current version |

---

### `ea_monitoring_flood_areas`

Normalized flood warning area definitions — MERGE upsert on `notation`.

| Column | Type | Description |
|--------|------|-------------|
| `notation` | STRING | Natural key — unique area code |
| `flood_area_uri` | STRING | EA Linked Data URI |
| `area_description` | STRING | Description |
| `county` | STRING | County name |
| `ea_area_name` | STRING | EA operational area |
| `river_or_sea` | STRING | Named water body |
| `polygon_url` | STRING | URL to fetch GeoJSON polygon |
| `label` | STRING | Display label |

---

### `ea_monitoring_readings`

Append-only time-series readings, partitioned by `reading_date`.

| Column | Type | Description |
|--------|------|-------------|
| `measure_uri` | STRING | Full URI of the measure |
| `station_id` | STRING | Station reference code |
| `parameter` | STRING | Parameter type |
| `period` | INT | Measurement period in seconds |
| `qualifier` | STRING | Qualifier label |
| `unit_name` | STRING | Unit of measurement |
| `reading_datetime` | TIMESTAMP | UTC reading timestamp |
| `reading_date` | DATE | **Partition column** — date of reading |
| `value` | DOUBLE | Observed value |

---

### `nrw_flood_zones`

Normalized NRW flood zone polygons. MERGE on `(layer_name, objectid)`. Both BNG and WGS84 WKTs stored.

| Column | Type | Description |
|--------|------|-------------|
| `layer_name` | STRING | WFS layer (e.g. `NRW_FLOODZONE_RIVERS`) |
| `objectid` | INT | GeoServer PK — natural key with `layer_name` |
| `feature_id` | STRING | WFS feature `@id` |
| `model_id` | STRING | Flood map model identifier (`mm_id`) |
| `publication_date` | DATE | Publication date (`pub_date`) |
| `risk_category` | STRING | Risk level or zone designation (English) |
| `risk_category_cy` | STRING | Risk level in Welsh (`risk_cy`) |
| `layer_category` | STRING | Derived: `fluvial_zone` \| `coastal_zone` \| `surface_water_zone` \| `combined_zone` \| `flood_storage` \| `risk_area` |
| `geometry_wkt_bng` | STRING | WKT polygon in EPSG:27700 (BNG) |
| `geometry_wkt_wgs84` | STRING | WKT polygon reprojected to EPSG:4326 (WGS84) |
| `properties_json` | STRING | Full properties JSON — all bilingual fields |

---

### `nrw_flood_risk_areas`

Normalized NRW flood risk assessment areas — 9 categories (3 sources × 3 receptors).

| Column | Type | Description |
|--------|------|-------------|
| `layer_name` | STRING | WFS source layer |
| `objectid` | INT | GeoServer PK |
| `risk_category` | STRING | Risk classification |
| `risk_source` | STRING | `rivers` \| `sea` \| `surface_water` |
| `risk_receptor` | STRING | `people` \| `economic` \| `environmental` |
| `geometry_wkt_bng` | STRING | WKT in EPSG:27700 |
| `geometry_wkt_wgs84` | STRING | WKT in EPSG:4326 |

---

### `nrw_flood_warning_areas`

NRW flood warning area definitions. MERGE on `fwis_code`.

| Column | Type | Description |
|--------|------|-------------|
| `objectid` | INT | GeoServer PK |
| `fwis_code` | STRING | FWIS code — natural key (analogous to EA `notation`) |
| `region` | STRING | NRW region name |
| `area_name` | STRING | Warning area name |
| `ta_code` | STRING | Flood Warning Direct TA code |
| `warning_area_name` | STRING | Full warning area name |
| `description` | STRING | Plain-text description |
| `geometry_wkt_bng` | STRING | WKT in EPSG:27700 |
| `geometry_wkt_wgs84` | STRING | WKT in EPSG:4326 |

---

## Gold Tables

### `ea_monitoring_station_catalog`

Denormalized EA station catalog with latest reading and active warning status. Full overwrite each run.

| Column | Type | Description |
|--------|------|-------------|
| `station_id` | STRING | Natural key |
| `station_name` | STRING | Station label |
| `river_name` | STRING | River name |
| `catchment_name` | STRING | Catchment name |
| `lat` | DOUBLE | WGS84 latitude |
| `lon` | DOUBLE | WGS84 longitude |
| `station_status` | STRING | Active \| Closed \| Suspended |
| `ea_area_name` | STRING | EA operational area |
| `latest_reading_datetime` | TIMESTAMP | Most recent reading timestamp |
| `latest_reading_value` | DOUBLE | Most recent reading value |
| `latest_reading_unit` | STRING | Unit of the reading |
| `has_active_warning` | BOOLEAN | TRUE if area has an active warning/alert |
| `active_warning_severity` | STRING | Severity of the active warning |

---

### `ea_monitoring_flood_event_history`

Full SCD2 history of EA flood warning events with derived analytics. Full overwrite each run.

| Column | Type | Description |
|--------|------|-------------|
| `flood_area_id` | STRING | Natural key |
| `severity` | STRING | Severity label |
| `severity_level` | INT | 1–4 |
| `time_raised` | TIMESTAMP | Warning raised |
| `is_current_warning` | BOOLEAN | TRUE if currently active |
| `warning_age_hours` | DOUBLE | Hours since raised (current warnings only) |
| `severity_category` | STRING | `severe` \| `warning` \| `alert` \| `inactive` |
| `_effective_from` | TIMESTAMP | SCD2 from |
| `_effective_to` | TIMESTAMP | SCD2 to (NULL = current) |

---

### `nrw_flood_zone_catalog`

Denormalized NRW flood zones with EA-equivalent zone numbers. Full overwrite each run.

| Column | Type | Description |
|--------|------|-------------|
| `layer_name` | STRING | WFS layer name |
| `objectid` | INT | Feature PK |
| `risk_category` | STRING | Risk level (English) |
| `risk_category_cy` | STRING | Risk level (Welsh) |
| `layer_category` | STRING | Simplified category |
| `flood_zone_number` | STRING | `FZ1` \| `FZ2` \| `FZ3` (EA-equivalent) |
| `geometry_wkt_bng` | STRING | WKT EPSG:27700 |
| `geometry_wkt_wgs84` | STRING | WKT EPSG:4326 |

---

### `nrw_flood_risk_catalog`

NRW 9-category risk assessment. Full overwrite each run.

| Column | Type | Description |
|--------|------|-------------|
| `layer_name` | STRING | WFS layer |
| `objectid` | INT | Feature PK |
| `risk_category` | STRING | Risk classification |
| `risk_source` | STRING | `rivers` \| `sea` \| `surface_water` |
| `risk_receptor` | STRING | `people` \| `economic` \| `environmental` |
| `geometry_wkt_bng` | STRING | WKT EPSG:27700 |
| `geometry_wkt_wgs84` | STRING | WKT EPSG:4326 |

---

### `nrw_flood_warning_area_catalog`

NRW warning area definitions. Full overwrite each run.

| Column | Type | Description |
|--------|------|-------------|
| `objectid` | INT | Feature PK |
| `fwis_code` | STRING | FWIS code (analogous to EA `notation`) |
| `region` | STRING | NRW region |
| `warning_area_name` | STRING | Full area name |
| `ea_equivalent_notation` | STRING | Set to `fwis_code` — use for cross-source joins with EA warning areas |
| `geometry_wkt_bng` | STRING | WKT EPSG:27700 |
| `geometry_wkt_wgs84` | STRING | WKT EPSG:4326 |

---

## Shared Gold Tables (from australia-flood)

### `au_flood_raster_catalog`

Multi-source raster catalog. UK data written with `_source = 'EA_DEPTH'` or `'EA_RISK'` (future phases). NRW does not write to this table (no rasters available for Wales).

| Column | Type | Description |
|--------|------|-------------|
| `raster_id` | STRING | SHA256-derived unique identifier |
| `resource_id` | STRING | Source dataset identifier |
| `volume_path` | STRING | ADLS Volume path |
| `file_format` | STRING | `TIF` \| `ASC` \| `NC` |
| `crs_epsg` | INT | Native CRS EPSG (27700 for EA) |
| `wgs84_bbox_min_lon` | DOUBLE | WGS84 bbox min longitude |
| `wgs84_bbox_min_lat` | DOUBLE | WGS84 bbox min latitude |
| `wgs84_bbox_max_lon` | DOUBLE | WGS84 bbox max longitude |
| `wgs84_bbox_max_lat` | DOUBLE | WGS84 bbox max latitude |
| `inferred_aep_pct` | DOUBLE | AEP as percentage (e.g. `1.0` for 1%/100yr) |
| `_source` | STRING | `NSW_SES` \| `BCC` \| `EA_DEPTH` \| `EA_RISK` \| `NRW_WALES` |
| `_state` | STRING | `NSW` \| `QLD` \| `England` \| `Wales` \| `Scotland` |

---

## AEP Reference (UK)

| UK Term | AEP % | Return Period | EA Zone | NRW Zone |
|---------|-------|---------------|---------|----------|
| High risk (≥3.3%) | 3.3 | 30 yr | Flood Zone 3 | Zone 3 (rivers/sea) |
| Medium risk (1%–3.3%) | 1.0 | 100 yr | Flood Zone 3 | Zone 3 |
| Low risk (0.1%–1%) | 0.1 | 1000 yr | Flood Zone 2 | Zone 2 |
| Very Low (<0.1%) | <0.1 | >1000 yr | Flood Zone 1 | Zone 1 |

---

## Severity Reference (EA Flood Warnings)

| `severity_level` | `severity` | `severity_category` | Meaning |
|-----------------|-----------|---------------------|---------|
| 1 | Severe Flood Warning | severe | Danger to life — take action |
| 2 | Flood Warning | warning | Flooding expected — prepare |
| 3 | Flood alert | alert | Flooding possible — be aware |
| 4 | Warning no longer in force | inactive | Warning removed |

---

## NRW Layer Reference

| Layer Name | Category | Description |
|-----------|----------|-------------|
| `NRW_FLOOD_WARNING` | Warning Areas | Flood warning area polygons (static definitions) |
| `NRW_FLOODZONE_RIVERS` | Flood Zones | Flood zone from rivers |
| `NRW_FLOODZONE_SEAS` | Flood Zones | Flood zone from sea |
| `NRW_FLOODZONE_RIVERS_SEAS_MERGED` | Flood Zones | Combined rivers + sea flood zone |
| `NRW_FLOODZONE_SURFACE_WATER_AND_SMALL_WATERCOURSES` | Flood Zones | Surface water flood zone |
| `NRW_FLOOD_RISK_FROM_RIVERS` | Risk Areas | Risk from rivers |
| `NRW_FLOOD_RISK_FROM_SEA` | Risk Areas | Risk from sea |
| `NRW_FLOOD_RISK_FROM_SURFACE_WATER_SMALL_WATERCOURSES` | Risk Areas | Risk from surface water |
| `NRW_AREA_BENEFITING_FROM_FLOOD_DEFENCE` | Defences | Areas behind flood defences |
| `NRW_FLOOD_RISK_AREAS` | Assessment | Floods Directive risk areas |
| `NRW_FLOODMAP_FLOOD_STORAGE` | Storage | Flood storage areas |
| `NRW_NATIONAL_FLOOD_RISK_RIVER_PEOPLE` | National Risk | River risk — people |
| `NRW_NATIONAL_FLOOD_RISK_RIVER_ECONOMIC` | National Risk | River risk — economic |
| `NRW_NATIONAL_FLOOD_RISK_RIVER_ENVIRO` | National Risk | River risk — environmental |
| `NRW_NATIONAL_FLOOD_RISK_SEA_PEOPLE` | National Risk | Sea risk — people |
| `NRW_NATIONAL_FLOOD_RISK_SEA_ECONOMIC` | National Risk | Sea risk — economic |
| `NRW_NATIONAL_FLOOD_RISK_SEA_ENVIRO` | National Risk | Sea risk — environmental |
| `NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_PEOPLE` | National Risk | Surface water risk — people |
| `NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_ECON` | National Risk | Surface water risk — economic |
| `NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_ENVIRO` | National Risk | Surface water risk — environmental |
