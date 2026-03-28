# Databricks notebook source
# EA Real-Time Flood Monitoring — Bronze Scrape
#
# Purpose : Fetches flood warnings, monitoring stations, flood areas, and
#           latest readings from the Environment Agency Real-Time Flood
#           Monitoring API and appends them to bronze Delta tables.
#
#           API base: https://environment.data.gov.uk/flood-monitoring
#           Auth   : None — Open Government Licence, no key required
#           CRS    : Stations provide both BNG (easting/northing) and WGS84 (lat/long)
#
# Layer   : Bronze  (ceg_delta_bronze_prnd.international_flood.ea_monitoring_*)
# Outputs : ceg_delta_bronze_prnd.international_flood.ea_monitoring_stations_scrape
#           ceg_delta_bronze_prnd.international_flood.ea_monitoring_floods_scrape
#           ceg_delta_bronze_prnd.international_flood.ea_monitoring_flood_areas_scrape
#           ceg_delta_bronze_prnd.international_flood.ea_monitoring_readings_scrape
#           ceg_delta_bronze_prnd.international_flood.pipeline_run_log
#
# Gotchas:
#   - Use ?latest endpoint for readings — single call returns latest reading per measure.
#     Do NOT crawl per-station readings; too many API calls.
#   - EA API is Linked Data / JSON-LD. Responses use @id, @type etc.
#   - SSL verification is ENABLED — EA has valid certs (unlike NSW SES).
#   - Stations return up to 4,500 records; paginate with _limit / _offset.
#   - Flood areas return ~1,300 records; paginate.
#   - Current warnings (/id/floods) returns only currently active warnings;
#     this is append-only in bronze to preserve history for SCD2 in silver.
#
# Version : 1.0.0

# COMMAND ----------

# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text(
    "run_id", "",
    "Run ID — leave blank to auto-generate a UUID"
)
dbutils.widgets.text(
    "scrape_version", "1.0.0",
    "Scrape Version — semantic version tag for this notebook"
)
dbutils.widgets.text(
    "station_page_size", "500",
    "Station Page Size — records per page when paginating /id/stations"
)
dbutils.widgets.text(
    "flood_area_page_size", "500",
    "Flood Area Page Size — records per page when paginating /id/floodAreas"
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

# ── Catalog / table paths ──────────────────────────────────────────────────
BRONZE_CATALOG = "ceg_delta_bronze_prnd"
BRONZE_SCHEMA  = "international_flood"
AUDIT_TABLE    = "pipeline_run_log"

FQN_AUDIT             = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{AUDIT_TABLE}"
FQN_STATIONS          = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.ea_monitoring_stations_scrape"
FQN_FLOODS            = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.ea_monitoring_floods_scrape"
FQN_FLOOD_AREAS       = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.ea_monitoring_flood_areas_scrape"
FQN_READINGS          = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.ea_monitoring_readings_scrape"

# ── Source constants ───────────────────────────────────────────────────────
EA_API_BASE      = "https://environment.data.gov.uk/flood-monitoring"
SOURCE_NAME      = "EA_MONITORING"
STATE            = "England"
NOTEBOOK_NAME    = "ea_01_bronze_scrape"
NOTEBOOK_VERSION = "1.0.0"
USER_AGENT       = "uk-flood-pipeline/1.0.0 (+https://github.com/MapGuru3910/uk-flood)"

# ── HTTP tuning ────────────────────────────────────────────────────────────
REQUEST_TIMEOUT    = 60   # seconds — EA API can be slow on large station lists
MAX_RETRIES        = 3
RETRY_BACKOFF_BASE = 2.0
RETRY_JITTER       = 0.5

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    level=logging.INFO,
    force=True,
)
log = logging.getLogger(NOTEBOOK_NAME)

# COMMAND ----------

# MAGIC %md ## 3. Parameter Resolution

# COMMAND ----------

RUN_ID             = dbutils.widgets.get("run_id").strip() or str(uuid.uuid4())
SCRAPE_VERSION     = dbutils.widgets.get("scrape_version").strip() or NOTEBOOK_VERSION
STATION_PAGE_SIZE  = int(dbutils.widgets.get("station_page_size") or 500)
AREA_PAGE_SIZE     = int(dbutils.widgets.get("flood_area_page_size") or 500)
SCRAPED_AT         = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"EA Monitoring Bronze Scrape  |  Run ID: {RUN_ID}")
log.info(f"Notebook version : {NOTEBOOK_VERSION}")
log.info(f"Scrape version   : {SCRAPE_VERSION}")
log.info(f"Station page size: {STATION_PAGE_SIZE}")
log.info(f"Area page size   : {AREA_PAGE_SIZE}")
log.info(f"Started at       : {SCRAPED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Schema Definitions

# COMMAND ----------

STATIONS_SCHEMA = StructType([
    StructField("station_uri",       StringType(),    True),
    StructField("station_id",        StringType(),    True),   # stationReference
    StructField("station_name",      StringType(),    True),   # label
    StructField("rloi_id",           StringType(),    True),
    StructField("river_name",        StringType(),    True),
    StructField("catchment_name",    StringType(),    True),
    StructField("town",              StringType(),    True),
    StructField("lat",               DoubleType(),    True),
    StructField("lon",               DoubleType(),    True),
    StructField("easting",           IntegerType(),   True),
    StructField("northing",          IntegerType(),   True),
    StructField("date_opened",       StringType(),    True),
    StructField("station_status",    StringType(),    True),
    StructField("ea_area_name",      StringType(),    True),
    StructField("measures_json",     StringType(),    True),
    StructField("raw_json",          StringType(),    True),
    StructField("_run_id",           StringType(),    False),
    StructField("_ingested_at",      TimestampType(), False),
    StructField("_scrape_version",   StringType(),    False),
    StructField("_is_current",       BooleanType(),   False),
    StructField("_source",           StringType(),    False),
    StructField("_state",            StringType(),    False),
])

FLOODS_SCHEMA = StructType([
    StructField("flood_warning_uri",    StringType(),    True),
    StructField("flood_area_id",        StringType(),    True),
    StructField("description",          StringType(),    True),
    StructField("severity",             StringType(),    True),
    StructField("severity_level",       IntegerType(),   True),
    StructField("is_tidal",             BooleanType(),   True),
    StructField("message",              StringType(),    True),
    StructField("time_raised",          StringType(),    True),
    StructField("time_severity_changed",StringType(),    True),
    StructField("time_message_changed", StringType(),    True),
    StructField("county",               StringType(),    True),
    StructField("river_or_sea",         StringType(),    True),
    StructField("ea_area_name",         StringType(),    True),
    StructField("flood_area_polygon",   StringType(),    True),
    StructField("raw_json",             StringType(),    True),
    StructField("_run_id",              StringType(),    False),
    StructField("_ingested_at",         TimestampType(), False),
    StructField("_scrape_version",      StringType(),    False),
    StructField("_is_current",          BooleanType(),   False),
    StructField("_source",              StringType(),    False),
    StructField("_state",               StringType(),    False),
])

FLOOD_AREAS_SCHEMA = StructType([
    StructField("flood_area_uri",    StringType(),    True),
    StructField("notation",          StringType(),    True),
    StructField("area_description",  StringType(),    True),
    StructField("county",            StringType(),    True),
    StructField("ea_area_name",      StringType(),    True),
    StructField("river_or_sea",      StringType(),    True),
    StructField("polygon_url",       StringType(),    True),
    StructField("label",             StringType(),    True),
    StructField("raw_json",          StringType(),    True),
    StructField("_run_id",           StringType(),    False),
    StructField("_ingested_at",      TimestampType(), False),
    StructField("_scrape_version",   StringType(),    False),
    StructField("_is_current",       BooleanType(),   False),
    StructField("_source",           StringType(),    False),
    StructField("_state",            StringType(),    False),
])

READINGS_SCHEMA = StructType([
    StructField("measure_uri",       StringType(),    True),
    StructField("station_id",        StringType(),    True),
    StructField("parameter",         StringType(),    True),
    StructField("period",            IntegerType(),   True),
    StructField("qualifier",         StringType(),    True),
    StructField("unit_name",         StringType(),    True),
    StructField("reading_datetime",  StringType(),    True),
    StructField("value",             DoubleType(),    True),
    StructField("raw_json",          StringType(),    True),
    StructField("_run_id",           StringType(),    False),
    StructField("_ingested_at",      TimestampType(), False),
    StructField("_scrape_version",   StringType(),    False),
    StructField("_is_current",       BooleanType(),   False),
    StructField("_source",           StringType(),    False),
    StructField("_state",            StringType(),    False),
])

# COMMAND ----------

# MAGIC %md ## 5. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_STATIONS} (
    station_uri       STRING     COMMENT 'Full URI identifier from EA API (@id field)',
    station_id        STRING     COMMENT 'Station reference code — natural key (e.g. 1029TH)',
    station_name      STRING     COMMENT 'Human-readable label from EA API',
    rloi_id           STRING     COMMENT 'River Levels on the Internet ID',
    river_name        STRING     COMMENT 'River name from EA API',
    catchment_name    STRING     COMMENT 'Catchment name from EA API',
    town              STRING     COMMENT 'Associated town or settlement',
    lat               DOUBLE     COMMENT 'WGS84 latitude',
    lon               DOUBLE     COMMENT 'WGS84 longitude',
    easting           INT        COMMENT 'BNG easting (EPSG:27700)',
    northing          INT        COMMENT 'BNG northing (EPSG:27700)',
    date_opened       STRING     COMMENT 'Date station was opened (raw ISO string)',
    station_status    STRING     COMMENT 'Station status URI from EA API',
    ea_area_name      STRING     COMMENT 'EA operational area name',
    measures_json     STRING     COMMENT 'JSON array of measure definitions at this station',
    raw_json          STRING     COMMENT 'Full raw JSON from EA API for this station record',
    _run_id           STRING     NOT NULL COMMENT 'UUID identifying the scrape run',
    _ingested_at      TIMESTAMP  NOT NULL COMMENT 'UTC timestamp when row was ingested',
    _scrape_version   STRING     NOT NULL COMMENT 'Semantic version of this notebook',
    _is_current       BOOLEAN    NOT NULL COMMENT 'TRUE for most recent record per station_id',
    _source           STRING     NOT NULL COMMENT 'Always EA_MONITORING',
    _state            STRING     NOT NULL COMMENT 'Always England'
)
USING DELTA
COMMENT 'Bronze: append-only scrape of EA monitoring stations. Use _is_current=TRUE for current state.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'station_id,_is_current'
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_FLOODS} (
    flood_warning_uri     STRING     COMMENT 'Full URI from EA API (@id)',
    flood_area_id         STRING     COMMENT 'Flood area notation — natural key',
    description           STRING     COMMENT 'Plain-text description of the affected area',
    severity              STRING     COMMENT 'Severity label: Severe Flood Warning | Flood Warning | Flood alert | Warning no longer in force',
    severity_level        INT        COMMENT 'Severity level: 1=Severe, 2=Warning, 3=Alert, 4=No longer in force',
    is_tidal              BOOLEAN    COMMENT 'Whether the warning is tidal in nature',
    message               STRING     COMMENT 'Full advisory message text',
    time_raised           STRING     COMMENT 'ISO timestamp when warning was raised',
    time_severity_changed STRING     COMMENT 'ISO timestamp of last severity change',
    time_message_changed  STRING     COMMENT 'ISO timestamp of last message change',
    county                STRING     COMMENT 'County name from floodArea',
    river_or_sea          STRING     COMMENT 'Named water body from floodArea',
    ea_area_name          STRING     COMMENT 'EA operational area name',
    flood_area_polygon    STRING     COMMENT 'URL to fetch the GeoJSON polygon for this flood area',
    raw_json              STRING     COMMENT 'Full raw JSON record from EA API',
    _run_id               STRING     NOT NULL,
    _ingested_at          TIMESTAMP  NOT NULL,
    _scrape_version       STRING     NOT NULL,
    _is_current           BOOLEAN    NOT NULL COMMENT 'TRUE for most recent record per flood_area_id',
    _source               STRING     NOT NULL,
    _state                STRING     NOT NULL
)
USING DELTA
COMMENT 'Bronze: append-only scrape of EA active flood warnings. Current warnings only — history preserved via append + _is_current pattern.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'flood_area_id,_is_current'
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_FLOOD_AREAS} (
    flood_area_uri    STRING     COMMENT 'Full URI from EA API (@id)',
    notation          STRING     COMMENT 'Unique notation code — natural key',
    area_description  STRING     COMMENT 'Description of the flood area',
    county            STRING     COMMENT 'County name',
    ea_area_name      STRING     COMMENT 'EA operational area name',
    river_or_sea      STRING     COMMENT 'Named water body',
    polygon_url       STRING     COMMENT 'URL to fetch GeoJSON polygon for this area',
    label             STRING     COMMENT 'Display label',
    raw_json          STRING     COMMENT 'Full raw JSON from EA API',
    _run_id           STRING     NOT NULL,
    _ingested_at      TIMESTAMP  NOT NULL,
    _scrape_version   STRING     NOT NULL,
    _is_current       BOOLEAN    NOT NULL COMMENT 'TRUE for most recent record per notation',
    _source           STRING     NOT NULL,
    _state            STRING     NOT NULL
)
USING DELTA
COMMENT 'Bronze: append-only scrape of EA flood warning area definitions.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'notation,_is_current'
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_READINGS} (
    measure_uri       STRING     COMMENT 'Full URI of the measure from EA API',
    station_id        STRING     COMMENT 'Station reference code',
    parameter         STRING     COMMENT 'Parameter type: level | flow | rainfall | groundWaterLevel',
    period            INT        COMMENT 'Measurement period in seconds (e.g. 900 = 15 min)',
    qualifier         STRING     COMMENT 'Qualifier label (e.g. Stage, Downstream Stage)',
    unit_name         STRING     COMMENT 'Unit name (e.g. m, m3/s)',
    reading_datetime  STRING     COMMENT 'ISO timestamp of the reading',
    value             DOUBLE     COMMENT 'Observed value in the specified unit',
    raw_json          STRING     COMMENT 'Full raw JSON of the reading record',
    _run_id           STRING     NOT NULL,
    _ingested_at      TIMESTAMP  NOT NULL,
    _scrape_version   STRING     NOT NULL,
    _is_current       BOOLEAN    NOT NULL,
    _source           STRING     NOT NULL,
    _state            STRING     NOT NULL
)
USING DELTA
COMMENT 'Bronze: append-only scrape of latest EA station readings (one per measure per run). Partitioned by ingestion date for efficient time-series queries.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'station_id,reading_datetime'
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_AUDIT} (
    run_id              STRING     NOT NULL,
    pipeline_stage      STRING     NOT NULL,
    notebook_name       STRING,
    notebook_version    STRING,
    start_time          TIMESTAMP  NOT NULL,
    end_time            TIMESTAMP,
    duration_seconds    DOUBLE,
    status              STRING,
    rows_read           LONG,
    rows_written        LONG,
    rows_merged         LONG,
    rows_rejected       LONG,
    error_message       STRING,
    extra_metadata      STRING,
    databricks_job_id   STRING,
    databricks_run_id   STRING,
    scrape_version      STRING
)
USING DELTA
COMMENT 'Pipeline audit log — one row per notebook stage per workflow run.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# COMMAND ----------

# MAGIC %md ## 6. HTTP Utilities

# COMMAND ----------

class ScrapeError(Exception):
    """Raised when an HTTP request fails after exhausting all retry attempts."""
    pass


def http_get(url: str, params: dict = None, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    """
    HTTP GET with exponential backoff retry.
    SSL verification is ENABLED — EA has valid certificates.
    """
    session_headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                timeout=timeout,
                verify=True,
                headers=session_headers,
            )
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                sleep_s = (RETRY_BACKOFF_BASE ** attempt) + random.uniform(0, RETRY_JITTER)
                log.warning(
                    f"HTTP attempt {attempt}/{MAX_RETRIES} failed [{url}]: {exc}. "
                    f"Retrying in {sleep_s:.1f}s"
                )
                time.sleep(sleep_s)
    raise ScrapeError(
        f"All {MAX_RETRIES} HTTP attempts failed for [{url}]: {last_exc}"
    ) from last_exc

# COMMAND ----------

# MAGIC %md ## 7. EA API Functions

# COMMAND ----------

def fetch_all_stations(page_size: int) -> list:
    """
    Fetches all EA monitoring stations via paginated GET /id/stations.
    Returns a list of raw station dicts from the API.
    """
    stations = []
    offset = 0
    while True:
        resp = http_get(
            f"{EA_API_BASE}/id/stations",
            params={"_limit": page_size, "_offset": offset},
        )
        data = resp.json()
        items = data.get("items", [])
        if not items:
            break
        stations.extend(items)
        log.info(f"  Stations: fetched {len(stations)} so far (offset={offset})")
        if len(items) < page_size:
            break
        offset += page_size
    log.info(f"  Stations: total {len(stations)} fetched")
    return stations


def fetch_current_floods() -> list:
    """
    Fetches all currently active flood warnings/alerts.
    The /id/floods endpoint returns only currently active warnings — no pagination needed.
    """
    resp = http_get(f"{EA_API_BASE}/id/floods")
    data = resp.json()
    items = data.get("items", [])
    log.info(f"  Flood warnings: {len(items)} currently active")
    return items


def fetch_all_flood_areas(page_size: int) -> list:
    """
    Fetches all flood warning area definitions via paginated GET /id/floodAreas.
    """
    areas = []
    offset = 0
    while True:
        resp = http_get(
            f"{EA_API_BASE}/id/floodAreas",
            params={"_limit": page_size, "_offset": offset},
        )
        data = resp.json()
        items = data.get("items", [])
        if not items:
            break
        areas.extend(items)
        log.info(f"  Flood areas: fetched {len(areas)} so far (offset={offset})")
        if len(items) < page_size:
            break
        offset += page_size
    log.info(f"  Flood areas: total {len(areas)} fetched")
    return areas


def fetch_latest_readings() -> list:
    """
    Fetches the latest reading from every measure in a single call.
    The ?latest parameter is recommended by EA for bulk reading retrieval
    (avoids per-station crawling).
    """
    resp = http_get(f"{EA_API_BASE}/data/readings", params={"latest": ""})
    data = resp.json()
    items = data.get("items", [])
    log.info(f"  Latest readings: {len(items)} readings fetched")
    return items

# COMMAND ----------

# MAGIC %md ## 8. Record Normalisation

# COMMAND ----------

def normalise_station(s: dict) -> dict:
    """Extracts a flat dict of station fields from the raw EA API response."""
    measures = s.get("measures", [])
    if isinstance(measures, dict):
        measures = [measures]

    # Parse easting/northing — may be floats in some API responses
    def _int_or_none(v):
        try:
            return int(float(v)) if v is not None else None
        except (TypeError, ValueError):
            return None

    return {
        "station_uri":    s.get("@id", ""),
        "station_id":     s.get("stationReference", ""),
        "station_name":   s.get("label", ""),
        "rloi_id":        str(s.get("RLOIid", "") or ""),
        "river_name":     s.get("riverName", ""),
        "catchment_name": s.get("catchmentName", ""),
        "town":           s.get("town", ""),
        "lat":            s.get("lat"),
        "lon":            s.get("long"),
        "easting":        _int_or_none(s.get("easting")),
        "northing":       _int_or_none(s.get("northing")),
        "date_opened":    s.get("dateOpened", ""),
        "station_status": s.get("status", "") if isinstance(s.get("status"), str) else str(s.get("status", "")),
        "ea_area_name":   s.get("eaAreaName", ""),
        "measures_json":  json.dumps(measures),
        "raw_json":       json.dumps(s),
    }


def normalise_flood(f: dict) -> dict:
    """Extracts a flat dict of flood warning fields from the raw EA API response."""
    flood_area = f.get("floodArea", {}) or {}
    if not isinstance(flood_area, dict):
        flood_area = {}

    return {
        "flood_warning_uri":     f.get("@id", ""),
        "flood_area_id":         flood_area.get("notation", f.get("floodAreaID", "")),
        "description":           f.get("description", ""),
        "severity":              f.get("severity", ""),
        "severity_level":        f.get("severityLevel"),
        "is_tidal":              f.get("isTidal"),
        "message":               f.get("message", ""),
        "time_raised":           f.get("timeRaised", ""),
        "time_severity_changed": f.get("timeSeverityChanged", ""),
        "time_message_changed":  f.get("timeMessageChanged", ""),
        "county":                flood_area.get("county", ""),
        "river_or_sea":          flood_area.get("riverOrSea", ""),
        "ea_area_name":          f.get("eaAreaName", ""),
        "flood_area_polygon":    flood_area.get("polygon", ""),
        "raw_json":              json.dumps(f),
    }


def normalise_flood_area(a: dict) -> dict:
    """Extracts a flat dict of flood area fields from the raw EA API response."""
    return {
        "flood_area_uri":   a.get("@id", ""),
        "notation":         a.get("notation", ""),
        "area_description": a.get("description", ""),
        "county":           a.get("county", ""),
        "ea_area_name":     a.get("eaAreaName", ""),
        "river_or_sea":     a.get("riverOrSea", ""),
        "polygon_url":      a.get("polygon", ""),
        "label":            a.get("label", ""),
        "raw_json":         json.dumps(a),
    }


def normalise_reading(r: dict) -> dict:
    """Extracts a flat dict of reading fields from the raw EA API response."""
    measure = r.get("measure", "")
    measure_uri = measure if isinstance(measure, str) else (measure.get("@id", "") if isinstance(measure, dict) else "")

    # Extract station_id from measure URI pattern:
    # .../id/stations/{station_id}/measures/{measure_name}
    station_id = ""
    import re
    m = re.search(r"/id/stations/([^/]+)/measures/", measure_uri)
    if m:
        station_id = m.group(1)

    return {
        "measure_uri":      measure_uri,
        "station_id":       station_id,
        "parameter":        r.get("parameter", ""),
        "period":           r.get("period"),
        "qualifier":        r.get("qualifier", ""),
        "unit_name":        r.get("unitName", ""),
        "reading_datetime": r.get("dateTime", ""),
        "value":            r.get("value"),
        "raw_json":         json.dumps(r),
    }

# COMMAND ----------

# MAGIC %md ## 9. Bronze Write Utilities

# COMMAND ----------

def _annotate_rows(rows: list, run_id: str, scraped_at, scrape_version: str) -> list:
    """Adds pipeline provenance columns to each row dict in-place."""
    for row in rows:
        row["_run_id"]         = run_id
        row["_ingested_at"]    = scraped_at
        row["_scrape_version"] = scrape_version
        row["_is_current"]     = True
        row["_source"]         = SOURCE_NAME
        row["_state"]          = STATE
    return rows


def write_bronze_table(rows: list, schema, fqn: str, natural_key: str,
                       run_id: str, scraped_at, scrape_version: str) -> int:
    """
    Annotates rows with provenance, expires superseded _is_current records
    for the natural key, and appends new rows to the named bronze table.

    Returns the count of rows written.
    """
    if not rows:
        log.warning(f"write_bronze_table({fqn}): no rows to write — skipping")
        return 0

    annotated = _annotate_rows(rows, run_id, scraped_at, scrape_version)

    pdf = pd.DataFrame(annotated)
    # Align to schema: add missing columns as None, reorder
    col_names = [f.name for f in schema.fields]
    for col in col_names:
        if col not in pdf.columns:
            pdf[col] = None
    pdf = pdf[col_names]

    sdf = spark.createDataFrame(pdf, schema=schema)

    # Expire previous _is_current rows for these natural keys
    natural_keys = [r[natural_key] for r in annotated if r.get(natural_key)]
    if natural_keys:
        key_df = spark.createDataFrame([(k,) for k in natural_keys], [natural_key])
        key_df.createOrReplaceTempView("_current_run_keys")
        spark.sql(f"""
            UPDATE {fqn}
            SET    _is_current = FALSE
            WHERE  {natural_key} IN (SELECT {natural_key} FROM _current_run_keys)
              AND  _is_current  = TRUE
              AND  _run_id     != '{run_id}'
        """)

    # Append new rows
    sdf.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(fqn)
    log.info(f"Bronze write: {len(annotated)} rows appended to {fqn}")
    return len(annotated)

# COMMAND ----------

# MAGIC %md ## 10. Audit Logging

# COMMAND ----------

def write_audit(
    run_id, stage, notebook_name, notebook_version,
    start_time, end_time, status,
    rows_read=0, rows_written=0, rows_merged=0, rows_rejected=0,
    error_message=None, extra_metadata=None,
) -> None:
    try:
        duration = (end_time - start_time).total_seconds() if end_time and start_time else None
        job_id, db_run_id = None, None
        try:
            ctx       = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            job_id    = ctx.jobId().getOrElse(None)
            db_run_id = ctx.idInJob().getOrElse(None)
        except Exception:
            pass
        record = [{
            "run_id":            run_id,
            "pipeline_stage":    stage,
            "notebook_name":     notebook_name,
            "notebook_version":  notebook_version,
            "start_time":        start_time,
            "end_time":          end_time,
            "duration_seconds":  duration,
            "status":            status,
            "rows_read":         rows_read,
            "rows_written":      rows_written,
            "rows_merged":       rows_merged,
            "rows_rejected":     rows_rejected,
            "error_message":     error_message,
            "extra_metadata":    json.dumps(extra_metadata) if extra_metadata else None,
            "databricks_job_id": str(job_id) if job_id else None,
            "databricks_run_id": str(db_run_id) if db_run_id else None,
            "scrape_version":    scrape_version,
        }]
        spark.createDataFrame(record).write.format("delta").mode("append").saveAsTable(FQN_AUDIT)
        log.info(f"Audit written: stage={stage} status={status} rows_written={rows_written}")
    except Exception as audit_exc:
        log.error(f"write_audit failed (non-fatal): {audit_exc}")

# COMMAND ----------

# MAGIC %md ## 11. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None
_counters        = {
    "stations": 0, "floods": 0, "flood_areas": 0, "readings": 0
}

try:
    log.info(f"Starting EA Monitoring bronze scrape | run_id={RUN_ID}")

    # ── Stage 1: Monitoring Stations ─────────────────────────────────────
    log.info("Stage 1 | Fetching monitoring stations")
    raw_stations = fetch_all_stations(STATION_PAGE_SIZE)
    station_rows = [normalise_station(s) for s in raw_stations]
    _counters["stations"] = write_bronze_table(
        station_rows, STATIONS_SCHEMA, FQN_STATIONS, "station_id",
        RUN_ID, SCRAPED_AT, SCRAPE_VERSION,
    )

    # ── Stage 2: Current Flood Warnings ───────────────────────────────────
    log.info("Stage 2 | Fetching current flood warnings")
    raw_floods = fetch_current_floods()
    flood_rows = [normalise_flood(f) for f in raw_floods]
    _counters["floods"] = write_bronze_table(
        flood_rows, FLOODS_SCHEMA, FQN_FLOODS, "flood_area_id",
        RUN_ID, SCRAPED_AT, SCRAPE_VERSION,
    )

    # ── Stage 3: Flood Warning Areas ──────────────────────────────────────
    log.info("Stage 3 | Fetching flood warning area definitions")
    raw_areas = fetch_all_flood_areas(AREA_PAGE_SIZE)
    area_rows = [normalise_flood_area(a) for a in raw_areas]
    _counters["flood_areas"] = write_bronze_table(
        area_rows, FLOOD_AREAS_SCHEMA, FQN_FLOOD_AREAS, "notation",
        RUN_ID, SCRAPED_AT, SCRAPE_VERSION,
    )

    # ── Stage 4: Latest Readings ──────────────────────────────────────────
    log.info("Stage 4 | Fetching latest readings (all stations, single call)")
    raw_readings = fetch_latest_readings()
    reading_rows = [normalise_reading(r) for r in raw_readings]
    _counters["readings"] = write_bronze_table(
        reading_rows, READINGS_SCHEMA, FQN_READINGS, "measure_uri",
        RUN_ID, SCRAPED_AT, SCRAPE_VERSION,
    )

    _pipeline_status = "success"
    total_written = sum(_counters.values())
    log.info(
        f"Run complete | stations={_counters['stations']} floods={_counters['floods']} "
        f"flood_areas={_counters['flood_areas']} readings={_counters['readings']} "
        f"total={total_written}"
    )

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Pipeline failed: {exc}", exc_info=True)
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "bronze",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_written     = sum(_counters.values()),
        error_message    = _pipeline_error,
        extra_metadata   = {
            "stations_scraped":    _counters.get("stations", 0),
            "floods_scraped":      _counters.get("floods", 0),
            "flood_areas_scraped": _counters.get("flood_areas", 0),
            "readings_scraped":    _counters.get("readings", 0),
            "station_page_size":   STATION_PAGE_SIZE,
            "area_page_size":      AREA_PAGE_SIZE,
        },
    )

# Publish run_id as Workflow task value for downstream tasks
try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
