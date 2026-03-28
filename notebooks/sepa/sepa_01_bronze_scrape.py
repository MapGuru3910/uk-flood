# Databricks notebook source
# SEPA Scotland — Bronze Scrape
#
# Purpose : Fetches real-time river level/flow data from SEPA's KiWIS Time Series
#           API and flood warnings from the SEPA RSS feed, then appends raw records
#           to bronze Delta tables.
#
#           API base : https://timeseries.sepa.org.uk/KiWIS/KiWIS
#           Auth     : None — Open Government Licence (Scotland), no key required
#           RSS feed : https://floodline.sepa.org.uk/floodupdates/feed/
#           CRS      : WGS84 (lat/long provided directly by KiWIS — no conversion needed)
#
#   ⚠️ KEY DIFFERENCE FROM EA API:
#       KiWIS returns arrays-of-arrays (first row = header), NOT key-value JSON.
#       Parser must unpack: headers = response[0]; rows = [dict(zip(headers, r)) for r in response[1:]]
#
#   ⚠️ NO BULK LATEST ENDPOINT:
#       Unlike EA's ?latest endpoint, KiWIS requires specifying ts_id values.
#       Strategy: get all stations → get timeseries list (filtered to 15minute/Stage or Flow)
#       → batch getTimeseriesValues for the last 4 hours.
#
# Layer   : Bronze  (ceg_delta_bronze_prnd.international_flood.*)
# Outputs : ceg_delta_bronze_prnd.international_flood.bronze_sepa_stations
#           ceg_delta_bronze_prnd.international_flood.bronze_sepa_readings
#           ceg_delta_bronze_prnd.international_flood.bronze_sepa_warnings
#           ceg_delta_bronze_prnd.international_flood.pipeline_run_log
#
# Job chaining: exits with run_id string for downstream notebooks.
#
# Schedule : Every 4 hours via Databricks Workflow.
# Version  : 1.0.0

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
    "period_hours", "4",
    "Period Hours — how many hours of readings to fetch per time series (e.g. 4)"
)
dbutils.widgets.text(
    "max_ts_per_batch", "50",
    "Max TS Per Batch — max ts_id values per getTimeseriesValues call (comma-separated)"
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import json
import logging
import random
import re
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

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

FQN_AUDIT     = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{AUDIT_TABLE}"
FQN_STATIONS  = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_sepa_stations"
FQN_READINGS  = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_sepa_readings"
FQN_WARNINGS  = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_sepa_warnings"

# ── Source constants ───────────────────────────────────────────────────────
KIWIS_BASE       = "https://timeseries.sepa.org.uk/KiWIS/KiWIS"
RSS_FEED_URL     = "https://floodline.sepa.org.uk/floodupdates/feed/"
SOURCE_NAME      = "SEPA_SCOTLAND"
STATE            = "Scotland"
NOTEBOOK_NAME    = "sepa_01_bronze_scrape"
NOTEBOOK_VERSION = "1.0.0"
USER_AGENT       = "uk-flood-pipeline/1.0.0 (+https://github.com/MapGuru3910/uk-flood)"

# ── Volume for raw storage ─────────────────────────────────────────────────
VOLUME_NAME  = "sepa_raw"
VOLUME_BASE  = f"/Volumes/{BRONZE_CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"

# ── HTTP tuning ────────────────────────────────────────────────────────────
REQUEST_TIMEOUT    = 60
MAX_RETRIES        = 3
RETRY_BACKOFF_BASE = 2.0
RETRY_JITTER       = 0.5

# ── KiWIS parameter filters ────────────────────────────────────────────────
# Fetch 15-minute stage (S) and flow (Q) readings — the primary real-time parameters
TARGET_PARAM_TYPES = {"S", "Q"}   # Stage (level) and Flow
TARGET_TS_NAME     = "15minute"   # Real-time 15-min cadence readings

# ── Logging ───────────────────────────────────────────────────────────────
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

RUN_ID         = dbutils.widgets.get("run_id").strip()         or str(uuid.uuid4())
SCRAPE_VERSION = dbutils.widgets.get("scrape_version").strip() or NOTEBOOK_VERSION
PERIOD_HOURS   = int(dbutils.widgets.get("period_hours").strip() or "4")
MAX_TS_BATCH   = int(dbutils.widgets.get("max_ts_per_batch").strip() or "50")
SCRAPED_AT     = datetime.now(timezone.utc)

PERIOD_ISO     = f"PT{PERIOD_HOURS}H"   # e.g. "PT4H" — KiWIS ISO 8601 duration

log.info("=" * 60)
log.info(f"SEPA Bronze Scrape  |  Run ID: {RUN_ID}")
log.info(f"Notebook version : {NOTEBOOK_VERSION}")
log.info(f"Scrape version   : {SCRAPE_VERSION}")
log.info(f"Period           : {PERIOD_ISO}")
log.info(f"Max TS per batch : {MAX_TS_BATCH}")
log.info(f"Started at       : {SCRAPED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Schema Definitions

# COMMAND ----------

STATIONS_STRUCT = StructType([
    StructField("station_no",            StringType(),    True),
    StructField("station_name",          StringType(),    True),
    StructField("station_latitude",      StringType(),    True),   # raw string from API
    StructField("station_longitude",     StringType(),    True),   # raw string from API
    StructField("catchment_name",        StringType(),    True),
    StructField("river_name",            StringType(),    True),
    StructField("station_raw_json",      StringType(),    True),   # full JSON row as captured
    # Pipeline provenance
    StructField("_run_id",               StringType(),    False),
    StructField("_ingested_at",          TimestampType(), False),
    StructField("_scrape_version",       StringType(),    False),
    StructField("_is_current",           BooleanType(),   False),
    StructField("_source",               StringType(),    False),
    StructField("_state",                StringType(),    False),
])

READINGS_STRUCT = StructType([
    StructField("ts_id",                 StringType(),    True),
    StructField("station_no",            StringType(),    True),
    StructField("ts_name",               StringType(),    True),
    StructField("parametertype_name",    StringType(),    True),
    StructField("unit_name",             StringType(),    True),
    StructField("reading_timestamp",     StringType(),    True),   # raw ISO string
    StructField("value",                 StringType(),    True),   # raw string; cast in silver
    # Pipeline provenance
    StructField("_run_id",               StringType(),    False),
    StructField("_ingested_at",          TimestampType(), False),
    StructField("_is_current",           BooleanType(),   False),
    StructField("_source",               StringType(),    False),
    StructField("_state",                StringType(),    False),
])

WARNINGS_STRUCT = StructType([
    StructField("warning_guid",          StringType(),    True),
    StructField("warning_title",         StringType(),    True),
    StructField("warning_link",          StringType(),    True),
    StructField("warning_description",   StringType(),    True),
    StructField("pub_date",              StringType(),    True),
    StructField("raw_xml",               StringType(),    True),
    # Pipeline provenance
    StructField("_run_id",               StringType(),    False),
    StructField("_ingested_at",          TimestampType(), False),
    StructField("_is_current",           BooleanType(),   False),
    StructField("_source",               StringType(),    False),
    StructField("_state",                StringType(),    False),
])

# COMMAND ----------

# MAGIC %md ## 5. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_SCHEMA}")

# ── bronze_sepa_stations ───────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_STATIONS} (
    station_no          STRING     COMMENT 'SEPA station number (natural key from KiWIS station_no).',
    station_name        STRING     COMMENT 'Human-readable station name.',
    station_latitude    STRING     COMMENT 'Raw latitude string as returned by KiWIS (WGS84).',
    station_longitude   STRING     COMMENT 'Raw longitude string as returned by KiWIS (WGS84).',
    catchment_name      STRING     COMMENT 'Catchment / river basin name.',
    river_name          STRING     COMMENT 'River name.',
    station_raw_json    STRING     COMMENT 'Full station row captured as JSON string for audit trail.',
    _run_id             STRING     NOT NULL COMMENT 'UUID of the scrape run that produced this row.',
    _ingested_at        TIMESTAMP  NOT NULL COMMENT 'UTC timestamp of ingestion.',
    _scrape_version     STRING     NOT NULL COMMENT 'Notebook semantic version at time of run.',
    _is_current         BOOLEAN    NOT NULL COMMENT 'TRUE if this is the most recent scraped record for this station_no.',
    _source             STRING     NOT NULL COMMENT 'Source system — always SEPA_SCOTLAND.',
    _state              STRING     NOT NULL COMMENT 'Region — always Scotland.'
)
USING DELTA
COMMENT 'Bronze: raw SEPA KiWIS station list snapshots. Append-only with _is_current flag.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'pipelines.autoOptimize.zOrderCols'   = 'station_no,_is_current'
)
""")

# ── bronze_sepa_readings ───────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_READINGS} (
    ts_id               STRING     COMMENT 'KiWIS time series ID.',
    station_no          STRING     COMMENT 'Parent station number.',
    ts_name             STRING     COMMENT 'Time series name (e.g. 15minute, Day.Mean).',
    parametertype_name  STRING     COMMENT 'Parameter type code: S=Stage, Q=Flow, RE=Rainfall.',
    unit_name           STRING     COMMENT 'Unit of measurement (e.g. m, m3/s).',
    reading_timestamp   STRING     COMMENT 'Raw ISO 8601 timestamp string from KiWIS.',
    value               STRING     COMMENT 'Raw observed value as string. NULL for missing data.',
    _run_id             STRING     NOT NULL COMMENT 'UUID of the scrape run that produced this row.',
    _ingested_at        TIMESTAMP  NOT NULL COMMENT 'UTC timestamp of ingestion.',
    _is_current         BOOLEAN    NOT NULL COMMENT 'TRUE for readings from the most recent run for this ts_id.',
    _source             STRING     NOT NULL COMMENT 'Source system — always SEPA_SCOTLAND.',
    _state              STRING     NOT NULL COMMENT 'Region — always Scotland.'
)
USING DELTA
COMMENT 'Bronze: raw SEPA KiWIS 15-minute river level and flow readings. Append-only.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'pipelines.autoOptimize.zOrderCols'   = 'station_no,ts_id'
)
""")

# ── bronze_sepa_warnings ───────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_WARNINGS} (
    warning_guid        STRING     COMMENT 'RSS item guid — unique identifier per warning item.',
    warning_title       STRING     COMMENT 'RSS item title (e.g. "Flood Warning — River Tay at Perth").',
    warning_link        STRING     COMMENT 'URL linking to full warning details on SEPA floodline.',
    warning_description STRING     COMMENT 'Full description text from RSS item.',
    pub_date            STRING     COMMENT 'Raw publication date string from RSS item.',
    raw_xml             STRING     COMMENT 'Full RSS <item> XML element as a string for audit trail.',
    _run_id             STRING     NOT NULL COMMENT 'UUID of the scrape run that produced this row.',
    _ingested_at        TIMESTAMP  NOT NULL COMMENT 'UTC timestamp of ingestion.',
    _is_current         BOOLEAN    NOT NULL COMMENT 'TRUE for warnings captured in the most recent run.',
    _source             STRING     NOT NULL COMMENT 'Source system — always SEPA_SCOTLAND.',
    _state              STRING     NOT NULL COMMENT 'Region — always Scotland.'
)
USING DELTA
COMMENT 'Bronze: raw SEPA RSS flood warning items. Append-only. Empty when no warnings are active (confirmed SEPA behaviour during non-flood periods).'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'pipelines.autoOptimize.zOrderCols'   = 'warning_guid,_is_current'
)
""")

# ── pipeline_run_log (shared audit table — idempotent) ─────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_AUDIT} (
    run_id              STRING     NOT NULL  COMMENT 'UUID shared across pipeline notebooks in one Workflow run.',
    pipeline_stage      STRING     NOT NULL  COMMENT 'Notebook stage: bronze | silver | gold | download | raster_ingest.',
    notebook_name       STRING               COMMENT 'Filename of the notebook.',
    notebook_version    STRING               COMMENT 'Semantic version of the notebook.',
    start_time          TIMESTAMP  NOT NULL,
    end_time            TIMESTAMP,
    duration_seconds    DOUBLE,
    status              STRING               COMMENT 'success | failed | partial',
    rows_read           LONG,
    rows_written        LONG,
    rows_merged         LONG,
    rows_rejected       LONG,
    error_message       STRING,
    extra_metadata      STRING               COMMENT 'JSON blob of stage-specific counters.',
    databricks_job_id   STRING,
    databricks_run_id   STRING,
    scrape_version      STRING
)
USING DELTA
COMMENT 'Pipeline audit log. One row per notebook stage per Workflow run.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── Ensure raw volume exists ───────────────────────────────────────────────
try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_SCHEMA}.{VOLUME_NAME}
        COMMENT 'SEPA raw downloads and snapshots volume'
    """)
    log.info(f"Volume {VOLUME_BASE} confirmed/created")
except Exception as e:
    log.warning(f"Could not create volume (may already exist): {e}")

# COMMAND ----------

# MAGIC %md ## 6. HTTP Utilities

# COMMAND ----------

class ScrapeError(Exception):
    """Raised when an HTTP request fails after exhausting all retry attempts."""
    pass


def http_get(url: str, params: dict = None, timeout: int = REQUEST_TIMEOUT,
             accept: str = "application/json") -> requests.Response:
    """
    HTTP GET with exponential backoff retry.
    SSL verification enabled — SEPA has valid certificates.
    Raises ScrapeError if all attempts are exhausted.
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": accept,
    }
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=headers,
                                timeout=timeout, verify=True)
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
    raise ScrapeError(f"All {MAX_RETRIES} HTTP attempts failed for [{url}]: {last_exc}") from last_exc


def kiwis_get(request: str, extra_params: dict = None) -> Any:
    """
    Calls the SEPA KiWIS API and returns the parsed JSON response.

    KiWIS returns arrays-of-arrays for list endpoints:
      response[0]  = column headers list   (e.g. ["station_no", "station_name", ...])
      response[1:] = data rows

    For getTimeseriesValues the response is a list of dicts (one per ts_id requested).

    Raises ScrapeError on HTTP failure. Returns raw parsed JSON.
    """
    params = {
        "service":    "kisters",
        "type":       "queryServices",
        "request":    request,
        "datasource": "0",
        "format":     "json",
    }
    if extra_params:
        params.update(extra_params)
    resp = http_get(KIWIS_BASE, params=params)
    return resp.json()

# COMMAND ----------

# MAGIC %md ## 7. KiWIS Response Parsing

# COMMAND ----------

def parse_kiwis_array_response(raw: Any, context: str = "") -> List[Dict]:
    """
    Parses KiWIS array-of-arrays response format into a list of dicts.

    Input format:
        [
          ["station_no", "station_name", "station_latitude", ...],  # header row
          ["15018", "Abbey St Bathans", "55.853", ...],              # data row
          ...
        ]

    Returns:
        [{"station_no": "15018", "station_name": "Abbey St Bathans", ...}, ...]

    Handles edge cases:
        - Empty list (no stations/data) → returns []
        - Single-row list (header only) → returns []
        - Non-list responses → raises ValueError
    """
    if not isinstance(raw, list):
        raise ValueError(f"Expected list from KiWIS {context}, got {type(raw).__name__}: {str(raw)[:200]}")
    if len(raw) == 0:
        log.warning(f"KiWIS {context}: empty response (no data)")
        return []
    if len(raw) == 1:
        log.warning(f"KiWIS {context}: only header row returned (no data rows)")
        return []

    headers = raw[0]
    rows    = raw[1:]
    result  = []
    for row in rows:
        if len(row) != len(headers):
            log.warning(f"KiWIS {context}: row length {len(row)} != header length {len(headers)} — skipping")
            continue
        result.append(dict(zip(headers, row)))
    return result

# COMMAND ----------

# MAGIC %md ## 8. Station List Fetch

# COMMAND ----------

def fetch_station_list() -> List[Dict]:
    """
    Fetches all SEPA monitoring stations from KiWIS getStationList.

    Returns list of dicts with keys:
        station_no, station_name, station_latitude, station_longitude,
        catchment_name, river_name

    SEPA typically returns 500+ stations across Scotland.
    No pagination is needed — KiWIS returns all stations in one call.
    """
    log.info("Fetching SEPA station list from KiWIS...")
    raw = kiwis_get(
        "getStationList",
        extra_params={
            "returnfields": "station_no,station_name,station_latitude,station_longitude,catchment_name,river_name",
            "station_no":   "*",
        }
    )
    stations = parse_kiwis_array_response(raw, context="getStationList")
    log.info(f"  → {len(stations)} stations retrieved")
    return stations

# COMMAND ----------

# MAGIC %md ## 9. Time Series Discovery

# COMMAND ----------

def fetch_timeseries_list() -> List[Dict]:
    """
    Fetches the full time series list from KiWIS getTimeseriesList.

    Returns list of dicts including:
        ts_id, station_no, ts_name, parametertype_name, unit_name

    We filter to TARGET_PARAM_TYPES (S=Stage, Q=Flow) and TARGET_TS_NAME
    (15minute) to identify which ts_id values to fetch values for.

    KiWIS does not require per-station calls — the global list is returned
    in a single call (may contain thousands of rows; typically ~5,000 for SEPA).
    """
    log.info("Fetching SEPA time series list from KiWIS...")
    raw = kiwis_get(
        "getTimeseriesList",
        extra_params={
            "returnfields": "ts_id,station_no,ts_name,parametertype_name,unit_name",
            "datasource":   "0",
        }
    )
    ts_list = parse_kiwis_array_response(raw, context="getTimeseriesList")
    log.info(f"  → {len(ts_list)} time series entries total")

    # Filter to real-time 15-minute Stage / Flow time series
    filtered = [
        ts for ts in ts_list
        if ts.get("parametertype_name") in TARGET_PARAM_TYPES
        and ts.get("ts_name") == TARGET_TS_NAME
    ]
    log.info(
        f"  → {len(filtered)} time series after filter "
        f"(param_types={TARGET_PARAM_TYPES}, ts_name='{TARGET_TS_NAME}')"
    )
    return filtered

# COMMAND ----------

# MAGIC %md ## 10. Readings Fetch (Batched)

# COMMAND ----------

def fetch_readings_batch(ts_entries: List[Dict], period: str, batch_size: int) -> List[Dict]:
    """
    Fetches getTimeseriesValues for a list of ts_id values in batches.

    KiWIS supports comma-separated ts_id values in a single request, reducing
    API call count from ~500 per run to ceil(500 / batch_size) calls.

    Parameters:
        ts_entries  : list of dicts from fetch_timeseries_list() (filtered)
        period      : ISO 8601 duration string (e.g. "PT4H") for the look-back window
        batch_size  : max ts_id values per API call

    Returns:
        flat list of reading dicts, each with keys:
            ts_id, station_no, ts_name, parametertype_name, unit_name,
            reading_timestamp, value

    KiWIS getTimeseriesValues response format (single ts_id example):
        [
          {
            "ts_id": "54512010",
            "rows": "16",
            "columns": "Timestamp,Value",
            "data": [
              ["2026-03-28T00:00:00.000Z", 0.779],
              ...
            ]
          },
          ...
        ]

    The outer list contains one entry per ts_id. Each entry has a "data" list
    of [timestamp_string, value] pairs. value may be None for missing readings.
    """
    all_readings = []
    ts_id_to_meta = {str(ts["ts_id"]): ts for ts in ts_entries}
    ts_ids = list(ts_id_to_meta.keys())

    total_batches = (len(ts_ids) + batch_size - 1) // batch_size
    log.info(f"Fetching readings for {len(ts_ids)} time series in {total_batches} batches (size={batch_size})")

    for batch_num, i in enumerate(range(0, len(ts_ids), batch_size), start=1):
        batch_ids = ts_ids[i : i + batch_size]
        ts_id_param = ",".join(batch_ids)

        try:
            raw = kiwis_get(
                "getTimeseriesValues",
                extra_params={
                    "ts_id":  ts_id_param,
                    "period": period,
                }
            )
        except ScrapeError as exc:
            log.error(f"Batch {batch_num}/{total_batches} failed: {exc} — skipping batch")
            continue

        # getTimeseriesValues returns a list of objects (one per ts_id), NOT arrays-of-arrays
        if not isinstance(raw, list):
            log.warning(f"Batch {batch_num}: unexpected response type {type(raw).__name__} — skipping")
            continue

        for ts_result in raw:
            if not isinstance(ts_result, dict):
                continue
            ts_id_str = str(ts_result.get("ts_id", ""))
            data_rows = ts_result.get("data", []) or []
            meta      = ts_id_to_meta.get(ts_id_str, {})

            if not data_rows:
                # Normal — no readings in the period window (station offline or no new data)
                continue

            for data_point in data_rows:
                if not isinstance(data_point, list) or len(data_point) < 2:
                    continue
                raw_ts, raw_val = data_point[0], data_point[1]
                all_readings.append({
                    "ts_id":              ts_id_str,
                    "station_no":         str(meta.get("station_no", "")),
                    "ts_name":            str(meta.get("ts_name", "")),
                    "parametertype_name": str(meta.get("parametertype_name", "")),
                    "unit_name":          str(meta.get("unit_name", "")),
                    "reading_timestamp":  str(raw_ts) if raw_ts is not None else None,
                    "value":              str(raw_val) if raw_val is not None else None,
                })

        if batch_num % 10 == 0 or batch_num == total_batches:
            log.info(f"  Batch {batch_num}/{total_batches} complete — {len(all_readings)} readings so far")

        # Polite delay between batches to avoid overwhelming the KiWIS server
        if i + batch_size < len(ts_ids):
            time.sleep(0.5)

    log.info(f"Total readings fetched: {len(all_readings)}")
    return all_readings

# COMMAND ----------

# MAGIC %md ## 11. RSS Flood Warnings Fetch

# COMMAND ----------

def fetch_rss_warnings() -> List[Dict]:
    """
    Fetches and parses the SEPA flood warnings RSS feed.

    SEPA uses RSS 2.0 at https://floodline.sepa.org.uk/floodupdates/feed/
    The feed is empty when no warnings are active (confirmed SEPA behaviour).
    During active flood events, each <item> represents one warning.

    RSS item fields captured:
        title, link, description, guid, pubDate

    Returns list of warning dicts. Empty list = no active warnings.
    """
    log.info(f"Fetching SEPA RSS flood warnings feed: {RSS_FEED_URL}")
    try:
        resp = http_get(RSS_FEED_URL, accept="application/rss+xml, text/xml")
        xml_text = resp.text
    except ScrapeError as exc:
        log.warning(f"RSS feed fetch failed: {exc} — returning empty warnings list")
        return []

    warnings = []
    try:
        root = ET.fromstring(xml_text)
        # RSS 2.0: <rss><channel><item>...</item></channel></rss>
        ns   = {}
        items = root.findall(".//item")
        log.info(f"  → {len(items)} RSS warning items found")

        for item in items:
            def _text(tag: str) -> Optional[str]:
                el = item.find(tag)
                return el.text.strip() if el is not None and el.text else None

            # Capture the raw <item> XML for full audit trail
            raw_xml_str = ET.tostring(item, encoding="unicode")

            guid  = _text("guid") or _text("link") or str(uuid.uuid4())
            warnings.append({
                "warning_guid":        guid,
                "warning_title":       _text("title"),
                "warning_link":        _text("link"),
                "warning_description": _text("description"),
                "pub_date":            _text("pubDate"),
                "raw_xml":             raw_xml_str,
            })

    except ET.ParseError as exc:
        log.error(f"RSS XML parse error: {exc} — returning empty warnings list")
        return []

    return warnings

# COMMAND ----------

# MAGIC %md ## 12. Bronze Delta Writes

# COMMAND ----------

def _add_provenance(rows: List[Dict], run_id: str, ingested_at, scrape_version: str) -> List[Dict]:
    """Annotates a list of row dicts with standard pipeline provenance fields."""
    for row in rows:
        row["_run_id"]         = run_id
        row["_ingested_at"]    = ingested_at
        row["_is_current"]     = True
        row["_source"]         = SOURCE_NAME
        row["_state"]          = STATE
    return rows


def _expire_previous_current(fqn: str, key_col: str, key_values: List[str], run_id: str):
    """
    Sets _is_current = FALSE for any existing rows in `fqn` where
    `key_col` is in `key_values`, _is_current = TRUE, and _run_id != run_id.
    Uses a temp view to avoid huge IN() lists on large datasets.
    """
    if not key_values:
        return
    view_name = f"_expire_keys_{key_col}_{uuid.uuid4().hex[:8]}"
    keys_df   = spark.createDataFrame([(k,) for k in key_values], [key_col])
    keys_df.createOrReplaceTempView(view_name)
    spark.sql(f"""
        UPDATE {fqn}
        SET    _is_current = FALSE
        WHERE  {key_col} IN (SELECT {key_col} FROM {view_name})
          AND  _is_current  = TRUE
          AND  _run_id     != '{run_id}'
    """)
    spark.catalog.dropTempView(view_name)


def write_bronze_stations(stations: List[Dict], run_id: str, ingested_at, scrape_version: str) -> int:
    """
    Writes SEPA station rows to bronze_sepa_stations.
    Expires previous _is_current rows before appending new ones.
    Returns rows_written.
    """
    if not stations:
        log.warning("write_bronze_stations: no rows to write")
        return 0

    rows = []
    for s in stations:
        rows.append({
            "station_no":        str(s.get("station_no", "") or ""),
            "station_name":      str(s.get("station_name", "") or ""),
            "station_latitude":  str(s.get("station_latitude", "") or ""),
            "station_longitude": str(s.get("station_longitude", "") or ""),
            "catchment_name":    str(s.get("catchment_name", "") or ""),
            "river_name":        str(s.get("river_name", "") or ""),
            "station_raw_json":  json.dumps(s),
        })

    rows = _add_provenance(rows, run_id, ingested_at, scrape_version)

    # Align columns to schema
    columns      = [f.name for f in STATIONS_STRUCT.fields]
    pdf          = pd.DataFrame(rows)
    for col in columns:
        if col not in pdf.columns:
            pdf[col] = None
    pdf = pdf[columns]

    sdf = spark.createDataFrame(pdf, schema=STATIONS_STRUCT)

    station_nos = [r["station_no"] for r in rows if r.get("station_no")]
    _expire_previous_current(FQN_STATIONS, "station_no", station_nos, run_id)

    sdf.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(FQN_STATIONS)
    log.info(f"Bronze stations: {len(rows)} rows appended to {FQN_STATIONS}")
    return len(rows)


def write_bronze_readings(readings: List[Dict], run_id: str, ingested_at) -> int:
    """
    Writes SEPA readings rows to bronze_sepa_readings.
    Readings are purely append-only (no _is_current expiry needed —
    each reading has a unique (ts_id, reading_timestamp) and we want full history).
    Returns rows_written.
    """
    if not readings:
        log.warning("write_bronze_readings: no rows to write")
        return 0

    rows = _add_provenance(readings[:], run_id, ingested_at, scrape_version=SCRAPE_VERSION)

    columns = [f.name for f in READINGS_STRUCT.fields]
    pdf     = pd.DataFrame(rows)
    for col in columns:
        if col not in pdf.columns:
            pdf[col] = None
    pdf = pdf[columns]

    sdf = spark.createDataFrame(pdf, schema=READINGS_STRUCT)
    sdf.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(FQN_READINGS)
    log.info(f"Bronze readings: {len(rows)} rows appended to {FQN_READINGS}")
    return len(rows)


def write_bronze_warnings(warnings: List[Dict], run_id: str, ingested_at) -> int:
    """
    Writes SEPA RSS warning rows to bronze_sepa_warnings.
    Expires previous _is_current rows before appending.
    Returns rows_written. Zero rows is normal when no warnings are active.
    """
    if not warnings:
        log.info("write_bronze_warnings: no active warnings to write (normal during non-flood periods)")
        # Still write a sentinel row so the audit trail shows the feed was checked
        # Actually, just write 0 rows — audit log captures the check via extra_metadata
        return 0

    rows = _add_provenance(warnings[:], run_id, ingested_at, scrape_version=SCRAPE_VERSION)

    columns = [f.name for f in WARNINGS_STRUCT.fields]
    pdf     = pd.DataFrame(rows)
    for col in columns:
        if col not in pdf.columns:
            pdf[col] = None
    pdf = pdf[columns]

    sdf = spark.createDataFrame(pdf, schema=WARNINGS_STRUCT)

    guids = [r["warning_guid"] for r in rows if r.get("warning_guid")]
    _expire_previous_current(FQN_WARNINGS, "warning_guid", guids, run_id)

    sdf.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(FQN_WARNINGS)
    log.info(f"Bronze warnings: {len(rows)} rows appended to {FQN_WARNINGS}")
    return len(rows)

# COMMAND ----------

# MAGIC %md ## 13. Audit Logging

# COMMAND ----------

def write_audit(
    run_id, stage, notebook_name, notebook_version,
    start_time, end_time, status,
    rows_read=0, rows_written=0, rows_merged=0, rows_rejected=0,
    error_message=None, extra_metadata=None,
):
    """
    Appends a single audit record to pipeline_run_log.
    Exceptions are swallowed — an audit failure must not mask pipeline errors.
    """
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
            "scrape_version":    NOTEBOOK_VERSION,
        }]
        (
            spark.createDataFrame(record)
            .write.format("delta")
            .mode("append")
            .saveAsTable(FQN_AUDIT)
        )
        log.info(f"Audit written: stage={stage} status={status} rows_written={rows_written}")
    except Exception as audit_exc:
        log.error(f"write_audit failed (non-fatal): {audit_exc}")

# COMMAND ----------

# MAGIC %md ## 14. Execute

# COMMAND ----------

_pipeline_start   = datetime.now(timezone.utc)
_pipeline_status  = "failed"
_pipeline_error   = None
_stations_written = 0
_readings_written = 0
_warnings_written = 0
_ts_count         = 0
_station_count    = 0

try:
    log.info(f"Starting SEPA bronze scrape | run_id={RUN_ID}")

    # ── 1. Station list ────────────────────────────────────────────────────
    stations     = fetch_station_list()
    _station_count = len(stations)
    _stations_written = write_bronze_stations(stations, RUN_ID, SCRAPED_AT, SCRAPE_VERSION)

    # ── 2. Time series discovery + readings ───────────────────────────────
    ts_entries = fetch_timeseries_list()
    _ts_count  = len(ts_entries)

    if ts_entries:
        readings = fetch_readings_batch(ts_entries, PERIOD_ISO, MAX_TS_BATCH)
        _readings_written = write_bronze_readings(readings, RUN_ID, SCRAPED_AT)
    else:
        log.warning("No 15-minute Stage/Flow time series found — skipping readings fetch")

    # ── 3. RSS flood warnings ─────────────────────────────────────────────
    warnings = fetch_rss_warnings()
    _warnings_written = write_bronze_warnings(warnings, RUN_ID, SCRAPED_AT)

    _pipeline_status = "success"
    log.info(
        f"SEPA bronze scrape complete | "
        f"stations={_stations_written} | "
        f"ts_discovered={_ts_count} | "
        f"readings={_readings_written} | "
        f"warnings={_warnings_written}"
    )

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"SEPA bronze scrape failed: {exc}", exc_info=True)
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
        rows_written     = _stations_written + _readings_written + _warnings_written,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "stations_total":    _station_count,
            "stations_written":  _stations_written,
            "ts_discovered":     _ts_count,
            "readings_written":  _readings_written,
            "warnings_found":    _warnings_written,
            "period":            PERIOD_ISO,
            "max_ts_batch":      MAX_TS_BATCH,
            "kiwis_base_url":    KIWIS_BASE,
            "rss_feed_url":      RSS_FEED_URL,
        },
    )

# Publish run_id for downstream tasks in Databricks Workflow
try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
