# Databricks notebook source
# EA Real-Time Flood Monitoring — Silver Normalisation
#
# Purpose : Reads the latest EA monitoring bronze scrape records and normalises
#           them into silver Delta tables:
#             - ea_monitoring_stations       (MERGE upsert on station_id)
#             - ea_monitoring_flood_events   (SCD Type 2 on flood_area_id — tracks severity transitions)
#             - ea_monitoring_flood_areas    (MERGE upsert on notation)
#             - ea_monitoring_readings       (append-only time-series, partitioned by reading_date)
#
# SCD Type 2 rationale for flood_events: warnings transition through severity
# levels (raised → escalated → de-escalated → removed). Tracking these transitions
# is analytically valuable — analogous to NSW SES project approval state changes.
#
# Layer   : Silver  (ceg_delta_silver_prnd.international_flood.ea_monitoring_*)
# Reads   : ceg_delta_bronze_prnd.international_flood.ea_monitoring_*_scrape
# Writes  : ceg_delta_silver_prnd.international_flood.ea_monitoring_stations
#           ceg_delta_silver_prnd.international_flood.ea_monitoring_flood_events
#           ceg_delta_silver_prnd.international_flood.ea_monitoring_flood_areas
#           ceg_delta_silver_prnd.international_flood.ea_monitoring_readings
#           ceg_delta_bronze_prnd.international_flood.pipeline_run_log
#
# Version : 1.0.0

# COMMAND ----------

# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text(
    "run_id", "",
    "Run ID — leave blank to auto-generate."
)
dbutils.widgets.text(
    "bronze_run_id", "",
    "Bronze Run ID — the _run_id from the bronze scrape to process. Blank = latest run."
)
dbutils.widgets.dropdown(
    "full_refresh", "false", ["true", "false"],
    "Full Refresh — if true, rebuilds all silver tables from complete bronze history."
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType,
    LongType, StringType, StructField, StructType, TimestampType,
)

# ── Catalog / table paths ──────────────────────────────────────────────────
BRONZE_CATALOG = "ceg_delta_bronze_prnd"
SILVER_CATALOG = "ceg_delta_silver_prnd"
SCHEMA         = "international_flood"

FQN_AUDIT              = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
FQN_B_STATIONS         = f"{BRONZE_CATALOG}.{SCHEMA}.ea_monitoring_stations_scrape"
FQN_B_FLOODS           = f"{BRONZE_CATALOG}.{SCHEMA}.ea_monitoring_floods_scrape"
FQN_B_FLOOD_AREAS      = f"{BRONZE_CATALOG}.{SCHEMA}.ea_monitoring_flood_areas_scrape"
FQN_B_READINGS         = f"{BRONZE_CATALOG}.{SCHEMA}.ea_monitoring_readings_scrape"

FQN_S_STATIONS         = f"{SILVER_CATALOG}.{SCHEMA}.ea_monitoring_stations"
FQN_S_FLOOD_EVENTS     = f"{SILVER_CATALOG}.{SCHEMA}.ea_monitoring_flood_events"
FQN_S_FLOOD_AREAS      = f"{SILVER_CATALOG}.{SCHEMA}.ea_monitoring_flood_areas"
FQN_S_READINGS         = f"{SILVER_CATALOG}.{SCHEMA}.ea_monitoring_readings"

NOTEBOOK_NAME    = "ea_02_silver_normalize"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "EA_MONITORING"
STATE            = "England"

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

RUN_ID        = dbutils.widgets.get("run_id").strip()       or str(uuid.uuid4())
BRONZE_RUN_ID = dbutils.widgets.get("bronze_run_id").strip() or None
FULL_REFRESH  = dbutils.widgets.get("full_refresh").lower() == "true"
PROCESSED_AT  = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"EA Monitoring Silver Normalisation  |  Run ID: {RUN_ID}")
log.info(f"Bronze run ID : {BRONZE_RUN_ID or 'latest'}")
log.info(f"Full refresh  : {FULL_REFRESH}")
log.info(f"Started at    : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.{SCHEMA}")

# ── ea_monitoring_stations ─────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_S_STATIONS} (
    station_id              STRING     NOT NULL  COMMENT 'Natural key — EA stationReference (e.g. 1029TH)',
    station_uri             STRING               COMMENT 'Full EA Linked Data URI',
    station_name            STRING               COMMENT 'Human-readable label',
    rloi_id                 STRING               COMMENT 'River Levels on the Internet ID',
    river_name              STRING               COMMENT 'River name',
    catchment_name          STRING               COMMENT 'Catchment / river basin name',
    town                    STRING               COMMENT 'Associated settlement',
    lat                     DOUBLE               COMMENT 'WGS84 latitude',
    lon                     DOUBLE               COMMENT 'WGS84 longitude',
    easting                 INT                  COMMENT 'BNG easting (EPSG:27700)',
    northing                INT                  COMMENT 'BNG northing (EPSG:27700)',
    date_opened             DATE                 COMMENT 'Date station opened',
    station_status          STRING               COMMENT 'Status label: Active | Closed | Suspended',
    ea_area_name            STRING               COMMENT 'EA operational area',
    measures_json           STRING               COMMENT 'JSON array of measure definitions',
    _is_current             BOOLEAN              COMMENT 'TRUE for active record',
    _bronze_run_id          STRING               COMMENT 'Source bronze _run_id',
    _silver_processed_at    TIMESTAMP            COMMENT 'UTC timestamp of silver write',
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Silver: EA monitoring stations — MERGE upsert on station_id.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'station_id'
)
""")

# ── ea_monitoring_flood_events (SCD Type 2) ────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_S_FLOOD_EVENTS} (
    flood_area_id           STRING     NOT NULL  COMMENT 'Flood area notation — natural key (e.g. 061WAF21PangSulm)',
    flood_warning_uri       STRING               COMMENT 'Full EA Linked Data URI for this warning',
    description             STRING               COMMENT 'Plain-text description of the area',
    severity                STRING               COMMENT 'Severity label',
    severity_level          INT                  COMMENT '1=Severe, 2=Warning, 3=Alert, 4=No longer in force',
    is_tidal                BOOLEAN              COMMENT 'Whether the warning is tidal',
    message                 STRING               COMMENT 'Full advisory message',
    time_raised             TIMESTAMP            COMMENT 'When the warning was originally raised',
    time_severity_changed   TIMESTAMP            COMMENT 'Most recent severity change timestamp',
    time_message_changed    TIMESTAMP            COMMENT 'Most recent message change timestamp',
    county                  STRING               COMMENT 'County name',
    river_or_sea            STRING               COMMENT 'Named water body',
    ea_area_name            STRING               COMMENT 'EA operational area',
    flood_area_polygon_url  STRING               COMMENT 'URL to fetch GeoJSON polygon for the warning area',
    _content_hash           STRING               COMMENT 'MD5 of key mutable fields — used for SCD2 change detection',
    _effective_from         TIMESTAMP  NOT NULL  COMMENT 'UTC timestamp when this version became current',
    _effective_to           TIMESTAMP            COMMENT 'UTC when superseded. NULL = current record',
    _is_current             BOOLEAN    NOT NULL  COMMENT 'TRUE for the current version',
    _bronze_run_id          STRING,
    _silver_processed_at    TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Silver: EA flood warning events — SCD Type 2 to track severity transitions over time.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'flood_area_id,_is_current'
)
""")

# ── ea_monitoring_flood_areas ──────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_S_FLOOD_AREAS} (
    notation                STRING     NOT NULL  COMMENT 'Unique area notation — natural key',
    flood_area_uri          STRING               COMMENT 'Full EA Linked Data URI',
    area_description        STRING               COMMENT 'Description of the flood area',
    county                  STRING               COMMENT 'County name',
    ea_area_name            STRING               COMMENT 'EA operational area name',
    river_or_sea            STRING               COMMENT 'Named water body',
    polygon_url             STRING               COMMENT 'URL to fetch GeoJSON polygon for this area',
    label                   STRING               COMMENT 'Display label',
    _is_current             BOOLEAN,
    _bronze_run_id          STRING,
    _silver_processed_at    TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Silver: EA flood warning area definitions — MERGE upsert on notation.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# ── ea_monitoring_readings ─────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_S_READINGS} (
    measure_uri       STRING     COMMENT 'Full URI of the measure',
    station_id        STRING     COMMENT 'Station reference code',
    parameter         STRING     COMMENT 'Parameter type: level | flow | rainfall',
    period            INT        COMMENT 'Measurement period in seconds',
    qualifier         STRING     COMMENT 'Qualifier label',
    unit_name         STRING     COMMENT 'Unit of measurement',
    reading_datetime  TIMESTAMP  COMMENT 'UTC timestamp of the reading',
    reading_date      DATE       COMMENT 'Date partition column',
    value             DOUBLE     COMMENT 'Observed value',
    _run_id           STRING,
    _ingested_at      TIMESTAMP,
    _source           STRING,
    _state            STRING
)
USING DELTA
PARTITIONED BY (reading_date)
COMMENT 'Silver: EA monitoring readings — append-only time-series, partitioned by reading_date.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'station_id,reading_datetime'
)
""")

# COMMAND ----------

# MAGIC %md ## 5. Load Bronze Records

# COMMAND ----------

def _resolve_bronze_run_id(fqn: str, supplied_run_id: Optional[str]) -> Optional[str]:
    """Resolves the bronze run_id to use — supplied, or latest from table."""
    if FULL_REFRESH:
        return None  # Process all current records
    if supplied_run_id:
        return supplied_run_id
    # Auto-resolve latest
    row = spark.sql(f"""
        SELECT _run_id FROM {fqn}
        ORDER BY _ingested_at DESC LIMIT 1
    """).collect()
    return row[0]["_run_id"] if row else None


def _load_bronze(fqn: str, run_id_filter: Optional[str]) -> pd.DataFrame:
    """Loads current bronze records, optionally filtered to a specific run_id."""
    if FULL_REFRESH or run_id_filter is None:
        df = spark.sql(f"SELECT * FROM {fqn} WHERE _is_current = TRUE")
    else:
        df = spark.sql(f"""
            SELECT * FROM {fqn}
            WHERE _run_id = '{run_id_filter}'
              AND _is_current = TRUE
        """)
    return df.toPandas()

# COMMAND ----------

# Resolve which bronze run to process
_b_run_id = _resolve_bronze_run_id(FQN_B_STATIONS, BRONZE_RUN_ID)
log.info(f"Processing bronze run_id: {_b_run_id or '(all current)'}")

stations_pdf  = _load_bronze(FQN_B_STATIONS,    _b_run_id)
floods_pdf    = _load_bronze(FQN_B_FLOODS,       _b_run_id)
flood_areas_pdf = _load_bronze(FQN_B_FLOOD_AREAS, _b_run_id)
readings_pdf  = _load_bronze(FQN_B_READINGS,     _b_run_id)

log.info(f"Loaded bronze: stations={len(stations_pdf)} floods={len(floods_pdf)} "
         f"flood_areas={len(flood_areas_pdf)} readings={len(readings_pdf)}")

# COMMAND ----------

# MAGIC %md ## 6. Utilities

# COMMAND ----------

def _parse_timestamp(raw: Optional[str]) -> Optional[datetime]:
    """Parse an ISO timestamp string defensively. Returns None on failure."""
    if not raw or not str(raw).strip():
        return None
    s = str(raw).strip()
    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S+00:00"]:
        try:
            return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _parse_date(raw: Optional[str]) -> Optional[str]:
    """Return ISO date string (YYYY-MM-DD) from a raw date string, or None."""
    if not raw or not str(raw).strip():
        return None
    s = str(raw).strip()
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else None


def _status_label(status_uri: str) -> str:
    """Converts an EA status URI to a short label."""
    if not status_uri:
        return "Unknown"
    if "Active" in status_uri:
        return "Active"
    if "Closed" in status_uri or "closed" in status_uri:
        return "Closed"
    if "Suspend" in status_uri or "suspend" in status_uri:
        return "Suspended"
    return str(status_uri).split("/")[-1]  # Last segment of URI


def _content_hash(values: list) -> str:
    """MD5 hash of concatenated string values — used for SCD2 change detection."""
    payload = "|".join(str(v) if v is not None else "" for v in values)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()

# COMMAND ----------

# MAGIC %md ## 7. Transform: Stations (MERGE Upsert)

# COMMAND ----------

_stations_written = 0

if not stations_pdf.empty:
    stations_pdf["_status_label"]        = stations_pdf["station_status"].apply(_status_label)
    stations_pdf["_date_opened_iso"]     = stations_pdf["date_opened"].apply(_parse_date)
    stations_pdf["_is_current"]          = True
    stations_pdf["_bronze_run_id"]       = stations_pdf["_run_id"]
    stations_pdf["_silver_processed_at"] = PROCESSED_AT
    stations_pdf["_source"]              = SOURCE_NAME
    stations_pdf["_state"]               = STATE

    station_cols = [
        "station_id", "station_uri", "station_name", "rloi_id",
        "river_name", "catchment_name", "town",
        "lat", "lon", "easting", "northing",
        "_date_opened_iso", "_status_label", "ea_area_name", "measures_json",
        "_is_current", "_bronze_run_id", "_silver_processed_at", "_source", "_state",
    ]

    s_sdf = spark.createDataFrame(
        stations_pdf[station_cols].rename(columns={
            "_date_opened_iso": "date_opened",
            "_status_label":    "station_status",
        })
    )
    s_sdf.createOrReplaceTempView("_incoming_stations")

    spark.sql(f"""
        MERGE INTO {FQN_S_STATIONS} AS target
        USING _incoming_stations AS source
        ON target.station_id = source.station_id
        WHEN MATCHED THEN UPDATE SET
            target.station_uri          = source.station_uri,
            target.station_name         = source.station_name,
            target.rloi_id              = source.rloi_id,
            target.river_name           = source.river_name,
            target.catchment_name       = source.catchment_name,
            target.town                 = source.town,
            target.lat                  = source.lat,
            target.lon                  = source.lon,
            target.easting              = source.easting,
            target.northing             = source.northing,
            target.date_opened          = source.date_opened,
            target.station_status       = source.station_status,
            target.ea_area_name         = source.ea_area_name,
            target.measures_json        = source.measures_json,
            target._is_current          = TRUE,
            target._bronze_run_id       = source._bronze_run_id,
            target._silver_processed_at = source._silver_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    _stations_written = s_sdf.count()
    log.info(f"Stations: {_stations_written} records merged into {FQN_S_STATIONS}")
else:
    log.warning("Stations: no bronze records found — skipping")

# COMMAND ----------

# MAGIC %md ## 8. Transform: Flood Events (SCD Type 2)

# COMMAND ----------

_floods_written = 0

if not floods_pdf.empty:
    # Parse timestamps
    floods_pdf["_time_raised"]            = floods_pdf["time_raised"].apply(_parse_timestamp)
    floods_pdf["_time_severity_changed"]  = floods_pdf["time_severity_changed"].apply(_parse_timestamp)
    floods_pdf["_time_message_changed"]   = floods_pdf["time_message_changed"].apply(_parse_timestamp)

    # Compute content hash for SCD2 change detection
    floods_pdf["_content_hash"] = floods_pdf.apply(
        lambda r: _content_hash([
            r["severity"], r["severity_level"], r["is_tidal"],
            r["message"], r["time_raised"], r["time_severity_changed"],
        ]),
        axis=1,
    )
    floods_pdf["_bronze_run_id"]       = floods_pdf["_run_id"]
    floods_pdf["_silver_processed_at"] = PROCESSED_AT
    floods_pdf["_source"]              = SOURCE_NAME
    floods_pdf["_state"]               = STATE

    event_cols = [
        "flood_area_id", "flood_warning_uri", "description",
        "severity", "severity_level", "is_tidal", "message",
        "_time_raised", "_time_severity_changed", "_time_message_changed",
        "county", "river_or_sea", "ea_area_name", "flood_area_polygon",
        "_content_hash", "_bronze_run_id", "_silver_processed_at", "_source", "_state",
    ]

    incoming_sdf = spark.createDataFrame(
        floods_pdf[event_cols].rename(columns={
            "_time_raised":           "time_raised",
            "_time_severity_changed": "time_severity_changed",
            "_time_message_changed":  "time_message_changed",
            "flood_area_polygon":     "flood_area_polygon_url",
        })
    )
    incoming_sdf.createOrReplaceTempView("_incoming_flood_events")

    # Identify changed + new flood_area_ids
    changed_df = spark.sql(f"""
        SELECT incoming.flood_area_id
        FROM   _incoming_flood_events AS incoming
        LEFT JOIN {FQN_S_FLOOD_EVENTS} AS current
          ON  incoming.flood_area_id = current.flood_area_id
          AND current._is_current = TRUE
        WHERE current.flood_area_id IS NULL
           OR incoming._content_hash != current._content_hash
    """)
    changed_df.createOrReplaceTempView("_changed_flood_event_ids")
    changed_count = changed_df.count()
    log.info(f"Flood events: {changed_count} new or changed records")

    if changed_count > 0:
        # Expire existing current rows
        spark.sql(f"""
            MERGE INTO {FQN_S_FLOOD_EVENTS} AS target
            USING _changed_flood_event_ids AS src
            ON target.flood_area_id = src.flood_area_id
               AND target._is_current = TRUE
            WHEN MATCHED THEN UPDATE SET
                target._is_current   = FALSE,
                target._effective_to = CURRENT_TIMESTAMP()
        """)
        # Insert new rows
        spark.sql(f"""
            INSERT INTO {FQN_S_FLOOD_EVENTS}
            SELECT
                flood_area_id,
                flood_warning_uri,
                description,
                severity,
                severity_level,
                is_tidal,
                message,
                time_raised,
                time_severity_changed,
                time_message_changed,
                county,
                river_or_sea,
                ea_area_name,
                flood_area_polygon_url,
                _content_hash,
                CURRENT_TIMESTAMP() AS _effective_from,
                NULL                AS _effective_to,
                TRUE                AS _is_current,
                _bronze_run_id,
                _silver_processed_at,
                _source,
                _state
            FROM _incoming_flood_events
            WHERE flood_area_id IN (SELECT flood_area_id FROM _changed_flood_event_ids)
        """)

    _floods_written = changed_count
    log.info(f"Flood events: SCD Type 2 complete — {changed_count} rows written")
else:
    log.info("Flood events: no active warnings in this bronze run (empty feed is valid)")

# COMMAND ----------

# MAGIC %md ## 9. Transform: Flood Areas (MERGE Upsert)

# COMMAND ----------

_flood_areas_written = 0

if not flood_areas_pdf.empty:
    flood_areas_pdf["_is_current"]          = True
    flood_areas_pdf["_bronze_run_id"]       = flood_areas_pdf["_run_id"]
    flood_areas_pdf["_silver_processed_at"] = PROCESSED_AT
    flood_areas_pdf["_source"]              = SOURCE_NAME
    flood_areas_pdf["_state"]               = STATE

    area_cols = [
        "notation", "flood_area_uri", "area_description", "county",
        "ea_area_name", "river_or_sea", "polygon_url", "label",
        "_is_current", "_bronze_run_id", "_silver_processed_at", "_source", "_state",
    ]
    for c in area_cols:
        if c not in flood_areas_pdf.columns:
            flood_areas_pdf[c] = None

    area_sdf = spark.createDataFrame(flood_areas_pdf[area_cols])
    area_sdf.createOrReplaceTempView("_incoming_flood_areas")

    spark.sql(f"""
        MERGE INTO {FQN_S_FLOOD_AREAS} AS target
        USING _incoming_flood_areas AS source
        ON target.notation = source.notation
        WHEN MATCHED THEN UPDATE SET
            target.flood_area_uri       = source.flood_area_uri,
            target.area_description     = source.area_description,
            target.county               = source.county,
            target.ea_area_name         = source.ea_area_name,
            target.river_or_sea         = source.river_or_sea,
            target.polygon_url          = source.polygon_url,
            target.label                = source.label,
            target._is_current          = TRUE,
            target._bronze_run_id       = source._bronze_run_id,
            target._silver_processed_at = source._silver_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    _flood_areas_written = area_sdf.count()
    log.info(f"Flood areas: {_flood_areas_written} records merged into {FQN_S_FLOOD_AREAS}")
else:
    log.warning("Flood areas: no bronze records found — skipping")

# COMMAND ----------

# MAGIC %md ## 10. Transform: Readings (Append-only)

# COMMAND ----------

_readings_written = 0

if not readings_pdf.empty:
    # Parse datetime and derive date partition column
    readings_pdf["_reading_ts"] = readings_pdf["reading_datetime"].apply(_parse_timestamp)
    readings_pdf["reading_date"] = readings_pdf["_reading_ts"].apply(
        lambda ts: ts.date() if ts is not None else None
    )

    readings_pdf["_run_id_col"]   = readings_pdf["_run_id"]
    readings_pdf["_ingested_at"]  = PROCESSED_AT
    readings_pdf["_source"]       = SOURCE_NAME
    readings_pdf["_state"]        = STATE

    reading_cols = [
        "measure_uri", "station_id", "parameter", "period",
        "qualifier", "unit_name", "_reading_ts", "reading_date", "value",
        "_run_id_col", "_ingested_at", "_source", "_state",
    ]
    for c in reading_cols:
        if c not in readings_pdf.columns:
            readings_pdf[c] = None

    readings_sdf = spark.createDataFrame(
        readings_pdf[reading_cols].rename(columns={
            "_reading_ts": "reading_datetime",
            "_run_id_col": "_run_id",
        })
    )

    # Append-only — readings are immutable time-series data; no MERGE needed
    (
        readings_sdf.write
        .format("delta")
        .mode("append")
        .partitionBy("reading_date")
        .saveAsTable(FQN_S_READINGS)
    )
    _readings_written = readings_sdf.count()
    log.info(f"Readings: {_readings_written} rows appended to {FQN_S_READINGS}")
else:
    log.warning("Readings: no bronze records found — skipping")

# COMMAND ----------

# MAGIC %md ## 11. Audit Logging

# COMMAND ----------

def write_audit(
    run_id, stage, notebook_name, notebook_version,
    start_time, end_time, status,
    rows_read=0, rows_written=0, rows_merged=0, rows_rejected=0,
    error_message=None, extra_metadata=None,
):
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
            "run_id": run_id, "pipeline_stage": stage,
            "notebook_name": notebook_name, "notebook_version": notebook_version,
            "start_time": start_time, "end_time": end_time, "duration_seconds": duration,
            "status": status, "rows_read": rows_read, "rows_written": rows_written,
            "rows_merged": rows_merged, "rows_rejected": rows_rejected,
            "error_message": error_message,
            "extra_metadata": json.dumps(extra_metadata) if extra_metadata else None,
            "databricks_job_id": str(job_id) if job_id else None,
            "databricks_run_id": str(db_run_id) if db_run_id else None,
            "scrape_version": NOTEBOOK_VERSION,
        }]
        spark.createDataFrame(record).write.format("delta").mode("append").saveAsTable(FQN_AUDIT)
    except Exception as exc:
        log.error(f"write_audit failed (non-fatal): {exc}")

# COMMAND ----------

# MAGIC %md ## 12. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None

try:
    log.info("EA Monitoring Silver normalisation complete")
    log.info(f"  Stations    : {_stations_written} rows merged")
    log.info(f"  Flood Events: {_floods_written} rows written (SCD2)")
    log.info(f"  Flood Areas : {_flood_areas_written} rows merged")
    log.info(f"  Readings    : {_readings_written} rows appended")
    _pipeline_status = "success"

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Silver pipeline failed: {exc}", exc_info=True)
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "silver",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_read        = len(stations_pdf) + len(floods_pdf) + len(flood_areas_pdf) + len(readings_pdf),
        rows_written     = _stations_written + _floods_written + _flood_areas_written + _readings_written,
        rows_merged      = _stations_written + _flood_areas_written,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "stations_written":    _stations_written,
            "flood_events_written": _floods_written,
            "flood_areas_written": _flood_areas_written,
            "readings_appended":   _readings_written,
            "bronze_run_id":       _b_run_id,
            "full_refresh":        FULL_REFRESH,
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
