# Databricks notebook source
# SEPA Scotland — Silver Normalisation
#
# Purpose : Reads the latest SEPA bronze scrape records and normalises them into
#           silver Delta tables:
#             - sepa_monitoring_stations   (MERGE upsert on station_no)
#             - sepa_monitoring_readings   (append-only, partitioned by reading_date)
#             - sepa_monitoring_warnings   (SCD Type 2 on warning_guid — tracks warning lifecycle)
#
# SCD Type 2 rationale for warnings: SEPA flood warnings can escalate (Alert →
# Warning → Severe Warning) or de-escalate. Tracking these transitions is valuable
# for post-event analysis — analogous to EA's flood event severity history.
#
# Layer   : Silver  (ceg_delta_silver_prnd.international_flood.sepa_monitoring_*)
# Reads   : ceg_delta_bronze_prnd.international_flood.bronze_sepa_stations
#           ceg_delta_bronze_prnd.international_flood.bronze_sepa_readings
#           ceg_delta_bronze_prnd.international_flood.bronze_sepa_warnings
# Writes  : ceg_delta_silver_prnd.international_flood.sepa_monitoring_stations
#           ceg_delta_silver_prnd.international_flood.sepa_monitoring_readings
#           ceg_delta_silver_prnd.international_flood.sepa_monitoring_warnings
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

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType,
    StringType, StructField, StructType, TimestampType,
)

# ── Catalog / table paths ──────────────────────────────────────────────────
BRONZE_CATALOG = "ceg_delta_bronze_prnd"
SILVER_CATALOG = "ceg_delta_silver_prnd"
SCHEMA         = "international_flood"

FQN_AUDIT         = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
FQN_B_STATIONS    = f"{BRONZE_CATALOG}.{SCHEMA}.bronze_sepa_stations"
FQN_B_READINGS    = f"{BRONZE_CATALOG}.{SCHEMA}.bronze_sepa_readings"
FQN_B_WARNINGS    = f"{BRONZE_CATALOG}.{SCHEMA}.bronze_sepa_warnings"

FQN_S_STATIONS    = f"{SILVER_CATALOG}.{SCHEMA}.sepa_monitoring_stations"
FQN_S_READINGS    = f"{SILVER_CATALOG}.{SCHEMA}.sepa_monitoring_readings"
FQN_S_WARNINGS    = f"{SILVER_CATALOG}.{SCHEMA}.sepa_monitoring_warnings"

NOTEBOOK_NAME    = "sepa_02_silver_normalize"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "SEPA_SCOTLAND"
STATE            = "Scotland"

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

RUN_ID        = dbutils.widgets.get("run_id").strip()        or str(uuid.uuid4())
BRONZE_RUN_ID = dbutils.widgets.get("bronze_run_id").strip() or None
FULL_REFRESH  = dbutils.widgets.get("full_refresh").lower() == "true"
PROCESSED_AT  = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"SEPA Silver Normalisation  |  Run ID: {RUN_ID}")
log.info(f"Bronze run ID : {BRONZE_RUN_ID or 'latest'}")
log.info(f"Full refresh  : {FULL_REFRESH}")
log.info(f"Started at    : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.{SCHEMA}")

# ── sepa_monitoring_stations ───────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_S_STATIONS} (
    station_no              STRING     NOT NULL  COMMENT 'SEPA KiWIS station number — natural key.',
    station_name            STRING               COMMENT 'Human-readable station label.',
    river_name              STRING               COMMENT 'River name.',
    catchment_name          STRING               COMMENT 'Catchment / river basin name.',
    lat                     DOUBLE               COMMENT 'WGS84 latitude (parsed from KiWIS station_latitude).',
    lon                     DOUBLE               COMMENT 'WGS84 longitude (parsed from KiWIS station_longitude).',
    easting                 INT                  COMMENT 'BNG easting (EPSG:27700) — NULL (not provided by KiWIS; use lat/lon instead).',
    northing                INT                  COMMENT 'BNG northing (EPSG:27700) — NULL (not provided by KiWIS; use lat/lon instead).',
    _is_current             BOOLEAN              COMMENT 'TRUE for the active record.',
    _bronze_run_id          STRING               COMMENT '_run_id of the source bronze row.',
    _silver_processed_at    TIMESTAMP            COMMENT 'UTC timestamp of silver write.',
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Silver: SEPA monitoring stations — MERGE upsert on station_no. KiWIS provides WGS84 coordinates directly; no BNG conversion needed.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'station_no'
)
""")

# ── sepa_monitoring_readings ───────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_S_READINGS} (
    ts_id               STRING     COMMENT 'KiWIS time series ID.',
    station_no          STRING     COMMENT 'Parent station number — FK to sepa_monitoring_stations.station_no.',
    ts_name             STRING     COMMENT 'Time series name (e.g. 15minute, Day.Mean).',
    parametertype_name  STRING     COMMENT 'Parameter type: S=Stage (level), Q=Flow.',
    unit_name           STRING     COMMENT 'Unit of measurement (e.g. m, m3/s).',
    reading_datetime    TIMESTAMP  COMMENT 'UTC timestamp of the reading (parsed from KiWIS ISO string).',
    reading_date        DATE       COMMENT 'Date partition column (derived from reading_datetime).',
    value               DOUBLE     COMMENT 'Observed value as DOUBLE. NULL for missing readings.',
    _run_id             STRING,
    _ingested_at        TIMESTAMP,
    _source             STRING,
    _state              STRING
)
USING DELTA
PARTITIONED BY (reading_date)
COMMENT 'Silver: SEPA 15-minute river level and flow readings — append-only, partitioned by reading_date. KiWIS 15-min cadence produces ~96 readings/day per active time series.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'station_no,reading_datetime'
)
""")

# ── sepa_monitoring_warnings (SCD Type 2) ─────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_S_WARNINGS} (
    warning_guid            STRING     NOT NULL  COMMENT 'RSS item guid — natural key.',
    warning_title           STRING               COMMENT 'RSS item title (e.g. "Flood Warning — River Tay at Perth").',
    warning_link            STRING               COMMENT 'URL to full SEPA warning details.',
    warning_description     STRING               COMMENT 'Full description text from RSS item.',
    pub_date                TIMESTAMP            COMMENT 'Parsed publication timestamp of the RSS item (UTC).',
    severity_label          STRING               COMMENT 'Inferred severity: Severe Flood Warning | Flood Warning | Flood Alert | Unknown. Parsed from warning_title.',
    severity_level          INT                  COMMENT '1=Severe, 2=Warning, 3=Alert, 4=Unknown/Removed. Derived from severity_label.',
    _content_hash           STRING               COMMENT 'MD5 of key mutable fields — used for SCD2 change detection.',
    _effective_from         TIMESTAMP  NOT NULL  COMMENT 'UTC when this version became current.',
    _effective_to           TIMESTAMP            COMMENT 'UTC when superseded. NULL = current record.',
    _is_current             BOOLEAN    NOT NULL  COMMENT 'TRUE for the current version.',
    _bronze_run_id          STRING,
    _silver_processed_at    TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Silver: SEPA RSS flood warnings — SCD Type 2 to track severity transitions. Empty during non-flood periods (confirmed SEPA behaviour).'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'warning_guid,_is_current'
)
""")

# COMMAND ----------

# MAGIC %md ## 5. Bronze Resolution Helper

# COMMAND ----------

def _resolve_bronze_run_id(fqn: str, supplied: Optional[str]) -> Optional[str]:
    """
    Resolves which bronze run_id to process.
    - FULL_REFRESH=true → None (process all _is_current records)
    - supplied non-empty → use as-is
    - blank → pick the latest _run_id from the bronze table
    """
    if FULL_REFRESH:
        return None
    if supplied:
        return supplied
    try:
        result = spark.sql(
            f"SELECT _run_id FROM {fqn} WHERE _is_current = TRUE ORDER BY _ingested_at DESC LIMIT 1"
        ).collect()
        return result[0]["_run_id"] if result else None
    except Exception as exc:
        log.warning(f"Could not resolve bronze run_id from {fqn}: {exc}")
        return None

# COMMAND ----------

# MAGIC %md ## 6. Severity Parsing (RSS Warnings)

# COMMAND ----------

def _parse_severity(title: Optional[str]):
    """
    Infers warning severity from the RSS item title string.

    SEPA severity levels (same 3-tier system as EA):
        Severe Flood Warning  → level 1
        Flood Warning         → level 2
        Flood Alert           → level 3
        Unknown/other         → level 4

    Returns (severity_label: str, severity_level: int)
    """
    if not title:
        return ("Unknown", 4)
    title_lower = title.lower()
    if "severe" in title_lower:
        return ("Severe Flood Warning", 1)
    elif "flood warning" in title_lower:
        return ("Flood Warning", 2)
    elif "flood alert" in title_lower or "alert" in title_lower:
        return ("Flood Alert", 3)
    else:
        return ("Unknown", 4)


def _content_hash(*fields) -> str:
    """Returns MD5 hex digest of concatenated field strings for SCD2 change detection."""
    combined = "|".join(str(f) if f is not None else "" for f in fields)
    return hashlib.md5(combined.encode("utf-8")).hexdigest()


def _safe_double(val) -> Optional[float]:
    """Safely converts a string value to float; returns None on failure."""
    if val is None:
        return None
    try:
        f = float(val)
        # KiWIS sometimes returns sentinel values for missing data; treat as None
        if f < -9990.0:
            return None
        return f
    except (ValueError, TypeError):
        return None


def _safe_timestamp(ts_str: Optional[str]) -> Optional[datetime]:
    """
    Parses ISO 8601 timestamp strings from KiWIS into UTC-aware datetimes.
    KiWIS returns formats like "2026-03-28T00:00:00.000Z" or "2026-03-28 00:00:00".
    Returns None on parse failure.
    """
    if not ts_str:
        return None
    ts_str = ts_str.strip()
    # Normalise: replace space with T, strip milliseconds suffix variations
    ts_str = ts_str.replace(" ", "T")
    # Try standard ISO formats
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT"):
        try:
            dt = datetime.strptime(ts_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    log.debug(f"_safe_timestamp: could not parse '{ts_str}'")
    return None

# COMMAND ----------

# MAGIC %md ## 7. Silver — Stations MERGE

# COMMAND ----------

def normalize_stations(bronze_run_id: Optional[str]) -> int:
    """
    Reads bronze station records and performs MERGE upsert into sepa_monitoring_stations.

    MERGE key: station_no
    All fields from the latest bronze record overwrite existing silver values.
    Returns rows_merged count.
    """
    # Load bronze records
    if FULL_REFRESH or not bronze_run_id:
        bronze_df = spark.sql(f"SELECT * FROM {FQN_B_STATIONS} WHERE _is_current = TRUE")
    else:
        bronze_df = spark.sql(f"""
            SELECT * FROM {FQN_B_STATIONS}
            WHERE _run_id = '{bronze_run_id}' AND _is_current = TRUE
        """)

    count = bronze_df.count()
    log.info(f"normalize_stations: {count} bronze station rows to process")
    if count == 0:
        log.warning("normalize_stations: no bronze records — skipping station MERGE")
        return 0

    # Normalise: parse lat/lon as DOUBLE, null-out BNG (not available from KiWIS)
    from pyspark.sql.functions import col as _col, lit, to_timestamp, current_timestamp
    norm_df = (
        bronze_df
        .filter(_col("station_no").isNotNull() & (_col("station_no") != ""))
        .select(
            _col("station_no").cast("string").alias("station_no"),
            _col("station_name").cast("string").alias("station_name"),
            _col("river_name").cast("string").alias("river_name"),
            _col("catchment_name").cast("string").alias("catchment_name"),
            _col("station_latitude").cast("double").alias("lat"),
            _col("station_longitude").cast("double").alias("lon"),
            lit(None).cast("int").alias("easting"),    # not available from KiWIS
            lit(None).cast("int").alias("northing"),   # not available from KiWIS
            lit(True).alias("_is_current"),
            _col("_run_id").alias("_bronze_run_id"),
            current_timestamp().alias("_silver_processed_at"),
            lit(SOURCE_NAME).alias("_source"),
            lit(STATE).alias("_state"),
        )
        .dropDuplicates(["station_no"])
    )

    # Register as temp view for MERGE SQL
    norm_df.createOrReplaceTempView("_sepa_stations_upsert")

    spark.sql(f"""
        MERGE INTO {FQN_S_STATIONS} AS target
        USING _sepa_stations_upsert AS source
        ON target.station_no = source.station_no
        WHEN MATCHED THEN UPDATE SET
            station_name         = source.station_name,
            river_name           = source.river_name,
            catchment_name       = source.catchment_name,
            lat                  = source.lat,
            lon                  = source.lon,
            easting              = source.easting,
            northing             = source.northing,
            _is_current          = source._is_current,
            _bronze_run_id       = source._bronze_run_id,
            _silver_processed_at = source._silver_processed_at,
            _source              = source._source,
            _state               = source._state
        WHEN NOT MATCHED THEN INSERT *
    """)

    spark.catalog.dropTempView("_sepa_stations_upsert")
    log.info(f"normalize_stations: MERGE complete — {count} source records processed")
    return count

# COMMAND ----------

# MAGIC %md ## 8. Silver — Readings (Append-Only)

# COMMAND ----------

def normalize_readings(bronze_run_id: Optional[str]) -> int:
    """
    Reads bronze reading records, parses timestamps and values, and appends
    to sepa_monitoring_readings (partitioned by reading_date).

    Readings are append-only — never MERGE'd. Historical readings from previous
    runs are preserved. Each row is uniquely identified by (ts_id, reading_datetime).

    Returns rows_written count.
    """
    if FULL_REFRESH or not bronze_run_id:
        bronze_df = spark.sql(f"SELECT * FROM {FQN_B_READINGS} WHERE _is_current = TRUE")
    else:
        bronze_df = spark.sql(f"""
            SELECT * FROM {FQN_B_READINGS}
            WHERE _run_id = '{bronze_run_id}' AND _is_current = TRUE
        """)

    count = bronze_df.count()
    log.info(f"normalize_readings: {count} bronze reading rows to process")
    if count == 0:
        log.warning("normalize_readings: no bronze reading records — skipping")
        return 0

    from pyspark.sql.functions import col as _col, lit, to_timestamp, to_date, current_timestamp

    norm_df = (
        bronze_df
        .filter(_col("reading_timestamp").isNotNull())
        .select(
            _col("ts_id").cast("string").alias("ts_id"),
            _col("station_no").cast("string").alias("station_no"),
            _col("ts_name").cast("string").alias("ts_name"),
            _col("parametertype_name").cast("string").alias("parametertype_name"),
            _col("unit_name").cast("string").alias("unit_name"),
            # Parse ISO timestamp → TIMESTAMP (KiWIS uses "2026-03-28T00:00:00.000Z" format)
            to_timestamp(
                F.regexp_replace(_col("reading_timestamp"), r"\.\d+Z$", ""),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).alias("reading_datetime"),
            # Derive date partition from timestamp string (faster than parsing full timestamp)
            to_date(F.substring(_col("reading_timestamp"), 1, 10), "yyyy-MM-dd").alias("reading_date"),
            # Cast value to DOUBLE; treat sentinel values (< -9990) as NULL
            F.when(
                _col("value").isNotNull() & (_col("value").cast("double") > -9990.0),
                _col("value").cast("double")
            ).otherwise(F.lit(None).cast("double")).alias("value"),
            _col("_run_id").alias("_run_id"),
            _col("_ingested_at").alias("_ingested_at"),
            lit(SOURCE_NAME).alias("_source"),
            lit(STATE).alias("_state"),
        )
        .filter(_col("reading_datetime").isNotNull())
    )

    # Deduplicate within this batch (KiWIS may return overlapping windows on re-runs)
    norm_df = norm_df.dropDuplicates(["ts_id", "reading_datetime"])

    rows_count = norm_df.count()
    log.info(f"normalize_readings: {rows_count} deduplicated rows to write")

    (
        norm_df.write
        .format("delta")
        .mode("append")
        .partitionBy("reading_date")
        .option("mergeSchema", "false")
        .saveAsTable(FQN_S_READINGS)
    )
    log.info(f"normalize_readings: {rows_count} rows appended to {FQN_S_READINGS}")
    return rows_count

# COMMAND ----------

# MAGIC %md ## 9. Silver — Warnings (SCD Type 2)

# COMMAND ----------

def normalize_warnings(bronze_run_id: Optional[str]) -> int:
    """
    Reads bronze warning records and applies SCD Type 2 logic.

    SCD Type 2 strategy:
    1. For each incoming warning (keyed on warning_guid), compute content_hash
       from key mutable fields (title, description, pub_date).
    2. Look up the current active record in silver (where _is_current = TRUE).
    3. If the hash matches → no change needed.
    4. If hash differs (title changed = severity escalation) → close the old version
       (_effective_to = now, _is_current = FALSE) and insert a new current version.
    5. New warnings not in silver → INSERT as current version.

    Returns rows_written (new + updated rows count).
    """
    if FULL_REFRESH or not bronze_run_id:
        bronze_df = spark.sql(f"SELECT * FROM {FQN_B_WARNINGS} WHERE _is_current = TRUE")
    else:
        bronze_df = spark.sql(f"""
            SELECT * FROM {FQN_B_WARNINGS}
            WHERE _run_id = '{bronze_run_id}' AND _is_current = TRUE
        """)

    count = bronze_df.count()
    log.info(f"normalize_warnings: {count} bronze warning rows to process")
    if count == 0:
        log.info("normalize_warnings: no active warnings in this run (normal during non-flood periods)")
        return 0

    # Collect bronze warnings and parse locally (small volume — at most a few dozen)
    incoming = bronze_df.collect()
    rows_written = 0

    for row in incoming:
        guid        = row["warning_guid"]
        title       = row["warning_title"]
        link        = row["warning_link"]
        description = row["warning_description"]
        pub_date_raw = row["pub_date"]
        bronze_run  = row["_run_id"]

        severity_label, severity_level = _parse_severity(title)
        pub_date_ts = _safe_timestamp(pub_date_raw)
        content_hash = _content_hash(guid, title, description, pub_date_raw)

        # Check existing current record
        existing = spark.sql(f"""
            SELECT _content_hash, _effective_from
            FROM   {FQN_S_WARNINGS}
            WHERE  warning_guid = '{guid}' AND _is_current = TRUE
            LIMIT 1
        """).collect()

        if existing and existing[0]["_content_hash"] == content_hash:
            # No change — skip
            continue

        if existing:
            # Close old version
            spark.sql(f"""
                UPDATE {FQN_S_WARNINGS}
                SET    _effective_to = CURRENT_TIMESTAMP(),
                       _is_current   = FALSE
                WHERE  warning_guid = '{guid}' AND _is_current = TRUE
            """)

        # Insert new version
        new_row_df = spark.createDataFrame([{
            "warning_guid":         guid,
            "warning_title":        title,
            "warning_link":         link,
            "warning_description":  description,
            "pub_date":             pub_date_ts,
            "severity_label":       severity_label,
            "severity_level":       severity_level,
            "_content_hash":        content_hash,
            "_effective_from":      PROCESSED_AT,
            "_effective_to":        None,
            "_is_current":          True,
            "_bronze_run_id":       bronze_run,
            "_silver_processed_at": PROCESSED_AT,
            "_source":              SOURCE_NAME,
            "_state":               STATE,
        }])
        new_row_df.write.format("delta").mode("append").saveAsTable(FQN_S_WARNINGS)
        rows_written += 1

    log.info(f"normalize_warnings: {rows_written} SCD2 insertions for {count} incoming warnings")
    return rows_written

# COMMAND ----------

# MAGIC %md ## 10. Audit Logging

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
        log.info(f"Audit written: stage={stage} status={status}")
    except Exception as audit_exc:
        log.error(f"write_audit failed (non-fatal): {audit_exc}")

# COMMAND ----------

# MAGIC %md ## 11. Execute

# COMMAND ----------

_pipeline_start     = datetime.now(timezone.utc)
_pipeline_status    = "failed"
_pipeline_error     = None
_stations_merged    = 0
_readings_written   = 0
_warnings_written   = 0

try:
    log.info(f"SEPA Silver Normalisation | run_id={RUN_ID}")

    # Resolve bronze run_id from the task value chain (if not supplied via widget)
    effective_bronze_run_id = BRONZE_RUN_ID
    if not effective_bronze_run_id and not FULL_REFRESH:
        try:
            effective_bronze_run_id = dbutils.jobs.taskValues.get(
                taskKey="sepa_01_bronze_scrape", key="run_id", debugValue=""
            )
        except Exception:
            pass
        effective_bronze_run_id = effective_bronze_run_id or _resolve_bronze_run_id(FQN_B_STATIONS, None)

    log.info(f"Using bronze_run_id: {effective_bronze_run_id or 'all current records (full refresh)'}")

    # ── Stations MERGE ─────────────────────────────────────────────────────
    _stations_merged = normalize_stations(effective_bronze_run_id)

    # ── Readings append ────────────────────────────────────────────────────
    _readings_written = normalize_readings(effective_bronze_run_id)

    # ── Warnings SCD2 ─────────────────────────────────────────────────────
    _warnings_written = normalize_warnings(effective_bronze_run_id)

    _pipeline_status = "success"
    log.info(
        f"SEPA silver normalisation complete | "
        f"stations_merged={_stations_merged} | "
        f"readings_appended={_readings_written} | "
        f"warnings_scd2={_warnings_written}"
    )

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"SEPA silver normalisation failed: {exc}", exc_info=True)
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
        rows_written     = _stations_merged + _readings_written + _warnings_written,
        rows_merged      = _stations_merged,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "stations_merged":    _stations_merged,
            "readings_appended":  _readings_written,
            "warnings_scd2":      _warnings_written,
            "bronze_run_id":      effective_bronze_run_id if 'effective_bronze_run_id' in dir() else None,
            "full_refresh":       FULL_REFRESH,
        },
    )

# Publish run_id for downstream tasks
try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
