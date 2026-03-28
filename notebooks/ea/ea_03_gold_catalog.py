# Databricks notebook source
# EA Real-Time Flood Monitoring — Gold Catalog
#
# Purpose : Reads the EA silver tables and produces gold-layer tables:
#             - ea_monitoring_station_catalog     — denormalized station + latest reading + status
#             - ea_monitoring_flood_event_history — full SCD2 warning history + derived analytics
#           Also writes to the shared au_flood_report_index with _source = 'EA_MONITORING'
#           (EA monitoring data is vector/tabular — report index entries are used for
#           flood event metadata, not PDF documents).
#
# Layer   : Gold  (ceg_delta_gold_prnd.international_flood.*)
# Reads   : ceg_delta_silver_prnd.international_flood.ea_monitoring_stations
#           ceg_delta_silver_prnd.international_flood.ea_monitoring_flood_events
#           ceg_delta_silver_prnd.international_flood.ea_monitoring_flood_areas
#           ceg_delta_silver_prnd.international_flood.ea_monitoring_readings
# Writes  : ceg_delta_gold_prnd.international_flood.ea_monitoring_station_catalog
#           ceg_delta_gold_prnd.international_flood.ea_monitoring_flood_event_history
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
    "silver_run_id", "",
    "Silver Run ID — informational only, used in audit metadata."
)
dbutils.widgets.dropdown(
    "full_refresh", "false", ["true", "false"],
    "Full Refresh — if true, rebuilds gold tables from all current silver data."
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import json
import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType,
    LongType, StringType, StructField, StructType, TimestampType,
)

# ── Catalog / table paths ──────────────────────────────────────────────────
BRONZE_CATALOG = "ceg_delta_bronze_prnd"
SILVER_CATALOG = "ceg_delta_silver_prnd"
GOLD_CATALOG   = "ceg_delta_gold_prnd"
SCHEMA         = "international_flood"

FQN_AUDIT              = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
FQN_S_STATIONS         = f"{SILVER_CATALOG}.{SCHEMA}.ea_monitoring_stations"
FQN_S_FLOOD_EVENTS     = f"{SILVER_CATALOG}.{SCHEMA}.ea_monitoring_flood_events"
FQN_S_FLOOD_AREAS      = f"{SILVER_CATALOG}.{SCHEMA}.ea_monitoring_flood_areas"
FQN_S_READINGS         = f"{SILVER_CATALOG}.{SCHEMA}.ea_monitoring_readings"

FQN_G_STATION_CATALOG  = f"{GOLD_CATALOG}.{SCHEMA}.ea_monitoring_station_catalog"
FQN_G_FLOOD_HISTORY    = f"{GOLD_CATALOG}.{SCHEMA}.ea_monitoring_flood_event_history"

NOTEBOOK_NAME    = "ea_03_gold_catalog"
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

RUN_ID        = dbutils.widgets.get("run_id").strip()        or str(uuid.uuid4())
SILVER_RUN_ID = dbutils.widgets.get("silver_run_id").strip() or None
FULL_REFRESH  = dbutils.widgets.get("full_refresh").lower() == "true"
PROCESSED_AT  = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"EA Monitoring Gold Catalog  |  Run ID: {RUN_ID}")
log.info(f"Silver run ID : {SILVER_RUN_ID or 'latest'}")
log.info(f"Full refresh  : {FULL_REFRESH}")
log.info(f"Started at    : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{SCHEMA}")

# ── ea_monitoring_station_catalog ──────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_G_STATION_CATALOG} (
    station_id              STRING     NOT NULL  COMMENT 'EA station reference — natural key',
    station_uri             STRING               COMMENT 'Full EA Linked Data URI',
    station_name            STRING               COMMENT 'Human-readable station label',
    rloi_id                 STRING               COMMENT 'River Levels on the Internet ID',
    river_name              STRING               COMMENT 'River name',
    catchment_name          STRING               COMMENT 'Catchment / river basin name',
    town                    STRING               COMMENT 'Associated settlement',
    lat                     DOUBLE               COMMENT 'WGS84 latitude',
    lon                     DOUBLE               COMMENT 'WGS84 longitude',
    easting                 INT                  COMMENT 'BNG easting (EPSG:27700)',
    northing                INT                  COMMENT 'BNG northing (EPSG:27700)',
    station_status          STRING               COMMENT 'Active | Closed | Suspended',
    ea_area_name            STRING               COMMENT 'EA operational area',
    date_opened             DATE                 COMMENT 'Date station opened',
    latest_reading_datetime TIMESTAMP            COMMENT 'Timestamp of most recent reading',
    latest_reading_value    DOUBLE               COMMENT 'Most recent observed value',
    latest_reading_unit     STRING               COMMENT 'Unit of the most recent reading',
    latest_reading_param    STRING               COMMENT 'Parameter type of the most recent reading',
    has_active_warning      BOOLEAN              COMMENT 'TRUE if station river/area has an active flood warning or alert',
    active_warning_severity STRING               COMMENT 'Severity label of the active warning (if any)',
    _gold_processed_at      TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Gold: denormalized EA monitoring station catalog with latest reading and active warning status. Full overwrite each run.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# ── ea_monitoring_flood_event_history ──────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_G_FLOOD_HISTORY} (
    flood_area_id           STRING     NOT NULL  COMMENT 'Flood area notation — natural key',
    flood_warning_uri       STRING               COMMENT 'EA Linked Data URI',
    description             STRING               COMMENT 'Plain-text description of the affected area',
    severity                STRING               COMMENT 'Severity label',
    severity_level          INT                  COMMENT '1=Severe, 2=Warning, 3=Alert, 4=No longer in force',
    is_tidal                BOOLEAN,
    message                 STRING               COMMENT 'Advisory message text',
    time_raised             TIMESTAMP            COMMENT 'When the warning was originally raised',
    time_severity_changed   TIMESTAMP            COMMENT 'Most recent severity change',
    county                  STRING,
    river_or_sea            STRING,
    ea_area_name            STRING,
    flood_area_polygon_url  STRING               COMMENT 'URL to fetch GeoJSON polygon for the area',
    -- Derived analytics columns
    is_current_warning      BOOLEAN              COMMENT 'TRUE if this is a currently active warning',
    warning_age_hours       DOUBLE               COMMENT 'Hours since warning was raised (for current warnings)',
    severity_category       STRING               COMMENT 'Simplified category: severe | warning | alert | inactive',
    -- Pipeline provenance
    _effective_from         TIMESTAMP            COMMENT 'SCD2 effective from timestamp',
    _effective_to           TIMESTAMP            COMMENT 'SCD2 effective to timestamp (NULL = current)',
    _is_current_version     BOOLEAN              COMMENT 'TRUE for the current SCD2 version',
    _gold_processed_at      TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Gold: full history of EA flood warning events, including all SCD2 severity transitions and derived analytics columns. Full overwrite each run.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# COMMAND ----------

# MAGIC %md ## 5. Build Station Catalog

# COMMAND ----------

# Join stations with their latest reading (if any)
station_catalog_df = spark.sql(f"""
    WITH latest_readings AS (
        SELECT
            station_id,
            reading_datetime,
            value                AS latest_reading_value,
            unit_name            AS latest_reading_unit,
            parameter            AS latest_reading_param,
            ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY reading_datetime DESC) AS rn
        FROM {FQN_S_READINGS}
    ),
    active_warnings AS (
        SELECT
            -- Match warnings to stations via river_name or ea_area_name
            -- This is an approximation; exact area-station mapping requires polygon join
            ea_area_name,
            severity             AS active_warning_severity,
            severity_level,
            ROW_NUMBER() OVER (PARTITION BY ea_area_name ORDER BY severity_level ASC) AS rn
        FROM {FQN_S_FLOOD_EVENTS}
        WHERE _is_current = TRUE
          AND severity_level <= 3   -- Levels 1-3 = active warnings/alerts
    )
    SELECT
        s.station_id,
        s.station_uri,
        s.station_name,
        s.rloi_id,
        s.river_name,
        s.catchment_name,
        s.town,
        s.lat,
        s.lon,
        s.easting,
        s.northing,
        s.station_status,
        s.ea_area_name,
        CAST(s.date_opened AS DATE)        AS date_opened,
        r.reading_datetime                 AS latest_reading_datetime,
        r.latest_reading_value,
        r.latest_reading_unit,
        r.latest_reading_param,
        CASE WHEN aw.ea_area_name IS NOT NULL THEN TRUE ELSE FALSE END AS has_active_warning,
        aw.active_warning_severity,
        CURRENT_TIMESTAMP()                AS _gold_processed_at,
        s._source,
        s._state
    FROM {FQN_S_STATIONS} AS s
    LEFT JOIN latest_readings AS r ON s.station_id = r.station_id AND r.rn = 1
    LEFT JOIN active_warnings AS aw ON s.ea_area_name = aw.ea_area_name AND aw.rn = 1
    WHERE s._is_current = TRUE
""")

_station_count = station_catalog_df.count()
log.info(f"Station catalog: {_station_count} rows assembled")

# Full overwrite — always the current state snapshot
station_catalog_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FQN_G_STATION_CATALOG)
log.info(f"Station catalog: written to {FQN_G_STATION_CATALOG}")

spark.sql(f"OPTIMIZE {FQN_G_STATION_CATALOG} ZORDER BY (ea_area_name, station_status)")

# COMMAND ----------

# MAGIC %md ## 6. Build Flood Event History

# COMMAND ----------

flood_history_df = spark.sql(f"""
    SELECT
        e.flood_area_id,
        e.flood_warning_uri,
        e.description,
        e.severity,
        e.severity_level,
        e.is_tidal,
        e.message,
        e.time_raised,
        e.time_severity_changed,
        e.county,
        e.river_or_sea,
        e.ea_area_name,
        e.flood_area_polygon_url,
        -- Derived analytics
        e._is_current                                        AS is_current_warning,
        CASE
            WHEN e._is_current = TRUE
            THEN ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(e.time_raised)) / 3600.0, 2)
            ELSE NULL
        END                                                  AS warning_age_hours,
        CASE
            WHEN e.severity_level = 1 THEN 'severe'
            WHEN e.severity_level = 2 THEN 'warning'
            WHEN e.severity_level = 3 THEN 'alert'
            ELSE 'inactive'
        END                                                  AS severity_category,
        e._effective_from,
        e._effective_to,
        e._is_current                                        AS _is_current_version,
        CURRENT_TIMESTAMP()                                  AS _gold_processed_at,
        e._source,
        e._state
    FROM {FQN_S_FLOOD_EVENTS} AS e
""")

_flood_count = flood_history_df.count()
log.info(f"Flood event history: {_flood_count} rows assembled (all SCD2 versions)")

flood_history_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FQN_G_FLOOD_HISTORY)
log.info(f"Flood event history: written to {FQN_G_FLOOD_HISTORY}")

spark.sql(f"OPTIMIZE {FQN_G_FLOOD_HISTORY} ZORDER BY (severity_level, county, _is_current_version)")

# COMMAND ----------

# MAGIC %md ## 7. Audit Logging

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

# MAGIC %md ## 8. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None

try:
    log.info("EA Monitoring Gold Catalog pipeline complete")
    log.info(f"  ea_monitoring_station_catalog      : {_station_count} rows")
    log.info(f"  ea_monitoring_flood_event_history  : {_flood_count} rows")
    _pipeline_status = "success"

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Gold pipeline failed: {exc}", exc_info=True)
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "gold",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_written     = _station_count + _flood_count,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "station_catalog_rows":  _station_count,
            "flood_history_rows":    _flood_count,
            "silver_run_id":         SILVER_RUN_ID,
            "full_refresh":          FULL_REFRESH,
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
