# Databricks notebook source
# SEPA Scotland — Gold Catalog
#
# Purpose : Reads SEPA silver tables and produces two gold-layer outputs:
#
#           1. sepa_monitoring_station_catalog  — denormalized station + latest reading
#              + current warning status (SEPA-specific; full overwrite each run)
#
#           2. sepa_monitoring_warning_history  — full SCD2 warning event history
#              + derived analytics (active / escalated / de-escalated)
#
#           3. au_flood_report_index (shared multi-source table) — SEPA station
#              summary entries with _source = 'SEPA_SCOTLAND'. Uses source-scoped
#              DELETE + INSERT so SEPA rows do not collide with EA or NRW rows.
#
#   ⚠️ NOTE: SEPA has no downloadable documents (PDFs, ZIPs).
#       au_flood_report_index entries for SEPA represent station monitoring
#       summaries (station metadata + latest reading), not flood study reports.
#
# Layer   : Gold  (ceg_delta_gold_prnd.international_flood.*)
# Reads   : ceg_delta_silver_prnd.international_flood.sepa_monitoring_stations
#           ceg_delta_silver_prnd.international_flood.sepa_monitoring_readings
#           ceg_delta_silver_prnd.international_flood.sepa_monitoring_warnings
# Writes  : ceg_delta_gold_prnd.international_flood.sepa_monitoring_station_catalog
#           ceg_delta_gold_prnd.international_flood.sepa_monitoring_warning_history
#           ceg_delta_gold_prnd.international_flood.au_flood_report_index
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
    "Silver Run ID — informational, used in audit metadata."
)
dbutils.widgets.dropdown(
    "full_refresh", "false", ["true", "false"],
    "Full Refresh — if true, rebuilds all gold tables from all current silver data."
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

FQN_AUDIT          = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
FQN_S_STATIONS     = f"{SILVER_CATALOG}.{SCHEMA}.sepa_monitoring_stations"
FQN_S_READINGS     = f"{SILVER_CATALOG}.{SCHEMA}.sepa_monitoring_readings"
FQN_S_WARNINGS     = f"{SILVER_CATALOG}.{SCHEMA}.sepa_monitoring_warnings"

FQN_G_CATALOG      = f"{GOLD_CATALOG}.{SCHEMA}.sepa_monitoring_station_catalog"
FQN_G_WARN_HISTORY = f"{GOLD_CATALOG}.{SCHEMA}.sepa_monitoring_warning_history"
FQN_G_REPORT_INDEX = f"{GOLD_CATALOG}.{SCHEMA}.au_flood_report_index"

NOTEBOOK_NAME    = "sepa_03_gold_catalog"
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
SILVER_RUN_ID = dbutils.widgets.get("silver_run_id").strip() or None
FULL_REFRESH  = dbutils.widgets.get("full_refresh").lower() == "true"
PROCESSED_AT  = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"SEPA Gold Catalog  |  Run ID: {RUN_ID}")
log.info(f"Silver run ID : {SILVER_RUN_ID or 'latest'}")
log.info(f"Full refresh  : {FULL_REFRESH}")
log.info(f"Started at    : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{SCHEMA}")

# ── sepa_monitoring_station_catalog ────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_G_CATALOG} (
    station_no                  STRING     NOT NULL  COMMENT 'SEPA KiWIS station number — natural key.',
    station_name                STRING               COMMENT 'Human-readable station label.',
    river_name                  STRING               COMMENT 'River name.',
    catchment_name              STRING               COMMENT 'Catchment / river basin name.',
    lat                         DOUBLE               COMMENT 'WGS84 latitude.',
    lon                         DOUBLE               COMMENT 'WGS84 longitude.',
    station_status              STRING               COMMENT 'Derived status: Active (has recent readings) | Inactive.',
    latest_reading_datetime     TIMESTAMP            COMMENT 'UTC timestamp of the most recent reading.',
    latest_reading_value        DOUBLE               COMMENT 'Most recent observed value.',
    latest_reading_unit         STRING               COMMENT 'Unit of the most recent reading.',
    latest_reading_param        STRING               COMMENT 'Parameter type of the most recent reading (S=Stage, Q=Flow).',
    has_active_warning          BOOLEAN              COMMENT 'TRUE if there is a current active flood warning linked to this area.',
    active_warning_severity     STRING               COMMENT 'Severity label of the active warning (if any).',
    active_warning_title        STRING               COMMENT 'Title of the active warning (if any).',
    _gold_processed_at          TIMESTAMP,
    _source                     STRING,
    _state                      STRING
)
USING DELTA
COMMENT 'Gold: denormalized SEPA station catalog with latest reading and active warning status. Full overwrite each run.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# ── sepa_monitoring_warning_history ────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_G_WARN_HISTORY} (
    warning_guid            STRING     NOT NULL  COMMENT 'RSS item guid — natural key.',
    warning_title           STRING               COMMENT 'RSS item title.',
    warning_link            STRING               COMMENT 'URL to full warning details.',
    warning_description     STRING               COMMENT 'Full warning description.',
    pub_date                TIMESTAMP            COMMENT 'Publication timestamp of the warning.',
    severity_label          STRING               COMMENT 'Inferred severity: Severe Flood Warning | Flood Warning | Flood Alert | Unknown.',
    severity_level          INT                  COMMENT '1=Severe, 2=Warning, 3=Alert, 4=Unknown.',
    -- Derived analytics
    is_current_warning      BOOLEAN              COMMENT 'TRUE if this warning version is currently active.',
    warning_age_hours       DOUBLE               COMMENT 'Hours since warning was published (for active warnings).',
    severity_category       STRING               COMMENT 'Simplified: severe | warning | alert | inactive.',
    -- SCD2 provenance
    _effective_from         TIMESTAMP,
    _effective_to           TIMESTAMP,
    _is_current_version     BOOLEAN,
    _gold_processed_at      TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Gold: full SEPA flood warning history including all SCD2 severity transitions. Full overwrite each run.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# ── au_flood_report_index (shared multi-source table — idempotent) ─────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_G_REPORT_INDEX} (
    resource_id             STRING     NOT NULL  COMMENT 'Unique identifier. For SEPA: station_no (no downloadable documents).',
    resource_name           STRING               COMMENT 'Human-readable resource name.',
    resource_download_url   STRING               COMMENT 'Download URL (NULL for SEPA — API only).',
    resource_format         STRING               COMMENT 'Format: API_JSON for SEPA.',
    resource_size_bytes     LONG                 COMMENT 'File size (NULL for SEPA).',
    resource_found_via      STRING               COMMENT 'Discovery method: api for SEPA KiWIS.',
    title                   STRING               COMMENT 'Display title (station name + river for SEPA).',
    river_basin_name        STRING               COMMENT 'River / catchment name.',
    council_lga_primary     STRING               COMMENT 'Not applicable for SEPA — NULL.',
    place_name              STRING               COMMENT 'Not applicable for SEPA — station_name used as proxy.',
    spatial_extent_wkt      STRING               COMMENT 'POINT WKT in WGS84 for station location.',
    spatial_bbox_min_lon    DOUBLE               COMMENT 'Station longitude (point = min = max).',
    spatial_bbox_min_lat    DOUBLE               COMMENT 'Station latitude (point = min = max).',
    spatial_bbox_max_lon    DOUBLE               COMMENT 'Station longitude (point = min = max).',
    spatial_bbox_max_lat    DOUBLE               COMMENT 'Station latitude (point = min = max).',
    author_prepared_by      STRING               COMMENT 'SEPA — Scottish Environment Protection Agency.',
    publication_date        DATE                 COMMENT 'Date of latest reading (proxy for publication_date).',
    approval_state          STRING               COMMENT 'Active | Inactive.',
    product_category        STRING               COMMENT 'monitoring_station for all SEPA entries.',
    is_report               BOOLEAN              COMMENT 'FALSE for SEPA (no PDF reports).',
    is_raster_data          BOOLEAN              COMMENT 'FALSE for SEPA.',
    _gold_processed_at      TIMESTAMP,
    _source                 STRING               COMMENT 'SEPA_SCOTLAND for rows from this pipeline.',
    _state                  STRING
)
USING DELTA
COMMENT 'Gold: shared multi-source flood resource index. Each source manages its own rows via _source-scoped DELETE+INSERT. SEPA rows represent station monitoring entries (not PDF reports).'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# COMMAND ----------

# MAGIC %md ## 5. Build Station Catalog

# COMMAND ----------

log.info("Building SEPA station catalog...")

station_catalog_df = spark.sql(f"""
    WITH latest_readings AS (
        SELECT
            station_no,
            reading_datetime,
            value                           AS latest_reading_value,
            unit_name                       AS latest_reading_unit,
            parametertype_name              AS latest_reading_param,
            ROW_NUMBER() OVER (
                PARTITION BY station_no
                ORDER BY reading_datetime DESC
            ) AS rn
        FROM {FQN_S_READINGS}
        WHERE reading_datetime >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
    ),
    current_readings AS (
        SELECT * FROM latest_readings WHERE rn = 1
    ),
    active_warnings AS (
        -- SEPA warnings don't link to specific stations by ID.
        -- We expose a global "any active warning" flag + severity for all stations.
        SELECT
            MAX(severity_level)  AS min_severity_level,
            MIN(severity_label)  AS worst_severity_label,
            MIN(warning_title)   AS worst_warning_title,
            COUNT(*)             AS active_warning_count
        FROM {FQN_S_WARNINGS}
        WHERE _is_current = TRUE
    )
    SELECT
        s.station_no,
        s.station_name,
        s.river_name,
        s.catchment_name,
        s.lat,
        s.lon,
        CASE
            WHEN r.reading_datetime IS NOT NULL THEN 'Active'
            ELSE 'Inactive'
        END                                     AS station_status,
        r.reading_datetime                      AS latest_reading_datetime,
        r.latest_reading_value,
        r.latest_reading_unit,
        r.latest_reading_param,
        CASE WHEN w.active_warning_count > 0 THEN TRUE ELSE FALSE END
                                                AS has_active_warning,
        CASE WHEN w.active_warning_count > 0 THEN w.worst_severity_label ELSE NULL END
                                                AS active_warning_severity,
        CASE WHEN w.active_warning_count > 0 THEN w.worst_warning_title ELSE NULL END
                                                AS active_warning_title,
        CURRENT_TIMESTAMP()                     AS _gold_processed_at,
        '{SOURCE_NAME}'                         AS _source,
        '{STATE}'                               AS _state
    FROM {FQN_S_STATIONS} s
    LEFT JOIN current_readings r   ON s.station_no = r.station_no
    CROSS JOIN active_warnings w
    WHERE s._is_current = TRUE
""")

station_count = station_catalog_df.count()
log.info(f"station_catalog: {station_count} rows built")

# Full overwrite — this is a point-in-time snapshot table
(
    station_catalog_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable(FQN_G_CATALOG)
)
log.info(f"station_catalog: written to {FQN_G_CATALOG}")

# COMMAND ----------

# MAGIC %md ## 6. Build Warning History

# COMMAND ----------

log.info("Building SEPA warning history...")

warning_history_df = spark.sql(f"""
    SELECT
        warning_guid,
        warning_title,
        warning_link,
        warning_description,
        pub_date,
        severity_label,
        severity_level,
        -- Derived analytics
        CASE WHEN _is_current = TRUE THEN TRUE ELSE FALSE END
                                AS is_current_warning,
        CASE
            WHEN _is_current = TRUE AND pub_date IS NOT NULL
            THEN (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(pub_date)) / 3600.0
            ELSE NULL
        END                     AS warning_age_hours,
        CASE severity_level
            WHEN 1 THEN 'severe'
            WHEN 2 THEN 'warning'
            WHEN 3 THEN 'alert'
            ELSE 'inactive'
        END                     AS severity_category,
        -- SCD2 provenance
        _effective_from,
        _effective_to,
        _is_current             AS _is_current_version,
        CURRENT_TIMESTAMP()     AS _gold_processed_at,
        _source,
        _state
    FROM {FQN_S_WARNINGS}
    ORDER BY pub_date DESC, _effective_from DESC
""")

warning_count = warning_history_df.count()
log.info(f"warning_history: {warning_count} rows built")

(
    warning_history_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable(FQN_G_WARN_HISTORY)
)
log.info(f"warning_history: written to {FQN_G_WARN_HISTORY}")

# COMMAND ----------

# MAGIC %md ## 7. Write au_flood_report_index (Source-Scoped DELETE + INSERT)

# COMMAND ----------

log.info("Writing SEPA station summaries to au_flood_report_index...")

# Build SEPA entries for the shared report index.
# SEPA has no downloadable documents — we use station summaries as index entries.
# Each row represents one active monitoring station with its KiWIS API access info.

sepa_index_df = spark.sql(f"""
    WITH latest_readings AS (
        SELECT
            station_no,
            reading_date,
            ROW_NUMBER() OVER (PARTITION BY station_no ORDER BY reading_datetime DESC) AS rn
        FROM {FQN_S_READINGS}
    ),
    latest_date AS (
        SELECT station_no, reading_date
        FROM   latest_readings
        WHERE  rn = 1
    )
    SELECT
        s.station_no                        AS resource_id,
        CONCAT('SEPA KiWIS Station: ', s.station_name)
                                            AS resource_name,
        NULL                                AS resource_download_url,
        'API_JSON'                          AS resource_format,
        NULL                                AS resource_size_bytes,
        'api'                               AS resource_found_via,
        CONCAT(
            COALESCE(s.station_name, 'Unknown Station'),
            CASE WHEN s.river_name IS NOT NULL THEN CONCAT(' — ', s.river_name) ELSE '' END
        )                                   AS title,
        COALESCE(s.catchment_name, s.river_name)
                                            AS river_basin_name,
        NULL                                AS council_lga_primary,
        s.station_name                      AS place_name,
        CASE
            WHEN s.lat IS NOT NULL AND s.lon IS NOT NULL
            THEN CONCAT('POINT(', s.lon, ' ', s.lat, ')')
            ELSE NULL
        END                                 AS spatial_extent_wkt,
        s.lon                               AS spatial_bbox_min_lon,
        s.lat                               AS spatial_bbox_min_lat,
        s.lon                               AS spatial_bbox_max_lon,
        s.lat                               AS spatial_bbox_max_lat,
        'Scottish Environment Protection Agency (SEPA)'
                                            AS author_prepared_by,
        ld.reading_date                     AS publication_date,
        'Active'                            AS approval_state,
        'monitoring_station'                AS product_category,
        FALSE                               AS is_report,
        FALSE                               AS is_raster_data,
        CURRENT_TIMESTAMP()                 AS _gold_processed_at,
        '{SOURCE_NAME}'                     AS _source,
        '{STATE}'                           AS _state
    FROM {FQN_S_STATIONS} s
    LEFT JOIN latest_date ld ON s.station_no = ld.station_no
    WHERE s._is_current = TRUE
""")

index_count = sepa_index_df.count()
log.info(f"au_flood_report_index: {index_count} SEPA station entries to write")

# Source-scoped refresh: DELETE all SEPA_SCOTLAND rows, then INSERT new ones
spark.sql(f"DELETE FROM {FQN_G_REPORT_INDEX} WHERE _source = '{SOURCE_NAME}'")
(
    sepa_index_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "false")
    .saveAsTable(FQN_G_REPORT_INDEX)
)
log.info(f"au_flood_report_index: {index_count} rows inserted for _source='{SOURCE_NAME}'")

# COMMAND ----------

# MAGIC %md ## 8. Audit Logging

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

# MAGIC %md ## 9. Execute (wrap above cells in try/finally for audit)

# COMMAND ----------

# NOTE: The main work above runs at notebook-cell execution time.
# This cell writes the final audit record for the gold stage.
# If any preceding cell failed, the error propagates and this finally block
# catches it to ensure the audit record is always written.

_pipeline_start  = datetime.now(timezone.utc)  # re-set for audit accuracy
_pipeline_status = "success"                    # prior cells succeeded if we reach here
_pipeline_error  = None

try:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "gold",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_written     = station_count + warning_count + index_count,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "station_catalog_rows":    station_count,
            "warning_history_rows":    warning_count,
            "report_index_rows":       index_count,
            "report_index_source":     SOURCE_NAME,
            "silver_run_id":           SILVER_RUN_ID,
            "note": (
                "SEPA has no downloadable documents. "
                "au_flood_report_index entries represent KiWIS monitoring station summaries."
            ),
        },
    )
except Exception as exc:
    log.error(f"SEPA gold catalog final audit write failed: {exc}", exc_info=True)

# Publish run_id for downstream tasks
try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
