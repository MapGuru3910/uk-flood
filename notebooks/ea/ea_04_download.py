# Databricks notebook source
# EA Real-Time Flood Monitoring — Download
#
# Purpose : Downloads any linked documents or attachments associated with EA
#           flood warnings and monitoring data to a Unity Catalog Volume.
#
# EA Monitoring API note:
#   The EA Real-Time Flood Monitoring API is a JSON/REST API providing:
#     - Tabular station metadata and readings (no file attachments)
#     - Flood warning data (text fields, no linked PDFs or reports)
#     - Flood area polygon URLs (GeoJSON endpoints, not downloadable files)
#
#   Unlike the NSW SES or BCC pipelines, there are NO bulk downloadable
#   documents (PDFs, ZIPs, SHP files) associated with the monitoring API.
#   The EA Flood Risk Products pipeline (ea_risk track) handles bulk downloads
#   of national datasets from the Defra Data Services Platform.
#
#   THIS NOTEBOOK therefore performs a graceful no-op with full audit logging.
#   It is retained in the 5-notebook pipeline structure for consistency and
#   to serve as the insertion point if EA publishes linked documents in future.
#
# Layer   : Bronze (writes files to Volume if any are found)
# Reads   : ceg_delta_silver_prnd.international_flood.ea_monitoring_flood_areas
# Writes  : /Volumes/ceg_delta_bronze_prnd/international_flood/ea_monitoring_raw/ (if applicable)
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

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import json
import logging
import uuid
from datetime import datetime, timezone

# ── Catalog / table paths ──────────────────────────────────────────────────
BRONZE_CATALOG = "ceg_delta_bronze_prnd"
SCHEMA         = "international_flood"
VOLUME_NAME    = "ea_monitoring_raw"
VOLUME_BASE    = f"/Volumes/{BRONZE_CATALOG}/{SCHEMA}/{VOLUME_NAME}"

FQN_AUDIT = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"

NOTEBOOK_NAME    = "ea_04_download"
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

RUN_ID       = dbutils.widgets.get("run_id").strip() or str(uuid.uuid4())
PROCESSED_AT = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"EA Monitoring Download  |  Run ID: {RUN_ID}")
log.info(f"Started at : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Data Source Assessment

# COMMAND ----------

# ── EA Monitoring API — Download Assessment ────────────────────────────────
#
# The EA Real-Time Flood Monitoring API (environment.data.gov.uk/flood-monitoring)
# exposes the following data types:
#
#   1. Monitoring stations    — JSON metadata (lat/long, river, measures)
#                               → Fully captured in bronze/silver tables. No file download needed.
#
#   2. Flood warnings         — JSON text fields (severity, message, timestamps)
#                               → Fully captured in bronze/silver tables. No file download needed.
#
#   3. Flood warning areas    — JSON with a `polygon` URL field
#                               The polygon URL returns GeoJSON when fetched.
#                               → GeoJSON is fetched inline in silver normalization for
#                                 use with Sedona ST_GeomFromGeoJSON.
#                               → The GeoJSON data is small per-feature (< 1KB typical)
#                                 and does not need separate file download to Volume.
#
#   4. Readings               — JSON scalar values (timestamp + numeric value)
#                               → Fully captured in bronze/silver tables. No file download needed.
#
# CONCLUSION: No bulk file downloads are required or applicable for the EA Monitoring pipeline.
#
# FUTURE EXTENSION POINTS:
#   - If EA publishes flood event PDF reports or linked documents via the API,
#     this notebook should implement the download pattern from nsw_ses_04_download.py.
#   - For flood area GeoJSON polygons larger than threshold, consider downloading
#     to Volume instead of fetching inline in silver.
#   - The EA Flood Risk Products pipeline (ea_risk track) handles large file downloads
#     from the Defra Data Services Platform (national SHP/GPKG ZIPs).
#
# ──────────────────────────────────────────────────────────────────────────

log.info("=" * 60)
log.info("EA Monitoring Download — GRACEFUL SKIP")
log.info("=" * 60)
log.info("")
log.info("Reason: The EA Real-Time Flood Monitoring API does not expose")
log.info("downloadable documents (PDFs, ZIPs, or bulk files).")
log.info("")
log.info("Data types and their handling:")
log.info("  - Monitoring stations : captured as JSON in bronze/silver tables")
log.info("  - Flood warnings      : captured as JSON in bronze/silver tables")
log.info("  - Flood area polygons : fetched inline via GeoJSON endpoint in silver")
log.info("  - Readings            : captured as scalar values in bronze/silver tables")
log.info("")
log.info("For EA bulk file downloads (national flood zone SHP/GPKG ZIPs),")
log.info("see the EA Risk Products pipeline (ea_risk track).")
log.info("")
log.info("This notebook is retained for pipeline structure consistency.")
log.info("It will be activated if EA publishes linked documents in future.")
log.info("=" * 60)

# Verify Volume exists (creates if needed for future use)
try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {BRONZE_CATALOG}.{SCHEMA}.{VOLUME_NAME}
        COMMENT 'EA monitoring raw downloads volume — reserved for future use if EA publishes linked documents'
    """)
    log.info(f"Volume {VOLUME_BASE} confirmed/created")
except Exception as e:
    log.warning(f"Could not create volume (may already exist): {e}")

_files_downloaded = 0
_files_skipped    = 0

# COMMAND ----------

# MAGIC %md ## 5. Audit Logging

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

# MAGIC %md ## 6. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "success"
_pipeline_error  = None

try:
    log.info(f"EA Monitoring Download notebook complete — graceful skip")
    log.info(f"  Files downloaded : {_files_downloaded}")
    log.info(f"  Files skipped    : {_files_skipped} (no downloadable attachments in EA Monitoring API)")

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Download pipeline failed unexpectedly: {exc}", exc_info=True)
    _pipeline_status = "failed"
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "download",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_written     = _files_downloaded,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "files_downloaded":  _files_downloaded,
            "files_skipped":     _files_skipped,
            "skip_reason": (
                "EA Real-Time Flood Monitoring API provides only JSON tabular data "
                "and scalar readings — no bulk file attachments. "
                "Flood area GeoJSON polygons are fetched inline in silver normalization."
            ),
            "next_pipeline": "For EA bulk downloads, use the ea_risk pipeline track.",
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
