# Databricks notebook source
# NRW Wales Flood Risk Products — Download
#
# Purpose : Handles bulk file downloads for the NRW Wales pipeline.
#
# NRW Data Source — Download Assessment:
#   NRW Wales flood data is accessed via the DataMapWales GeoServer WFS endpoint.
#   WFS (Web Feature Service) delivers vector features INLINE as part of the
#   GetFeature response — there are no separate downloadable files (ZIPs, SHPs,
#   or PDFs) associated with these layers.
#
#   Unlike the NSW SES or EA Depth pipelines (which download ZIP archives
#   from a portal), NRW features are fetched inline in the bronze scrape notebook
#   (nrw_01_bronze_scrape.py) and stored directly in Delta tables.
#
#   THIS NOTEBOOK is therefore a graceful no-op with full audit logging.
#   It is retained in the 5-notebook pipeline structure for consistency.
#
# Comparison to other pipelines:
#
#   Pipeline     Data Access Method     Files to Download
#   ──────────── ────────────────────── ─────────────────────────────────
#   NSW SES      CKAN API               ZIP/PDF files → Volume
#   BCC          ODS API                ZIP/PDF files → Volume
#   EA Monitoring REST API              None (JSON only)
#   EA Depth     Defra bulk download    per-grid ASC ZIPs → Volume
#   EA Risk      Defra bulk download    national SHP/GPKG ZIPs → Volume
#   NRW Wales    WFS GetFeature         None (GeoJSON inline) ← THIS PIPELINE
#
# Future extension: If NRW publishes supplementary reports, hazard maps, or
#   bulk download links alongside the WFS, this notebook should implement the
#   download pattern from nsw_ses_04_download.py.
#
# Layer   : Bronze (Volume writes — not applicable; audit log only)
# Reads   : ceg_delta_silver_prnd.international_flood.nrw_flood_zones (for context)
# Writes  : ceg_delta_bronze_prnd.international_flood.pipeline_run_log
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
VOLUME_NAME    = "nrw_risk_raw"
VOLUME_BASE    = f"/Volumes/{BRONZE_CATALOG}/{SCHEMA}/{VOLUME_NAME}"

FQN_AUDIT = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"

NOTEBOOK_NAME    = "nrw_04_download"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "NRW_WALES"
STATE            = "Wales"

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
log.info(f"NRW Wales Download  |  Run ID: {RUN_ID}")
log.info(f"Started at : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Data Source Assessment

# COMMAND ----------

log.info("=" * 60)
log.info("NRW Wales Download — GRACEFUL SKIP")
log.info("=" * 60)
log.info("")
log.info("Reason: NRW Wales flood data is accessed via OGC WFS (Web Feature Service).")
log.info("WFS delivers vector features inline as GeoJSON in the GetFeature response.")
log.info("There are NO separate downloadable files (ZIPs, SHPs, PDFs) to download.")
log.info("")
log.info("Data access comparison:")
log.info("  NRW Wales  : WFS GetFeature → GeoJSON inline → bronze table (NO download step)")
log.info("  NSW SES    : CKAN API → ZIP/PDF files → Volume (download step required)")
log.info("  EA Depth   : Defra platform → per-grid ASC ZIPs → Volume (download step required)")
log.info("")
log.info("NRW WFS features are captured in nrw_01_bronze_scrape.py and stored")
log.info("as properties_json + geometry_json in the nrw_flood_layers_scrape table.")
log.info("")
log.info("Geometry processing (EPSG:27700 → EPSG:4326 via Sedona ST_Transform)")
log.info("is performed in nrw_02_silver_normalize.py — no Volume storage needed.")
log.info("")
log.info("This notebook is retained for pipeline structure consistency.")
log.info("=" * 60)

# Verify Volume exists (reserved for future use — e.g. if NRW publishes bulk downloads)
try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {BRONZE_CATALOG}.{SCHEMA}.{VOLUME_NAME}
        COMMENT 'NRW Wales raw downloads volume — reserved for future bulk downloads if NRW publishes them'
    """)
    log.info(f"Volume {VOLUME_BASE} confirmed/created (reserved for future use)")
except Exception as e:
    log.warning(f"Could not create volume (may already exist): {e}")

# Summary of what IS available in Delta tables (for operator reference)
try:
    zone_count = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM ceg_delta_silver_prnd.{SCHEMA}.nrw_flood_zones
        WHERE _is_current = TRUE
    """).collect()[0]["n"]
    log.info(f"Current NRW flood zone records in silver: {zone_count}")
except Exception as e:
    log.info(f"Could not query silver table count (may not exist yet): {e}")

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
    log.info("NRW Wales Download notebook complete — graceful skip")
    log.info(f"  Files downloaded : {_files_downloaded}")
    log.info(f"  Files skipped    : {_files_skipped} (WFS data is inline — no file downloads)")

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
            "files_downloaded": _files_downloaded,
            "files_skipped":    _files_skipped,
            "skip_reason": (
                "NRW Wales data is delivered via OGC WFS GetFeature as inline GeoJSON. "
                "Features are stored directly in Delta tables in the bronze scrape step. "
                "No bulk file downloads exist for this source."
            ),
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
