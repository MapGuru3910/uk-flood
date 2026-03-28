# Databricks notebook source
# SEPA Scotland — Download
#
# Purpose : Handles the download step for the SEPA Scotland pipeline.
#
# SEPA KiWIS API note:
#   The SEPA KiWIS Time Series API (timeseries.sepa.org.uk/KiWIS) is a pure
#   REST/JSON API providing:
#     - Station metadata (lat/long, river, catchment)
#     - 15-minute real-time readings (stage, flow)
#     - Time series definitions
#
#   There are NO bulk downloadable files (ZIPs, SHPs, PDFs, ASC grids) associated
#   with the SEPA KiWIS monitoring pipeline.
#
#   Unlike the EA pipeline (which has associated flood study PDFs via the Defra
#   Data Services Platform), SEPA provides all monitoring data via the KiWIS API
#   directly. The data is captured in bronze Delta tables during sepa_01_bronze_scrape.
#
#   THIS NOTEBOOK performs a graceful no-op with full audit logging.
#   It is retained in the 5-notebook pipeline structure for:
#     1. Consistency with the standard medallion pipeline pattern
#     2. Future use if SEPA publishes bulk spatial data downloads
#     3. As a hook for SEPA flood map data (if/when it becomes available post-2020 cyberattack)
#
# FUTURE EXTENSION POINTS:
#   - SEPA Flood Maps: SEPA's web map viewer (map.sepa.org.uk/floodmaps/) shows
#     flood risk data but no WFS/WMS endpoints are currently available for bulk download.
#     The 2020 SEPA cyberattack destroyed many internal systems; services are being rebuilt.
#     When SEPA publishes spatial data (expected via spatialdata.gov.scot or beta.sepa.scot),
#     this notebook should implement download similar to nrw_04_vector_ingest.py.
#
#   - Scottish SSDI (spatialdata.gov.scot): The Scottish Spatial Data Infrastructure
#     may eventually host Flood Risk Assessment (FRA) outputs and Potentially Vulnerable
#     Areas (PVAs) from SEPA's second cycle Flood Risk Management Plans (2022-2028).
#     Monitor: https://spatialdata.gov.scot/geonetwork/
#
#   - SEPA Annual Flood Reports: SEPA publishes annual flood reports as PDFs at
#     https://www.sepa.org.uk/environment/water/flooding/flood-reports/ but these
#     are narrative documents, not machine-readable spatial data suitable for download.
#
# Layer   : Bronze (writes files to Volume if any are found — none expected currently)
# Reads   : ceg_delta_silver_prnd.international_flood.sepa_monitoring_stations (count check only)
# Writes  : /Volumes/ceg_delta_bronze_prnd/international_flood/sepa_raw/ (if applicable)
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
SILVER_CATALOG = "ceg_delta_silver_prnd"
SCHEMA         = "international_flood"
VOLUME_NAME    = "sepa_raw"
VOLUME_BASE    = f"/Volumes/{BRONZE_CATALOG}/{SCHEMA}/{VOLUME_NAME}"

FQN_AUDIT      = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
FQN_S_STATIONS = f"{SILVER_CATALOG}.{SCHEMA}.sepa_monitoring_stations"

NOTEBOOK_NAME    = "sepa_04_download"
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

RUN_ID       = dbutils.widgets.get("run_id").strip() or str(uuid.uuid4())
PROCESSED_AT = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"SEPA Download  |  Run ID: {RUN_ID}")
log.info(f"Started at : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Data Source Assessment

# COMMAND ----------

# ── SEPA Scotland — Download Assessment ───────────────────────────────────
#
# The SEPA KiWIS Time Series API exposes the following data types:
#
#   DATA TYPE              FORMAT        DOWNLOAD?   HANDLING
#   ────────────────────── ─────────────── ─────────── ─────────────────────────────────
#   Monitoring stations    JSON API       NO          Bronze/silver table: bronze_sepa_stations
#   15-min readings        JSON API       NO          Bronze/silver table: bronze_sepa_readings
#   Flood warnings         RSS/XML        NO          Bronze/silver table: bronze_sepa_warnings
#
# SEPA FLOOD RISK SPATIAL DATA — CURRENT STATUS (as of 2026-03-28):
#
#   SEPA suffered a major cyberattack in December 2020 that destroyed many internal
#   systems, including their data publication infrastructure. As a result:
#
#   1. map.sepa.org.uk/floodmaps/ — Web viewer available but no WFS/WMS API
#      The flood maps web application is running but there are no confirmed public
#      endpoints for programmatic access to flood zone geometries.
#
#   2. open.sepa.org.uk — UNREACHABLE (HTTP connection refused as of 2026-03-28)
#      The old SEPA open data portal does not respond. It was a victim of the
#      2020 cyberattack and has not been rebuilt.
#
#   3. spatialdata.gov.scot — The Scottish Government's SSDI (Spatial Data
#      Infrastructure) may eventually host SEPA flood risk data. Currently
#      returning 403 for ES search API; catalog is accessible but relevant
#      datasets have not been confirmed at this time.
#
#   4. beta.sepa.scot — SEPA's rebuilt data services are being deployed.
#      Monitor for WFS/WMS flood risk endpoints.
#
# CONCLUSION: No bulk file downloads are applicable for SEPA at this time.
#
# ─────────────────────────────────────────────────────────────────────────

log.info("=" * 60)
log.info("SEPA Scotland Download — GRACEFUL SKIP")
log.info("=" * 60)
log.info("")
log.info("Reason: The SEPA KiWIS API provides monitoring data via JSON REST API only.")
log.info("No bulk downloadable files (ZIPs, SHP, ASC, PDF) are available for SEPA.")
log.info("")
log.info("Data types and current handling:")
log.info("  - Monitoring stations : captured via KiWIS JSON API in bronze/silver tables")
log.info("  - 15-min readings     : captured via KiWIS JSON API in bronze/silver tables")
log.info("  - RSS flood warnings  : captured via RSS XML in bronze/silver tables")
log.info("")
log.info("SEPA Flood Risk Spatial Data — BLOCKED (as of 2026-03-28):")
log.info("  - map.sepa.org.uk/floodmaps/  : web viewer only, no WFS/WMS API")
log.info("  - open.sepa.org.uk            : unreachable (2020 cyberattack impact)")
log.info("  - spatialdata.gov.scot        : under investigation")
log.info("  - beta.sepa.scot              : monitor for future spatial data services")
log.info("")
log.info("Future extension: Implement spatial download in this notebook when SEPA")
log.info("publishes flood risk geometries (PVAs, FRA outputs) via WFS or bulk download.")
log.info("Expected source: spatialdata.gov.scot or beta.sepa.scot")
log.info("")
log.info("This notebook is retained for pipeline structure consistency.")
log.info("=" * 60)

# Verify volume exists (reserved for future spatial data downloads)
try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {BRONZE_CATALOG}.{SCHEMA}.{VOLUME_NAME}
        COMMENT 'SEPA Scotland raw downloads volume — reserved for future spatial data downloads once SEPA rebuilds its data services post-2020 cyberattack'
    """)
    log.info(f"Volume {VOLUME_BASE} confirmed/created")
except Exception as e:
    log.warning(f"Could not create volume (may already exist): {e}")

# Informational: log current station count from silver
try:
    station_count = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {FQN_S_STATIONS} WHERE _is_current = TRUE"
    ).collect()[0]["cnt"]
    log.info(f"Current active SEPA monitoring stations in silver: {station_count}")
except Exception as e:
    log.warning(f"Could not count silver stations: {e}")
    station_count = 0

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
    log.info(f"SEPA Download notebook complete — graceful skip")
    log.info(f"  Files downloaded : {_files_downloaded}")
    log.info(f"  Files skipped    : {_files_skipped} (no downloadable attachments for SEPA KiWIS API)")

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
            "files_downloaded":      _files_downloaded,
            "files_skipped":         _files_skipped,
            "active_silver_stations": station_count if 'station_count' in dir() else 0,
            "skip_reason": (
                "SEPA KiWIS API is JSON REST only — no bulk file attachments. "
                "SEPA flood risk spatial data is not publicly available for download "
                "due to the impact of the December 2020 SEPA cyberattack on their "
                "data publication infrastructure. Monitor spatialdata.gov.scot and "
                "beta.sepa.scot for future availability."
            ),
            "future_data_sources": [
                "https://spatialdata.gov.scot/geonetwork/ — Scottish SSDI (PVAs, FRA outputs)",
                "https://beta.sepa.scot — SEPA rebuilt data services",
                "https://map.sepa.org.uk/floodmaps/ — web viewer (no API currently)",
            ],
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
