# Databricks notebook source
# EA Real-Time Flood Monitoring — Raster Ingest
#
# Purpose : Handles raster ingest for the EA Real-Time Flood Monitoring pipeline.
#
# EA Monitoring API note:
#   The EA Real-Time Flood Monitoring API provides tabular/vector data only:
#     - Station locations (point features with WGS84 lat/long + BNG easting/northing)
#     - Flood warning areas (polygon geometries via GeoJSON endpoint)
#     - Readings (scalar time-series values)
#     - Flood warnings (tabular event metadata)
#
#   There are NO raster datasets associated with the EA Monitoring pipeline.
#   Raster data (modelled flood depth grids) belongs to the separate EA Depth
#   pipeline (ea_depth track), which ingests per-grid-square ASC files from the
#   Defra Data Services Platform into au_flood_raster_catalog.
#
#   THIS NOTEBOOK is a placeholder that:
#     1. Logs a clear explanation of why raster ingest is skipped
#     2. Creates the au_flood_raster_catalog table if it does not exist
#     3. Writes an audit record so the pipeline run log is complete
#     4. Exits cleanly so the Databricks Workflow succeeds
#
# For actual raster ingest from EA modelled depth data, see the ea_depth pipeline.
#
# Layer   : Gold (au_flood_raster_catalog — not written by this notebook)
# Reads   : Nothing (placeholder)
# Writes  : ceg_delta_bronze_prnd.international_flood.pipeline_run_log (audit only)
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
GOLD_CATALOG   = "ceg_delta_gold_prnd"
SCHEMA         = "international_flood"

FQN_AUDIT          = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
FQN_RASTER_CATALOG = f"{GOLD_CATALOG}.{SCHEMA}.au_flood_raster_catalog"

NOTEBOOK_NAME    = "ea_05_raster_ingest"
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
log.info(f"EA Monitoring Raster Ingest  |  Run ID: {RUN_ID}")
log.info(f"Started at : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Raster Data Assessment

# COMMAND ----------

# ── EA Monitoring — Raster Ingest Assessment ───────────────────────────────
#
# The EA Real-Time Flood Monitoring API is a VECTOR/TABULAR pipeline.
# It provides:
#
#   DATA TYPE              GEOMETRY        RASTER?
#   ────────────────────── ─────────────── ───────
#   Monitoring stations    Point (WGS84)   NO
#   Flood warning areas    Polygon (BNG)   NO
#   Readings               None (scalar)   NO
#   Flood warnings         None (tabular)  NO
#
# There are no raster datasets (GeoTIFF, ASC grids, netCDF, etc.) produced
# by or associated with the EA Monitoring pipeline.
#
# Raster datasets from the Environment Agency are found in:
#
#   Pipeline Track         Description                    Sedona Table
#   ──────────────────── ─────────────────────────────── ──────────────────────────────
#   ea_depth             Modelled fluvial flood depths   au_flood_raster_catalog
#                        (per OS grid-square ASC ZIPs    (_source = 'EA_DEPTH')
#                        from Defra Data Services)
#
#   ea_risk_products     Flood Depth Grid 20m            au_flood_raster_catalog
#                        (national-scale raster product) (_source = 'EA_RISK')
#
# SKIP REASON: EA Monitoring is vector/tabular only — raster ingest not applicable.
# ──────────────────────────────────────────────────────────────────────────

log.info("=" * 60)
log.info("EA Monitoring Raster Ingest — PLACEHOLDER SKIP")
log.info("=" * 60)
log.info("")
log.info("SKIP REASON: The EA Real-Time Flood Monitoring pipeline produces")
log.info("vector and tabular data only (station points, warning polygons,")
log.info("scalar readings). No raster datasets are involved.")
log.info("")
log.info("For EA raster data, use:")
log.info("  - ea_depth pipeline   : modelled fluvial flood depth grids")
log.info("                          (per-grid-square ASC ZIPs from Defra)")
log.info("  - ea_risk pipeline    : national Flood Depth Grid 20m raster")
log.info("")
log.info("Both ea_depth and ea_risk pipelines write to au_flood_raster_catalog")
log.info("with _source = 'EA_DEPTH' and _source = 'EA_RISK' respectively.")
log.info("=" * 60)

# Ensure au_flood_raster_catalog exists (shared table — other pipelines write to it)
try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FQN_RASTER_CATALOG} (
            raster_id                   STRING     NOT NULL  COMMENT 'SHA256-derived unique identifier for this raster file',
            resource_id                 STRING               COMMENT 'Source dataset/resource identifier',
            volume_path                 STRING               COMMENT 'ADLS Unity Catalog Volume path to the raster file',
            original_filename           STRING               COMMENT 'Original filename of the raster',
            file_format                 STRING               COMMENT 'File format: TIF | ASC | NC | GRD',
            file_size_bytes             LONG                 COMMENT 'File size in bytes',
            source_state                STRING               COMMENT 'State/region covered: NSW | QLD | England | Wales | Scotland',
            source_agency               STRING               COMMENT 'Agency that produced the data (e.g. Environment Agency)',
            study_name                  STRING               COMMENT 'Study or dataset name',
            project_id                  STRING               COMMENT 'Source project or dataset UUID',
            council_lga_primary         STRING               COMMENT 'Primary council or LGA (if applicable)',
            inferred_aep_pct            DOUBLE               COMMENT 'Inferred Annual Exceedance Probability percentage',
            inferred_return_period_yr   INT                  COMMENT 'Inferred return period in years',
            value_type                  STRING               COMMENT 'Raster value type: depth | wse | extent | risk',
            crs_epsg                    INT                  COMMENT 'EPSG code of native raster CRS',
            band_count                  INT                  COMMENT 'Number of raster bands',
            width_pixels                INT                  COMMENT 'Raster width in pixels',
            height_pixels               INT                  COMMENT 'Raster height in pixels',
            cell_size_x                 DOUBLE               COMMENT 'Cell width in native CRS units',
            cell_size_y                 DOUBLE               COMMENT 'Cell height in native CRS units',
            min_value                   DOUBLE               COMMENT 'Minimum raster value (non-nodata)',
            max_value                   DOUBLE               COMMENT 'Maximum raster value (non-nodata)',
            nodata_value                DOUBLE               COMMENT 'No-data value',
            native_envelope_wkt         STRING               COMMENT 'Bounding box in native CRS as WKT',
            wgs84_bbox_min_lon          DOUBLE               COMMENT 'WGS84 bounding box min longitude',
            wgs84_bbox_min_lat          DOUBLE               COMMENT 'WGS84 bounding box min latitude',
            wgs84_bbox_max_lon          DOUBLE               COMMENT 'WGS84 bounding box max longitude',
            wgs84_bbox_max_lat          DOUBLE               COMMENT 'WGS84 bounding box max latitude',
            ingest_timestamp            TIMESTAMP            COMMENT 'UTC timestamp when this row was written',
            _source                     STRING               COMMENT 'Source system: NSW_SES | BCC | EA_DEPTH | EA_RISK | NRW_WALES',
            _state                      STRING               COMMENT 'Country/state: NSW | QLD | England | Wales | Scotland'
        )
        USING DELTA
        COMMENT 'Gold: shared multi-source raster catalog. Each source writes via _source-scoped refresh. Requires Apache Sedona for raster metadata extraction.'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed'       = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)
    log.info(f"Shared table {FQN_RASTER_CATALOG} confirmed/created")
except Exception as e:
    log.warning(f"Could not create {FQN_RASTER_CATALOG} (may already exist): {e}")

_rasters_ingested = 0

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
    log.info("EA Monitoring Raster Ingest — placeholder complete (no rasters to ingest)")
    log.info(f"  Rasters ingested : {_rasters_ingested} (expected 0 — EA Monitoring is vector/tabular only)")

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Raster ingest pipeline failed unexpectedly: {exc}", exc_info=True)
    _pipeline_status = "failed"
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "raster_ingest",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_written     = _rasters_ingested,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "rasters_ingested": _rasters_ingested,
            "skip_reason": (
                "EA Real-Time Flood Monitoring pipeline is vector/tabular only. "
                "Raster data (modelled flood depth grids) is handled by the ea_depth "
                "and ea_risk pipeline tracks, which write to au_flood_raster_catalog "
                "with _source = 'EA_DEPTH' and _source = 'EA_RISK'."
            ),
            "pipeline_type":  "vector_and_tabular",
            "data_sources":   ["monitoring_stations", "flood_warnings", "flood_areas", "readings"],
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
