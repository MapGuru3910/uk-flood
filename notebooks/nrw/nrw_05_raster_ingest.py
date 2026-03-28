# Databricks notebook source
# NRW Wales Flood Risk Products — Raster Ingest
#
# Purpose : Handles raster ingest for the NRW Wales flood risk pipeline.
#
# NRW Data Source — Raster Assessment:
#   Natural Resources Wales (NRW) publishes flood risk data exclusively as
#   VECTOR datasets via the DataMapWales GeoServer WFS endpoint. These are:
#     - Polygon flood zones (EPSG:27700, MultiPolygon geometry)
#     - Polygon risk assessment areas (rivers, sea, surface water)
#     - Polygon flood warning areas
#     - Polygon areas benefiting from flood defences
#
#   NRW does NOT publish:
#     - Raster depth grids (like EA modelled flood depth ASC files)
#     - GeoTIFF flood extent rasters
#     - Any other raster products for public download
#
#   This is a structural difference from the EA pipeline:
#
#   Pipeline      Raster Data Available?   Source
#   ──────────── ─────────────────────── ───────────────────────────────────
#   EA Depth      ✅ Yes                  Defra: per-grid-square ASC ZIPs
#   EA Risk       ✅ Yes                  Defra: Flood Depth Grid 20m
#   NRW Wales     ❌ No                   Vector-only (WFS)
#   SEPA Scotland ❌ No                   No public raster downloads
#
#   THIS NOTEBOOK is therefore a placeholder that logs a clear explanation,
#   ensures the shared au_flood_raster_catalog table exists, and exits cleanly.
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

NOTEBOOK_NAME    = "nrw_05_raster_ingest"
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
log.info(f"NRW Wales Raster Ingest  |  Run ID: {RUN_ID}")
log.info(f"Started at : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Raster Data Assessment

# COMMAND ----------

log.info("=" * 60)
log.info("NRW Wales Raster Ingest — PLACEHOLDER SKIP")
log.info("=" * 60)
log.info("")
log.info("SKIP REASON: NRW Wales publishes VECTOR data only via WFS.")
log.info("No raster datasets (GeoTIFF, ASC grids, etc.) are available")
log.info("from Natural Resources Wales for public download.")
log.info("")
log.info("NRW Wales data types:")
log.info("  ✅ Flood zone polygons           — EPSG:27700 MultiPolygon via WFS")
log.info("  ✅ Flood risk area polygons       — EPSG:27700 MultiPolygon via WFS")
log.info("  ✅ Flood warning area polygons    — EPSG:27700 MultiPolygon via WFS")
log.info("  ✅ Flood defence area polygons    — EPSG:27700 MultiPolygon via WFS")
log.info("  ❌ Depth grid rasters             — NOT AVAILABLE for Wales")
log.info("  ❌ Flood extent rasters           — NOT AVAILABLE for Wales")
log.info("")
log.info("Raster data for UK coverage (England only):")
log.info("  ea_depth pipeline  : modelled fluvial depth grids (per-grid-square ASC)")
log.info("  ea_risk pipeline   : national Flood Depth Grid 20m product")
log.info("  Both write to au_flood_raster_catalog with _source = 'EA_DEPTH' / 'EA_RISK'")
log.info("")
log.info("If NRW publishes raster products in future:")
log.info("  1. Add download logic in nrw_04_download.py")
log.info("  2. Implement Sedona RS_* extraction in this notebook")
log.info("  3. Write to au_flood_raster_catalog with _source = 'NRW_WALES'")
log.info("=" * 60)

# Ensure the shared au_flood_raster_catalog exists (may be created by EA pipelines first)
try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FQN_RASTER_CATALOG} (
            raster_id                   STRING     NOT NULL  COMMENT 'SHA256-derived unique identifier',
            resource_id                 STRING               COMMENT 'Source dataset/resource identifier',
            volume_path                 STRING               COMMENT 'ADLS Unity Catalog Volume path',
            original_filename           STRING               COMMENT 'Original raster filename',
            file_format                 STRING               COMMENT 'TIF | ASC | NC | GRD',
            file_size_bytes             LONG,
            source_state                STRING               COMMENT 'England | Wales | Scotland | NSW | QLD',
            source_agency               STRING               COMMENT 'Environment Agency | NRW | SEPA | NSW SES | BCC',
            study_name                  STRING,
            project_id                  STRING,
            council_lga_primary         STRING,
            inferred_aep_pct            DOUBLE               COMMENT 'AEP as percentage (e.g. 1.0 for 1%)',
            inferred_return_period_yr   INT,
            value_type                  STRING               COMMENT 'depth | wse | extent | risk',
            crs_epsg                    INT                  COMMENT 'EPSG code of native CRS',
            band_count                  INT,
            width_pixels                INT,
            height_pixels               INT,
            cell_size_x                 DOUBLE               COMMENT 'Cell width in native CRS units',
            cell_size_y                 DOUBLE               COMMENT 'Cell height in native CRS units',
            min_value                   DOUBLE,
            max_value                   DOUBLE,
            nodata_value                DOUBLE,
            native_envelope_wkt         STRING               COMMENT 'Bounding box WKT in native CRS',
            wgs84_bbox_min_lon          DOUBLE,
            wgs84_bbox_min_lat          DOUBLE,
            wgs84_bbox_max_lon          DOUBLE,
            wgs84_bbox_max_lat          DOUBLE,
            ingest_timestamp            TIMESTAMP,
            _source                     STRING               COMMENT 'NSW_SES | BCC | EA_DEPTH | EA_RISK | NRW_WALES',
            _state                      STRING
        )
        USING DELTA
        COMMENT 'Gold: shared multi-source raster catalog. Sources write via _source-scoped refresh.'
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
    log.info("NRW Wales Raster Ingest — placeholder complete (no rasters available)")
    log.info(f"  Rasters ingested : {_rasters_ingested} (expected 0 — NRW Wales is vector-only)")

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Raster ingest failed unexpectedly: {exc}", exc_info=True)
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
                "NRW Wales publishes flood data as vector polygons via OGC WFS only. "
                "No raster depth grids or flood extent rasters are available for Wales "
                "from Natural Resources Wales. "
                "For Wales raster coverage, monitor NRW for future data releases."
            ),
            "pipeline_type":   "vector_only",
            "data_layers":     [
                "NRW_FLOODZONE_RIVERS", "NRW_FLOODZONE_SEAS",
                "NRW_FLOOD_RISK_FROM_RIVERS", "NRW_FLOOD_RISK_FROM_SEA",
                "NRW_FLOOD_WARNING", "NRW_AREA_BENEFITING_FROM_FLOOD_DEFENCE",
            ],
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
