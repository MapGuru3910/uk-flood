# Databricks notebook source
# SEPA Scotland — Raster Ingest
#
# Purpose : Handles raster ingest for the SEPA Scotland monitoring pipeline.
#
# SEPA Scotland — Raster Data Status (as of 2026-03-28):
#
#   The SEPA Scotland pipeline is a monitoring and vector/tabular pipeline.
#   SEPA does NOT publish raster flood data (depth grids, WSE surfaces) for
#   public download at this time.
#
#   The primary reason is the SEPA cyberattack of December 2020:
#   SEPA suffered a devastating ransomware attack that destroyed much of their
#   internal IT infrastructure, including data publication systems. Recovery
#   has been ongoing, and many data services that existed pre-2020 have not
#   been re-established.
#
#   Current SEPA raster data situation:
#
#   1. SEPA Flood Maps (map.sepa.org.uk/floodmaps/)
#      - Web application: ✅ Available (rebuilt post-cyberattack)
#      - WMS/WFS API:     ❌ Endpoints returning 404 or unavailable
#      - Bulk download:   ❌ No confirmed download mechanism
#      - Data source:     ArcGIS-based web map (internal tiles, not public rasters)
#
#   2. SEPA Flood Risk Assessment (FRA) Products
#      - Potentially Vulnerable Areas (PVAs): Available on web but no API/download
#      - Flood Hazard Maps (extent, depth, velocity): Not publicly available
#      - Flood Risk Maps (people, economic, environmental): Not publicly available
#      - Expected publication route: spatialdata.gov.scot (second cycle FRMP 2022-2028)
#
#   3. open.sepa.org.uk — UNREACHABLE (confirmed 2026-03-28)
#      The old SEPA open data portal is offline. It previously hosted some
#      GIS datasets but has not been recovered post-cyberattack.
#
#   4. Scotland's Spatial Data Infrastructure (spatialdata.gov.scot)
#      - GeoNetwork catalog: ✅ Accessible
#      - Elasticsearch search API: ❌ Returns 403 Forbidden
#      - Flood risk datasets: ⚠️ Under investigation — may be published in future
#
#   5. beta.sepa.scot
#      - New SEPA data services platform under development
#      - Monitor for WFS/WMS/download endpoints for flood risk data
#
#   CONTRAST WITH EA AND NRW:
#   - EA publishes modelled flood depth grids as per-grid-square ASC ZIP files
#     via the Defra Data Services Platform (see ea_depth pipeline track)
#   - NRW publishes flood risk polygons via WFS (see nrw_04_vector_ingest.py)
#   - SEPA has NO public equivalent for either raster or vector flood risk data
#
#   THIS NOTEBOOK:
#     1. Logs a comprehensive explanation of why raster ingest is skipped
#     2. Ensures au_flood_raster_catalog exists (shared table — other pipelines write to it)
#     3. Writes an audit record so the pipeline run log is complete
#     4. Exits cleanly so the Databricks Workflow succeeds
#
#   ACTIVATION: When SEPA publishes raster flood data (depth grids, hazard maps),
#   implement raster ingest here following the EA depth pipeline pattern:
#   - Fetch raster URLs/downloads from the appropriate SEPA/spatialdata.gov.scot endpoint
#   - Download to /Volumes/ceg_delta_bronze_prnd/international_flood/sepa_raw/
#   - Use Apache Sedona RS_* functions to extract raster metadata
#   - Write to au_flood_raster_catalog with _source = 'SEPA_RISK'
#
# Layer   : Gold (au_flood_raster_catalog — not written by this notebook, only ensured)
# Reads   : Nothing
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

NOTEBOOK_NAME    = "sepa_05_raster_ingest"
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
log.info(f"SEPA Raster Ingest  |  Run ID: {RUN_ID}")
log.info(f"Started at : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Raster Data Assessment

# COMMAND ----------

log.info("=" * 60)
log.info("SEPA Scotland Raster Ingest — PLACEHOLDER SKIP")
log.info("=" * 60)
log.info("")
log.info("SKIP REASON: SEPA flood risk raster data is not publicly available")
log.info("for programmatic download (as of 2026-03-28).")
log.info("")
log.info("Root cause: December 2020 SEPA Cyberattack")
log.info("  A devastating ransomware attack destroyed much of SEPA's IT infrastructure")
log.info("  including data publication systems. Recovery is ongoing.")
log.info("")
log.info("Current SEPA raster data landscape:")
log.info("  DATA TYPE                        STATUS")
log.info("  ─────────────────────────────── ──────────────────────────────────────────────")
log.info("  Flood depth grids                ❌ Not published (no public download)")
log.info("  Flood hazard maps (extent/depth) ❌ Not published for download")
log.info("  Flood risk maps (FRA outputs)    ❌ Not published for download")
log.info("  Potentially Vulnerable Areas     ⚠️  Web viewer only (no WFS/download)")
log.info("  Flood zone web map tiles         ❌ Internal ArcGIS tiles, not rasters")
log.info("  open.sepa.org.uk                 ❌ UNREACHABLE (cyberattack victim)")
log.info("")
log.info("Future monitoring points:")
log.info("  1. spatialdata.gov.scot — Scottish SSDI for FRMP 2022-2028 FRA outputs")
log.info("  2. beta.sepa.scot — SEPA's rebuilt data services platform")
log.info("  3. SEPA Flood Risk Management Plans cycle 2 completion (expected ~2027)")
log.info("")
log.info("When SEPA publishes raster data, implement using Apache Sedona RS_* functions:")
log.info("  - RS_FromGeoTiff / RS_FromNetCDF for raster loading")
log.info("  - RS_BandCount / RS_Width / RS_Height for metadata")
log.info("  - RS_Envelope / RS_ConvexHull for spatial extent")
log.info("  - Write to au_flood_raster_catalog with _source = 'SEPA_RISK'")
log.info("")
log.info("Contrast with available UK raster data:")
log.info("  - EA (England): modelled flood depth grids (ea_depth pipeline)")
log.info("  - NRW (Wales) : flood zone polygons via WFS (vector, not raster)")
log.info("  - SEPA (Scotland): ❌ none currently")
log.info("=" * 60)

# Ensure au_flood_raster_catalog exists (shared table — EA depth pipeline writes to it)
try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FQN_RASTER_CATALOG} (
            raster_id                   STRING     NOT NULL  COMMENT 'SHA256-derived unique identifier for this raster file.',
            resource_id                 STRING               COMMENT 'Source dataset/resource identifier.',
            volume_path                 STRING               COMMENT 'ADLS Unity Catalog Volume path to the raster file.',
            original_filename           STRING               COMMENT 'Original filename of the raster.',
            file_format                 STRING               COMMENT 'File format: TIF | ASC | NC | GRD.',
            file_size_bytes             LONG                 COMMENT 'File size in bytes.',
            source_state                STRING               COMMENT 'Region covered: NSW | QLD | England | Wales | Scotland.',
            source_agency               STRING               COMMENT 'Agency that produced the data (e.g. SEPA).',
            study_name                  STRING               COMMENT 'Study or dataset name.',
            project_id                  STRING               COMMENT 'Source project or dataset UUID.',
            council_lga_primary         STRING               COMMENT 'Primary council or LGA (if applicable).',
            inferred_aep_pct            DOUBLE               COMMENT 'Inferred Annual Exceedance Probability percentage.',
            inferred_return_period_yr   INT                  COMMENT 'Inferred return period in years.',
            value_type                  STRING               COMMENT 'Raster value type: depth | wse | extent | risk.',
            crs_epsg                    INT                  COMMENT 'EPSG code of native raster CRS.',
            band_count                  INT                  COMMENT 'Number of raster bands.',
            width_pixels                INT                  COMMENT 'Raster width in pixels.',
            height_pixels               INT                  COMMENT 'Raster height in pixels.',
            cell_size_x                 DOUBLE               COMMENT 'Cell width in native CRS units.',
            cell_size_y                 DOUBLE               COMMENT 'Cell height in native CRS units.',
            min_value                   DOUBLE               COMMENT 'Minimum raster value (non-nodata).',
            max_value                   DOUBLE               COMMENT 'Maximum raster value (non-nodata).',
            nodata_value                DOUBLE               COMMENT 'No-data value.',
            native_envelope_wkt         STRING               COMMENT 'Bounding box in native CRS as WKT.',
            wgs84_bbox_min_lon          DOUBLE               COMMENT 'WGS84 bounding box min longitude.',
            wgs84_bbox_min_lat          DOUBLE               COMMENT 'WGS84 bounding box min latitude.',
            wgs84_bbox_max_lon          DOUBLE               COMMENT 'WGS84 bounding box max longitude.',
            wgs84_bbox_max_lat          DOUBLE               COMMENT 'WGS84 bounding box max latitude.',
            ingest_timestamp            TIMESTAMP            COMMENT 'UTC timestamp when this row was written.',
            _source                     STRING               COMMENT 'Source system: NSW_SES | BCC | EA_DEPTH | EA_RISK | NRW_WALES | SEPA_RISK.',
            _state                      STRING               COMMENT 'Country/state: NSW | QLD | England | Wales | Scotland.'
        )
        USING DELTA
        COMMENT 'Gold: shared multi-source raster catalog. Each source writes via _source-scoped refresh. SEPA_RISK rows will be added here when SEPA publishes flood risk rasters.'
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
    log.info("SEPA Raster Ingest — placeholder complete (0 rasters ingested — expected)")

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"SEPA raster ingest failed unexpectedly: {exc}", exc_info=True)
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
                "SEPA Scotland does not publish flood risk raster data for public download. "
                "Root cause: December 2020 SEPA cyberattack destroyed data publication "
                "infrastructure. Recovery is ongoing. "
                "Monitor spatialdata.gov.scot and beta.sepa.scot for future availability. "
                "When rasters are available, ingest using Apache Sedona RS_* functions "
                "and write to au_flood_raster_catalog with _source = 'SEPA_RISK'."
            ),
            "pipeline_type":     "monitoring_and_tabular",
            "data_sources":      ["kiwis_stations", "kiwis_readings", "rss_warnings"],
            "future_raster_sources": [
                "spatialdata.gov.scot — FRMP cycle 2 FRA outputs (expected ~2027)",
                "beta.sepa.scot — SEPA rebuilt data services",
                "map.sepa.org.uk/floodmaps/ — WFS/WMS when available",
            ],
            "sedona_raster_functions_to_use": [
                "RS_FromGeoTiff", "RS_BandCount", "RS_Width", "RS_Height",
                "RS_Envelope", "RS_ConvexHull", "RS_MinValues", "RS_MaxValues",
            ],
            "target_gold_table_source_value": "SEPA_RISK",
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
