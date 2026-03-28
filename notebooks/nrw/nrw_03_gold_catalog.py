# Databricks notebook source
# NRW Wales Flood Risk Products — Gold Catalog
#
# Purpose : Reads NRW silver tables and produces gold-layer catalogs:
#             - nrw_flood_zone_catalog         — flood zone polygons with risk classification
#             - nrw_flood_risk_catalog         — 9-category risk assessment
#             - nrw_flood_warning_area_catalog — warning area definitions with FWIS codes
#
# Write strategy: Source-scoped DELETE + INSERT for shared tables.
# NRW-specific catalogs use full overwrite (current snapshot each run).
#
# Layer   : Gold  (ceg_delta_gold_prnd.international_flood.*)
# Reads   : ceg_delta_silver_prnd.international_flood.nrw_*
# Writes  : ceg_delta_gold_prnd.international_flood.nrw_flood_zone_catalog
#           ceg_delta_gold_prnd.international_flood.nrw_flood_risk_catalog
#           ceg_delta_gold_prnd.international_flood.nrw_flood_warning_area_catalog
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
FQN_S_FLOOD_ZONES      = f"{SILVER_CATALOG}.{SCHEMA}.nrw_flood_zones"
FQN_S_FLOOD_RISK       = f"{SILVER_CATALOG}.{SCHEMA}.nrw_flood_risk_areas"
FQN_S_FLOOD_WARNINGS   = f"{SILVER_CATALOG}.{SCHEMA}.nrw_flood_warning_areas"
FQN_S_FLOOD_DEFENCES   = f"{SILVER_CATALOG}.{SCHEMA}.nrw_flood_defences"

FQN_G_ZONE_CATALOG     = f"{GOLD_CATALOG}.{SCHEMA}.nrw_flood_zone_catalog"
FQN_G_RISK_CATALOG     = f"{GOLD_CATALOG}.{SCHEMA}.nrw_flood_risk_catalog"
FQN_G_WARNING_CATALOG  = f"{GOLD_CATALOG}.{SCHEMA}.nrw_flood_warning_area_catalog"

NOTEBOOK_NAME    = "nrw_03_gold_catalog"
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

RUN_ID        = dbutils.widgets.get("run_id").strip()        or str(uuid.uuid4())
SILVER_RUN_ID = dbutils.widgets.get("silver_run_id").strip() or None
FULL_REFRESH  = dbutils.widgets.get("full_refresh").lower() == "true"
PROCESSED_AT  = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"NRW Wales Gold Catalog  |  Run ID: {RUN_ID}")
log.info(f"Silver run ID : {SILVER_RUN_ID or 'latest'}")
log.info(f"Full refresh  : {FULL_REFRESH}")
log.info(f"Started at    : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_G_ZONE_CATALOG} (
    layer_name              STRING     NOT NULL  COMMENT 'WFS source layer name',
    objectid                INT        NOT NULL  COMMENT 'GeoServer PK',
    feature_id              STRING,
    model_id                STRING               COMMENT 'Flood map model identifier',
    publication_date        DATE                 COMMENT 'Publication date of this feature version',
    risk_category           STRING               COMMENT 'Risk level or zone designation (English)',
    risk_category_cy        STRING               COMMENT 'Risk level in Welsh (Cymraeg)',
    layer_category          STRING               COMMENT 'Simplified category: fluvial_zone | coastal_zone | surface_water_zone | combined_zone | flood_storage | risk_area',
    flood_zone_number       STRING               COMMENT 'Derived EA-equivalent flood zone: FZ1 | FZ2 | FZ3',
    geometry_wkt_bng        STRING               COMMENT 'WKT polygon in EPSG:27700 (BNG)',
    geometry_wkt_wgs84      STRING               COMMENT 'WKT polygon in EPSG:4326 (WGS84)',
    _gold_processed_at      TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Gold: NRW Wales flood zone polygons (all zone layers combined). Full overwrite each run.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_G_RISK_CATALOG} (
    layer_name              STRING     NOT NULL,
    objectid                INT        NOT NULL,
    feature_id              STRING,
    risk_category           STRING               COMMENT 'Risk level classification',
    risk_source             STRING               COMMENT 'rivers | sea | surface_water',
    risk_receptor           STRING               COMMENT 'people | economic | environmental',
    geometry_wkt_bng        STRING,
    geometry_wkt_wgs84      STRING,
    _gold_processed_at      TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Gold: NRW Wales flood risk assessment catalog — 9 categories (rivers/sea/SW × people/econ/enviro). Full overwrite each run.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_G_WARNING_CATALOG} (
    objectid                INT        NOT NULL,
    fwis_code               STRING               COMMENT 'FWIS code — the canonical flood warning area identifier',
    feature_id              STRING,
    region                  STRING               COMMENT 'NRW region name',
    area_name               STRING,
    ta_code                 STRING               COMMENT 'Flood Warning Direct TA code',
    warning_area_name       STRING,
    description             STRING,
    geometry_wkt_bng        STRING,
    geometry_wkt_wgs84      STRING,
    ea_equivalent_notation  STRING               COMMENT 'Analogous to EA floodArea notation — use fwis_code for cross-source joins',
    _gold_processed_at      TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Gold: NRW Wales flood warning area definitions. FWIS codes are the canonical identifiers for cross-source analysis. Full overwrite each run.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# COMMAND ----------

# MAGIC %md ## 5. Build Flood Zone Catalog

# COMMAND ----------

zone_gold_df = spark.sql(f"""
    SELECT
        layer_name,
        objectid,
        feature_id,
        model_id,
        publication_date,
        risk_category,
        risk_category_cy,
        layer_category,
        -- Derive EA-equivalent flood zone number from risk_category
        CASE
            WHEN LOWER(risk_category) LIKE '%zone 3%' OR LOWER(risk_category) LIKE '%parth 3%'   THEN 'FZ3'
            WHEN LOWER(risk_category) LIKE '%zone 2%' OR LOWER(risk_category) LIKE '%parth 2%'   THEN 'FZ2'
            WHEN LOWER(risk_category) LIKE '%zone 1%' OR LOWER(risk_category) LIKE '%parth 1%'   THEN 'FZ1'
            WHEN LOWER(layer_category) IN ('flood_storage', 'risk_area')                          THEN NULL
            ELSE NULL
        END                                             AS flood_zone_number,
        geometry_wkt_bng,
        geometry_wkt_wgs84,
        CURRENT_TIMESTAMP()                             AS _gold_processed_at,
        _source,
        _state
    FROM {FQN_S_FLOOD_ZONES}
    WHERE _is_current = TRUE
""")

_zone_count = zone_gold_df.count()
log.info(f"Flood zone catalog: {_zone_count} rows assembled")

zone_gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FQN_G_ZONE_CATALOG)
log.info(f"Flood zone catalog: written to {FQN_G_ZONE_CATALOG}")

spark.sql(f"OPTIMIZE {FQN_G_ZONE_CATALOG} ZORDER BY (layer_category, risk_category)")

# COMMAND ----------

# MAGIC %md ## 6. Build Flood Risk Catalog

# COMMAND ----------

risk_gold_df = spark.sql(f"""
    SELECT
        layer_name,
        objectid,
        feature_id,
        risk_category,
        risk_source,
        risk_receptor,
        geometry_wkt_bng,
        geometry_wkt_wgs84,
        CURRENT_TIMESTAMP() AS _gold_processed_at,
        _source,
        _state
    FROM {FQN_S_FLOOD_RISK}
    WHERE _is_current = TRUE
""")

_risk_count = risk_gold_df.count()
log.info(f"Flood risk catalog: {_risk_count} rows assembled")

risk_gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FQN_G_RISK_CATALOG)
log.info(f"Flood risk catalog: written to {FQN_G_RISK_CATALOG}")

spark.sql(f"OPTIMIZE {FQN_G_RISK_CATALOG} ZORDER BY (risk_source, risk_receptor)")

# COMMAND ----------

# MAGIC %md ## 7. Build Warning Area Catalog

# COMMAND ----------

warning_gold_df = spark.sql(f"""
    SELECT
        objectid,
        fwis_code,
        feature_id,
        region,
        area_name,
        ta_code,
        warning_area_name,
        description,
        geometry_wkt_bng,
        geometry_wkt_wgs84,
        -- ea_equivalent: FWIS code is analogous to EA's flood area notation
        fwis_code                   AS ea_equivalent_notation,
        CURRENT_TIMESTAMP()         AS _gold_processed_at,
        _source,
        _state
    FROM {FQN_S_FLOOD_WARNINGS}
    WHERE _is_current = TRUE
""")

_warning_count = warning_gold_df.count()
log.info(f"Warning area catalog: {_warning_count} rows assembled")

warning_gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FQN_G_WARNING_CATALOG)
log.info(f"Warning area catalog: written to {FQN_G_WARNING_CATALOG}")

spark.sql(f"OPTIMIZE {FQN_G_WARNING_CATALOG} ZORDER BY (region, fwis_code)")

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
    except Exception as exc:
        log.error(f"write_audit failed (non-fatal): {exc}")

# COMMAND ----------

# MAGIC %md ## 9. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None

try:
    log.info("NRW Wales Gold Catalog pipeline complete")
    log.info(f"  nrw_flood_zone_catalog         : {_zone_count} rows")
    log.info(f"  nrw_flood_risk_catalog         : {_risk_count} rows")
    log.info(f"  nrw_flood_warning_area_catalog : {_warning_count} rows")
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
        rows_written     = _zone_count + _risk_count + _warning_count,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "flood_zone_catalog_rows":      _zone_count,
            "flood_risk_catalog_rows":      _risk_count,
            "warning_area_catalog_rows":    _warning_count,
            "silver_run_id":                SILVER_RUN_ID,
            "full_refresh":                 FULL_REFRESH,
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
