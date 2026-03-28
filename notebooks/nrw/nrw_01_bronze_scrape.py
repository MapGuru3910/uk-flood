# Databricks notebook source
# NRW Wales Flood Risk Products — Bronze Scrape
#
# Purpose : Fetches flood zone and risk layer features from the Natural Resources
#           Wales (NRW) WFS service on DataMapWales and appends them to a bronze
#           Delta table with full history.
#
#           WFS endpoint: https://datamap.gov.wales/geoserver/inspire-nrw/wfs
#           Auth: None — publicly accessible
#           CRS:  EPSG:27700 (British National Grid) — same as EA risk products
#           Protocol: OGC WFS 2.0.0
#
# Key layers ingested (20 confirmed live layers):
#   - NRW_FLOODZONE_RIVERS         — Flood zone polygons (rivers)
#   - NRW_FLOODZONE_SEAS           — Flood zone polygons (seas)
#   - NRW_FLOODZONE_RIVERS_SEAS_MERGED — Combined flood zones
#   - NRW_FLOODZONE_SURFACE_WATER_AND_SMALL_WATERCOURSES
#   - NRW_FLOOD_RISK_FROM_RIVERS   — Risk of flooding (rivers)
#   - NRW_FLOOD_RISK_FROM_SEA      — Risk of flooding (sea)
#   - NRW_FLOOD_RISK_FROM_SURFACE_WATER_SMALL_WATERCOURSES
#   - NRW_FLOOD_WARNING            — Flood warning area polygons
#   - NRW_AREA_BENEFITING_FROM_FLOOD_DEFENCE
#   - NRW_FLOOD_RISK_AREAS         — Floods Directive risk areas
#   - NRW_FLOODMAP_FLOOD_STORAGE
#   - NRW_NATIONAL_FLOOD_RISK_RIVER_PEOPLE (+ ECONOMIC, ENVIRO)
#   - NRW_NATIONAL_FLOOD_RISK_SEA_PEOPLE (+ ECONOMIC, ENVIRO)
#   - NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_PEOPLE (+ ECON, ENVIRO)
#
# Bilingual data: NRW layers include Welsh (Cymraeg) fields with _cy suffix
# (e.g. risk / risk_cy). Both are stored; English is canonical for analytics.
#
# CRS note: WFS GeoJSON output returns coordinates in the NATIVE CRS (EPSG:27700).
# Values will be in the 100,000–700,000 range (eastings/northings), NOT degrees.
# Reprojection to WGS84 is performed in silver using Sedona ST_Transform.
#
# Pagination: WFS 2.0.0 supports count/startIndex. Use count=5000 batches.
#
# Layer   : Bronze  (ceg_delta_bronze_prnd.international_flood.nrw_flood_layers_scrape)
# Outputs : ceg_delta_bronze_prnd.international_flood.nrw_flood_layers_scrape
#           ceg_delta_bronze_prnd.international_flood.pipeline_run_log
#
# Version : 1.0.0

# COMMAND ----------

# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text(
    "run_id", "",
    "Run ID — leave blank to auto-generate a UUID"
)
dbutils.widgets.text(
    "scrape_version", "1.0.0",
    "Scrape Version — semantic version tag for this notebook"
)
dbutils.widgets.text(
    "page_size", "5000",
    "WFS Page Size — features per GetFeature request (count parameter)"
)
dbutils.widgets.text(
    "layer_filter", "",
    "Layer Filter — comma-separated layer names to scrape (blank = all 20 layers)"
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

# ── Catalog / table paths ──────────────────────────────────────────────────
BRONZE_CATALOG = "ceg_delta_bronze_prnd"
BRONZE_SCHEMA  = "international_flood"
AUDIT_TABLE    = "pipeline_run_log"

FQN_AUDIT      = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{AUDIT_TABLE}"
FQN_NRW_LAYERS = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.nrw_flood_layers_scrape"

# ── Source constants ───────────────────────────────────────────────────────
WFS_BASE         = "https://datamap.gov.wales/geoserver/inspire-nrw/wfs"
WFS_NAMESPACE    = "inspire-nrw"
SOURCE_NAME      = "NRW_WALES"
STATE            = "Wales"
NOTEBOOK_NAME    = "nrw_01_bronze_scrape"
NOTEBOOK_VERSION = "1.0.0"
USER_AGENT       = "uk-flood-pipeline/1.0.0 (+https://github.com/MapGuru3910/uk-flood)"

# ── All 20 confirmed NRW flood layers ─────────────────────────────────────
ALL_NRW_LAYERS = [
    "NRW_FLOOD_WARNING",
    "NRW_FLOODZONE_RIVERS",
    "NRW_FLOODZONE_SEAS",
    "NRW_FLOODZONE_RIVERS_SEAS_MERGED",
    "NRW_FLOODZONE_SURFACE_WATER_AND_SMALL_WATERCOURSES",
    "NRW_FLOOD_RISK_FROM_RIVERS",
    "NRW_FLOOD_RISK_FROM_SEA",
    "NRW_FLOOD_RISK_FROM_SURFACE_WATER_SMALL_WATERCOURSES",
    "NRW_AREA_BENEFITING_FROM_FLOOD_DEFENCE",
    "NRW_FLOOD_RISK_AREAS",
    "NRW_FLOODMAP_FLOOD_STORAGE",
    "NRW_NATIONAL_FLOOD_RISK_RIVER_PEOPLE",
    "NRW_NATIONAL_FLOOD_RISK_RIVER_ECONOMIC",
    "NRW_NATIONAL_FLOOD_RISK_RIVER_ENVIRO",
    "NRW_NATIONAL_FLOOD_RISK_SEA_PEOPLE",
    "NRW_NATIONAL_FLOOD_RISK_SEA_ECONOMIC",
    "NRW_NATIONAL_FLOOD_RISK_SEA_ENVIRO",
    "NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_PEOPLE",
    "NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_ECON",
    "NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_ENVIRO",
]

# ── HTTP tuning ────────────────────────────────────────────────────────────
REQUEST_TIMEOUT    = 120  # seconds — large WFS responses can be slow
MAX_RETRIES        = 3
RETRY_BACKOFF_BASE = 3.0  # slightly longer for GeoServer under load
RETRY_JITTER       = 1.0

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

RUN_ID         = dbutils.widgets.get("run_id").strip() or str(uuid.uuid4())
SCRAPE_VERSION = dbutils.widgets.get("scrape_version").strip() or NOTEBOOK_VERSION
PAGE_SIZE      = int(dbutils.widgets.get("page_size") or 5000)
_layer_raw     = dbutils.widgets.get("layer_filter").strip()
LAYER_FILTER   = [l.strip().upper() for l in _layer_raw.split(",") if l.strip()] if _layer_raw else []
SCRAPED_AT     = datetime.now(timezone.utc)

LAYERS_TO_SCRAPE = [l for l in ALL_NRW_LAYERS if not LAYER_FILTER or l in LAYER_FILTER]

log.info("=" * 60)
log.info(f"NRW Wales Bronze Scrape  |  Run ID: {RUN_ID}")
log.info(f"Notebook version  : {NOTEBOOK_VERSION}")
log.info(f"Scrape version    : {SCRAPE_VERSION}")
log.info(f"WFS page size     : {PAGE_SIZE}")
log.info(f"Layers to scrape  : {len(LAYERS_TO_SCRAPE)} of {len(ALL_NRW_LAYERS)}")
log.info(f"Layer filter      : {LAYER_FILTER or 'ALL'}")
log.info(f"Started at        : {SCRAPED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Schema Definitions

# COMMAND ----------

NRW_LAYERS_SCHEMA = StructType([
    StructField("layer_name",       StringType(),    False),  # e.g. NRW_FLOODZONE_RIVERS
    StructField("feature_id",       StringType(),    True),   # WFS feature @id
    StructField("objectid",         IntegerType(),   True),   # Integer PK from GeoServer
    StructField("properties_json",  StringType(),    True),   # All feature properties as JSON
    StructField("geometry_json",    StringType(),    True),   # GeoJSON geometry (BNG coords)
    StructField("geometry_type",    StringType(),    True),   # MultiPolygon | Polygon etc.
    StructField("feature_count",    LongType(),      True),   # Total features in this layer (from response)
    StructField("_run_id",          StringType(),    False),
    StructField("_ingested_at",     TimestampType(), False),
    StructField("_scrape_version",  StringType(),    False),
    StructField("_is_current",      BooleanType(),   False),
    StructField("_source",          StringType(),    False),
    StructField("_state",           StringType(),    False),
])

# COMMAND ----------

# MAGIC %md ## 5. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_NRW_LAYERS} (
    layer_name      STRING     NOT NULL  COMMENT 'WFS layer name (e.g. NRW_FLOODZONE_RIVERS)',
    feature_id      STRING               COMMENT 'WFS feature @id (e.g. NRW_FLOODZONE_RIVERS.1)',
    objectid        INT                  COMMENT 'GeoServer integer primary key',
    properties_json STRING               COMMENT 'All feature properties serialized as JSON (preserves bilingual fields)',
    geometry_json   STRING               COMMENT 'GeoJSON geometry in native EPSG:27700 (BNG) coordinates',
    geometry_type   STRING               COMMENT 'GeoJSON geometry type: MultiPolygon | Polygon',
    feature_count   LONG                 COMMENT 'Total feature count for this layer in the WFS response',
    _run_id         STRING     NOT NULL,
    _ingested_at    TIMESTAMP  NOT NULL,
    _scrape_version STRING     NOT NULL,
    _is_current     BOOLEAN    NOT NULL  COMMENT 'TRUE for most recent record per (layer_name, objectid)',
    _source         STRING     NOT NULL  COMMENT 'Always NRW_WALES',
    _state          STRING     NOT NULL  COMMENT 'Always Wales'
)
USING DELTA
COMMENT 'Bronze: append-only scrape of NRW Wales WFS flood layers. All 20 flood layers stored in one table, distinguished by layer_name. Use _is_current=TRUE for current state.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'layer_name,_is_current'
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_AUDIT} (
    run_id              STRING     NOT NULL,
    pipeline_stage      STRING     NOT NULL,
    notebook_name       STRING,
    notebook_version    STRING,
    start_time          TIMESTAMP  NOT NULL,
    end_time            TIMESTAMP,
    duration_seconds    DOUBLE,
    status              STRING,
    rows_read           LONG,
    rows_written        LONG,
    rows_merged         LONG,
    rows_rejected       LONG,
    error_message       STRING,
    extra_metadata      STRING,
    databricks_job_id   STRING,
    databricks_run_id   STRING,
    scrape_version      STRING
)
USING DELTA
COMMENT 'Pipeline audit log.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# COMMAND ----------

# MAGIC %md ## 6. HTTP Utilities

# COMMAND ----------

class ScrapeError(Exception):
    pass


def http_get(url: str, params: dict = None, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    """HTTP GET with exponential backoff retry. SSL ENABLED for DataMapWales."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout, verify=True, headers=headers)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                sleep_s = (RETRY_BACKOFF_BASE ** attempt) + random.uniform(0, RETRY_JITTER)
                log.warning(
                    f"HTTP attempt {attempt}/{MAX_RETRIES} failed [{url[:80]}]: {exc}. "
                    f"Retrying in {sleep_s:.1f}s"
                )
                time.sleep(sleep_s)
    raise ScrapeError(f"All {MAX_RETRIES} HTTP attempts failed for [{url[:80]}]: {last_exc}") from last_exc

# COMMAND ----------

# MAGIC %md ## 7. WFS Fetch Functions

# COMMAND ----------

def fetch_wfs_layer(layer_name: str, page_size: int) -> list:
    """
    Fetches all features for a WFS layer using paginated GetFeature requests.

    Uses WFS 2.0.0 with GeoJSON output. Pagination via count/startIndex.
    Coordinates are in EPSG:27700 (BNG) — NOT WGS84.

    Returns list of flat dicts ready for bronze table.
    """
    type_name = f"{WFS_NAMESPACE}:{layer_name}"
    all_rows = []
    start_index = 0
    total_features_hint = None

    while True:
        params = {
            "service":      "WFS",
            "version":      "2.0.0",
            "request":      "GetFeature",
            "typeName":     type_name,
            "outputFormat": "application/json",
            "count":        page_size,
            "startIndex":   start_index,
        }

        try:
            resp = http_get(WFS_BASE, params=params)
        except ScrapeError as exc:
            log.error(f"  [{layer_name}] WFS fetch failed at startIndex={start_index}: {exc}")
            break

        try:
            fc = resp.json()
        except ValueError as exc:
            log.error(f"  [{layer_name}] JSON parse failed at startIndex={start_index}: {exc}")
            break

        features = fc.get("features", [])
        if not features:
            log.info(f"  [{layer_name}] No more features at startIndex={start_index} — done")
            break

        # Extract totalFeatures hint from GeoServer response (may not always be present)
        if total_features_hint is None:
            total_features_hint = fc.get("totalFeatures") or fc.get("numberMatched") or None

        for feat in features:
            props = feat.get("properties") or {}
            geom  = feat.get("geometry")
            all_rows.append({
                "layer_name":      layer_name,
                "feature_id":      feat.get("id", ""),
                "objectid":        props.get("objectid"),
                "properties_json": json.dumps(props),
                "geometry_json":   json.dumps(geom) if geom else None,
                "geometry_type":   geom.get("type", "") if geom else None,
                "feature_count":   total_features_hint,
            })

        log.info(
            f"  [{layer_name}] Fetched {len(all_rows)} features so far "
            f"(page size={len(features)}, startIndex={start_index})"
        )

        if len(features) < page_size:
            break  # Last page
        start_index += page_size
        # Polite delay between WFS pages
        time.sleep(0.5)

    return all_rows

# COMMAND ----------

# MAGIC %md ## 8. Bronze Write

# COMMAND ----------

def write_layer_bronze(rows: list, layer_name: str, run_id: str,
                       scraped_at, scrape_version: str) -> int:
    """
    Annotates rows with provenance, expires superseded _is_current records
    for (layer_name, objectid), and appends to the bronze table.
    """
    if not rows:
        log.warning(f"  [{layer_name}] No features to write")
        return 0

    for row in rows:
        row["_run_id"]         = run_id
        row["_ingested_at"]    = scraped_at
        row["_scrape_version"] = scrape_version
        row["_is_current"]     = True
        row["_source"]         = SOURCE_NAME
        row["_state"]          = STATE

    pdf = pd.DataFrame(rows)
    col_names = [f.name for f in NRW_LAYERS_SCHEMA.fields]
    for col in col_names:
        if col not in pdf.columns:
            pdf[col] = None
    pdf = pdf[col_names]

    sdf = spark.createDataFrame(pdf, schema=NRW_LAYERS_SCHEMA)

    # Expire previous _is_current rows for this layer's objectids
    objectids = [r["objectid"] for r in rows if r.get("objectid") is not None]
    if objectids:
        oid_df = spark.createDataFrame([(int(o),) for o in objectids], ["objectid"])
        oid_df.createOrReplaceTempView("_current_run_oids")
        spark.sql(f"""
            UPDATE {FQN_NRW_LAYERS}
            SET    _is_current = FALSE
            WHERE  layer_name  = '{layer_name}'
              AND  objectid IN (SELECT objectid FROM _current_run_oids)
              AND  _is_current = TRUE
              AND  _run_id != '{run_id}'
        """)

    sdf.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(FQN_NRW_LAYERS)
    log.info(f"  [{layer_name}] {len(rows)} rows appended to {FQN_NRW_LAYERS}")
    return len(rows)

# COMMAND ----------

# MAGIC %md ## 9. Audit Logging

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
            "scrape_version": scrape_version,
        }]
        spark.createDataFrame(record).write.format("delta").mode("append").saveAsTable(FQN_AUDIT)
    except Exception as exc:
        log.error(f"write_audit failed (non-fatal): {exc}")

# COMMAND ----------

# MAGIC %md ## 10. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None
_layer_counts    = {}
_total_errors    = 0

try:
    log.info(f"Starting NRW Wales bronze scrape | run_id={RUN_ID}")
    log.info(f"Scraping {len(LAYERS_TO_SCRAPE)} layers: {LAYERS_TO_SCRAPE}")

    for layer_name in LAYERS_TO_SCRAPE:
        log.info(f"Scraping layer: {layer_name}")
        try:
            rows = fetch_wfs_layer(layer_name, PAGE_SIZE)
            written = write_layer_bronze(rows, layer_name, RUN_ID, SCRAPED_AT, SCRAPE_VERSION)
            _layer_counts[layer_name] = written
        except Exception as exc:
            log.error(f"Layer {layer_name} failed: {exc}", exc_info=True)
            _layer_counts[layer_name] = -1
            _total_errors += 1
            # Continue with remaining layers — partial success is better than full failure

    total_written = sum(v for v in _layer_counts.values() if v > 0)
    _pipeline_status = "success" if _total_errors == 0 else "partial"

    log.info(f"NRW scrape complete | total_written={total_written} errors={_total_errors}")
    for layer, count in _layer_counts.items():
        status_str = str(count) if count >= 0 else "FAILED"
        log.info(f"  {layer}: {status_str} rows")

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Pipeline failed: {exc}", exc_info=True)
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "bronze",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_written     = sum(v for v in _layer_counts.values() if v > 0),
        rows_rejected    = _total_errors,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "layers_scraped":  len(LAYERS_TO_SCRAPE),
            "layers_failed":   _total_errors,
            "layer_counts":    _layer_counts,
            "page_size":       PAGE_SIZE,
            "layer_filter":    LAYER_FILTER or "ALL",
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
