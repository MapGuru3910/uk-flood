# Databricks notebook source
# NRW Wales Flood Risk Products — Silver Normalisation
#
# Purpose : Reads NRW bronze WFS scrape records and normalises them into
#           typed silver Delta tables by data category:
#             - nrw_flood_zones         (MERGE on (layer_name, objectid))
#             - nrw_flood_risk_areas    (MERGE on (layer_name, objectid))
#             - nrw_flood_warning_areas (MERGE on fwis_code)
#             - nrw_flood_defences      (MERGE on (layer_name, objectid))
#
# CRS handling:
#   - WFS features arrive in EPSG:27700 (British National Grid)
#   - Coordinates in geometry_json are BNG eastings/northings (not degrees)
#   - ST_GeomFromGeoJSON creates geometry in EPSG:27700 (GeoServer default)
#   - ST_Transform(geom, 'EPSG:27700', 'EPSG:4326') reprojects to WGS84
#   - Both BNG WKT and WGS84 WKT are stored in silver
#
# Bilingual fields:
#   - risk (English) and risk_cy (Welsh) are both stored
#   - English fields are canonical for analytics joins
#
# Sedona note: ST_GeomFromGeoJSON + ST_Transform + ST_AsText are used for
# geometry parsing. Sedona must be installed as a cluster library.
#
# Layer   : Silver  (ceg_delta_silver_prnd.international_flood.nrw_*)
# Reads   : ceg_delta_bronze_prnd.international_flood.nrw_flood_layers_scrape
# Writes  : ceg_delta_silver_prnd.international_flood.nrw_flood_zones
#           ceg_delta_silver_prnd.international_flood.nrw_flood_risk_areas
#           ceg_delta_silver_prnd.international_flood.nrw_flood_warning_areas
#           ceg_delta_silver_prnd.international_flood.nrw_flood_defences
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
    "bronze_run_id", "",
    "Bronze Run ID — the _run_id from the bronze scrape to process. Blank = latest run."
)
dbutils.widgets.dropdown(
    "full_refresh", "false", ["true", "false"],
    "Full Refresh — if true, rebuilds all silver tables from complete bronze history."
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType,
    LongType, StringType, StructField, StructType, TimestampType,
)

# Sedona context — required for ST_* spatial functions
from sedona.spark import SedonaContext

# ── Catalog / table paths ──────────────────────────────────────────────────
BRONZE_CATALOG = "ceg_delta_bronze_prnd"
SILVER_CATALOG = "ceg_delta_silver_prnd"
SCHEMA         = "international_flood"

FQN_AUDIT              = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
FQN_B_LAYERS           = f"{BRONZE_CATALOG}.{SCHEMA}.nrw_flood_layers_scrape"

FQN_S_FLOOD_ZONES      = f"{SILVER_CATALOG}.{SCHEMA}.nrw_flood_zones"
FQN_S_FLOOD_RISK       = f"{SILVER_CATALOG}.{SCHEMA}.nrw_flood_risk_areas"
FQN_S_FLOOD_WARNINGS   = f"{SILVER_CATALOG}.{SCHEMA}.nrw_flood_warning_areas"
FQN_S_FLOOD_DEFENCES   = f"{SILVER_CATALOG}.{SCHEMA}.nrw_flood_defences"

NOTEBOOK_NAME    = "nrw_02_silver_normalize"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "NRW_WALES"
STATE            = "Wales"

# ── Layer classification ───────────────────────────────────────────────────
# Map each WFS layer to a category and target silver table
FLOOD_ZONE_LAYERS = {
    "NRW_FLOODZONE_RIVERS",
    "NRW_FLOODZONE_SEAS",
    "NRW_FLOODZONE_RIVERS_SEAS_MERGED",
    "NRW_FLOODZONE_SURFACE_WATER_AND_SMALL_WATERCOURSES",
    "NRW_FLOOD_RISK_AREAS",
    "NRW_FLOODMAP_FLOOD_STORAGE",
}
FLOOD_RISK_LAYERS = {
    "NRW_FLOOD_RISK_FROM_RIVERS",
    "NRW_FLOOD_RISK_FROM_SEA",
    "NRW_FLOOD_RISK_FROM_SURFACE_WATER_SMALL_WATERCOURSES",
    "NRW_NATIONAL_FLOOD_RISK_RIVER_PEOPLE",
    "NRW_NATIONAL_FLOOD_RISK_RIVER_ECONOMIC",
    "NRW_NATIONAL_FLOOD_RISK_RIVER_ENVIRO",
    "NRW_NATIONAL_FLOOD_RISK_SEA_PEOPLE",
    "NRW_NATIONAL_FLOOD_RISK_SEA_ECONOMIC",
    "NRW_NATIONAL_FLOOD_RISK_SEA_ENVIRO",
    "NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_PEOPLE",
    "NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_ECON",
    "NRW_NATIONAL_FLOOD_RISK_SURFACE_WATER_ENVIRO",
}
WARNING_AREA_LAYERS = {"NRW_FLOOD_WARNING"}
DEFENCE_LAYERS      = {"NRW_AREA_BENEFITING_FROM_FLOOD_DEFENCE"}

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
BRONZE_RUN_ID = dbutils.widgets.get("bronze_run_id").strip() or None
FULL_REFRESH  = dbutils.widgets.get("full_refresh").lower() == "true"
PROCESSED_AT  = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"NRW Wales Silver Normalisation  |  Run ID: {RUN_ID}")
log.info(f"Bronze run ID : {BRONZE_RUN_ID or 'latest'}")
log.info(f"Full refresh  : {FULL_REFRESH}")
log.info(f"Started at    : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Initialise Sedona Context

# COMMAND ----------

sedona = SedonaContext.create(spark)
log.info("Sedona context initialised")

# COMMAND ----------

# MAGIC %md ## 5. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.{SCHEMA}")

# ── nrw_flood_zones ────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_S_FLOOD_ZONES} (
    layer_name              STRING     NOT NULL  COMMENT 'WFS source layer (e.g. NRW_FLOODZONE_RIVERS)',
    objectid                INT        NOT NULL  COMMENT 'GeoServer integer PK — natural key with layer_name',
    feature_id              STRING               COMMENT 'WFS feature @id (e.g. NRW_FLOODZONE_RIVERS.1)',
    model_id                STRING               COMMENT 'mm_id — flood map model identifier',
    publication_date        DATE                 COMMENT 'pub_date — publication date of this feature',
    risk_category           STRING               COMMENT 'risk field (English) — risk level or zone designation',
    risk_category_cy        STRING               COMMENT 'risk_cy field (Welsh / Cymraeg)',
    layer_category          STRING               COMMENT 'Derived category: flood_zone | flood_storage | risk_area',
    geometry_wkt_bng        STRING               COMMENT 'Well-Known Text of polygon in EPSG:27700 (BNG)',
    geometry_wkt_wgs84      STRING               COMMENT 'Well-Known Text of polygon reprojected to EPSG:4326 (WGS84)',
    properties_json         STRING               COMMENT 'Full properties JSON — preserves all fields including bilingual',
    _is_current             BOOLEAN,
    _bronze_run_id          STRING,
    _silver_processed_at    TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Silver: NRW Wales flood zone polygons (rivers, seas, surface water, storage). MERGE upsert on (layer_name, objectid).'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'layer_name,risk_category'
)
""")

# ── nrw_flood_risk_areas ───────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_S_FLOOD_RISK} (
    layer_name              STRING     NOT NULL,
    objectid                INT        NOT NULL,
    feature_id              STRING,
    risk_category           STRING               COMMENT 'Flood risk classification (from properties)',
    risk_source             STRING               COMMENT 'Source of risk: rivers | sea | surface_water',
    risk_receptor           STRING               COMMENT 'Type of receptor: people | economic | environmental',
    geometry_wkt_bng        STRING               COMMENT 'WKT in EPSG:27700',
    geometry_wkt_wgs84      STRING               COMMENT 'WKT reprojected to EPSG:4326',
    properties_json         STRING               COMMENT 'Full properties JSON',
    _is_current             BOOLEAN,
    _bronze_run_id          STRING,
    _silver_processed_at    TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Silver: NRW Wales flood risk assessment areas (9 categories: rivers/sea/SW × people/economic/environmental). MERGE upsert on (layer_name, objectid).'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'layer_name,risk_source,risk_receptor'
)
""")

# ── nrw_flood_warning_areas ────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_S_FLOOD_WARNINGS} (
    objectid                INT        NOT NULL  COMMENT 'GeoServer PK',
    fwis_code               STRING               COMMENT 'Flood Warning Information Service code — natural key',
    feature_id              STRING,
    region                  STRING               COMMENT 'NRW region name',
    area_name               STRING               COMMENT 'Warning area name (area field)',
    ta_code                 STRING               COMMENT 'fwd_tacode — Flood Warning Direct TA code',
    warning_area_name       STRING               COMMENT 'fwa_name — Flood Warning Area full name',
    description             STRING               COMMENT 'Plain-text area description',
    geometry_wkt_bng        STRING,
    geometry_wkt_wgs84      STRING,
    properties_json         STRING,
    _is_current             BOOLEAN,
    _bronze_run_id          STRING,
    _silver_processed_at    TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Silver: NRW Wales flood warning area definitions. These are static polygon definitions, not live warning status. MERGE on fwis_code.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'fwis_code,region'
)
""")

# ── nrw_flood_defences ─────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_S_FLOOD_DEFENCES} (
    layer_name              STRING     NOT NULL,
    objectid                INT        NOT NULL,
    feature_id              STRING,
    geometry_wkt_bng        STRING,
    geometry_wkt_wgs84      STRING,
    properties_json         STRING               COMMENT 'Full properties JSON — defence-specific fields',
    _is_current             BOOLEAN,
    _bronze_run_id          STRING,
    _silver_processed_at    TIMESTAMP,
    _source                 STRING,
    _state                  STRING
)
USING DELTA
COMMENT 'Silver: NRW Wales areas benefiting from flood defences. MERGE on (layer_name, objectid).'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# COMMAND ----------

# MAGIC %md ## 6. Load Bronze Records

# COMMAND ----------

def _load_bronze(run_id_filter: Optional[str]) -> "pyspark.sql.DataFrame":
    if FULL_REFRESH or run_id_filter is None:
        df = spark.sql(f"SELECT * FROM {FQN_B_LAYERS} WHERE _is_current = TRUE")
    else:
        df = spark.sql(f"""
            SELECT * FROM {FQN_B_LAYERS}
            WHERE _run_id = '{run_id_filter}'
              AND _is_current = TRUE
        """)
    return df

# COMMAND ----------

# Resolve bronze run_id
if not FULL_REFRESH and not BRONZE_RUN_ID:
    row = spark.sql(f"""
        SELECT _run_id FROM {FQN_B_LAYERS}
        ORDER BY _ingested_at DESC LIMIT 1
    """).collect()
    _b_run_id = row[0]["_run_id"] if row else None
    log.info(f"Resolved latest bronze run_id: {_b_run_id}")
else:
    _b_run_id = BRONZE_RUN_ID
    log.info(f"Using supplied bronze run_id: {_b_run_id or '(all current)'}")

bronze_df = _load_bronze(_b_run_id)
bronze_df.cache()
total_bronze = bronze_df.count()
log.info(f"Loaded {total_bronze} bronze rows")

# COMMAND ----------

# MAGIC %md ## 7. Geometry Processing Utilities

# COMMAND ----------

def _process_geometries(df):
    """
    Adds geometry_wkt_bng and geometry_wkt_wgs84 columns using Sedona.

    Strategy:
      1. Parse geometry_json (BNG GeoJSON) → Sedona geometry object via ST_GeomFromGeoJSON
      2. Convert to WKT in native BNG: ST_AsText(geom) → geometry_wkt_bng
      3. Reproject to WGS84: ST_Transform(geom, 'EPSG:27700', 'EPSG:4326')
      4. Convert reprojected to WKT: ST_AsText(geom_wgs84) → geometry_wkt_wgs84

    Handles NULL geometry_json gracefully (outer CASE WHEN).
    """
    df.createOrReplaceTempView("_geom_input")
    return spark.sql("""
        SELECT
            *,
            CASE
                WHEN geometry_json IS NOT NULL AND geometry_json != 'null'
                THEN ST_AsText(ST_GeomFromGeoJSON(geometry_json))
                ELSE NULL
            END AS geometry_wkt_bng,
            CASE
                WHEN geometry_json IS NOT NULL AND geometry_json != 'null'
                THEN ST_AsText(
                    ST_Transform(
                        ST_GeomFromGeoJSON(geometry_json),
                        'EPSG:27700',
                        'EPSG:4326'
                    )
                )
                ELSE NULL
            END AS geometry_wkt_wgs84
        FROM _geom_input
    """)

# COMMAND ----------

# MAGIC %md ## 8. Property Extraction Utilities

# COMMAND ----------

def _extract_prop(props_json: str, key: str, default=None):
    """Extract a single property value from a JSON string, returning default on failure."""
    if not props_json:
        return default
    try:
        props = json.loads(props_json)
        return props.get(key, default)
    except (json.JSONDecodeError, TypeError):
        return default


def _derive_risk_source_receptor(layer_name: str) -> tuple:
    """
    Derives (risk_source, risk_receptor) from the layer name for flood risk layers.

    e.g. NRW_NATIONAL_FLOOD_RISK_RIVER_PEOPLE → ('rivers', 'people')
         NRW_FLOOD_RISK_FROM_SEA              → ('sea', None)
    """
    name = layer_name.upper()
    # Source
    if "RIVER" in name:
        source = "rivers"
    elif "SEA" in name:
        source = "sea"
    elif "SURFACE_WATER" in name or "SURFACE" in name:
        source = "surface_water"
    else:
        source = None
    # Receptor
    if "PEOPLE" in name:
        receptor = "people"
    elif "ECONOMIC" in name or "ECON" in name:
        receptor = "economic"
    elif "ENVIRO" in name:
        receptor = "environmental"
    else:
        receptor = None
    return source, receptor


def _derive_zone_category(layer_name: str) -> str:
    """Derives a simplified category label for flood zone layers."""
    name = layer_name.upper()
    if "STORAGE" in name:
        return "flood_storage"
    if "RISK_AREA" in name:
        return "risk_area"
    if "SURFACE_WATER" in name or "SMALL_WATERCO" in name:
        return "surface_water_zone"
    if "SEAS" in name or "SEA" in name:
        return "coastal_zone"
    if "RIVERS" in name:
        return "fluvial_zone"
    if "MERGED" in name:
        return "combined_zone"
    return "flood_zone"

# COMMAND ----------

# MAGIC %md ## 9. Process Flood Zones

# COMMAND ----------

_zones_written = 0

zone_df_raw = bronze_df.filter(F.col("layer_name").isin(list(FLOOD_ZONE_LAYERS)))
if zone_df_raw.count() > 0:
    zone_with_geom = _process_geometries(zone_df_raw)

    zone_prepared = spark.sql(f"""
        SELECT
            layer_name,
            objectid,
            feature_id,
            get_json_object(properties_json, '$.mm_id')   AS model_id,
            get_json_object(properties_json, '$.pub_date') AS publication_date_raw,
            get_json_object(properties_json, '$.risk')    AS risk_category,
            get_json_object(properties_json, '$.risk_cy') AS risk_category_cy,
            geometry_wkt_bng,
            geometry_wkt_wgs84,
            properties_json,
            TRUE                AS _is_current,
            '{_b_run_id}'       AS _bronze_run_id,
            CURRENT_TIMESTAMP() AS _silver_processed_at,
            '{SOURCE_NAME}'     AS _source,
            '{STATE}'           AS _state
        FROM _geom_input_processed
    """) if False else (
        zone_with_geom
        .withColumn("model_id",            F.get_json_object("properties_json", "$.mm_id"))
        .withColumn("publication_date_raw", F.get_json_object("properties_json", "$.pub_date"))
        .withColumn("risk_category",        F.get_json_object("properties_json", "$.risk"))
        .withColumn("risk_category_cy",     F.get_json_object("properties_json", "$.risk_cy"))
        .withColumn("_is_current",          F.lit(True))
        .withColumn("_bronze_run_id",       F.lit(_b_run_id or ""))
        .withColumn("_silver_processed_at", F.lit(PROCESSED_AT.isoformat()).cast(TimestampType()))
        .withColumn("_source",              F.lit(SOURCE_NAME))
        .withColumn("_state",               F.lit(STATE))
    )

    # Add derived layer_category (requires Python UDF since Spark doesn't know the function)
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType as ST
    derive_cat_udf = udf(_derive_zone_category, ST())

    zone_final = (
        zone_prepared
        .withColumn("layer_category", derive_cat_udf("layer_name"))
        .withColumn("publication_date", F.to_date("publication_date_raw"))
        .select(
            "layer_name", "objectid", "feature_id",
            "model_id",
            "publication_date",
            "risk_category", "risk_category_cy", "layer_category",
            "geometry_wkt_bng", "geometry_wkt_wgs84",
            "properties_json",
            "_is_current", "_bronze_run_id", "_silver_processed_at", "_source", "_state",
        )
    )
    zone_final.createOrReplaceTempView("_incoming_flood_zones")

    spark.sql(f"""
        MERGE INTO {FQN_S_FLOOD_ZONES} AS target
        USING _incoming_flood_zones AS source
        ON target.layer_name = source.layer_name
           AND target.objectid = source.objectid
        WHEN MATCHED THEN UPDATE SET
            target.feature_id           = source.feature_id,
            target.model_id             = source.model_id,
            target.publication_date     = source.publication_date,
            target.risk_category        = source.risk_category,
            target.risk_category_cy     = source.risk_category_cy,
            target.layer_category       = source.layer_category,
            target.geometry_wkt_bng     = source.geometry_wkt_bng,
            target.geometry_wkt_wgs84   = source.geometry_wkt_wgs84,
            target.properties_json      = source.properties_json,
            target._is_current          = TRUE,
            target._bronze_run_id       = source._bronze_run_id,
            target._silver_processed_at = source._silver_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    _zones_written = zone_final.count()
    log.info(f"Flood zones: {_zones_written} records merged into {FQN_S_FLOOD_ZONES}")
else:
    log.info("Flood zones: no matching bronze records in this run")

# COMMAND ----------

# MAGIC %md ## 10. Process Flood Risk Areas

# COMMAND ----------

_risk_written = 0

risk_df_raw = bronze_df.filter(F.col("layer_name").isin(list(FLOOD_RISK_LAYERS)))
if risk_df_raw.count() > 0:
    risk_with_geom = _process_geometries(risk_df_raw)

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType as ST

    risk_source_udf = udf(lambda ln: _derive_risk_source_receptor(ln)[0], ST())
    risk_receptor_udf = udf(lambda ln: _derive_risk_source_receptor(ln)[1], ST())

    risk_final = (
        risk_with_geom
        .withColumn("risk_source",          risk_source_udf("layer_name"))
        .withColumn("risk_receptor",         risk_receptor_udf("layer_name"))
        .withColumn("risk_category",         F.get_json_object("properties_json", "$.risk")
                    .cast(StringType()))
        .withColumn("_is_current",           F.lit(True))
        .withColumn("_bronze_run_id",        F.lit(_b_run_id or ""))
        .withColumn("_silver_processed_at",  F.lit(PROCESSED_AT.isoformat()).cast(TimestampType()))
        .withColumn("_source",               F.lit(SOURCE_NAME))
        .withColumn("_state",                F.lit(STATE))
        .select(
            "layer_name", "objectid", "feature_id",
            "risk_category", "risk_source", "risk_receptor",
            "geometry_wkt_bng", "geometry_wkt_wgs84",
            "properties_json",
            "_is_current", "_bronze_run_id", "_silver_processed_at", "_source", "_state",
        )
    )
    risk_final.createOrReplaceTempView("_incoming_flood_risk")

    spark.sql(f"""
        MERGE INTO {FQN_S_FLOOD_RISK} AS target
        USING _incoming_flood_risk AS source
        ON target.layer_name = source.layer_name
           AND target.objectid = source.objectid
        WHEN MATCHED THEN UPDATE SET
            target.feature_id           = source.feature_id,
            target.risk_category        = source.risk_category,
            target.risk_source          = source.risk_source,
            target.risk_receptor        = source.risk_receptor,
            target.geometry_wkt_bng     = source.geometry_wkt_bng,
            target.geometry_wkt_wgs84   = source.geometry_wkt_wgs84,
            target.properties_json      = source.properties_json,
            target._is_current          = TRUE,
            target._bronze_run_id       = source._bronze_run_id,
            target._silver_processed_at = source._silver_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    _risk_written = risk_final.count()
    log.info(f"Flood risk areas: {_risk_written} records merged into {FQN_S_FLOOD_RISK}")
else:
    log.info("Flood risk areas: no matching bronze records in this run")

# COMMAND ----------

# MAGIC %md ## 11. Process Flood Warning Areas

# COMMAND ----------

_warnings_written = 0

warn_df_raw = bronze_df.filter(F.col("layer_name").isin(list(WARNING_AREA_LAYERS)))
if warn_df_raw.count() > 0:
    warn_with_geom = _process_geometries(warn_df_raw)

    warn_final = (
        warn_with_geom
        .withColumn("fwis_code",          F.get_json_object("properties_json", "$.fwis_code"))
        .withColumn("region",             F.get_json_object("properties_json", "$.region"))
        .withColumn("area_name",          F.get_json_object("properties_json", "$.area"))
        .withColumn("ta_code",            F.get_json_object("properties_json", "$.fwd_tacode"))
        .withColumn("warning_area_name",  F.get_json_object("properties_json", "$.fwa_name"))
        .withColumn("description",        F.get_json_object("properties_json", "$.descrip"))
        .withColumn("_is_current",        F.lit(True))
        .withColumn("_bronze_run_id",     F.lit(_b_run_id or ""))
        .withColumn("_silver_processed_at", F.lit(PROCESSED_AT.isoformat()).cast(TimestampType()))
        .withColumn("_source",            F.lit(SOURCE_NAME))
        .withColumn("_state",             F.lit(STATE))
        .select(
            "objectid", "fwis_code", "feature_id",
            "region", "area_name", "ta_code", "warning_area_name", "description",
            "geometry_wkt_bng", "geometry_wkt_wgs84",
            "properties_json",
            "_is_current", "_bronze_run_id", "_silver_processed_at", "_source", "_state",
        )
    )
    warn_final.createOrReplaceTempView("_incoming_flood_warnings")

    spark.sql(f"""
        MERGE INTO {FQN_S_FLOOD_WARNINGS} AS target
        USING _incoming_flood_warnings AS source
        ON target.fwis_code = source.fwis_code
        WHEN MATCHED THEN UPDATE SET
            target.objectid             = source.objectid,
            target.feature_id           = source.feature_id,
            target.region               = source.region,
            target.area_name            = source.area_name,
            target.ta_code              = source.ta_code,
            target.warning_area_name    = source.warning_area_name,
            target.description          = source.description,
            target.geometry_wkt_bng     = source.geometry_wkt_bng,
            target.geometry_wkt_wgs84   = source.geometry_wkt_wgs84,
            target.properties_json      = source.properties_json,
            target._is_current          = TRUE,
            target._bronze_run_id       = source._bronze_run_id,
            target._silver_processed_at = source._silver_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    _warnings_written = warn_final.count()
    log.info(f"Flood warning areas: {_warnings_written} records merged into {FQN_S_FLOOD_WARNINGS}")
else:
    log.info("Flood warning areas: no matching bronze records in this run")

# COMMAND ----------

# MAGIC %md ## 12. Process Flood Defences

# COMMAND ----------

_defences_written = 0

def_df_raw = bronze_df.filter(F.col("layer_name").isin(list(DEFENCE_LAYERS)))
if def_df_raw.count() > 0:
    def_with_geom = _process_geometries(def_df_raw)

    def_final = (
        def_with_geom
        .withColumn("_is_current",          F.lit(True))
        .withColumn("_bronze_run_id",        F.lit(_b_run_id or ""))
        .withColumn("_silver_processed_at",  F.lit(PROCESSED_AT.isoformat()).cast(TimestampType()))
        .withColumn("_source",               F.lit(SOURCE_NAME))
        .withColumn("_state",                F.lit(STATE))
        .select(
            "layer_name", "objectid", "feature_id",
            "geometry_wkt_bng", "geometry_wkt_wgs84",
            "properties_json",
            "_is_current", "_bronze_run_id", "_silver_processed_at", "_source", "_state",
        )
    )
    def_final.createOrReplaceTempView("_incoming_flood_defences")

    spark.sql(f"""
        MERGE INTO {FQN_S_FLOOD_DEFENCES} AS target
        USING _incoming_flood_defences AS source
        ON target.layer_name = source.layer_name
           AND target.objectid = source.objectid
        WHEN MATCHED THEN UPDATE SET
            target.feature_id           = source.feature_id,
            target.geometry_wkt_bng     = source.geometry_wkt_bng,
            target.geometry_wkt_wgs84   = source.geometry_wkt_wgs84,
            target.properties_json      = source.properties_json,
            target._is_current          = TRUE,
            target._bronze_run_id       = source._bronze_run_id,
            target._silver_processed_at = source._silver_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    _defences_written = def_final.count()
    log.info(f"Flood defences: {_defences_written} records merged into {FQN_S_FLOOD_DEFENCES}")
else:
    log.info("Flood defences: no matching bronze records in this run")

# COMMAND ----------

bronze_df.unpersist()

# COMMAND ----------

# MAGIC %md ## 13. Audit Logging

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

# MAGIC %md ## 14. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None

try:
    log.info("NRW Wales Silver normalisation complete")
    log.info(f"  Flood zones        : {_zones_written} rows merged")
    log.info(f"  Flood risk areas   : {_risk_written} rows merged")
    log.info(f"  Warning areas      : {_warnings_written} rows merged")
    log.info(f"  Flood defences     : {_defences_written} rows merged")
    _pipeline_status = "success"

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Silver pipeline failed: {exc}", exc_info=True)
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "silver",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_read        = total_bronze,
        rows_written     = _zones_written + _risk_written + _warnings_written + _defences_written,
        rows_merged      = _zones_written + _risk_written + _warnings_written + _defences_written,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "flood_zones_written":    _zones_written,
            "flood_risk_written":     _risk_written,
            "warning_areas_written":  _warnings_written,
            "flood_defences_written": _defences_written,
            "bronze_run_id":          _b_run_id,
            "full_refresh":           FULL_REFRESH,
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
