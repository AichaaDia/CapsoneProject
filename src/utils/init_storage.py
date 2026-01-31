# src/utils/init_storage.py

def init_storage(spark, dbutils):
    CATALOG = "workspace"
    SCHEMA = "ipsldata"
    VOLUME = "capstoneipsl"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

    VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

    PATHS = {
       

        # BRONZE
        "bronze_main": f"{VOLUME_ROOT}/data/bronze/main",
        "bronze_enrich": f"{VOLUME_ROOT}/data/bronze/enrich",

        # SILVER
        "silver_main": f"{VOLUME_ROOT}/data/silver/main_clean",
        "silver_enrich": f"{VOLUME_ROOT}/data/silver/enrich_clean",
        "silver_joined": f"{VOLUME_ROOT}/data/silver/joined",

        # GOLD
        "gold_marts": f"{VOLUME_ROOT}/data/gold/marts",
        "gold_aggregates": f"{VOLUME_ROOT}/data/gold/aggregates",
        "gold_exports": f"{VOLUME_ROOT}/data/gold/exports",


    }

    for path in PATHS.values():
        dbutils.fs.mkdirs(path)

    return PATHS
