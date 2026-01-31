# ==========================================================
# GOLD EXPORTS (EXPORT-ONLY SCRIPT)
# - Source : GOLD AGGREGATES (déjà calculés)
# - Outputs :
#     * Parquet (analytics)
#     * CSV (BI-ready)
# ==========================================================

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------
BASE_PATH = "/Volumes/workspace/ipsldata/capstoneipsl"

GOLD_AGG_BASE_PATH = f"{BASE_PATH}/data/gold/aggregates"
GOLD_EXPORT_PATH = f"{BASE_PATH}/data/gold/exports"

# Liste explicite des agrégations existantes
AGG_LEVELS = ["daily", "weekly", "monthly"]

# ----------------------------------------------------------
# EXPORT FUNCTIONS
# ----------------------------------------------------------
def export_parquet(df, output_path):
    df.write.mode("overwrite").parquet(output_path)


def export_csv(df, output_path, single_file=True):
    writer = df
    if single_file:
        writer = writer.coalesce(1)

    writer.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

# ----------------------------------------------------------
# EXPORT LOOP (SANS RECALCUL)
# ----------------------------------------------------------
for level in AGG_LEVELS:

    input_path = f"{GOLD_AGG_BASE_PATH}/{level}"

    print(f"▶ Reading Gold aggregate: {input_path}")

    df = spark.read.parquet(input_path)

    # Parquet export (BI / analytics)
    export_parquet(
        df,
        f"{GOLD_EXPORT_PATH}/{level}_parquet"
    )

    # CSV export (BI-ready)
    export_csv(
        df,
        f"{GOLD_EXPORT_PATH}/{level}_csv",
        single_file=True
    )

print("✅ GOLD EXPORTS COMPLETED (PARQUET + CSV)")
