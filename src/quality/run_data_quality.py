from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime
from data_quality_checks import run_quality_checks

BASE_PATH = "/Volumes/workspace/ipsldata/capstoneipsl"
SILVER_JOINED_PATH = f"{BASE_PATH}/data/silver/joined"
OUTPUT_PATH = f"{BASE_PATH}/reports/data_quality"

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet(SILVER_JOINED_PATH)

run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

results = run_quality_checks(df, run_id)

# Ajoutez le timestamp à chaque résultat
for result in results:
    result["timestamp"] = datetime.now()


schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("check_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("metric_value", DoubleType(), True),
    StructField("threshold", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Create DataFrame with explicit schema
results_df = spark.createDataFrame(results, schema=schema)

print("=== Données avec le nouveau schéma ===")
results_df.show (truncate=False)

# Export résultats
results_df.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/data_quality_results.parquet")
results_df.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_PATH}/data_quality_results.csv")

print(f"\nExport terminé. Fichiers écrits dans: {OUTPUT_PATH}")