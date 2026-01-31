from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Chemins
SILVER_PATH = "/Volumes/workspace/ipsldata/capstoneipsl/data/silver/main_clean"
DELTA_PATH  = "/Volumes/workspace/ipsldata/capstoneipsl/data/silver/main_clean_delta"

# Lire Silver Parquet
df = spark.read.parquet(SILVER_PATH)

# Écrire en Delta
(df.write
   .format("delta")
   .mode("overwrite")
   .save(DELTA_PATH))

print("✅ Delta créé dans main_clean_delta")
