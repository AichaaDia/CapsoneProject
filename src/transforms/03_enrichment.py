# ==========================================================
# 03_enrichment.py
# Enrichment & Join -> SILVER (joined) + Optimisations
# ==========================================================

from pyspark.sql import functions as F
from datetime import date

# -----------------------------
# Global paths
# -----------------------------
BASE_PATH = "/Volumes/workspace/ipsldata/capstoneipsl"

SILVER_MAIN_PATH = f"{BASE_PATH}/data/silver/main_clean"
SILVER_ENRICH_PATH = f"{BASE_PATH}/data/silver/enrich_clean"
SILVER_JOINED_PATH = f"{BASE_PATH}/data/silver/joined"

print("🚀 Starting enrichment pipeline")

# ==========================================================
# 1. READ SILVER DATA
# ==========================================================
df_transactions = spark.read.parquet(SILVER_MAIN_PATH)
df_clients = spark.read.parquet(SILVER_ENRICH_PATH)

print("✅ Silver datasets loaded")

# ==========================================================
# 2. RENAME COLUMNS TO AVOID AMBIGUITY
# ==========================================================
df_clients = df_clients.withColumnRenamed("year", "client_year")

# ==========================================================
# 3. JOIN DATASETS (Broadcast join pour réduire shuffle)
# ==========================================================
# Utilisation de broadcast join car le dataset client est beaucoup plus petit que transactions
df_joined = (
    df_transactions.alias("t")
    .join(
        F.broadcast(df_clients.alias("c")),  # Broadcast join -> optimisation shuffle
        on="client_id",
        how="left"
    )
)

print("✅ Join completed")

# ==========================================================
# 3b. CACHE DATAFRAME
# ==========================================================
# On cache df_joined car il sera réutilisé pour plusieurs transformations (features, nulls, select final)
#df_joined.cache()

# ==========================================================
# 4. FEATURE ENGINEERING
# ==========================================================
# Feature 1: High value transaction (>1000)
df_joined = df_joined.withColumn(
    "high_value_txn_flag",
    F.when(F.col("amount") > 1000, 1).otherwise(0)
)

# Feature 2: Loan flag
df_joined = df_joined.withColumn("loan_flag", F.col("loan_flag").cast("integer"))

# Feature 3: Client tenure in days
df_joined = df_joined.withColumn(
    "client_tenure_days",
    F.datediff(F.col("transaction_date"), F.col("created_at"))
)

# Feature 4: Weekend transaction flag
df_joined = df_joined.withColumn(
    "weekend_flag",
    F.when(F.dayofweek(F.col("transaction_date")).isin([1, 7]), 1).otherwise(0)
)

# Feature 5: Negative balance flag
df_joined = df_joined.withColumn(
    "balance_flag",
    F.when(F.col("balance_after") < 0, 1).otherwise(0)
)

print("✅ Feature engineering completed")

# ==========================================================
# 5. HANDLE NULLS POST JOIN
# ==========================================================
df_joined = df_joined.fillna({
    "contact": "UNKNOWN",
    "client_tenure_days": -1,
    "high_value_txn_flag": 0,
    "loan_flag": -1,
    "weekend_flag": 0,
    "balance_flag": 0
})

# ==========================================================
# 6. BASIC VALIDATION
# ==========================================================
assert df_joined.count() > 0, "❌ Joined dataset is empty"
print("✅ Enriched dataset validated")

# ==========================================================
# 7. SELECT FINAL COLUMNS FOR GOLD
# ==========================================================
# Sélection précoce pour réduire les shuffles lors de l'écriture
df_joined = df_joined.select(
    "transaction_id",
    "client_id",
    "amount",
    "transaction_date",
    "year",           # transaction year
    "contact",
    "created_at",
    "loan_flag",
    "high_value_txn_flag",
    "client_tenure_days",
    "weekend_flag",
    "balance_flag"
)

# ==========================================================
# 8. WRITE JOINED SILVER DATA (partitionné pour optimisation)
# ==========================================================
(
    df_joined
    .write
    .mode("overwrite")
    .partitionBy("year")  # Partitionnement -> meilleure parallélisation + lecture sélective
    .parquet(SILVER_JOINED_PATH)  # Format Parquet -> optimal pour Spark
)

print("🎯 Silver joined/enriched dataset successfully written")

# ==========================================================
# 9. DISPLAY SAMPLE
# ==========================================================
print("Sample joined dataset:")
display(df_joined.limit(10))

# ==========================================================
# ==========================================================
# 10. CLEAR CACHE
# ==========================================================
# Sur Serverless, pas de cache à libérer, mais on peut supprimer référence si besoin
del df_joined

