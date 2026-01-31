# ==========================================================
# 02_cleaning_silver.py
# Cleaning & Standardization -> SILVER + Optimisations
# ==========================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date

# -----------------------------
# Global paths
# -----------------------------
BASE_PATH = "/Volumes/workspace/ipsldata/capstoneipsl"

BRONZE_MAIN_PATH = f"{BASE_PATH}/data/bronze/main"
BRONZE_ENRICH_PATH = f"{BASE_PATH}/data/bronze/enrich"

SILVER_MAIN_PATH = f"{BASE_PATH}/data/silver/main_clean"
SILVER_ENRICH_PATH = f"{BASE_PATH}/data/silver/enrich_clean"

run_date = date.today().isoformat()

print("🚀 Starting Silver cleaning pipeline")

# ==========================================================
# 1. READ BRONZE DATA
# ==========================================================
# Lecture optimisée : format parquet déjà optimisé pour Spark
df_transactions = spark.read.parquet(BRONZE_MAIN_PATH)
df_clients = spark.read.parquet(BRONZE_ENRICH_PATH)

print("✅ Bronze data loaded")

# ==========================================================
# 2. SCHEMA & TYPE STANDARDIZATION
# ==========================================================
# Standardisation des types pour éviter erreurs downstream et améliorer les performances

# ---- Transactions ----
df_transactions = (
    df_transactions
    .withColumn("transaction_id", F.col("transaction_id").cast(StringType()))
    .withColumn("client_id", F.col("client_id").cast(StringType()))
    .withColumn("account_id", F.col("account_id").cast(StringType()))
    .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
    .withColumn("amount", F.coalesce(F.col("amount").cast(DoubleType()), F.lit(0.0)))
    .withColumn("currency", F.col("currency").cast(StringType()))
    .withColumn("transaction_type", F.col("transaction_type").cast(StringType()))
    .withColumn("channel", F.col("channel").cast(StringType()))
    .withColumn("status", F.col("status").cast(StringType()))
    .withColumn("balance_after", F.coalesce(F.col("balance_after").cast(DoubleType()), F.lit(0.0)))
    .withColumn("year", F.col("year").cast(IntegerType()))
)

# ---- Clients ----
df_clients = (
    df_clients
    .withColumn("client_id", F.col("client_id").cast(StringType()))
    .withColumn("age", F.coalesce(F.col("age").cast(IntegerType()), F.lit(-1)))
    .withColumn("job", F.col("job").cast(StringType()))
    .withColumn("marital", F.col("marital").cast(StringType()))
    .withColumn("education", F.col("education").cast(StringType()))
    .withColumn("default", F.col("default").cast(StringType()))
    .withColumn("balance", F.coalesce(F.col("balance").cast(DoubleType()), F.lit(0.0)))
    .withColumn("housing", F.col("housing").cast(StringType()))
    .withColumn("loan_flag",
                F.when(F.lower(F.col("loan")) == "yes", 1)
                 .when(F.lower(F.col("loan")) == "no", 0)
                 .otherwise(-1))
    .withColumn("contact", F.col("contact").cast(StringType()))
    .withColumn("campaign", F.coalesce(F.col("campaign").cast(IntegerType()), F.lit(-1)))
    .withColumn("pdays", F.coalesce(F.col("pdays").cast(IntegerType()), F.lit(-1)))
    .withColumn("previous", F.coalesce(F.col("previous").cast(IntegerType()), F.lit(0)))
    .withColumn("poutcome", F.col("poutcome").cast(StringType()))
    .withColumn("created_at", F.to_date("created_at", "yyyy-MM-dd"))
    .withColumn("year", F.col("year").cast(IntegerType()))
    .drop("loan")
)

print("✅ Schema enforced")

# ==========================================================
# 3. DUPLICATES HANDLING
# ==========================================================
# Suppression des doublons pour éviter surcharge downstream
df_transactions = df_transactions.dropDuplicates(["transaction_id"])
df_clients = df_clients.dropDuplicates(["client_id"])

print("✅ Duplicates removed")

# ==========================================================
# 3b. OPTIONNEL: CACHE
# ==========================================================
# Ici on ne fait pas de caching car le nettoyage est simple et on n’a pas de gros recalculs
# Caching est plus utile pour df_joined (enrichment) ou data réutilisée plusieurs fois

# ==========================================================
# 4. BASIC VALIDATIONS
# ==========================================================
assert df_transactions.count() > 0, "❌ Transactions dataset is empty"
assert df_clients.count() > 0, "❌ Clients dataset is empty"

print("✅ Basic validations passed")

# ==========================================================
# 5. WRITE SILVER DATA
# ==========================================================
# Partitionnement par année pour accélérer les lectures futures
(
    df_transactions
    .write
    .mode("overwrite")
    .partitionBy("year")  # Optimisation lecture / parallélisation
    .parquet(SILVER_MAIN_PATH)
)

(
    df_clients
    .write
    .mode("overwrite")
    .partitionBy("year")  # Optimisation lecture / parallélisation
    .parquet(SILVER_ENRICH_PATH)
)

print("🎯 Silver layer successfully written")

# ==========================================================
# 6. DISPLAY SAMPLE DATA
# ==========================================================
# Affichage pour vérifier que tout est OK
print("Sample Transactions (Silver main_clean):")
display(spark.read.parquet(SILVER_MAIN_PATH).limit(10))

print("Sample Clients (Silver enrich_clean):")
display(spark.read.parquet(SILVER_ENRICH_PATH).limit(10))
