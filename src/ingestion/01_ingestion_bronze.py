#  INGESTION — DATASET PRINCIPAL (Transactions)



from datetime import date

run_date = date.today().isoformat()

BASE_PATH = "/Volumes/workspace/ipsldata/capstoneipsl"

# Lire toutes les années automatiquement
df_transactions = (
    spark.read
    .option("header", True)
    .csv(f"{BASE_PATH}/raw/transactions")
)

# Vérification (OPTIONNEL mais conseillé)
df_transactions.select("year").distinct().show()

# Écriture en Bronze (main)
df_transactions.write.mode("overwrite").parquet(
    f"{BASE_PATH}/data/bronze/main/run_date={run_date}"
)


# INGESTION — DATASET ENRICHISSEMENT (Clients)

# Lire toutes les années automatiquement
df_clients = (
    spark.read
    .option("header", True)
    .csv(f"{BASE_PATH}/raw/clients")
)

# Vérification
df_clients.select("year").distinct().show()

# Écriture en Bronze (enrich)
df_clients.write.mode("overwrite").parquet(
    f"{BASE_PATH}/data/bronze/enrich/run_date={run_date}"
)

print("✅ Clients (toutes les années) ingérés en Bronze")


print("✅ Transactions (toutes les années) ingérées en Bronze")

# Afficher un échantillon des transactions
print("Sample Transactions (Bronze):")
display(df_transactions.limit(10))

# Afficher un échantillon des clients
print("Sample Clients (Bronze):")
display(df_clients.limit(10))

