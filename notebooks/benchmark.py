# ============================================================
# Benchmark AVANT / APRÈS optimisation (Parquet vs Delta)
# Résultats stockés dans reports/benchmarks/ à la racine
# ============================================================

import sys
import os
import time
import io
from datetime import datetime
from pyspark.sql import SparkSession

# ------------------------------------------------------------
# Ajouter src/ au PYTHONPATH (Databricks)
# ------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

# ------------------------------------------------------------
# Imports projet
# ------------------------------------------------------------
from configs.paths import SILVER_MAIN, SILVER_DELTA
from utils.file_utils import compter_parquet

# ------------------------------------------------------------
# Chemin correct pour les benchmarks (racine reports)
# ------------------------------------------------------------
REPORTS_BENCHMARKS = os.path.join(PROJECT_ROOT, "reports", "benchmarks")

# ------------------------------------------------------------
# Spark
# ------------------------------------------------------------
spark = SparkSession.builder.getOrCreate()

# ------------------------------------------------------------
# Timestamp (historique)
# ------------------------------------------------------------
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# ============================================================
# BENCHMARK AVANT — PARQUET
# ============================================================
start_before = time.time()
df_before = spark.read.parquet(SILVER_MAIN)
end_before = time.time()

nb_files_before, size_bytes_before = compter_parquet(SILVER_MAIN, dbutils)
size_mb_before = round(size_bytes_before / (1024 * 1024), 2)

# Plan Spark
old_stdout = sys.stdout
sys.stdout = buffer = io.StringIO()
df_before.explain(mode="simple")
sys.stdout = old_stdout
plan_before = buffer.getvalue()

# Écriture fichier AVANT
before_file = os.path.join(
    REPORTS_BENCHMARKS,
    f"benchmark_parquet_avant_{timestamp}.md"
)
with open(before_file, "w") as f:
    f.write("# Benchmark AVANT optimisation — Parquet\n\n")
    f.write(f"**Date :** {timestamp}\n\n")
    f.write(f"- ⏱ Durée pipeline : {round(end_before - start_before, 2)} s\n")
    f.write(f"- 📄 Nombre de fichiers : {nb_files_before}\n")
    f.write(f"- 💾 Taille totale : {size_mb_before} MB\n\n")
    f.write("## Plan d'exécution Spark\n```text\n")
    f.write(plan_before)
    f.write("\n```\n")
print(f"✅ Benchmark AVANT enregistré : {before_file}")

# ============================================================
# BENCHMARK APRÈS — DELTA LAKE
# ============================================================
start_after = time.time()
df_after = spark.read.format("delta").load(SILVER_DELTA)
end_after = time.time()

nb_files_after, size_bytes_after = compter_parquet(SILVER_DELTA, dbutils)
size_mb_after = round(size_bytes_after / (1024 * 1024), 2)

# Plan Spark
old_stdout = sys.stdout
sys.stdout = buffer = io.StringIO()
df_after.explain(mode="simple")
sys.stdout = old_stdout
plan_after = buffer.getvalue()

# Écriture fichier APRÈS
after_file = os.path.join(
    REPORTS_BENCHMARKS,
    f"benchmark_delta_apres_{timestamp}.md"
)
with open(after_file, "w") as f:
    f.write("# Benchmark APRÈS optimisation — Delta Lake\n\n")
    f.write(f"**Date :** {timestamp}\n\n")
    f.write(f"- ⏱ Durée pipeline : {round(end_after - start_after, 2)} s\n")
    f.write(f"- 📄 Nombre de fichiers : {nb_files_after}\n")
    f.write(f"- 💾 Taille totale : {size_mb_after} MB\n\n")
    f.write("## Plan d'exécution Spark\n```text\n")
    f.write(plan_after)
    f.write("\n```\n")
print(f"✅ Benchmark APRÈS enregistré : {after_file}")

# ============================================================
# RÉSUMÉ COMPARATIF
# ============================================================
summary_file = os.path.join(
    REPORTS_BENCHMARKS,
    f"benchmark_comparatif_{timestamp}.md"
)
with open(summary_file, "w") as f:
    f.write("# Comparaison Benchmarks Parquet vs Delta\n\n")
    f.write(f"**Date :** {timestamp}\n\n")
    f.write("| Format | Durée (s) | Nb fichiers | Taille (MB) |\n")
    f.write("|--------|-----------|-------------|-------------|\n")
    f.write(
        f"| Parquet (AVANT) | {round(end_before - start_before, 2)} | "
        f"{nb_files_before} | {size_mb_before} |\n"
    )
    f.write(
        f"| Delta (APRÈS) | {round(end_after - start_after, 2)} | "
        f"{nb_files_after} | {size_mb_after} |\n"
    )
print(f"✅ Résumé comparatif enregistré : {summary_file}")
print("\n🎉 Benchmarks terminés avec succès")
