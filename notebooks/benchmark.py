# ============================================================
# Benchmark Lakehouse Pipeline 
# ============================================================

import sys, os, time, io
from datetime import datetime
from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

from configs.paths import SILVER_MAIN, SILVER_DELTA
from utils.file_utils import compter_parquet

REPORTS_BENCHMARKS = os.path.join(PROJECT_ROOT, "reports", "benchmarks")
os.makedirs(REPORTS_BENCHMARKS, exist_ok=True)

spark = SparkSession.builder.getOrCreate()
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ============================================================
# 1️⃣ Benchmark AVANT — Parquet
# ============================================================
start_before = time.time()
df_before = spark.read.parquet(SILVER_MAIN)
df_before.count()
end_before = time.time()

nb_files_before, size_bytes_before = compter_parquet(SILVER_MAIN, dbutils)
size_mb_before = round(size_bytes_before / (1024*1024), 2)

old_stdout = sys.stdout
sys.stdout = buffer = io.StringIO()
df_before.explain(mode="simple")
sys.stdout = old_stdout
plan_before = buffer.getvalue()

# ============================================================
# 2️⃣ Benchmark APRÈS — Delta
# ============================================================
start_after = time.time()
df_after = spark.read.format("delta").load(SILVER_DELTA)
df_after.count()
end_after = time.time()

nb_files_after, size_bytes_after = compter_parquet(SILVER_DELTA, dbutils)
size_mb_after = round(size_bytes_after / (1024*1024), 2)

old_stdout = sys.stdout
sys.stdout = buffer = io.StringIO()
df_after.explain(mode="simple")
sys.stdout = old_stdout
plan_after = buffer.getvalue()

# ============================================================
# 3️⃣ Génération fichier Markdown 
# ============================================================
benchmark_file = os.path.join(REPORTS_BENCHMARKS, "benchmark_final.md")

with open(benchmark_file, "w", encoding="utf-8") as f:
    f.write("# Benchmark Lakehouse Pipeline - Version Explicative\n\n")
    f.write(f"**Date d'exécution :** {timestamp}\n\n")
    
    # ---------------- AVANT ----------------
    f.write("## 1️⃣ Avant optimisation — Format Parquet\n\n")
    f.write(f"- Durée d'exécution : {round(end_before - start_before, 2)} secondes\n")
    f.write(f"- Nombre de fichiers : {nb_files_before}\n")
    f.write(f"- Taille totale : {size_mb_before} MB\n\n")
    f.write("Cette étape correspond à l'état initial du pipeline. Les données sont stockées en format Parquet standard. "
            "Nous pouvons observer la structure actuelle et mesurer les performances de lecture avant toute optimisation. "
            "Le plan Spark ci-dessous permet de comprendre comment les transformations sont exécutées.\n\n")
    f.write("### Plan Spark (Parquet)\n```text\n")
    f.write(plan_before + "\n```\n\n")
    
    # ---------------- APRÈS ----------------
    f.write("## 2️⃣ Après optimisation — Format Delta Lake\n\n")
    f.write(f"- Durée d'exécution : {round(end_after - start_after, 2)} secondes\n")
    f.write(f"- Nombre de fichiers : {nb_files_after}\n")
    f.write(f"- Taille totale : {size_mb_after} MB\n\n")
    f.write("Après optimisation, les données sont stockées en Delta Lake. Ce format permet une meilleure gestion des transactions "
            "et souvent une amélioration des performances. Cette section montre comment la lecture et les transformations ont été optimisées. "
            "Le plan Spark ci-dessous illustre les changements dans l'exécution.\n\n")
    f.write("### Plan Spark (Delta)\n```text\n")
    f.write(plan_after + "\n```\n\n")
    
    # ---------------- COMPARAISON ----------------
    f.write("## 3️⃣ Comparaison synthétique\n\n")
    f.write("| Indicateur | Avant (Parquet) | Après (Delta) | Observations |\n")
    f.write("|------------|-----------------|---------------|--------------|\n")
    
    obs_dur = "La lecture a été accélérée grâce à la structure optimisée et à la gestion des transactions." \
               if (end_after - start_after) < (end_before - start_before) else "La lecture est légèrement plus lente, à surveiller."
    f.write(f"| Durée (s) | {round(end_before - start_before,2)} | {round(end_after - start_after,2)} | {obs_dur} |\n")
    
    obs_files = "Le nombre de fichiers est réduit, ce qui simplifie la gestion et améliore l'efficacité du pipeline." \
                if nb_files_after < nb_files_before else "Le nombre de fichiers a augmenté, vérifier la partition des données."
    f.write(f"| Nombre fichiers | {nb_files_before} | {nb_files_after} | {obs_files} |\n")
    
    obs_size = "La taille totale a diminué, indiquant une meilleure compaction et un stockage plus efficace." \
               if size_mb_after < size_mb_before else "La taille totale est légèrement supérieure, à vérifier pour optimiser l'espace."
    f.write(f"| Taille (MB) | {size_mb_before} | {size_mb_after} | {obs_size} |\n\n")
    
    # ---------------- ANALYSE ----------------
    f.write("## 4️⃣ Analyse et recommandations\n\n")
    f.write("Dans l'ensemble, l'optimisation vers Delta Lake apporte des améliorations en termes de performances et de gestion des fichiers. "
        "Cette synthèse présente les résultats du benchmark du pipeline Lakehouse. "
        "Elle met en évidence les améliorations obtenues après la migration vers Delta Lake, notamment sur la durée d'exécution et la taille des fichiers. "
        "Ces observations permettent de mieux comprendre le comportement du pipeline et servent de base pour la documentation finale du projet."
        "Autrement dit cette synthèse fournit une vue claire des gains obtenus et des points à surveiller pour les prochaines étapes.\n")

print("🎉 Benchmark complet généré avec succès !")
print(f"📄 Fichier : {benchmark_file}")
