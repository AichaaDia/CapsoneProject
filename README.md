# Capstone Project – Data Engineering Lakehouse
**Cours :** Data Engineering, IPSL DIC3 Informatique  
**Enseignant :** Mbaye Babacar Gueye, Ph.D  

---
## 1. Introduction
Ce projet construit une **plateforme Data Engineering Lakehouse en PySpark local (Batch)**, avec **enrichissement multi-sources**, sur **Databricks**.  
Il respecte les contraintes locales : **dataset principal ≥ 8 Go**, stockage via **Unity Catalog Volumes**, pipeline **Bronze → Silver → Gold**.  

Objectifs :
- Ingestion d’un dataset principal de transactions bancaires (≥8 Go)
- Ingestion d’une source d’enrichissement clients (~1.5 Go)
- Nettoyage et standardisation des données
- Jointure multi-sources et génération de features
- Création de datasets Gold pour analyse et BI
- Contrôles qualité et benchmarks

---
## 2. Structure du projet

Le projet est organisé selon une architecture **Lakehouse modulaire**, permettant une séparation claire des responsabilités et un enchaînement logique des traitements **Bronze → Silver → Quality → Gold → Export**.

```text

CapsoneProject/
├── notebooks/
│ ├── 04_gold_outputs.py # Génération des tables Gold (mart, agrégations, exports)
│ ├── benchmark.py # Benchmark des performances du pipeline
│ └── main_clean_delta.py # Conversion des données Silver Parquet vers Delta Lake
│
├── reports/
│ ├── benchmarks/ # Rapports de benchmarks de performance
│ ├── data_quality/
│ │ └── data_quality_report.md # Rapport Markdown des résultats de Data Quality
│ └── logs/ # Logs d’exécution et métriques du pipeline
│     └──execution_logs
│     └──metrics
│     └──spark-ui
├── src/
│ ├── configs/
│ │ └── paths.py # Centralisation des chemins Bronze, Silver, Gold
│ │
│ ├── ingestion/
│ │ └── 01_ingestion_bronze.py # Ingestion des fichiers CSV bruts vers la couche Bronze
│ │
│ ├── quality/
│ │ ├── init.py
│ │ ├── data_quality_checks.py # Définition des règles de qualité des données
│ │ └── run_data_quality.py # Exécution des contrôles et export des résultats
│ │
│ ├── transforms/
│ │ ├── 02_cleaning_silver.py # Nettoyage, typage et déduplication (Silver clean)
│ │ ├── 03_enrichment.py # Jointure et feature engineering (Silver joined)
│ │ ├── build_gold_aggregates.py # Calcul des agrégations Gold (journalier, hebdo, mensuel)
│ │ ├── build_gold_exports.py # Export des données Gold en Parquet et CSV BI-ready
│ │ ├── build_gold_mart.py # Construction de la table analytique Gold (mart)
│ │ └── testOptimisation # Scripts exploratoires de tests d’optimisation
│ │
│ └── utils/
│ ├── file_utils.py # Fonctions utilitaires (comptage fichiers Parquet)
│ ├── init_storage.py # Création du catalog, schema et volumes Databricks
│ └── logging_utils.py # Journalisation et suivi d’exécution du pipeline
│
├── .gitignore
└── README.md
```
---


### Description des composants

- **notebooks/** : scripts et notebooks Databricks utilisés pour l’exécution, le monitoring et les démonstrations.
- **src/ingestion/** : ingestion des données brutes dans la couche Bronze.
- **src/transforms/** : nettoyage, enrichissement et préparation des données Silver et Gold.
- **src/quality/** : implémentation et exécution des contrôles de qualité des données.
- **src/utils/** : fonctions techniques transverses (initialisation du stockage, logs, utilitaires).
- **reports/** : rapports générés automatiquement (qualité des données et benchmarks).
- **configs/** : centralisation des chemins et paramètres globaux du projet.


### Explication des fichiers (par ordre d’exécution du pipeline)

**Chemin :** `src/utils/init_storage.py`  
Ce script initialise le catalog, le schema et le volume Databricks, puis crée l’arborescence des dossiers Bronze, Silver et Gold.

**Chemin :** `src/configs/paths.py`  
Ce fichier centralise tous les chemins d’accès aux datasets et aux dossiers de rapports du projet.

**Chemin :** `src/ingestion/01_ingestion_bronze.py`  
Ce script ingère les fichiers CSV bruts (transactions et clients) et les écrit en format Parquet dans la couche Bronze.

**Chemin :** `src/transforms/02_cleaning_silver.py`  
Ce script nettoie les données Bronze, standardise les types, gère les valeurs manquantes et supprime les doublons avant écriture en Silver.

**Chemin :** `src/transforms/03_enrichment.py`  
Ce script joint les datasets Silver transactions et clients, réalise le feature engineering et produit le dataset Silver enrichi.

**Chemin :** `src/quality/data_quality_checks.py`  
Ce fichier définit les règles de contrôles de qualité appliquées aux données Silver enrichies.

**Chemin :** `src/quality/run_data_quality.py`  
Ce script exécute les contrôles de Data Quality, calcule les métriques et exporte les résultats en Parquet et CSV.

**Chemin :** `notebooks/main_clean_delta.py`  
Ce script convertit les données Silver du format Parquet vers Delta Lake afin d’optimiser la gestion transactionnelle.

**Chemin :** `notebooks/04_gold_outputs.py`  
Ce script construit la table analytique Gold, calcule les agrégations temporelles et génère les exports analytiques.

**Chemin :** `src/transforms/build_gold_mart.py`  
Ce script sélectionne et structure les colonnes finales pour la table analytique Gold (mart).

**Chemin :** `src/transforms/build_gold_aggregates.py`  
Ce script calcule les agrégations journalières, hebdomadaires et mensuelles à partir des données Gold.

**Chemin :** `src/transforms/build_gold_exports.py`  
Ce script exporte les données Gold existantes en formats Parquet et CSV BI-ready sans recalcul.

**Chemin :** `notebooks/benchmark.py`  
Ce script mesure les performances du pipeline avant et après optimisation et génère un rapport de benchmark.

**Chemin :** `src/utils/file_utils.py`  
Ce fichier fournit des fonctions utilitaires pour compter les fichiers Parquet et leur taille totale.

**Chemin :** `src/utils/logging_utils.py`  
Ce script assure la journalisation de l’exécution du pipeline, des métriques et des contrôles de qualité.

**Chemin :** `reports/data_quality/data_quality_report.md`  
Ce fichier contient le rapport Markdown présentant les résultats détaillés des contrôles de qualité des données.

**Chemin :** `reports/benchmarks/benchmark_prin.md`  
Ce fichier contient le rapport Markdown synthétisant les résultats des benchmarks de performance.

**Chemin :** `reports/logs/`  
Ce dossier stocke les logs d’exécution et les métriques générées automatiquement par le pipeline.


---
## 3. Datasets
### 3.1 Dataset principal – Transactions
- Volume : ≥ 8 Go (CSV multiples, partitionnés par année)
- Chemin Unity Catalog : `/Volumes/workspace/ipsldata/capstoneipsl/data/bronze/raw/main`
- Champs principaux : transaction_id, client_id, account_id, transaction_date, amount, currency, transaction_type, channel, status, balance_after

### 3.2 Dataset d’enrichissement – Clients
- Volume : ~1.5 Go
- Chemin Unity Catalog : `/Volumes/workspace/ipsldata/capstoneipsl/data/bronze/raw/enrich`
- Champs principaux : client_id, age, job, marital, education, default, balance, housing, loan, contact, campaign, pdays, previous, poutcome, created_at

---
## 4. Pipeline de traitement
### 4.1 Bronze – Raw
- Ingestion brute, aucun nettoyage
- Idempotence via `overwrite` ou partition `year`

### 4.2 Silver – Clean
- Nettoyage, typage, gestion nulls/doublons
- Standardisation des catégories et dates
- Écriture Parquet partitionné par `year`, `month`

### 4.3 Enrichissement – jointure
- Jointure `Silver main` + `Silver enrich` sur `client_id`
- Création de nouvelles features :
  - `high_value_txn_flag`
  - `loan_flag`
  - `client_tenure_days`
  - `weekend_flag`
  - `balance_flag`
- Optimisations :
  - Broadcast join
  - Sélection précoce des colonnes
  - Partitionnement Parquet

### 4.4 Gold – Outputs
- Marts : table analytique principale
- Aggregates : agrégations temporelles
- Exports BI-ready : Parquet et CSV

---
## 5. Qualité des données
- Contrôles appliqués :
  1. Nulls dans colonnes critiques
  2. Doublons sur identifiants
  3. Cohérence montants et soldes
  4. Validité des dates
- Rapport stocké : `/Volumes/workspace/ipsldata/capstoneipsl/reports/data_quality/`
- Table : check_name, status, metric_value, threshold, run_id

---
## 6. Performance & Optimisation
- Partitionnement Parquet
- Broadcast join
- Sélection précoce des colonnes
- Benchmarks dans `reports/benchmarks/`

---
## 7. Configuration et utilitaires
- configs/paths.py : chemins Bronze/Silver/Gold
- src/utils/init_storage.py : création automatique des dossiers
- src/utils/benchmark.py : tests de performance

---
## 8. Exécution

---
## 9. Livrables
- Dépôt Git : code + configs + README.md
- Rapport final ≥ 6 pages
- Data Quality Report + Benchmarks
- Démo : notebook + vidéo + logs


