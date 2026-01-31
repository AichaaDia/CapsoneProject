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
```
project/
├── data/
│   ├── bronze/
│   │   └── raw/
│   │       ├── main/         # transactions
│   │       │   └── year=YYYY/
│   │       └── enrich/       # clients
│   │           └── year=YYYY/
│   ├── silver/
│   │   ├── main_clean/       # transactions nettoyées
│   │   ├── enrich_clean/     # clients nettoyés
│   │   └── joined/           # jointure main + enrich
│   └── gold/
│       ├── marts/            # table analytique principale
│       ├── aggregates/       # agrégations temporelles
│       └── exports/          # exports légers Parquet/CSV
├── src/
│   ├── ingestion/            # scripts ingestion Bronze
│   ├── transforms/           # nettoyage & transformations Silver
│   ├── quality/              # contrôles qualité
│   └── utils/                # fonctions utilitaires (init_storage, benchmark)
├── notebooks/                # notebooks Databricks pour tests et démo
├── configs/                  # fichiers de configuration (paths, paramètres)
├── reports/
│   ├── data_quality/         # rapports qualité
│   └── benchmarks/           # mesures performance
└── README.md
```

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
  - `high_value_txn_flag` : transaction > 1000
  - `loan_flag` : indicateur si le client a un prêt
  - `client_tenure_days` : nombre de jours entre `transaction_date` et `created_at`
  - `weekend_flag` : transaction le samedi ou dimanche
  - `balance_flag` : solde négatif après transaction
- Optimisations :
  - Broadcast join pour réduire shuffles
  - Sélection précoce des colonnes
  - Partitionnement Parquet par `year`

### 4.4 Gold – Outputs
- Marts : table analytique principale
- Aggregates : agrégations temporelles (jour/semaine/mois)
- Exports BI-ready : Parquet et CSV légers

---
## 5. Qualité des données
- Contrôles appliqués :
  1. Nulls dans colonnes critiques (transaction_id, client_id, amount)
  2. Doublons sur transaction_id / client_id
  3. Valeurs cohérentes amount / balance_after
  4. Format date valide (transaction_date, created_at)
- Rapport stocké : reports/data_quality/
- Table : check_name, status, metric_value, threshold, run_id

---
## 6. Performance & Optimisation
- Partitionnement Parquet (year, month)
- Broadcast join sur dataset client
- Sélection précoce des colonnes
- Caching pour transformations multiples (optionnel)
- Benchmarks : reports/benchmarks/
- Mesures : durée pipeline, taille outputs, nb fichiers, logs Spark UI (optionnel)

---
## 7. Configuration et utilitaires
- configs/paths.py : chemins Bronze/Silver/Gold
- src/utils/init_storage.py : création automatique des dossiers
- src/utils/benchmark.py : chronométrage et tests lecture/écriture

---
## 8. Exécution


---
## 9. Livrables
- Dépôt Git : code + configs + README.md
- Rapport final ≥ 6 pages
- Data Quality Report + Benchmarks
- Démo : notebook + vidéo + logs
