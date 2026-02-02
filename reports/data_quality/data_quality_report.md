# Rapport de Qualité des Données

## 1. Introduction

La qualité des données constitue un enjeu central dans tout projet de Data Engineering, en particulier lorsqu’il s’agit de produire des jeux de données analytiques destinés à des usages décisionnels ou à des outils de Business Intelligence.

Dans le cadre de ce projet, une série de contrôles de qualité a été mise en place afin de garantir la fiabilité, la cohérence et l’exploitabilité des données produites par le pipeline Lakehouse. Ces contrôles permettent de détecter d’éventuelles anomalies et de valider que les données respectent les règles métier et techniques définies.

---

## 2. Positionnement des contrôles dans le pipeline

Les contrôles de qualité sont exécutés **après la phase d’enrichissement**, sur le jeu de données joint issu de la couche Silver (`df_joined`), et **avant la production de la couche Gold**.

Ce positionnement permet :
- de valider les données nettoyées et enrichies,
- d’éviter la propagation d’erreurs vers les marts et agrégations,
- de garantir la fiabilité des indicateurs exposés aux utilisateurs finaux.

---

## 3. Méthodologie des contrôles

Les contrôles de qualité sont implémentés sous forme d’une fonction PySpark dédiée (`run_quality_checks`).  
Pour chaque exécution du pipeline, un identifiant unique `run_id` est généré afin d’assurer la traçabilité des résultats.

Chaque contrôle produit :
- une métrique mesurée (`metric_value`),
- un seuil de référence (`threshold`),
- un statut (`PASS` ou `FAIL`).

Les résultats sont centralisés dans une table structurée contenant les champs :
`run_id`, `check_name`, `status`, `metric_value`, `threshold`.

Le **threshold (seuil)** représente la valeur maximale ou minimale acceptable pour une métrique donnée. Il permet d’évaluer automatiquement la conformité des données.

---

## 4. Description des contrôles implémentés

### 4.1 Dataset non vide
Ce contrôle vérifie que le dataset final contient au moins une ligne.

- Objectif : éviter toute propagation de données vides
- Métrique : nombre total de lignes
- Seuil : > 0

---

### 4.2 Présence de l’identifiant de transaction
Ce contrôle vérifie que la colonne `transaction_id` ne contient aucune valeur nulle.

- Objectif : garantir l’identification unique de chaque transaction
- Métrique : nombre de valeurs nulles
- Seuil : 0

---

### 4.3 Présence de l’identifiant client
Ce contrôle s’assure que chaque transaction est associée à un client valide.

- Objectif : garantir l’intégrité référentielle
- Métrique : nombre de `client_id` nuls
- Seuil : 0

---

### 4.4 Montants positifs
Ce contrôle vérifie l’absence de montants négatifs.

- Objectif : détecter les anomalies financières
- Métrique : nombre de montants négatifs
- Seuil : 0

---

### 4.5 Présence de la date de transaction
Ce contrôle vérifie que toutes les transactions disposent d’une date valide.

- Objectif : permettre les analyses temporelles
- Métrique : nombre de dates nulles
- Seuil : 0

---

### 4.6 Cohérence entre la date et l’année
Ce contrôle compare l’année extraite de `transaction_date` avec la colonne `year`.

- Objectif : garantir la cohérence temporelle et le bon partitionnement
- Métrique : nombre de lignes incohérentes
- Seuil : 0

---

### 4.7 Unicité des identifiants de transaction
Ce contrôle détecte les doublons sur la colonne `transaction_id`.

- Objectif : éviter les doubles comptages dans les analyses
- Métrique : nombre d’identifiants dupliqués
- Seuil : 0

---

### 4.8 Ratio de transactions à forte valeur
Ce contrôle mesure la proportion de transactions considérées comme à forte valeur.

- Objectif : détecter des distributions anormales
- Métrique : ratio de transactions à forte valeur
- Seuil : ≤ 20 %

---


## 5. Résultats des contrôles de qualité

Les contrôles de qualité ont été exécutés sur le jeu de données enrichi issu de la couche Silver.  
Les résultats sont consolidés dans une table structurée générée automatiquement par le pipeline.

Les fichiers suivants ont été produits :
- `data_quality_results.parquet` : format analytique exploitable par Spark
- `data_quality_results.csv` : format lisible et exportable

Ces fichiers sont stockés dans le dossier :
`/Volumes/workspace/ipsldata/capstoneipsl/reports/data_quality/`

### 5.1 Structure des résultats

La table des résultats contient les champs suivants :

- `run_id` : identifiant unique de l’exécution
- `check_name` : nom du contrôle appliqué
- `status` : résultat du contrôle (PASS / FAIL)
- `metric_value` : valeur mesurée
- `threshold` : seuil de validation
- `timestamp` : date et heure d’exécution

### 5.2 Extrait des résultats obtenus

| check_name | status | metric_value | threshold |
|-----------|--------|--------------|-----------|
| dataset_not_empty | PASS | > 0 | > 0 |
| transaction_id_not_null | PASS | 0 | 0 |
| client_id_not_null | PASS | 0 | 0 |
| amount_positive | PASS | 0 | 0 |
| transaction_date_not_null | PASS | 0 | 0 |
| year_consistency | PASS | 0 | 0 |
| transaction_id_unique | PASS | 0 | 0 |
| high_value_ratio | PASS | 0.1342 | ≤ 0.20 |

*(Les résultats complets sont disponibles dans les fichiers exportés.)*

### 5.3 Interprétation des résultats

L’ensemble des contrôles exécutés retourne un statut **PASS**, indiquant que :
- les données sont complètes,
- les identifiants sont uniques et cohérents,
- les règles métier essentielles sont respectées,
- aucune anomalie critique n’a été détectée.

Ces résultats confirment que le jeu de données est **fiable et prêt pour une exploitation analytique et décisionnelle**, notamment dans la couche Gold et pour les exports BI.


---

## 6. Conclusion sur la qualité des données

Les contrôles de qualité mis en place confirment que les données produites par le pipeline sont **complètes, cohérentes et fiables**.  
Ce dispositif permet de sécuriser la couche Gold et de garantir que les jeux de données analytiques et les exports BI reposent sur une base de données de qualité maîtrisée.
