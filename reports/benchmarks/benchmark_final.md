# Benchmark Lakehouse Pipeline - Version Explicative

**Date d'exécution :** 2026-02-02 18:21:13

## 1️⃣ Avant optimisation — Format Parquet

- Durée d'exécution : 0.54 secondes
- Nombre de fichiers : 412
- Taille totale : 4988.99 MB

Cette étape correspond à l'état initial du pipeline. Les données sont stockées en format Parquet standard. Nous pouvons observer la structure actuelle et mesurer les performances de lecture avant toute optimisation. Le plan Spark ci-dessous permet de comprendre comment les transformations sont exécutées.

### Plan Spark (Parquet)
```text
== Physical Plan ==
*(1) ColumnarToRow
+- PhotonResultStage
   +- PhotonScan parquet [transaction_id#13159,client_id#13160,account_id#13161,transaction_date#13162,amount#13163,currency#13164,transaction_type#13165,channel#13166,status#13167,balance_after#13168,run_date#13169,year#13170] DataFilters: [], DictionaryFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/silver/main_clean], OptionalDataFilters: [], PartitionFilters: [], ReadSchema: struct<transaction_id:string,client_id:string,account_id:string,transaction_date:date,amount:doub..., RequiredDataFilters: []


== Photon Explanation ==
The query is fully supported by Photon.

```

## 2️⃣ Après optimisation — Format Delta Lake

- Durée d'exécution : 0.3 secondes
- Nombre de fichiers : 41
- Taille totale : 4987.34 MB

Après optimisation, les données sont stockées en Delta Lake. Ce format permet une meilleure gestion des transactions et souvent une amélioration des performances. Cette section montre comment la lecture et les transformations ont été optimisées. Le plan Spark ci-dessous illustre les changements dans l'exécution.

### Plan Spark (Delta)
```text
== Physical Plan ==
*(1) ColumnarToRow
+- PhotonResultStage
   +- PhotonScan parquet [transaction_id#13347,client_id#13348,account_id#13349,transaction_date#13350,amount#13351,currency#13352,transaction_type#13353,channel#13354,status#13355,balance_after#13356,run_date#13357,year#13358] DataFilters: [], DictionaryFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/silver/main_cl..., OptionalDataFilters: [], PartitionFilters: [], ReadSchema: struct<transaction_id:string,client_id:string,account_id:string,transaction_date:date,amount:doub..., RequiredDataFilters: []


== Photon Explanation ==
The query is fully supported by Photon.

```

## 3️⃣ Comparaison synthétique

| Indicateur | Avant (Parquet) | Après (Delta) | Observations |
|------------|-----------------|---------------|--------------|
| Durée (s) | 0.54 | 0.3 | La lecture a été accélérée grâce à la structure optimisée et à la gestion des transactions. |
| Nombre fichiers | 412 | 41 | Le nombre de fichiers est réduit, ce qui simplifie la gestion et améliore l'efficacité du pipeline. |
| Taille (MB) | 4988.99 | 4987.34 | La taille totale a diminué, indiquant une meilleure compaction et un stockage plus efficace. |

## 4️⃣ Analyse et recommandations

Dans l'ensemble, l'optimisation vers Delta Lake apporte des améliorations en termes de performances et de gestion des fichiers. Cette synthèse présente les résultats du benchmark du pipeline Lakehouse. Elle met en évidence les améliorations obtenues après la migration vers Delta Lake, notamment sur la durée d'exécution et la taille des fichiers. Ces observations permettent de mieux comprendre le comportement du pipeline et servent de base pour la documentation finale du projet.Autrement dit cette synthèse fournit une vue claire des gains obtenus et des points à surveiller pour les prochaines étapes.
