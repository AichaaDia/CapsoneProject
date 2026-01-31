# Benchmark APRÈS optimisation — Delta Lake

**Date :** 2026-01-30_23-45-54

- ⏱ Durée pipeline : 0.0 s
- 📄 Nombre de fichiers : 41
- 💾 Taille totale : 4987.34 MB

## Plan d'exécution Spark
```text
== Physical Plan ==
*(1) ColumnarToRow
+- PhotonResultStage
   +- PhotonScan parquet [transaction_id#13482,client_id#13483,account_id#13484,transaction_date#13485,amount#13486,currency#13487,transaction_type#13488,channel#13489,status#13490,balance_after#13491,run_date#13492,year#13493] DataFilters: [], DictionaryFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/silver/main_cl..., OptionalDataFilters: [], PartitionFilters: [], ReadSchema: struct<transaction_id:string,client_id:string,account_id:string,transaction_date:date,amount:doub..., RequiredDataFilters: []


== Photon Explanation ==
The query is fully supported by Photon.

```
