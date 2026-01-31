# Benchmark APRÈS optimisation — Delta Lake

**Date :** 2026-01-30_21-51-14

- ⏱ Durée pipeline : 0.0 s
- 📄 Nombre de fichiers : 41
- 💾 Taille totale : 4987.34 MB

## Plan d'exécution Spark
```text
== Physical Plan ==
*(1) ColumnarToRow
+- PhotonResultStage
   +- PhotonScan parquet [transaction_id#13790,client_id#13791,account_id#13792,transaction_date#13793,amount#13794,currency#13795,transaction_type#13796,channel#13797,status#13798,balance_after#13799,run_date#13800,year#13801] DataFilters: [], DictionaryFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/silver/main_cl..., OptionalDataFilters: [], PartitionFilters: [], ReadSchema: struct<transaction_id:string,client_id:string,account_id:string,transaction_date:date,amount:doub..., RequiredDataFilters: []


== Photon Explanation ==
The query is fully supported by Photon.

```
