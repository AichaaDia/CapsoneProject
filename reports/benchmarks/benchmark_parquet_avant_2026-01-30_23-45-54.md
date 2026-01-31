# Benchmark AVANT optimisation — Parquet

**Date :** 2026-01-30_23-45-54

- ⏱ Durée pipeline : 0.0 s
- 📄 Nombre de fichiers : 412
- 💾 Taille totale : 4988.99 MB

## Plan d'exécution Spark
```text
== Physical Plan ==
*(1) ColumnarToRow
+- PhotonResultStage
   +- PhotonScan parquet [transaction_id#13418,client_id#13419,account_id#13420,transaction_date#13421,amount#13422,currency#13423,transaction_type#13424,channel#13425,status#13426,balance_after#13427,run_date#13428,year#13429] DataFilters: [], DictionaryFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/silver/main_clean], OptionalDataFilters: [], PartitionFilters: [], ReadSchema: struct<transaction_id:string,client_id:string,account_id:string,transaction_date:date,amount:doub..., RequiredDataFilters: []


== Photon Explanation ==
The query is fully supported by Photon.

```
