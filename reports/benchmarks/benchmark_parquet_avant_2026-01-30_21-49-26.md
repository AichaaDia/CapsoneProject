# Benchmark AVANT optimisation — Parquet

**Date :** 2026-01-30_21-49-26

- ⏱ Durée pipeline : 0.0 s
- 📄 Nombre de fichiers : 412
- 💾 Taille totale : 4988.99 MB

## Plan d'exécution Spark
```text
== Physical Plan ==
*(1) ColumnarToRow
+- PhotonResultStage
   +- PhotonScan parquet [transaction_id#13159,client_id#13160,account_id#13161,transaction_date#13162,amount#13163,currency#13164,transaction_type#13165,channel#13166,status#13167,balance_after#13168,run_date#13169,year#13170] DataFilters: [], DictionaryFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/silver/main_clean], OptionalDataFilters: [], PartitionFilters: [], ReadSchema: struct<transaction_id:string,client_id:string,account_id:string,transaction_date:date,amount:doub..., RequiredDataFilters: []


== Photon Explanation ==
The query is fully supported by Photon.

```
