import pyspark.sql.functions as F

def run_quality_checks(df, run_id):

    results = []

    total_rows = df.count()

    # 1. Dataset non vide
    results.append({
        "check_name": "dataset_not_empty",
        "status": "PASS" if total_rows > 0 else "FAIL",
        "metric_value": total_rows,
        "threshold": "> 0",
        "run_id": run_id
    })

    # 2. transaction_id non null
    null_tx = df.filter(F.col("transaction_id").isNull()).count()
    results.append({
        "check_name": "transaction_id_not_null",
        "status": "PASS" if null_tx == 0 else "FAIL",
        "metric_value": null_tx,
        "threshold": "0",
        "run_id": run_id
    })

    # 3. client_id non null
    null_clients = df.filter(F.col("client_id").isNull()).count()
    results.append({
        "check_name": "client_id_not_null",
        "status": "PASS" if null_clients == 0 else "FAIL",
        "metric_value": null_clients,
        "threshold": "0",
        "run_id": run_id
    })

    # 4. amount positif
    negative_amounts = df.filter(F.col("amount") < 0).count()
    results.append({
        "check_name": "amount_positive",
        "status": "PASS" if negative_amounts == 0 else "FAIL",
        "metric_value": negative_amounts,
        "threshold": "0",
        "run_id": run_id
    })

    # 5. transaction_date non null
    null_dates = df.filter(F.col("transaction_date").isNull()).count()
    results.append({
        "check_name": "transaction_date_not_null",
        "status": "PASS" if null_dates == 0 else "FAIL",
        "metric_value": null_dates,
        "threshold": "0",
        "run_id": run_id
    })

    # 6. cohérence année
    invalid_year = df.filter(F.year("transaction_date") != F.col("year")).count()
    results.append({
        "check_name": "year_consistency",
        "status": "PASS" if invalid_year == 0 else "FAIL",
        "metric_value": invalid_year,
        "threshold": "0",
        "run_id": run_id
    })

    # 7. unicité transaction_id
    duplicates = (
        df.groupBy("transaction_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    results.append({
        "check_name": "transaction_id_unique",
        "status": "PASS" if duplicates == 0 else "FAIL",
        "metric_value": duplicates,
        "threshold": "0",
        "run_id": run_id
    })

    # 8. high value transactions ratio
    ratio = df.filter(F.col("high_value_txn_flag") == 1).count() / total_rows
    results.append({
        "check_name": "high_value_ratio",
        "status": "PASS" if ratio <= 0.20 else "FAIL",
        "metric_value": round(ratio, 4),
        "threshold": "<= 0.20",
        "run_id": run_id
    })

    return results
