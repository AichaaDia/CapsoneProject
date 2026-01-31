def aggregate_transactions(df_joined):
    
   
    df_joined = df_joined.withColumn("week", F.weekofyear("transaction_date"))
    df_joined = df_joined.withColumn("month", F.month("transaction_date"))
    
    # Agrégation journalière
    daily_agg = df_joined.groupBy("transaction_date").agg(
        F.count("transaction_id").alias("nb_transactions"),
        F.sum("amount").alias("total_amount"),
        F.sum("high_value_txn_flag").alias("high_value_count")
    )
    
    # Agrégation hebdomadaire
    weekly_agg = df_joined.groupBy("year", "week").agg(
        F.count("transaction_id").alias("nb_transactions"),
        F.sum("amount").alias("total_amount"),
        F.sum("high_value_txn_flag").alias("high_value_count")
    )
    
    # Agrégation mensuelle
    monthly_agg = df_joined.groupBy("year", "month").agg(
        F.count("transaction_id").alias("nb_transactions"),
        F.sum("amount").alias("total_amount"),
        F.sum("high_value_txn_flag").alias("high_value_count")
    )
    
    return {
        "daily": daily_agg,
        "weekly": weekly_agg,
        "monthly": monthly_agg
    }