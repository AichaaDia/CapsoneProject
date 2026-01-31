def build_transactions_mart(df_joined):
    return df_joined.select(
        "transaction_id",
        "client_id",
        "transaction_date",
        "year",
        "amount",
        "loan_flag",
        "high_value_txn_flag",
        "weekend_flag",
        "balance_flag",
        "client_tenure_days"
    )
