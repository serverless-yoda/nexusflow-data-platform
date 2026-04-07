import dlt

@dlt.table(name="dim_customers_silver")
def dim_customers_silver():
    # 1. Read from Bronze
    # 2. Apply Cleansing (PII Hashing)
    return dlt.read("customers_bronze").selectExpr(
        "customer_id",
        "sha2(email, 256) as email_hashed",
        "current_timestamp() as processing_time"
    )

# Implementing SCD Type 2 automatically
dlt.apply_changes(
    target = "dim_customers_gold",
    source = "dim_customers_silver",
    keys = ["customer_id"],
    sequence_by = "processing_time",
    stored_as_scd_type = 2 # Automatically tracks history with __start_at/__end_at
)