import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

SILVER_FILE = (
    "silver/orders/"
    "order_date=2026-02-01/part-0002.parquet"
)

GOLD_PATH = (
    "gold/daily_sales/"
    "date=2026-02-09"
)

os.makedirs(GOLD_PATH, exist_ok=True)

table = pq.read_table(SILVER_FILE)
df = table.to_pandas()

# Gold transformation

gold_metrics = (
    df.groupby('order_date')
    .agg(
        total_orders = ('order_id', 'count'),
        unique_customers = ('user_id', 'nunique'),
        total_revenue = ('total_amount', 'sum'),
        avg_order_value = ('total_amount', 'mean'),
        min_order_value = ('total_amount', 'min'),
        max_order_value = ('total_amount', 'max')
    )
    .reset_index()
)

gold_metrics['revenue_per_customer'] = (
    gold_metrics['total_revenue'] / gold_metrics['unique_customers']
)

gold_table = pa.Table.from_pandas(gold_metrics)

pq.write_table(
    gold_table,
    f"{GOLD_PATH}/part-0003.parquet",
    compression="snappy"
)

print("✅ Gold daily sales analytics Parquet created")
