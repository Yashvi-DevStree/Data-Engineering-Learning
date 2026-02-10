import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

BRONZE_FILE = (
    "bronze/orders/"
    "ingestion_date=2026-02-01/part-0001.parquet"
)

SILVER_PATH = (
    "silver/orders/"
    "order_date=2026-02-01"
)

os.makedirs(SILVER_PATH, exist_ok=True)
df = pd.read_parquet(BRONZE_FILE)

# convert order_date to datetime, remove duplicates, filter invalid amount
df['order_date'] = pd.to_datetime(df['order_date']).dt.date
df = df.drop_duplicates(subset=['order_id'])
df = df[df['total_amount'] > 0]

schema = pa.schema([
    ("order_id", pa.int64()),
    ("user_id", pa.int64()),
    # ("order_date", pa.date32()),
    ("total_amount", pa.float64()),
    ("ingestion_date", pa.string())
])

table = pa.Table.from_pandas(df, schema=schema)

pq.write_table(
    table,
    f"{SILVER_PATH}/part-0002.parquet",
    compression="snappy"
)

print("✅ Silver Parquet created")