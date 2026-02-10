import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

RAW_PATH = "raw/orders/2026-02-01.csv"
BRONZE_PATH = (
    "bronze/orders/"
    "ingestion_date=2026-02-01"
)

os.makedirs(BRONZE_PATH, exist_ok=True)
df = pd.read_csv(RAW_PATH)

# add ingestion timestamp
df['ingestion_date'] = "2026-02-01"

table = pa.Table.from_pandas(df)
pq.write_table(
    table,
    f"{BRONZE_PATH}/part-0001.parquet",
    compression="snappy"
)

print("✅ Bronze Parquet created")
