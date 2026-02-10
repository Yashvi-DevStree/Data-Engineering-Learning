import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import os
import time

def practice():
    filepath = 'books.parquet'

    # Step 1: Read the Parquet file
    if not os.path.exists(filepath):
        print("Parquet file does not exist.")
        return
    
    df = pd.read_parquet(filepath)
    print(f"✓ Read {len(df)} rows from {filepath}\n")

    # Step 2: Data Analysis
    # a) Total number of books
    total_books = len(df)
    print(f"Total number of books: {total_books}")

    # b) Average price of books
    avg_price = df['price'].mean()
    print(f"Average price of books: ${avg_price:.2f}")

    # c. Number of books in stock
    in_stock_count = df['in_stock'].sum()
    print(f"Number of books in stock: {in_stock_count}")

    # Step 3: Print Schema
    print("\n Schema: ")
    for col, dtype in df.dtypes.items():
        print(f" - {col}: {dtype}")

    
    # Bonus: Using PyArrow to see more detailed schema
    print("Detailed Schema (using PyArrow):")
    
    parquet_file = pq.ParquetFile(filepath)
    print(parquet_file.schema)
    
    # Metadata
    print("\nFile Metadata:")
    print(f"  Number of row groups: {parquet_file.num_row_groups}")
    print(f"  Total rows: {parquet_file.metadata.num_rows}")

if __name__ == "__main__":
    practice()

