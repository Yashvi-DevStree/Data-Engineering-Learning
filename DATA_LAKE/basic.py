import pandas as pd
# import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
# from datetime import datetime, timedelta
import os
# import time

def create_parquet():
     
    # Step 1: Create book data
     data = {
        'book_id': [1, 2, 3, 4, 5],
        'title': [
            'Python Data Engineering',
            'SQL for Beginners',
            'Apache Spark Guide',
            'Data Lakes Explained',
            'ETL Best Practices'
        ],
        'author': [
            'John Smith',
            'Jane Doe',
            'Bob Johnson',
            'Alice Williams',
            'Charlie Brown'
        ],
        'price': [29.99, 24.99, 34.99, 19.99, 27.99],
        'in_stock': [True, True, False, True, True]
    }
     
     df = pd.DataFrame(data)
     print(f"✓ Created DataFrame with {len(df)} books")

    #  Step 2: Convert to Parquet
     filepath = 'books.parquet'
     df.to_parquet(filepath, index=False)

    #  Step 3: Verify Parquet file creation
     if os.path.exists(filepath):
        file_size = os.path.getsize(filepath)
        print(f"✓ File exists!")
        print(f"✓ File size: {file_size / 1024:.2f} KB")
     else:
         print("File creation failed.")

if __name__ == "__main__":
    create_parquet()