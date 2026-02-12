from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

spark = SparkSession.builder.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def practice():
    sales_data = [
        (1, 'North', 'Laptop', 1200, '2024-01-15'),
        (2, 'South', 'Phone', 800, '2024-01-16'),
        (3, 'North', 'Tablet', 600, '2024-01-17'),
        (4, 'East', 'Laptop', 1200, '2024-01-18'),
        (5, 'North', 'Phone', 850, '2024-01-19'),
        (6, 'South', 'Laptop', 1100, '2024-01-20'),
        (7, 'East', 'Tablet', 650, '2024-01-21'),
        (8, 'North', 'Laptop', 1250, '2024-01-22'),
        (9, 'South', 'Phone', 780, '2024-01-23'),
        (10, 'East', 'Laptop', 1150, '2024-01-24')
    ]
    columns = ['transaction_id', 'region', 'product', 'amount', 'date']

    df = spark.createDataFrame(sales_data, schema=columns)

    # print("\n✓ Created sales DataFrame with 10 transactions\n")
    # df.show()

    # Task 1: Total sales by region
    # print("\n[Task 1] Total sales by region:")
    df.groupBy('region')\
    .agg(sum('amount').alias('total_sales'))\
    .orderBy('total_sales')\
    # .show()

    # Task 2: Average sales by product
    # print("\n[Task 2] Average sales by product :")
    df.groupBy('product')\
    .agg(avg('amount').alias('avg_sales'))\
    .orderBy('avg_sales', ascending=False)\
    # .show()

    # Task 3:Transaction Count by region and product
    # print("\n[Task 3] Transaction Count by region and product :")
    df.groupBy('region', 'product')\
    .agg(count('*').alias('transaction_count'))\
    .orderBy('region', 'product')\
    # .show()

    # Task 4: Region with total_sales > 3000
    # print("\n[Task 4] Region with total sales > 3000 :")
    df.groupBy('region')\
    .agg(sum('amount').alias('total_sales'))\
    .filter(col('total_sales') > 3000)\
    # .show()

    # Task 5: Multiple aggregations
    print("\n[Task 5] Comprehensive metrics by region :")
    df.groupBy('region').agg(
        count('*').alias('num_transactions'),
        sum('amount').alias('total_sales'),
        avg('amount').alias('avg_sales'),
        min('amount').alias('min_sales'),
        max('amount').alias('max_sales')
    ).show()

    # Bonus: Product Performance
    print("\nBonus: Product performance metrics:")
    df.groupBy('product').agg(
        count('*').alias('num_sold'),
        sum('amount').alias('revenue'),
        avg('amount').alias('avg_price')
    ).orderBy('revenue', ascending=False).show()

    # Bonus: Using agg with dictionary (alternative syntax)
    print("\nAlternative syntax using dictionary:")
    df.groupBy('region').agg({
        'amount': 'sum',
        'transaction_id': 'count'
    }).show()

practice()