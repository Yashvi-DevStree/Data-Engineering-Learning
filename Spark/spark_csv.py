from pyspark.sql import SparkSession
from pathlib import Path

spark = SparkSession.builder \
    .appName("DataEngineeringPractice") \
    .getOrCreate()

print("Spark session started")

csv_file = "employee.csv"
df = spark.read.csv(str(csv_file), header=True, inferSchema=True)

print("\n--- Show Data ---")
df.show()

print("\n--- Schema ---")
df.printSchema()

print("\n--- Row Count ---")
print(f"Total rows: {df.count()}")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(str(csv_file))


print("\n--- Selected Columns (name, dept, salary) ---")
df.select("name", "dept", "salary").show(5)

print("\n--- Filtered Data (salary > 70000) ---")
df_filtered = df.filter(df.salary > 70000).show(5)
