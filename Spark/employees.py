from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Employees').getOrCreate()

schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('salary', DoubleType(), True),
    StructField('hire_date', DateType(), True),
    StructField('role', StringType(), True)
])

df = spark.read.schema(schema).csv('employees.csv')
df.show()


# Basic Data Exploration

# Number of rows
# count = df.count()

# Column names
# columns = df.columns  # ['name', 'age', 'role']

# Schema
# df.printSchema()

# Data types
# print(df.dtypes)  # [('name', 'string'), ('age', 'bigint')]

# Summary statistics
# df.describe().show()

# Specific columns
# df.describe('age', 'salary').show()


# Selecting Columns

# Single column
# df.select('name').show()

# Multiple columns
# df.select('name', 'age').show()

# Using col() function
# df.select(col('name'), col('age')).show()

# Adding new columns
# df = df.withColumn('age_plus_ten', col('age') + 10)

# All columns
# df.select('*').show()


# Filtering rows

# Single condition
# 1. df.filter(col('age') > 30).show()
# 2. df.filter(df['age'] > 30).show()
# 3. df.filter('age > 30').show()
# 4. df.where(col('age') > 30).show() same as filter

# Multiple conditions (AND)
df.filter((col('age') > 25) & (col('role') == 'Director')).show()

# Multiple conditions (OR)
df.filter((col('age') < 30) | (col('role') == "Manager")).show()

# Not condition
df.filter(~(col('role') == "Intern")).show()

# Filtering with SQL expression
df.filter("age > 30 AND role = 'Director'").show()

# Sorting Data
# Ascending order
df.sort(col('age').asc()).show()

# Descending order
df.sort(col('salary').desc()).show()

# in
df.filter(col('role').isin("Manager", 'Director')).show()


# Common transformations

# 1. Add/Modify column
df.withColumn('senior_employee', col('age') > 40).show()

# 2. Drop column
df.drop('age_plus_ten', 'senior_employee').show()

# 3. Rename column
df.withColumnRenamed('monthly_salary', 'salary').show()

# 4. Distinct values
df.distinct().show()

# 5. Drop duplicates
df.dropDuplicates(['name', 'age']).show()

# 6.sort data
df.orderBy('age').show()
df.orderBy(col('salary').desc()).show()