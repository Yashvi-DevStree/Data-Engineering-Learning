from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import os

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

spark = SparkSession.builder.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def window_functions():

    employees_data = [
        (1, 'Alice', 'Engineering', 95000),
        (2, 'Bob', 'Engineering', 87000),
        (3, 'Charlie', 'Engineering', 92000),
        (4, 'Diana', 'Sales', 78000),
        (5, 'Eve', 'Sales', 82000),
        (6, 'Frank', 'Sales', 75000),
        (7, 'Grace', 'Marketing', 71000),
        (8, 'Henry', 'Marketing', 73000),
        (9, 'Ivy', 'Marketing', 69000)
    ]
    columns = ['id', 'name', 'department', 'salary']
    
    df = spark.createDataFrame(employees_data, columns)
    

    # Task 1: Rank by salary within department
    # print("\n[Task 1] Rank employees by salary within department:")

    window_spec = Window.partitionBy('department').orderBy(col('salary').desc())

    ranked_df = df.withColumn('dense_rank', dense_rank().over(window_spec))\
    .withColumn('row_number', row_number().over(window_spec))

    ranked_df.orderBy('department', 'dense_rank').show()

    
    # Task 2: Top 2 earners per department
    print("\n[Task 2] Top 2 earners per department:")
    
    top_2 = ranked_df.filter(col('row_number') <= 2)\
    .select('name', 'department', 'salary', 'row_number')\
    .orderBy('department', 'row_number')

    top_2.show()

     
    # Task 3: Running total of salaries
    print("\n[Task 3] Running total of salaries:")

    running_window = Window.partitionBy('department').orderBy('salary').rowsBetween(Window.unboundedPreceding, Window.currentRow)

    running_total = df.withColumn('running_total', sum('salary').over(running_window)).orderBy('department', 'salary')
    running_total.show()

    
    # Task 4: Compare to department average
    print("\n[Task 4] Compare each salary to department average:")

    dept_window = Window.partitionBy('department')
    
    comparison_df = df.withColumn('dept_avg_salary', 
        avg('salary').over(dept_window)) \
        .withColumn('diff_from_avg', 
            col('salary') - col('dept_avg_salary')) \
        .withColumn('pct_diff', 
            ((col('salary') - col('dept_avg_salary')) / col('dept_avg_salary') * 100))
    
    comparison_df.select(
        'name', 'department', 'salary', 'dept_avg_salary', 
        'diff_from_avg', 'pct_diff'
    ).orderBy('department', 'salary', ascending=False).show()


    # Task 5: Percentile rank
    print("\n[Task 5] Salary percentile within department:")
    
    percentile_df = df.withColumn('percentile_rank', 
        percent_rank().over(window_spec))
    
    percentile_df.orderBy('department', col('percentile_rank').desc()).show()


window_functions()