from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import os

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

spark = SparkSession.builder.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def advanced_operations():
    random.seed(42)
    base_data = datetime(2024, 1, 1)
    sales_data = []
    for i in range(30):
        date = base_data + timedelta(days=i)
        sales = random.randint(5000, 15000)
        sales_data.append((i+1, date.strftime('%Y-%m-%d'), sales))

    columns = ['day_num', 'date', 'sales']
    df = spark.createDataFrame(sales_data, columns)
        

    # Task 1: Day-over-day change
    print("\n[Task 1] Day-over-day sales change:")

    window_spec = Window.orderBy('date')

    dod_df = df.withColumn('prev_day_sales', lag('sales', 1).over(window_spec))\
    .withColumn('dod_change', col('sales') - col('prev_day_sales'))

    dod_df.select('date', 'sales', 'prev_day_sales', 'dod_change').show(10)


    # Task 2: 7-day moving average
    print("\n[Task 2] 7-day moving average:")

    moving_avg_window = Window.orderBy('date').rowsBetween(-6, 0)   # Current + 6 previous = 7 days

    ma_df = df.withColumn('sales_7day_ma', avg('sales').over(moving_avg_window))\
    .withColumn('vs_7day_avg', col('sales') - col('sales_7day_ma'))

    ma_df.select('date', 'sales', 'sales_7day_ma', 'vs_7day_avg').show(10)


    # Task 3: Year-to-date cumulative
    print("\n[Task 3] Year-to-date cumulative sales:")

    ytd_window = Window.orderBy('date')\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    ytd_df = df.withColumn('ytd_sales', sum('sales').over(ytd_window))
    ytd_df.select('date', 'sales', 'ytd_sales').show(10)


    # Task 4: Identify growth streaks
    print("\n[Task 4] Identify consecutive days of growth:")

    # Add previous and next day sales
    streak_df = df.withColumn('prev_sales', lag('sales', 1).over(window_spec))\
    .withColumn('next_sales', lead('sales', 1).over(window_spec))\
    .withColumn('is_growing', when(col('sales') > col('prev_sales'), 1).otherwise(0))\
    .withColumn('growth_vs_prev', when(
        col('sales') > col('prev_sales'), col('sales') - col('prev_sales'))\
        .otherwise(0)
    )

    # Show days with growth
    growth_days = streak_df.filter(col('is_growing') == 1)\
    .select('date', 'sales', 'prev_sales', 'is_growing' ,'growth_vs_prev')

    growth_days.show(30)

    # Bonus: Multiple window analytics in one view
    print("\nBonus: Comprehensive daily analytics:")
    
    comprehensive = df \
        .withColumn('prev_day', lag('sales', 1).over(window_spec)) \
        .withColumn('next_day', lead('sales', 1).over(window_spec)) \
        .withColumn('7day_avg', avg('sales').over(moving_avg_window)) \
        .withColumn('ytd_total', sum('sales').over(ytd_window)) \
        .withColumn('max_so_far', max('sales').over(ytd_window)) \
        .withColumn('is_record', 
            when(col('sales') == col('max_so_far'), 'Yes').otherwise('No'))
    
    comprehensive.select(
        'date', 'sales', 'prev_day', '7day_avg', 
        'ytd_total', 'is_record'
    ).show(10)


advanced_operations()