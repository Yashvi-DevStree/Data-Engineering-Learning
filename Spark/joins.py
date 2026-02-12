from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import os

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

spark = SparkSession.builder.getOrCreate()

def joins():
    customers_data = [
        (1, 'Alice', 'alice@email.com'),
        (2, 'Bob', 'bob@email.com'),
        (3, 'Charlie', 'charlie@email.com'),
        (4, 'Diana', 'diana@email.com')
    ]
    customers_cols = ['customer_id', 'name', 'email']
    customers = spark.createDataFrame(customers_data, customers_cols)

    orders_data = [
        (101, 1, '2024-01-15', 250.00),
        (102, 1, '2024-01-20', 180.00),
        (103, 2, '2024-01-18', 320.00),
        (104, 3, '2024-01-22', 150.00),
        (105, 1, '2024-01-25', 420.00)
    ]
    orders_cols = ['order_id', 'customer_id', 'order_date', 'amount']
    orders = spark.createDataFrame(orders_data, orders_cols)
    
    
    # Task 1: Inner Join: Customers with their orders

    print("\n[Task 1] Inner Join: Customers with their orders: ")
    inner_join = customers.join(orders, 'customer_id', 'inner')
    inner_join.show()


    # Task 2: Left Join: All customers 

    print("\n[Task 2] Left Join: All customers (including those with no orders): ")
    left_join = customers.join(orders, 'customer_id', 'left')
    left_join.show()


    # Task 3: Right join

    print("\n[Task 3] Right join - All orders with customer info:")
    right_join = customers.join(orders, 'customer_id', 'right')
    right_join.show()


    # Task 4: Left anti join - Customers with NO orders

    print("\n[Task 4] Left anti join - Customers who never ordered:")
    anti_join = customers.join(orders, 'customer_id', 'left_anti')
    anti_join.show()
    

    # Task 5: Left semi join - Customers with at least one order (no order details)
   
    print("\n[Task 5] Left semi join - Customers who have ordered (no order details):")
    semi_join = customers.join(orders, 'customer_id', 'left_semi')
    semi_join.show()

    
    # Bonus: Multiple joins
    
    print("\nBonus: Order summary with customer names and totals:")
    order_summary = customers.join(orders, 'customer_id', 'inner')\
    .groupBy('customer_id', 'name')\
    .agg(
        count('order_id').alias('num_orders'),
        sum('amount').alias('total_spent')
    ).orderBy('total_spent', ascending=False)

    order_summary.show()

joins()
