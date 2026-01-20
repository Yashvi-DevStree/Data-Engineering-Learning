from collections import defaultdict
import csv

# 1. Calculate total revenue for each order 

total_revenue = 0.0

with open("order.csv", 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        total_revenue += int(row['Quantity']) * int(row['Price'])
        print(f" Order Id: {row['Id']}, Total Revenue: {total_revenue}")
    
# 2. Calculate total revenue per category

category_revenue = defaultdict(int)
with open("order.csv", 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        revenue = int(row['Quantity']) * int(row['Price'])
        category_revenue[row['Category']] += revenue

for category, revenue in category_revenue.items():
    print(f"Category: {category}, Total Revenue: {revenue}")

# 3. Find the customer who spent the most

customer_spent = defaultdict(int)
with open("order.csv", 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        customer_spent[row['Name']] += int(row['Quantity']) * int(row['Price'])

top_customer = max(customer_spent, key=customer_spent.get)
print(f"Top Customer: {top_customer}, Amount Spent: {customer_spent[top_customer]}")

# 4. Filter orders by category 'Electronics' and write to a new CSV file

with open("order.csv", 'r') as infile, open("electronic_orders.csv", 'w', newline='') as outfile:
    reader = csv.DictReader(infile)
    fields = reader.fieldnames
    writer = csv.DictWriter(outfile, fieldnames=fields)
    writer.writeheader()
    for row in reader:
        if row['Category'] == 'Electronics':
            writer.writerow(row)

# 5. Increase price by 5% for Clothing category and write to a new CSV file

with open("order.csv", 'r') as infile, open("updated_orders.csv", 'w', newline='') as outfile:
    reader = csv.DictReader(infile)
    fields = reader.fieldnames
    writer = csv.DictWriter(outfile, fieldnames=fields)
    writer.writeheader()
    for row in reader:
        if row['Category'] == 'Clothing':
            row['Price'] = int(int(row['Price']) * 1.05)
            writer.writerow(row)

#  6. Count number of orders per customer

customer_order_count = defaultdict(int)
with open("order.csv", 'r') as file:
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        customer_order_count[row[1]] += 1
    
print(dict(customer_order_count))
# for customer, count in customer_order_count.items():
#     print(f"Customer: {customer}, No. of Orders: {count}")
