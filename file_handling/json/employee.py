from collections import defaultdict
import csv

# 1. Read and display the contents of csv file

# with open("employee.csv", 'r') as file:
#     reader = csv.reader(file)
#     for row in reader:
#         print(row)

#  2. Read csv using DictReader and print specific columns

# with open("employee.csv", 'r') as file:
#     dict_reader = csv.DictReader(file)
#     for row in dict_reader:
#         print(f"Name: {row['Name']}, Department: {row['Department']}")

#  3. Print employees working in Engineering department

# with open("employee.csv", 'r') as file:
#     reader = csv.DictReader(file)
#     for row in reader:
#         if row.get("Department") == 'Engineering':
#             print(row.get("Name"))

#  4. Calculate average salary of all employees

# with open("employee.csv", 'r') as file:
#     reader = csv.DictReader(file)
#     total_salary = 0
#     count = 0
#     for row in reader:
#         total_salary += int(row['Salary'])
#         count += 1
#         average_salary = total_salary / count
#     print(f"Name: {row['Name']}, Average Salary: {average_salary}, Total Salary: {row['Salary']}")

#  5. List employees who joined after 2020

# with open("employee.csv", 'r') as file:
#     reader = csv.DictReader(file)
#     for row in reader:
#         if row['Joining_Date'] > '2020-12-31':
#             print(f"Name: {row['Name']}, Joining Date: {row['Joining_Date']}")

# 6. Count number of employees in each department

# with open("employee.csv", 'r') as file:
#     reader = csv.DictReader(file)
#     department_count = {}
#     for row in reader:
#         dept = row['Department']
#         if dept in department_count:
#             department_count[dept] += 1
#         else:
#             department_count[dept] = 1
#     for dept, count, in department_count.items():
#         print(f"Department: {dept} | Count: {count}")

#  6.2 using defaultdict

# with open("employee.csv", 'r') as file:
#     reader = csv.DictReader(file)
#     department_count = defaultdict(int)
#     for row in reader:
#         dept = row['Department']
#         department_count[dept] += 1
#     for dept, count in department_count.items():
#         print(f"Department: {dept}, Count: {count}")
    
#  7. Find the highest paid employee

# with open("employee.csv", 'r') as file:
#     reader = csv.DictReader(file)
#     highest_paid = 0
#     for row in reader:
#         if int(row['Salary']) > highest_paid:
#             highest_paid = int(row['Salary'])
#     print(f"Highest Paid Employee: {row['Name']}, Salary: {highest_paid}")   

# 8. Write all Engineering employees to a new CSV file

# with open("employee.csv", 'r') as infile, open("engineering_employees.csv", 'w', newline='') as outfile:
    # reader = csv.DictReader(infile)
    # fields = reader.fieldnames
    # writer = csv.DictWriter(outfile, fieldnames=fields)
    # writer.writeheader()
    # for row in reader:
    #     if row['Department'] == 'Engineering':
    #         writer.writerow(row)

# 9. Increase salary of all employees by 10% and write to a new CSV file

# with open("employee.csv", 'r') as infile, open("updated_salaries.csv", 'w', newline='') as outfile:
    # reader = csv.DictReader(infile)
    # fields = reader.fieldnames
    # writer = csv.DictWriter(outfile, fieldnames=fields)
    # writer.writeheader()
    # for row in reader:
    #     row['Salary'] = int(int(row['Salary']) * 1.1)
    #     writer.writerow(row) 

# 10. List employees sorted by joining date

# with open("employee.csv", 'r') as file:
    # reader = csv.DictReader(file)
    # employees = sorted(reader, key=lambda x: x['Joining_Date'])
    # for row in employees:
    #     print(f"Name: {row['Name']}, Joining Date: {row['Joining_Date']}")

