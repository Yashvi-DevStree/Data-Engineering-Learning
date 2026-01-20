from collections import defaultdict
import json
import csv

# 1. Get all employees from Engineering department

# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     for emp in employees:
#         if emp.get("department") == "Engineering":
#             print(emp.get("name"))

#  2. Get employees with salary greater than 70000

# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     for emp in employees:
#         if emp.get("salary") > 70000:
#             print(emp.get("name"))

#  3. Count number of employees per deaprtment

# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     dept_count = defaultdict(int)
#     for emp in employees:
#         dept_count[emp.get("department")] += 1
#     print(dict(dept_count))

# 4. Get active employees

# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     for emp in employees:
#         if emp.get("is_active"):
#             print(emp.get("name"))

#  5. Find the highest paid employee

# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     highest_paid = 0
#     for emp in employees:
#         if emp.get("salary") > highest_paid:
#             highest_paid = emp["salary"]
#     print(emp.get("name"))

#  6. Get employees who have skills in python

# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     for emp in employees:
#         if "Python" in emp.get("skills"):                                                          
#             print(emp.get("name"))

# 7. Get employees with more than 5 years of experience       

# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     for emp in employees:
#         if emp.get("experience_years") > 5:
#             print(emp.get("name"))

# 8. Calculate average experience per department

# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     total_exp = defaultdict(int)
#     count = defaultdict(int)
#     for emp in employees:
#         dept = emp.get("department")
#         total_exp[dept] += emp.get("experience_years")
#         count[dept] += 1
# for dept in total_exp:
#     print(dept, total_exp[dept] / count[dept])

# 9. Get list of all unique skills

# skills = set()
# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     for emp in employees:
#         skills.update(emp.get("skills"))
# print(skills)

# 10. Count how many employees know SQL

# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     for emp in employees:
#         if "SQL" in emp.get("skills"):
#             print(emp.get("name"))

# 11. count active vs inactive employees

# status = {"active": 0, "inactive": 0}
# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     for emp in employees:
#         if emp.get("is_active"):
#             status["active"] += 1
#         else:
#             status["inactive"] += 1
# print(status)

# 12. Create a department-wise salary report

# with open("employee.json", 'r') as file:
#     employees = json.load(file)
#     dept_salary = defaultdict(int)
#     for emp in employees:
#         dept_salary[emp.get("department")] += emp.get("salary")
# print(dict(dept_salary))

#  13. Convert JSON to CSV file
#  13.1 using csv module

# with open("employee.json", 'r') as infile: 
#     employees = json.load(infile)
# 
# with open("employees.csv", 'w', newline='') as outfile:
#     writer = csv.writer(outfile)
#     if employees:
#         header = employees[0].keys()
#         writer.writerow(header)
#         for emp in employees:
#             writer.writerow(emp.values())
# print("JSON data has been written to employees.csv successfully.")

# 13.2 using DictWriter

# with open("employee.json", 'r') as infile:
#     employees = json.load(infile)

# with open("employees.csv", 'w', newline='') as outfile:
#     if employees:
#         header = employees[0].keys()
#         writer = csv.DictWriter(outfile, fieldnames=header)
#         writer.writeheader()
#         writer.writerows(employees)
# print("JSON data has been written to employees.csv successfully.")