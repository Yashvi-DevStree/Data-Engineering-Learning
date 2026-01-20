import json

employees = [
    {
        "emp_id": 101,
        "name": "Amit Sharma",
        "department": "Engineering",
        "role": "Backend Developer",
        "age": 28,
        "salary": 75000,
        "experience_years": 4,
        "location": "Bangalore",
        "skills": ["Python", "SQL", "PostgreSQL"],
        "is_active": True
    },
    {
        "emp_id": 102,
        "name": "Priya Mehta",
        "department": "HR",
        "role": "HR Manager",
        "age": 32,
        "salary": 65000,
        "experience_years": 7,
        "location": "Mumbai",
        "skills": ["Recruitment", "Communication"],
        "is_active": True
    },
    {
        "emp_id": 103,
        "name": "Rahul Verma",
        "department": "Engineering",
        "role": "Data Engineer",
        "age": 35,
        "salary": 90000,
        "experience_years": 9,
        "location": "Hyderabad",
        "skills": ["Python", "Spark", "AWS", "SQL"],
        "is_active": True
    },
    {
        "emp_id": 104,
        "name": "Neha Kapoor",
        "department": "Marketing",
        "role": "Marketing Executive",
        "age": 26,
        "salary": 55000,
        "experience_years": 3,
        "location": "Delhi",
        "skills": ["SEO", "Content Marketing"],
        "is_active": False
    },
    {
        "emp_id": 105,
        "name": "Karan Patel",
        "department": "Engineering",
        "role": "Tech Lead",
        "age": 41,
        "salary": 120000,
        "experience_years": 15,
        "location": "Pune",
        "skills": ["System Design", "Python", "Cloud"],
        "is_active": True
    }
]

with open("employee.json", 'w') as file:
    json.dump(employees, file, indent=4)
print("JSON file created successfully.")