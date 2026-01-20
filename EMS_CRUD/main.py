from crud import create_employee, get_employees, update_salary, remove_employee

while True:
    print("\nEmployee Management System")
    print("1. Add Employee")
    print("2. View Employees")
    print("3. Update Salary")
    print("4. Delete Employee")
    print("5. Exit")

    choice = input("Enter choice: ")
    if choice == '1':
        emp_id = int(input("Enter Employee ID:"))
        name = input("Name: ")
        dept = input("Department: ")
        salary = int(input("Salary: "))
        create_employee(emp_id, name, dept, salary)
    
    elif choice == "2":
        get_employees()

    elif choice == "3":
        emp_id = int(input("Emp ID: "))
        salary = int(input("New Salary: "))
        update_salary(emp_id, salary)

    elif choice == "4":
        emp_id = int(input("Emp ID: "))
        remove_employee(emp_id)

    elif choice == "5":
        print("Exiting...")
        break

    else:
        print("Invalid choice")
