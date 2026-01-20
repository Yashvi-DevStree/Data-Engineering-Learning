from db import get_connection
import psycopg2

def create_employee(emp_id, name, department, salary):
    try:
        with get_connection() as conn:
            with conn.cursor() as curr:
                curr.execute('insert into employees (emp_id, name, department, salary) values (%s, %s, %s, %s) ', (emp_id, name, department, salary))
        print("Employee created successfully.")

    except psycopg2.IntegrityError:
        print("Error: Employee with this ID already exists.")

    finally:
        curr.close()
        conn.close()

def get_employees():
    try:
        with get_connection() as conn:
            with conn.cursor() as curr:
                curr.execute('select * from employees')
                rows = curr.fetchall()
                print("Employee List:")
                for row in rows:
                    print(f"ID: {row[0]}, Name: {row[1]}, Department: {row[2]}, Salary: {row[3]}")
    
    except psycopg2.Error as e:
        print("Error fetching employees:", e)

    finally:
        curr.close()
        conn.close()

def update_salary(emp_id, new_salary):
    try:
        with get_connection() as conn:
            with conn.cursor() as curr:
                curr.execute('update employees set salary = %s where emp_id = %s', (new_salary, emp_id))
                
                if curr.rowcount == 0:
                    print("Employee not found")
                else:
                    print("Salary updated successfully")

    except psycopg2.Error as e:
        conn.rollback()
        print("Error updating salary:", e)

    finally:
        curr.close()
        conn.close()
                                                                                                    
def remove_employee(emp_id):     
    try:
        with get_connection() as conn:
            with conn.cursor() as curr:
                curr.execute('DELETE FROM employees WHERE emp_id = %s', (emp_id,))
                
                if curr.rowcount == 0:                                                                                                      
                    print("Employee not found")  
                else:
                    print("Employee removed successfully")

    except psycopg2.Error as e:
        conn.rollback()
        print("Error removing employee:", e)

    finally:
        curr.close()
        conn.close()