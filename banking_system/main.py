from sqlalchemy.exc import SQLAlchemyError
from database import engine, SessionLocal, Base
from models import Customer, Branch, Account, Transaction
from services import deposit, withdraw, transfer, get_accounts_by_customer, get_accounts_with_branch

def seed_data(session):
    """Insert initial sample data"""

    branch1 = Branch(name="Main Branch", city="Ahmedabad")
    branch2 = Branch(name="City Branch", city="Surat")

    customer1 = Customer(name="Amit Shah", email="amit@gmail.com")
    customer2 = Customer(name="Rahul Mehta", email="rahul@gmail.com")

    acc1 = Account(account_number="ACC1001", balance=5000, customer=customer1, branch=branch1)
    acc2 = Account(account_number="ACC1002", balance=3000, customer=customer1, branch=branch1)
    acc3 = Account(account_number="ACC1003", balance=7000, customer=customer2, branch=branch2)

    session.add_all([
        branch1, branch2,
        customer1, customer2,
        acc1, acc2, acc3
    ])
    session.commit()

def main():
    Base.metadata.create_all(engine)
    session = SessionLocal()

    try: 
        seed_data(session)
        deposit(session, account_id=1, amount=2000)
        withdraw(session, account_id=2, amount=1000)
        transfer(session, from_account_id=1, to_account_id=3, amount=1500)

        print("\nAccounts of Customer 1:")
        print(get_accounts_by_customer(session, customer_id=1))

        print("\nAccount with Branch Details:")
        print(get_accounts_with_branch(session))

    except (ValueError, SQLAlchemyError) as e:
        print("Error:", e)
    finally:
        session.close()

if __name__ == "__main__":
    main()