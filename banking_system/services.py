from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from models import Account, Transaction, Customer, Branch

def validate_amount(amount):
    if amount <= 0:
        raise ValueError("Amount must be greater than zero.")

def get_account(session, account_id):
    account = session.query(Account).filter(Account.id == account_id).first()
    if not account:
        raise ValueError(f"Account with ID {account_id} does not exist.")
    return account

def deposit(session, account_id, amount):
    try:
        validate_amount(amount)
        account = get_account(session, account_id)
        
        account.balance += amount
        txn = Transaction(type='CREDIT', amount=amount, account_id=account_id)

        session.add(txn)
        session.commit()
        return account.balance
    
    except (ValueError, SQLAlchemyError) as e:
        session.rollback()
        raise e
    
def withdraw(session, account_id, amount):
    try:
        validate_amount(amount)
        account = get_account(session, account_id)

        if account.balance < amount:
            raise ValueError("Insufficient funds for withdrawal")
        
        account.balance -= amount
        tnx = Transaction(type='DEBIT', amount=amount, account=account)

        session.add(tnx)
        session.commit()
        return account.balance
    except (ValueError, SQLAlchemyError) as e:
        session.rollback()
        raise e
    
def transfer(session, from_account_id, to_account_id, amount):
    try:
        validate_amount(amount)

        from_acc = get_account(session, from_account_id)
        to_acc = get_account(session, to_account_id)

        if from_acc.balance < amount:
            raise ValueError("Insufficient balance for transfer")

        from_acc.balance -= amount
        to_acc.balance += amount

        session.add_all([
            Transaction(type="TRANSFER", amount=amount, account=from_acc),
            Transaction(type="TRANSFER", amount=amount, account=to_acc)
        ])

        session.commit()

    except (ValueError, SQLAlchemyError) as e:
        session.rollback()
        raise e

def get_accounts_by_customer(session, customer_id):
    try:
        return (
            session.query(Account)
            .filter(Account.customer_id == customer_id)
            .all()
        )
    except SQLAlchemyError as e:
        raise e
    
def get_accounts_with_branch(session):
    try:
        return (
            session.query(
                Account.account_number,
                Account.balance,
                Branch.name.label("branch_name"),
                Branch.city.label("branch_city")
            )
            .join(Branch, Account.branch_id == Branch.id)
            .all()
        )
    except SQLAlchemyError as e:
        raise e