from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float
from database import Base
from datetime import datetime

class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    accounts = relationship("Account", back_populates="customer")

class Branch(Base):
    __tablename__ = 'branches'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    city = Column(String(50))      
    accounts = relationship("Account", back_populates="branch")

class Account(Base):
    __tablename__ = 'accounts'

    id = Column(Integer, primary_key=True)
    account_number = Column(String(20), unique=True, nullable=False)
    customer_id = Column(Integer, ForeignKey('customers.id'))
    branch_id = Column(Integer, ForeignKey('branches.id'))
    balance = Column(Float, default=0.0)
    customer = relationship("Customer", back_populates='accounts')
    branch = relationship("Branch", back_populates='accounts')
    transactions = relationship("Transaction", back_populates="account")

class Transaction(Base):
    __tablename__ = 'transactions'    

    id = Column(Integer, primary_key=True)
    type = Column(String)  # CREDIT / DEBIT / TRANSFER
    amount = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    account = relationship("Account", back_populates="transactions")