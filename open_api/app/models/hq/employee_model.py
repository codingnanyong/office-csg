from sqlalchemy import Column, String, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class EmployeeSchema(Base):
    __tablename__ = "employee"
    __table_args__ = (PrimaryKeyConstraint('user_id', name="employee_pkey"),  {"schema": "services"})

    user_id = Column(String, primary_key = True, index = True)  
    user_name = Column(String, nullable = False )  