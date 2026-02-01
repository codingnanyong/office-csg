from sqlalchemy import Column, Integer, String, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class AnnualSchema(Base):
    __tablename__ = "annual"
    __table_args__ = (
        PrimaryKeyConstraint('year', 'factory', name = 'annual_pkey'),  
        {'schema': 'services'})

    year = Column(Integer,primary_key=True, index = True)
    factory = Column(String, primary_key=True, index=True)  
    plancnt = Column(Integer, default=0)  
    planqty = Column(Integer, default=0) 
    prodcnt = Column(Integer, default=0) 
    prodqty = Column(Integer, default=0)