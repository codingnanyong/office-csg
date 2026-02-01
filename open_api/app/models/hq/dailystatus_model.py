from sqlalchemy import Column, String, Numeric, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DailystatusSchema(Base):
    __tablename__ = "dailystatus"
    __table_args__ = (PrimaryKeyConstraint('factory', 'opcd', name="dailystatus_pkey"),  {"schema": "services"})

    factory = Column(String, primary_key=True, index=True)  
    opcd = Column(String, primary_key=True, index=True)  
    opname = Column(String, nullable=True)
    oplocalname = Column(String, nullable=True)
    plancnt = Column(Numeric, nullable=True)
    planqty = Column(Numeric, nullable=True)
    prodcnt = Column(Numeric, nullable=True)
    prodqty = Column(Numeric, nullable=True)
    rate = Column(Numeric, nullable=True)