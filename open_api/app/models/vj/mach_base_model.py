from sqlalchemy import Column, String, Numeric, TIMESTAMP, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class MachBaseSchema(Base):
    __abstract__ = True 

    date = Column(String, primary_key=True)
    zone = Column(String, primary_key=True)
    mach_id = Column(String,primary_key=True)
    part_cd = Column(String, primary_key=True)
    part_nm_en = Column(String)
    cycle_dt = Column(Integer)
    current_wo_date = Column(TIMESTAMP)
    previous_wo_date = Column(TIMESTAMP)
    total_qty = Column(Numeric)
    total_qty_recent = Column(Numeric)
    min_qty = Column(Numeric)
    max_qty = Column(Numeric)
    stock_qty = Column(Numeric)
    rn = Column(Integer)