from sqlalchemy import Column, String, Numeric, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class wsHisSchema(Base):
    __tablename__ = "ws_his"
    __table_args__ = (
        PrimaryKeyConstraint('ws_no', 'op_cd', 'status', name='ws_his_pkey'),
        {'schema': 'services'}
    )

    ws_no = Column(String, nullable=False)
    op_cd = Column(String, nullable=False)
    plan_date = Column(String, nullable=True)
    plan_qty = Column(Numeric, nullable=True)
    prod_date = Column(String, nullable=True)
    prod_time = Column(String, nullable=True)
    prod_qty = Column(Numeric, nullable=True)
    status = Column(String, nullable=False)
