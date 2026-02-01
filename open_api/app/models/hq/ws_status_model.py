from sqlalchemy import Column, String, Numeric, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class wsStatusSchema(Base):
    __tablename__ = "ws_status"
    __table_args__ = (
        PrimaryKeyConstraint('ws_no', 'op_cd', 'status', name='ws_status_pkey'),
        {'schema': 'services'}
    )

    ws_no = Column(String, nullable=False)
    op_cd = Column(String, nullable=False)
    pm = Column(String, nullable=True)
    model = Column(String, nullable=True)
    season_cd = Column(String, nullable=True)
    bom_id = Column(String, nullable=True)
    style_cd = Column(String, nullable=True)
    dev_colorway_id = Column(String, nullable=True)
    plan_date = Column(String, nullable=True)
    prod_date = Column(String, nullable=True)
    status = Column(String, nullable=False)
    prod_qty = Column(Numeric,nullable = True)