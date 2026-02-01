from sqlalchemy import Column, String, Numeric, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class wsDetailSchema(Base):
    __tablename__ = "ws_detail"
    __table_args__ = (
        PrimaryKeyConstraint('ws_no', name='ws_detail_pkey'),
        {'schema': 'services'}
    )

    ws_no = Column(String, primary_key=True, index=True)
    dpa = Column(String, nullable=True)
    bom_id = Column(String, nullable=True)
    st_cd = Column(String, nullable=True)
    sub_st_cd = Column(String, nullable=True)
    season_cd = Column(String, nullable=True)
    category = Column(String, nullable=True) 
    dev_name = Column(String, nullable=True)
    style_cd = Column(String, nullable=True)
    sample_ets = Column(String, nullable=True)
    sample_qty = Column(Numeric(18, 6), nullable=True)
    sample_size = Column(String, nullable=True)
    prod_factory = Column(String, nullable=True)
    gender = Column(String, nullable=True)
    td = Column(String, nullable=True)
    model_id = Column(String, nullable=True)
    pcc_pm = Column(String, nullable=True)
    rework_yn = Column(String, nullable=True)
    dev_style_id = Column(String, nullable=True)
    dev_colorway_id = Column(String, nullable=True)
    dev_style_number = Column(String, nullable=True)
