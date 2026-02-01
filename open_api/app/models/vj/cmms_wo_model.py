from sqlalchemy import Column, String, Numeric, TIMESTAMP, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class WorkOrderSchema(Base):
    __tablename__ = "cmms_workorders"
    __table_args__ = (
        Index("idx_wo_yymm", "wo_yymm"),
        {"schema": "services"}
    )

    company_cd = Column(String, nullable=True)
    zone = Column(String, nullable=True)
    mach_id = Column(String, nullable=True)
    wo_type = Column(String, nullable=True)
    wo_yymm = Column(String, nullable=True)
    wo_orgn = Column(String, nullable=True)
    wo_no = Column(Numeric, nullable=True)
    wo_class = Column(String, nullable=True)
    work_type = Column(String, nullable=True)
    wo_status = Column(String, nullable=True)
    request_date = Column(TIMESTAMP, nullable=True)
    wo_date = Column(TIMESTAMP, nullable=True)
    problem_date = Column(TIMESTAMP, nullable=True)
    defe_date = Column(TIMESTAMP, nullable=True)
    solu_date = Column(TIMESTAMP, nullable=True)
    defe_nm_en = Column(String, nullable=True)

    __mapper_args__ = {
        "primary_key": [wo_yymm, wo_orgn, wo_no]
    }
