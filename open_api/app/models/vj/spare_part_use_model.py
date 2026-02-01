from sqlalchemy import Column, String, Integer, Numeric, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class SparePartUseSchema(Base):
    __tablename__ = "monthly_use"
    __table_args__ = (
        PrimaryKeyConstraint('zone', 'mach_id', 'part_cd', 'year', 'month', name='monthly_use_pkey'),
        {'schema': 'services'}
    )

    zone = Column(String, nullable=False)
    mach_id = Column(String, nullable=False)
    part_cd = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    monthly_usage = Column(Numeric, nullable=True)
