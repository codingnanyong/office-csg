from sqlalchemy import Column, String, Numeric,PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class IpPrfSchema(Base):
    __tablename__ = "ip_performacne"
    __table_args__ = (
        PrimaryKeyConstraint('zone', 'mach_id', 'ymd', name='ip_performacne_pkey'),
        {'schema': 'services'}
    )

    zone = Column(String, nullable=False)
    mach_id = Column(String, nullable=False)
    ymd = Column(String, nullable=False)
    total_qty = Column(Numeric)