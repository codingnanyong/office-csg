from sqlalchemy import Column, String, Text, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class SensorSchema(Base):
    __tablename__ = "sensor"
    __table_args__ = (
        PrimaryKeyConstraint('sensor_id', name='sensor_pkey'),
        {'schema': 'services'}
    )

    sensor_id = Column(String(50), nullable=False)
    device_id = Column(String(50), nullable=False)
    mach_id = Column(String(50), nullable=False)
    company_cd = Column(String(50), nullable=False)
    name = Column(String(255), nullable=True)
    addr = Column(Text, nullable=True)
    descn = Column(Text, nullable=True)