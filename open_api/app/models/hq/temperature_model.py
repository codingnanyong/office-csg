from sqlalchemy import Column, String, Double, TIMESTAMP, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TemperatureSchema(Base):
    __tablename__ = "temperature"
    __table_args__ = (
        PrimaryKeyConstraint('sensor_id','capture_dt', name='temperature_pkey'),
        {'schema': 'services'}
    )
    
    ymd = Column(String(8), nullable=False)
    hmsf = Column(String(16), nullable=False)
    sensor_id = Column(String(50), nullable=False)
    device_id = Column(String(50), nullable=False)
    capture_dt = Column(TIMESTAMP, nullable=False)
    t1 = Column(Double, nullable=False)
    t2 = Column(Double, nullable=False)