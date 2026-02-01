from sqlalchemy import Column, String, Float, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from datetime import datetime
from typing import Optional

Base = declarative_base()

# SQLAlchemy Model
class TemperatureData(Base):
    __tablename__ = "temperature_raw"
    
    id = Column(Integer, primary_key=True, index=True)
    ymd = Column(String)
    hmsf = Column(String)
    sensor_id = Column(String, index=True)
    device_id = Column(String)
    capture_dt = Column(DateTime, index=True)
    t1 = Column(Float)  # temperature
    t2 = Column(Float)  # humidity
    t3 = Column(Float)  # pcv_temperature
    t4 = Column(Float)
    t5 = Column(Float)
    t6 = Column(Float)
    upload_yn = Column(String)
    upload_dt = Column(DateTime)
    extract_time = Column(DateTime)

# Pydantic Models
class TemperatureDataResponse(BaseModel):
    sensor_id: str
    temperature: float
    humidity: float
    pcv_temperature: float
    timestamp: str
    
    class Config:
        from_attributes = True

class TemperatureDataCreate(BaseModel):
    sensor_id: str
    temperature: float
    humidity: float
    pcv_temperature: float
    timestamp: datetime
