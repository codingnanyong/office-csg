"""
Database models for SQLAlchemy
"""
from sqlalchemy import Column, String, Integer, Numeric, DateTime, Boolean
from app.core.database import Base


class Temperature(Base):
    """온도 데이터 테이블 모델"""
    __tablename__ = "temperature"
    __table_args__ = {"schema": "flet_montrg"}

    ymd = Column(String(8), primary_key=True, index=True)  # yyyyMMdd format
    hour = Column(Integer, primary_key=True, index=True)  # HH format
    sensor_id = Column(String(50), primary_key=True, index=True)
    pcv_temperature_max = Column(Numeric(5, 2), nullable=True)
    pcv_temperature_min = Column(Numeric(5, 2), nullable=True)
    pcv_temperature_avg = Column(Numeric(5, 2), nullable=True)
    temperature_max = Column(Numeric(5, 2), nullable=True)
    temperature_min = Column(Numeric(5, 2), nullable=True)
    temperature_avg = Column(Numeric(5, 2), nullable=True)
    humidity_max = Column(Numeric(5, 2), nullable=True)
    humidity_min = Column(Numeric(5, 2), nullable=True)
    humidity_avg = Column(Numeric(5, 2), nullable=True)
    calc_dt = Column(DateTime, nullable=True)


class Location(Base):
    """위치 정보 테이블 모델"""
    __tablename__ = "locations"
    __table_args__ = {"schema": "flet_montrg"}

    loc_id = Column(String(10), primary_key=True, index=True)
    factory = Column(String(50), nullable=True)
    building = Column(String(50), nullable=True)
    floor = Column(Integer, nullable=True)
    area = Column(String(50), nullable=True)
    location_name = Column(String(200), nullable=True)
    upd_dt = Column(DateTime, nullable=True)
    is_active = Column(Boolean, nullable=True, default=True)
