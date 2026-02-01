"""
Database models for SQLAlchemy
"""
from sqlalchemy import Column, String, DateTime, Integer, Boolean, text
from app.core.database import Base


class Location(Base):
    """위치 정보 데이터베이스 모델"""
    __tablename__ = "locations"
    __table_args__ = {"schema": "flet_montrg"}

    loc_id = Column(String(10), primary_key=True, index=True)
    plant = Column(String(50), nullable=True)
    factory = Column(String(50), nullable=True)
    building = Column(String(50), nullable=True)
    floor = Column(Integer, nullable=True)
    area = Column(String(50), nullable=True)
    location_name = Column(String(200), nullable=True)
    upd_dt = Column(DateTime, nullable=True)
    is_active = Column(Boolean, nullable=True, default=True)


class Sensor(Base):
    """센서 정보 데이터베이스 모델"""
    __tablename__ = "sensors"
    __table_args__ = {"schema": "flet_montrg"}

    sensor_id = Column(String(50), primary_key=True, index=True)
    loc_id = Column(String(10), nullable=True)
    name = Column(String(100), nullable=True)
    upd_dt = Column(DateTime, nullable=True)
