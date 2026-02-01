"""
Database models for SQLAlchemy
"""
from sqlalchemy import Column, Integer, String, Numeric, DateTime, Boolean, Text, text, PrimaryKeyConstraint
from app.core.database import Base


class TemperatureRaw(Base):
    """온도 센서 원시 데이터 모델 (TimescaleDB)"""
    __tablename__ = "temperature_raw"
    __table_args__ = {"schema": "flet_montrg"}

    ymd = Column(String(8), nullable=False)
    hmsf = Column(String(20), nullable=False)
    sensor_id = Column(String(20), nullable=False, index=True)
    device_id = Column(String(20), nullable=False)
    capture_dt = Column(DateTime, nullable=False, index=True)
    t1 = Column(String(20), nullable=True)
    t2 = Column(String(20), nullable=True)
    t3 = Column(String(20), nullable=True)
    t4 = Column(String(20), nullable=True)
    t5 = Column(String(20), nullable=True)
    t6 = Column(String(20), nullable=True)
    upload_yn = Column(String(1), nullable=True)
    upload_dt = Column(DateTime, nullable=True, index=True)
    extract_time = Column(DateTime, nullable=True)
    load_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint('capture_dt', 'ymd', 'hmsf', 'sensor_id'),
        {"schema": "flet_montrg"}
    )


