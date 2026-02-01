"""
Database models for SQLAlchemy
"""
from sqlalchemy import Column, BigInteger, Integer, String, Boolean, DateTime, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from app.core.database import Base


class SensorThresholdMap(Base):
    """센서-임계치 매핑 데이터베이스 모델"""
    __tablename__ = "sensor_threshold_map"
    __table_args__ = {"schema": "flet_montrg"}

    map_id = Column(BigInteger, primary_key=True, autoincrement=True)
    sensor_id = Column(String(50), nullable=False, index=True)
    threshold_id = Column(Integer, nullable=False, index=True)
    duration_seconds = Column(Integer, server_default=text("60"), nullable=False)
    enabled = Column(Boolean, server_default=text("true"), nullable=False, index=True)
    effective_from = Column(DateTime(timezone=True), nullable=True, index=True)
    effective_to = Column(DateTime(timezone=True), nullable=True, index=True)
    upd_dt = Column(DateTime(timezone=True), server_default=text('CURRENT_TIMESTAMP'), nullable=True)
