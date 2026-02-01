"""
Database models for SQLAlchemy
"""
from sqlalchemy import Column, BigInteger, Integer, String, Numeric, DateTime, Text, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from app.core.database import Base


class Alert(Base):
    """알람 데이터베이스 모델"""
    __tablename__ = "alerts"
    __table_args__ = {"schema": "flet_montrg"}

    alert_id = Column(BigInteger, primary_key=True, autoincrement=True)
    alert_time = Column(DateTime(timezone=True), server_default=text('now()'), nullable=False)
    loc_id = Column(String(10), nullable=True)
    sensor_id = Column(String(50), nullable=False, index=True)
    alert_type = Column(String(50), nullable=False)
    alert_level = Column(String(20), nullable=False, index=True)
    threshold_id = Column(Integer, nullable=False, index=True)
    threshold_map_id = Column(BigInteger, nullable=True)
    threshold_type = Column(String(50), nullable=False, index=True)
    threshold_level = Column(String(20), nullable=False)
    measured_value = Column(Numeric(10, 3), nullable=True)
    threshold_min = Column(Numeric(10, 3), nullable=True)
    threshold_max = Column(Numeric(10, 3), nullable=True)
    message = Column(Text, nullable=True)