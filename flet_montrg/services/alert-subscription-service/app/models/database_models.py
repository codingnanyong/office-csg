"""
Database models for SQLAlchemy
"""
from sqlalchemy import Column, BigInteger, Integer, String, Boolean, DateTime, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from app.core.database import Base


class AlertSubscription(Base):
    """알람 구독 데이터베이스 모델"""
    __tablename__ = "alert_subscriptions"
    __table_args__ = {"schema": "flet_montrg"}

    subscription_id = Column(BigInteger, primary_key=True, autoincrement=True)
    plant = Column(String(50), nullable=True)
    factory = Column(String(50), nullable=True)
    building = Column(String(50), nullable=True)
    floor = Column(Integer, nullable=True)
    area = Column(String(50), nullable=True)
    sensor_id = Column(String(50), nullable=True, index=True)
    threshold_type = Column(String(50), nullable=True)
    min_level = Column(String(20), nullable=True)
    subscriber = Column(String(100), nullable=False, index=True)
    notify_type = Column(String(20), server_default=text("'email'"), nullable=False)
    notify_id = Column(String(200), nullable=False)
    enabled = Column(Boolean, server_default=text("true"), nullable=False, index=True)
    upd_dt = Column(DateTime(timezone=True), server_default=text('CURRENT_TIMESTAMP'), nullable=False)
