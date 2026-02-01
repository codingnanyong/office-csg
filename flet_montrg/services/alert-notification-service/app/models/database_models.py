"""
Database models for SQLAlchemy
"""
from sqlalchemy import Column, BigInteger, Integer, String, DateTime, Text, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from app.core.database import Base


class AlertNotification(Base):
    """알림 발송 데이터베이스 모델"""
    __tablename__ = "alert_notifications"
    __table_args__ = {"schema": "flet_montrg"}

    notification_id = Column(BigInteger, primary_key=True, autoincrement=True)
    alert_id = Column(BigInteger, nullable=False, index=True)
    subscription_id = Column(BigInteger, nullable=False, index=True)
    notify_type = Column(String(20), nullable=False)
    notify_id = Column(String(200), nullable=False)
    status = Column(String(20), server_default=text("'PENDING'"), nullable=False, index=True)
    try_count = Column(Integer, server_default=text("0"), nullable=False)
    created_time = Column(DateTime(timezone=True), server_default=text('now()'), nullable=False)
    last_try_time = Column(DateTime(timezone=True), nullable=True, index=True)
    sent_time = Column(DateTime(timezone=True), nullable=True)
    fail_reason = Column(Text, nullable=True)
