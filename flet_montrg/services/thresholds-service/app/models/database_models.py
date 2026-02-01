"""
Database models for SQLAlchemy
"""
from sqlalchemy import Column, Integer, String, Numeric, DateTime, text
from app.core.database import Base


class Thresholds(Base):
    """임계치 데이터베이스 모델"""
    __tablename__ = "thresholds"
    __table_args__ = {"schema": "flet_montrg"}

    threshold_id = Column(Integer, primary_key=True, autoincrement=True)
    threshold_type = Column(String, nullable=False)  # 임계치 타입
    level = Column(String, nullable=False)  # 레벨
    min_value = Column(Numeric, nullable=True)  # 최소값
    max_value = Column(Numeric, nullable=True)  # 최대값
    upd_dt = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))
