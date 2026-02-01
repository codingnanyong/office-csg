"""SQLAlchemy model for the `rollgap` table."""

from datetime import datetime
from sqlalchemy import Column, String, DateTime, Float, CHAR, ForeignKey, Index

from app.core.database import Base


class IpRollgap(Base):
    """Represents a row in the `rollgap` table."""

    __tablename__ = "rollgap"
    __table_args__ = (
        Index("rollgap_idx", "sensor_id", "capture_dt", postgresql_ops={"capture_dt": "DESC"}),
        Index("rollgap_latest_idx", "sensor_id", "capture_dt", postgresql_ops={"sensor_id": "ASC", "capture_dt": "DESC"}),
        {"schema": "public"},
    )

    ymd = Column(String(8), primary_key=True, nullable=False)
    hmsf = Column(String(12), primary_key=True, nullable=False)
    sensor_id = Column(String(20), ForeignKey("sensor.sensor_id", ondelete="CASCADE"), primary_key=True, nullable=False)
    device_id = Column(String(20), ForeignKey("device.device_id", ondelete="CASCADE"), nullable=False)
    capture_dt = Column(DateTime, nullable=False)
    roll_temp = Column(Float, nullable=False)
    gap_left = Column(Float, nullable=False)
    gap_right = Column(Float, nullable=False)
    upload_yn = Column(CHAR(1), nullable=True)
    upload_dt = Column(DateTime, nullable=True)
    din1 = Column(String(1), nullable=True)
    din2 = Column(String(1), nullable=True)

    def __repr__(self) -> str:
        return (
            f"IpRollgap(sensor_id='{self.sensor_id}', capture_dt={self.capture_dt}, "
            f"gap_left={self.gap_left}, gap_right={self.gap_right})"
        )

