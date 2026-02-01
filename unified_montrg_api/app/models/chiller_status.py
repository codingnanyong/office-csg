"""SQLAlchemy model for the `status` table."""

from datetime import datetime
from sqlalchemy import Column, String, DateTime, PrimaryKeyConstraint, Index

from app.core.database import Base


class ChillerStatus(Base):
    """Represents a row in the `status` table."""

    __tablename__ = "status"
    __table_args__ = (
        PrimaryKeyConstraint("device_id", "upd_dt"),
        Index("status_upd_dt_idx", "device_id", "upd_dt", postgresql_ops={"upd_dt": "DESC"}),
        {"schema": "public"}
    )

    device_id = Column(String(30), nullable=False)
    water_in_temp = Column(String(30), nullable=True)
    water_out_temp = Column(String(30), nullable=True)
    external_temp = Column(String(30), nullable=False)
    discharge_temp_1 = Column(String(30), nullable=False)
    discharge_temp_2 = Column(String(30), nullable=False)
    discharge_temp_3 = Column(String(30), nullable=False)
    discharge_temp_4 = Column(String(30), nullable=False)
    sv_temp = Column(String(30), nullable=False)
    digitals = Column(String(50), nullable=False)
    upd_dt = Column(DateTime, nullable=False)

    def __repr__(self) -> str:
        return f"ChillerStatus(device_id='{self.device_id}', upd_dt={self.upd_dt})"


class ChillerDevice(Base):
    """Represents a row in the `device` table."""

    __tablename__ = "device"
    __table_args__ = ({"schema": "public"})

    device_id = Column(String(30), primary_key=True, nullable=False)

    def __repr__(self) -> str:
        return f"ChillerDevice(device_id='{self.device_id}')"

