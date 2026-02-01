"""SQLAlchemy model for the `gold.ipi_temperature_matching` foreign table."""

from datetime import datetime
from typing import Optional, List

from sqlalchemy import Column, BigInteger, Integer, Numeric, String, DateTime, PrimaryKeyConstraint, and_
from sqlalchemy.orm import relationship

from app.core.database import Base


class IpiQualityTemperature(Base):
    """Represents a row in the `gold.ipi_temperature_matching` foreign table."""

    __tablename__ = "ipi_temperature_matching"
    __table_args__ = {"schema": "gold"}

    osnd_id = Column(BigInteger, primary_key=True, nullable=False)
    osnd_dt = Column(DateTime, primary_key=True, nullable=False)
    machine_cd = Column(String(10), nullable=True)
    station = Column(Integer, nullable=True)
    station_rl = Column(String(1), nullable=True)
    mold_id = Column(String(10), nullable=True)
    reason_cd = Column(String(100), nullable=True)
    size_cd = Column(String(20), nullable=True)
    lr_cd = Column(String(1), nullable=True)
    osnd_bt_qty = Column(Integer, nullable=True)
    temp_l_count = Column(Integer, default=0, nullable=True)
    temp_l_avg = Column(Numeric(10, 3), nullable=True)
    temp_l_min = Column(Numeric(10, 3), nullable=True)
    temp_l_max = Column(Numeric(10, 3), nullable=True)
    temp_u_count = Column(Integer, default=0, nullable=True)
    temp_u_avg = Column(Numeric(10, 3), nullable=True)
    temp_u_min = Column(Numeric(10, 3), nullable=True)
    temp_u_max = Column(Numeric(10, 3), nullable=True)

    # Relationship to detail records (composite key: osnd_id, osnd_dt)
    # Note: FDW tables don't expose foreign keys, so we must specify primaryjoin explicitly
    details = relationship(
        "IpiQualityTemperatureDetail",
        primaryjoin="and_(IpiQualityTemperature.osnd_id == IpiQualityTemperatureDetail.osnd_id, "
                    "IpiQualityTemperature.osnd_dt == IpiQualityTemperatureDetail.osnd_dt)",
        foreign_keys="[IpiQualityTemperatureDetail.osnd_id, IpiQualityTemperatureDetail.osnd_dt]",
        back_populates="main",
        order_by="IpiQualityTemperatureDetail.seq_no",
    )

    def __repr__(self) -> str:
        return (
            f"IpiQualityTemperature(osnd_id={self.osnd_id}, osnd_dt={self.osnd_dt}, "
            f"machine_cd='{self.machine_cd}', reason_cd='{self.reason_cd}')"
        )


class IpiQualityTemperatureDetail(Base):
    """Represents a row in the `gold.ipi_temperature_matching_detail` foreign table."""

    __tablename__ = "ipi_temperature_matching_detail"
    __table_args__ = (
        PrimaryKeyConstraint("osnd_id", "osnd_dt", "temp_type", "measurement_time"),
        {"schema": "gold"},
    )

    osnd_id = Column(BigInteger, primary_key=True, nullable=False)
    osnd_dt = Column(DateTime, primary_key=True, nullable=False)
    temp_type = Column(String(1), primary_key=True, nullable=False)
    measurement_time = Column(DateTime, primary_key=True, nullable=False)
    temperature = Column(Numeric(10, 3), nullable=False)
    seq_no = Column(Integer, nullable=True)

    # Relationship to main record (composite key: osnd_id, osnd_dt)
    # Note: FDW tables don't expose foreign keys, so we must specify primaryjoin explicitly
    main = relationship(
        "IpiQualityTemperature",
        primaryjoin="and_(IpiQualityTemperatureDetail.osnd_id == IpiQualityTemperature.osnd_id, "
                    "IpiQualityTemperatureDetail.osnd_dt == IpiQualityTemperature.osnd_dt)",
        foreign_keys="[IpiQualityTemperatureDetail.osnd_id, IpiQualityTemperatureDetail.osnd_dt]",
        back_populates="details",
    )

    def __repr__(self) -> str:
        return (
            f"IpiQualityTemperatureDetail(osnd_id={self.osnd_id}, osnd_dt={self.osnd_dt}, "
            f"temp_type='{self.temp_type}', measurement_time={self.measurement_time}, "
            f"temperature={self.temperature})"
        )