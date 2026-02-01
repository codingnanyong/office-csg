"""SQLAlchemy model for the `gold.machine_grade` foreign table."""

from datetime import date
from typing import Optional

from sqlalchemy import Column, Date, Integer, Numeric, String, Text

from app.core.database import Base


class MachineGrade(Base):
    """Represents a row in the `gold.machine_grade` foreign table."""

    __tablename__ = "machine_grade"
    __table_args__ = {"schema": "gold"}

    mc_nm = Column(String(50), primary_key=True, nullable=False)
    ym = Column(Date, primary_key=True, nullable=False)
    failures_12m = Column(Integer, default=0, nullable=False)
    operation_time = Column(Integer, default=0, nullable=False)
    repair_time = Column(Integer, default=0, nullable=False)
    operation_time_day = Column(Numeric(10, 7), nullable=True)
    mtbf_min = Column(Numeric(12, 7), nullable=True)
    mttr_min = Column(Numeric(12, 7), nullable=True)
    availability = Column(Numeric(10, 9), nullable=True)
    machine_group = Column(String(50), nullable=True)
    mtbf_z = Column(Numeric(12, 9), nullable=True)
    mttr_z = Column(Numeric(12, 9), nullable=True)
    grade = Column(String(1), nullable=True)
    comment = Column(Text, nullable=True)
    pm_cycle_week = Column(Integer, nullable=True)

    def __repr__(self) -> str:
        return (
            f"MachineGrade(mc_nm='{self.mc_nm}', ym={self.ym}, "
            f"failures_12m={self.failures_12m}, operation_time={self.operation_time}, "
            f"repair_time={self.repair_time}, grade='{self.grade}')"
        )

