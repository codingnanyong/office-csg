"""SQLAlchemy model for the `gold.banbury_anomaly_result` foreign table."""

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Integer, String, Boolean, Numeric, DateTime

from app.core.database import Base


class BanburyAnomalyResult(Base):
    """Represents a row in the `gold.banbury_anomaly_result` foreign table."""

    __tablename__ = "banbury_anomaly_result"
    __table_args__ = {"schema": "gold"}

    # Primary Key (assuming 'no' is the primary key)
    no: str = Column(String(100), primary_key=True, nullable=False)

    # Shift information
    shift: Optional[int] = Column(Integer, nullable=True)

    # Cycle time information
    cycle_start: datetime = Column(DateTime(timezone=True), nullable=False)
    cycle_end: datetime = Column(DateTime(timezone=True), nullable=False)

    # Mode information
    mode: Optional[str] = Column(String(10), nullable=True)

    # Mix duration
    mix_duration_sec: float = Column(Numeric(10, 1), nullable=False)

    # Temperature
    max_temp: Optional[float] = Column(Numeric(10, 2), nullable=True)

    # Stage and anomaly flags
    is_3_stage: bool = Column(Boolean, nullable=False)
    is_anomaly: bool = Column(Boolean, nullable=False)

    # Anomaly probability
    anomaly_prob: float = Column(Numeric(5, 4), nullable=False)

    # Counts
    filtered_num: int = Column(Integer, nullable=False)
    peak_count: int = Column(Integer, nullable=False)

    # Result flag
    result: bool = Column(Boolean, nullable=False)

    def __repr__(self) -> str:
        return (
            f"<BanburyAnomalyResult("
            f"no='{self.no}', "
            f"cycle_start={self.cycle_start}, "
            f"cycle_end={self.cycle_end}, "
            f"is_anomaly={self.is_anomaly}, "
            f"anomaly_prob={self.anomaly_prob}"
            f")>"
        )

