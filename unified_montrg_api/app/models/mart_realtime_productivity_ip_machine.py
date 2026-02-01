"""SQLAlchemy model for the `mart_realtime_productivity_ip_machine` table."""

from sqlalchemy import Column, DateTime, Numeric, String

from app.core.database import Base


class MartRealtimeProductivityIpMachine(Base):
    """Represents a row in the realtime IP machine productivity table."""

    __tablename__ = "mart_realtime_productivity_ip_machine"
    __table_args__ = {"schema": "silver"}

    zone_cd = Column(String, primary_key=True, nullable=False)
    machine_cd = Column(String, primary_key=True, nullable=False)
    plan_qty = Column(Numeric, nullable=True)
    prod_qty = Column(Numeric, nullable=True)
    defect_qty = Column(Numeric, nullable=True)
    defect_rate = Column(Numeric, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return (
            "MartRealtimeProductivityIpMachine("
            f"zone_cd='{self.zone_cd}', machine_cd='{self.machine_cd}'"
            ")"
        )
