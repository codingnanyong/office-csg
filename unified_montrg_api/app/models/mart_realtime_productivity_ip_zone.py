"""SQLAlchemy model for the `mart_realtime_productivity_ip_zone` table."""

from sqlalchemy import Column, DateTime, Numeric, String

from app.core.database import Base


class MartRealtimeProductivityIpZone(Base):
    """Represents a row in the realtime IP zone productivity table."""

    __tablename__ = "mart_realtime_productivity_ip_zone"
    __table_args__ = {"schema": "silver"}

    zone_cd = Column(String, primary_key=True, nullable=False)
    plan_qty = Column(Numeric, nullable=True)
    prod_qty = Column(Numeric, nullable=True)
    defect_qty = Column(Numeric, nullable=True)
    defect_rate = Column(Numeric, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return f"MartRealtimeProductivityIpZone(zone_cd='{self.zone_cd}')"
