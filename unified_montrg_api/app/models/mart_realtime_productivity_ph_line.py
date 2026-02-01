"""SQLAlchemy model for the `mart_realtime_productivity_ph_line` table."""

from sqlalchemy import Column, DateTime, Numeric, String

from app.core.database import Base


class MartRealtimeProductivityPhLine(Base):
    """Represents a row in the realtime PH line productivity table."""

    __tablename__ = "mart_realtime_productivity_ph_line"
    __table_args__ = {"schema": "silver"}

    line_cd = Column(String, primary_key=True, nullable=False)
    plan_qty = Column(Numeric, nullable=True)
    prod_qty = Column(Numeric, nullable=True)
    defect_qty = Column(Numeric, nullable=True)
    defect_rate = Column(Numeric, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return f"MartRealtimeProductivityPhLine(line_cd='{self.line_cd}')"
