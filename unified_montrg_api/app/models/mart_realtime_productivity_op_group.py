"""SQLAlchemy model for the `mart_realtime_productivity_op_group` table."""

from sqlalchemy import Column, DateTime, Numeric, String

from app.core.database import Base


class MartRealtimeProductivityOpGroup(Base):
    """Represents a row in the realtime OP group productivity table."""

    __tablename__ = "mart_realtime_productivity_op_group"
    __table_args__ = {"schema": "silver"}

    op_group = Column(String, primary_key=True, nullable=False)
    plan_qty = Column(Numeric, nullable=True)
    prod_qty = Column(Numeric, nullable=True)
    defect_qty = Column(Numeric, nullable=True)
    defect_rate = Column(Numeric, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return f"MartRealtimeProductivityOpGroup(op_group='{self.op_group}')"
