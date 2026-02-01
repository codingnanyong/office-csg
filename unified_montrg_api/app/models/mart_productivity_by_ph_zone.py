"""SQLAlchemy model for the `mart_productivity_by_ph_zone` table."""

from sqlalchemy import Column, Date, DateTime, String, Numeric, BigInteger

from app.core.database import Base


class MartProductivityByPhZone(Base):
    """Represents a row in the `mart_productivity_by_ph_zone` table."""

    __tablename__ = "mart_productivity_by_ph_zone"
    __table_args__ = {"schema": "silver"}

    business_date = Column(Date, primary_key=True, nullable=False)
    line_group = Column(String, primary_key=True, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=True)
    plan_qty = Column(Numeric, nullable=True)
    prod_qty = Column(Numeric, nullable=True)
    defect_qty = Column(BigInteger, nullable=True)
    quality_rate = Column(Numeric, nullable=True)

    def __repr__(self) -> str:
        return (
            f"MartProductivityByPhZone(business_date={self.business_date}, "
            f"line_group='{self.line_group}')"
        )

