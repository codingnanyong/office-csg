"""SQLAlchemy model for the `mart_productivity_by_ip_zone` table."""

from sqlalchemy import Column, Date, DateTime, String, Numeric, BigInteger

from app.core.database import Base


class MartProductivityByIpZone(Base):
    """Represents a row in the `mart_productivity_by_ip_zone` table."""

    __tablename__ = "mart_productivity_by_ip_zone"
    __table_args__ = {"schema": "silver"}

    business_date = Column(Date, primary_key=True, nullable=False)
    zone_cd = Column(String, primary_key=True, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=True)
    plan_qty = Column(Numeric, nullable=True)
    prod_qty = Column(BigInteger, nullable=True)
    defect_qty = Column(BigInteger, nullable=True)
    quality_rate = Column(Numeric, nullable=True)

    def __repr__(self) -> str:
        return (
            f"MartProductivityByIpZone(business_date={self.business_date}, "
            f"zone_cd='{self.zone_cd}')"
        )
