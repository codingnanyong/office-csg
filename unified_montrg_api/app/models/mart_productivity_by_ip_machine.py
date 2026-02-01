"""SQLAlchemy model for the `mart_productivity_by_ip_machine` table."""

from sqlalchemy import Column, Date, DateTime, String, Numeric, BigInteger

from app.core.database import Base


class MartProductivityByIpMachine(Base):
    """Represents a row in the `mart_productivity_by_ip_machine` table."""

    __tablename__ = "mart_productivity_by_ip_machine"
    __table_args__ = {"schema": "silver"}

    business_date = Column(Date, primary_key=True, nullable=False)
    machine_cd = Column(String, primary_key=True, nullable=False)
    plan_qty = Column(Numeric, nullable=True)
    prod_qty = Column(BigInteger, nullable=True)
    defect_qty = Column(BigInteger, nullable=True)
    quality_rate = Column(Numeric, nullable=True)

    def __repr__(self) -> str:
        return (
            f"MartProductivityByIpMachine(business_date={self.business_date}, "
            f"machine_cd='{self.machine_cd}')"
        )

