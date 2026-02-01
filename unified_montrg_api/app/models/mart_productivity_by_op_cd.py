"""SQLAlchemy model for the `mart_productivity_by_op_cd` table."""

from sqlalchemy import Column, Date, DateTime, String, Numeric
from app.core.database import Base

class MartProductivityByOpCd(Base):
    """Represents a row in the `mart_productivity_by_op_cd` table."""

    __tablename__ = "mart_productivity_by_op_cd"
    __table_args__ = {"schema": "silver"}

    business_date = Column(Date, primary_key=True, nullable=False)
    op_cd = Column(String, primary_key=True, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=True)
    op_group = Column(String, nullable=True)
    plan_qty = Column(Numeric, nullable=True)
    prod_qty = Column(Numeric, nullable=True)
    defect_qty = Column(Numeric, nullable=True)
    quality_rate = Column(Numeric, nullable=True)

    def __repr__(self) -> str:
        return (
            f"MartProductivityByOpCd(business_date={self.business_date}, "
            f"op_cd='{self.op_cd}', op_group='{self.op_group}')"
        )

