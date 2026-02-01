"""SQLAlchemy model for the `gold.economin_lifespan` foreign table."""

from sqlalchemy import Column, Integer, Numeric, String

from app.core.database import Base


class EconominLifespan(Base):
    """Represents a row in the `gold.economin_lifespan` foreign table."""

    __tablename__ = "economin_lifespan"
    __table_args__ = {"schema": "gold"}

    machine_group = Column(String(50), primary_key=True, nullable=False)
    use_year = Column(Integer, primary_key=True, nullable=False)
    maint_cost_y = Column(Numeric(12), nullable=False)
    depreciation_expense = Column(Numeric(12), nullable=False)
    total_annual_cost = Column(Numeric(12), nullable=False)

    def __repr__(self) -> str:
        return (
            f"EconominLifespan(machine_group='{self.machine_group}', "
            f"use_year={self.use_year}, maint_cost_y={self.maint_cost_y}, "
            f"depreciation_expense={self.depreciation_expense}, "
            f"total_annual_cost={self.total_annual_cost})"
        )

