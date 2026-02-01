"""SQLAlchemy model for the `gold.spare_part_inventory` foreign table."""

from typing import Optional

from sqlalchemy import Column, Integer, String

from app.core.database import Base


class SparePartInventory(Base):
    """Represents a row in the `gold.spare_part_inventory` foreign table."""

    __tablename__ = "spare_part_inventory"
    __table_args__ = {"schema": "gold"}

    mach_group = Column(String(50), primary_key=True, nullable=False)
    part_cd = Column(String(50), primary_key=True, nullable=False)
    part_nm = Column(String(200), nullable=False)
    stock = Column(Integer, default=0, nullable=False)
    mach_id = Column(String(50), nullable=False)
    last_rep_dt = Column(String(8), nullable=True)
    baseline_stock = Column(Integer, default=0, nullable=False)
    exp_cycle = Column(Integer, default=0, nullable=False)
    exp_rep_dt = Column(String(8), nullable=True)
    exp_orde_dt = Column(String(8), nullable=True)

    def __repr__(self) -> str:
        return (
            f"SparePartInventory(mach_group='{self.mach_group}', "
            f"part_cd='{self.part_cd}', part_nm='{self.part_nm}', "
            f"stock={self.stock}, mach_id='{self.mach_id}')"
        )
