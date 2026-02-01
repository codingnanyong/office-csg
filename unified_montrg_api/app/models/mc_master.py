"""SQLAlchemy model for the `mc_master` table."""

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base

from app.core.database import Base


class McMaster(Base):
    """Represents a row in the `bronze.mc_master` table."""

    __tablename__ = "mc_master"
    __table_args__ = {"schema": "bronze"}

    # Primary Key
    machine_cd: str = Column(String(20), primary_key=True, nullable=False)

    # Company & Plant Information
    company_name: str = Column(String(20), nullable=False)
    plant_cd: str = Column(String(5), nullable=False)

    # Process & Operation Information
    process_name: str = Column(String(10), nullable=False)
    op_cd: str = Column(String(5), nullable=False)

    # Line & Machine Information
    line_no: int = Column(Integer, nullable=False)
    machine_no: int = Column(Integer, nullable=False)

    # Update Information
    upd_dt: Optional[datetime] = Column(DateTime, nullable=True)
    upd_id: str = Column(String(20), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<McMaster("
            f"machine_cd='{self.machine_cd}', "
            f"company_name='{self.company_name}', "
            f"plant_cd='{self.plant_cd}', "
            f"process_name='{self.process_name}', "
            f"line_no={self.line_no}, "
            f"machine_no={self.machine_no}"
            f")>"
        )

