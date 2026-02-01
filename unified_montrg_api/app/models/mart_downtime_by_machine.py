"""SQLAlchemy model for the `mart_downtime_by_machine` table."""

from sqlalchemy import Column, String

from app.core.database import Base
from app.models.base import DowntimeBaseMixin


class MartDowntimeByMachine(Base, DowntimeBaseMixin):
    """Represents a row in the `mart_downtime_by_machine` table."""

    __tablename__ = "mart_downtime_by_machine"
    __table_args__ = {"schema": "silver"}

    machine_cd = Column(String, primary_key=True, nullable=False)
    line_cd = Column(String, nullable=True)
    mes_machine_nm = Column(String, nullable=True)

    def __repr__(self) -> str:
        return (
            f"MartDowntimeByMachine(business_date={self.business_date}, "
            f"machine_cd='{self.machine_cd}')"
        )

