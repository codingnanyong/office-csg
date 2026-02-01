"""SQLAlchemy model for the `mart_downtime_by_line` table."""

from sqlalchemy import Column, String

from app.core.database import Base
from app.models.base import DowntimeBaseMixin


class MartDowntimeByLine(Base, DowntimeBaseMixin):
    """Represents a row in the `mart_downtime_by_line` table."""

    __tablename__ = "mart_downtime_by_line"
    __table_args__ = {"schema": "silver"}

    line_cd = Column(String, primary_key=True, nullable=False)

    def __repr__(self) -> str:
        return (
            f"MartDowntimeByLine(business_date={self.business_date}, "
            f"line_cd='{self.line_cd}')"
        )

