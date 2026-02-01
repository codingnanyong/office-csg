"""SQLAlchemy model for the `rtf_data` table."""

from sqlalchemy import BigInteger, Column, DateTime, Index, Numeric
from sqlalchemy.dialects.mysql import INTEGER as MySQLInteger

from app.core.database import Base


class RtfData(Base):
    """Represents a row in the `rtf_data` table."""

    __tablename__ = "rtf_data"
    __table_args__ = (
        Index("RxDate", "RxDate"),
        Index("PID", "PID"),
        Index("PID_RxDate", "PID", "RxDate"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_general_ci"},
    )

    SeqNo = Column(BigInteger, primary_key=True, autoincrement=True)
    PID = Column(MySQLInteger(unsigned=True), nullable=True)
    RxDate = Column(DateTime, nullable=True)
    Pvalue = Column(Numeric(20, 6), nullable=True)

    def __repr__(self) -> str:
        return (
            f"RtfData(SeqNo={self.SeqNo!r}, PID={self.PID!r}, RxDate={self.RxDate!r}, "
            f"Pvalue={self.Pvalue!r})"
        )

