"""SQLAlchemy model for the `bas_location` table."""

from sqlalchemy import Column, String, Integer, Date, DateTime

from app.core.database import Base


class BasLocation(Base):
    """Represents a row in the `bronze.bas_location` table."""

    __tablename__ = "bas_location"
    __table_args__ = {"schema": "bronze"}

    company_cd = Column(String(1), primary_key=True, nullable=False)
    loc_cd = Column(String(13), primary_key=True, nullable=False)
    loc_nm = Column(String(64), nullable=False)
    loc_type = Column(String(10), nullable=False)
    high1_cd = Column(String(13), nullable=True)
    high2_cd = Column(String(13), nullable=True)
    high3_cd = Column(String(13), nullable=True)
    use_yn = Column(String(1), nullable=False)
    sort_no = Column(Integer, nullable=True)
    line_cd = Column(String(20), nullable=True)
    shift_type = Column(String(10), nullable=True)
    cost_cd = Column(String(10), nullable=True)
    remark = Column(String(256), nullable=True)
    reg_user = Column(String(30), nullable=False)
    reg_ip = Column(String(60), nullable=False)
    reg_date = Column(DateTime, nullable=False)
    upd_user = Column(String(30), nullable=True)
    upd_ip = Column(String(60), nullable=True)
    upd_date = Column(DateTime, nullable=True)
    werks = Column(String(4), nullable=True)

    def __repr__(self) -> str:
        return (
            f"BasLocation(company_cd='{self.company_cd}', "
            f"loc_cd='{self.loc_cd}', loc_nm='{self.loc_nm}')"
        )

