"""ORM models for core.* tables. Match init-db.sql."""
from datetime import date, datetime, time

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    Time,
)
from sqlalchemy.orm import Mapped, mapped_column

from shared.database import Base

SCHEMA = "core"


class LineMst(Base):
    __tablename__ = "line_mst"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    line_code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    line_name: Mapped[str | None] = mapped_column(String(200))


class EquipMst(Base):
    __tablename__ = "equip_mst"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    line_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.line_mst.id", ondelete="CASCADE"), nullable=False)
    equip_code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    type: Mapped[str | None] = mapped_column(String(100))


class SensorMst(Base):
    __tablename__ = "sensor_mst"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    equip_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.equip_mst.id", ondelete="CASCADE"), nullable=False)
    sensor_code: Mapped[str] = mapped_column(String(50), nullable=False)
    unit: Mapped[str | None] = mapped_column(String(20))
    lsl_val: Mapped[float | None] = mapped_column(Float)
    usl_val: Mapped[float | None] = mapped_column(Float)
    lcl_val: Mapped[float | None] = mapped_column(Float)
    ucl_val: Mapped[float | None] = mapped_column(Float)


class WorkerMst(Base):
    __tablename__ = "worker_mst"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    worker_code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    dept_name: Mapped[str | None] = mapped_column(String(100))


class ShiftCfg(Base):
    __tablename__ = "shift_cfg"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    shift_name: Mapped[str] = mapped_column(String(50), nullable=False)
    start_time: Mapped[time] = mapped_column(Time, nullable=False)
    end_time: Mapped[time] = mapped_column(Time, nullable=False)


class KpiCfg(Base):
    __tablename__ = "kpi_cfg"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    equip_id: Mapped[int] = mapped_column(
        ForeignKey(f"{SCHEMA}.equip_mst.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    std_cycle_time: Mapped[float | None] = mapped_column(Float)
    target_oee: Mapped[float | None] = mapped_column(Float)


class AlarmCfg(Base):
    __tablename__ = "alarm_cfg"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alarm_code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    severity: Mapped[str | None] = mapped_column(String(20))
    description: Mapped[str | None] = mapped_column(Text)


class MaintCfg(Base):
    __tablename__ = "maint_cfg"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    maint_type: Mapped[str] = mapped_column(String(50), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)


class Measurement(Base):
    __tablename__ = "measurement"
    __table_args__ = {"schema": SCHEMA}

    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True, nullable=False)
    equip_id: Mapped[int] = mapped_column(
        Integer, ForeignKey(f"{SCHEMA}.equip_mst.id", ondelete="RESTRICT"), primary_key=True, nullable=False
    )
    sensor_id: Mapped[int] = mapped_column(
        Integer, ForeignKey(f"{SCHEMA}.sensor_mst.id", ondelete="RESTRICT"), primary_key=True, nullable=False
    )
    value: Mapped[float | None] = mapped_column(Float)


class StatusHis(Base):
    __tablename__ = "status_his"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    equip_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.equip_mst.id", ondelete="RESTRICT"), nullable=False)
    status_code: Mapped[str | None] = mapped_column(Text)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True, nullable=False)
    end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ProdHis(Base):
    __tablename__ = "prod_his"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True, nullable=False)
    equip_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.equip_mst.id", ondelete="RESTRICT"), nullable=False)
    total_cnt: Mapped[int] = mapped_column(Integer, default=0)
    good_cnt: Mapped[int] = mapped_column(Integer, default=0)
    defect_cnt: Mapped[int] = mapped_column(Integer, default=0)


class AlarmHis(Base):
    __tablename__ = "alarm_his"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    equip_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.equip_mst.id", ondelete="RESTRICT"), nullable=False)
    alarm_def_id: Mapped[int] = mapped_column(
        ForeignKey(f"{SCHEMA}.alarm_cfg.id", ondelete="RESTRICT"), nullable=False
    )
    trigger_val: Mapped[float | None] = mapped_column(Float)
    alarm_type: Mapped[str | None] = mapped_column(String(50))


class MaintHis(Base):
    __tablename__ = "maint_his"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    equip_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.equip_mst.id", ondelete="RESTRICT"), nullable=False)
    maint_def_id: Mapped[int] = mapped_column(
        ForeignKey(f"{SCHEMA}.maint_cfg.id", ondelete="RESTRICT"), nullable=False
    )
    alarm_his_id: Mapped[int | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.alarm_his.id", ondelete="SET NULL"), unique=True
    )
    worker_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.worker_mst.id", ondelete="SET NULL"))
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    maint_desc: Mapped[str | None] = mapped_column(Text)


class ShiftMap(Base):
    __tablename__ = "shift_map"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    work_date: Mapped[date] = mapped_column(Date, nullable=False)
    shift_def_id: Mapped[int] = mapped_column(
        ForeignKey(f"{SCHEMA}.shift_cfg.id", ondelete="RESTRICT"), nullable=False
    )
    worker_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.worker_mst.id", ondelete="RESTRICT"), nullable=False)
    line_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.line_mst.id", ondelete="RESTRICT"), nullable=False)
    equip_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.equip_mst.id", ondelete="SET NULL"))


class KpiSum(Base):
    __tablename__ = "kpi_sum"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    calc_date: Mapped[date] = mapped_column(Date, nullable=False)
    shift_def_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.shift_cfg.id", ondelete="SET NULL"))
    line_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.line_mst.id", ondelete="SET NULL"))
    equip_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.equip_mst.id", ondelete="SET NULL"))
    availability: Mapped[float | None] = mapped_column(Float)
    performance: Mapped[float | None] = mapped_column(Float)
    quality: Mapped[float | None] = mapped_column(Float)
    oee: Mapped[float | None] = mapped_column(Float)
    mttr: Mapped[float | None] = mapped_column(Float)
    mtbf: Mapped[float | None] = mapped_column(Float)
