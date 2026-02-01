"""Service layer for RTF data operations."""

from datetime import datetime, timedelta
from typing import Optional, Tuple

from sqlalchemy import Select, select

from app.core.database import get_session, resolve_db_alias
from app.models.rtf_data import RtfData
from app.schemas.rtf_data import HmiDataResponse, HmiData


async def fetch_rtf_data_by_pid(
    pid: int,
    limit: int,
    db_alias: Optional[str] = None,
) -> Optional[HmiDataResponse]:
    """Fetch RTF data by PID without additional filters."""
    return await _fetch_rtf_data(pid=pid, limit=limit, db_alias=db_alias)


async def fetch_rtf_data_by_pid_and_range(
    pid: int,
    limit: int,
    start_key: Optional[str],
    end_key: Optional[str],
    db_alias: Optional[str] = None,
) -> Optional[HmiDataResponse]:
    """Fetch RTF data filtered by an explicit date-key range."""
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None

    if start_key:
        start_dt, start_dt_end = _resolve_date_key(start_key)
        if not end_key:
            end_dt = start_dt_end
    if end_key:
        end_start, end_dt_candidate = _resolve_date_key(end_key)
        if not start_key:
            start_dt = end_start
        end_dt = end_dt_candidate

    if start_dt and end_dt and start_dt >= end_dt:
        raise ValueError("start_key must represent a period earlier than end_key")

    return await _fetch_rtf_data(pid=pid, limit=limit, db_alias=db_alias, start=start_dt, end=end_dt)


async def fetch_rtf_data_by_pid_and_date_key(
    pid: int,
    limit: int,
    date_key: str,
    db_alias: Optional[str] = None,
) -> Optional[HmiDataResponse]:
    """Fetch RTF data filtered by a date key (YYYY, YYYYMM, YYYYMMDD)."""
    start, end = _resolve_date_key(date_key)
    return await _fetch_rtf_data(pid=pid, limit=limit, db_alias=db_alias, start=start, end=end)


async def fetch_rtf_data_latest(
    pid: int,
    db_alias: Optional[str] = None,
) -> Optional[HmiDataResponse]:
    """Fetch the most recent RTF data for a given PID."""
    alias = resolve_db_alias(db_alias)

    async with get_session(alias) as session:
        stmt: Select = (
            select(RtfData.SeqNo, RtfData.RxDate, RtfData.Pvalue, RtfData.PID)
            .where(RtfData.PID == pid)
            .order_by(RtfData.RxDate.desc())
            .limit(1)
        )

        result = await session.execute(stmt)
        row = result.first()

    if not row:
        return None

    formatted_date = row.RxDate.isoformat(sep=" ") if row.RxDate is not None else None
    pvalue = round(row.Pvalue, 3) if row.Pvalue is not None else None
    
    pid_with_padding = f"{row.PID:011d}" if row.PID is not None else f"{pid:011d}"

    return HmiDataResponse(
        PID=pid_with_padding,
        values=[HmiData(rxdate=formatted_date, pvalue=pvalue)]
    )


async def _fetch_rtf_data(
    pid: int,
    limit: int,
    db_alias: Optional[str],
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> Optional[HmiDataResponse]:
    alias = resolve_db_alias(db_alias)

    async with get_session(alias) as session:
        stmt: Select = (
            select(RtfData.SeqNo, RtfData.RxDate, RtfData.Pvalue, RtfData.PID)
            .where(RtfData.PID == pid)
            .order_by(RtfData.SeqNo.asc())
            .limit(limit)
        )

        if start is not None:
            stmt = stmt.where(RtfData.RxDate >= start)
        if end is not None:
            stmt = stmt.where(RtfData.RxDate < end)

        result = await session.execute(stmt)
        rows = result.all()

    if not rows:
        return None

    values = []
    for row in rows:
        formatted_date = row.RxDate.isoformat(sep=" ") if row.RxDate is not None else None
        pvalue = round(row.Pvalue, 3) if row.Pvalue is not None else None
        values.append(HmiData(rxdate=formatted_date, pvalue=pvalue))

    pid_with_padding = f"{rows[0].PID:011d}" if rows[0].PID is not None else f"{pid:011d}"

    return HmiDataResponse(PID=pid_with_padding, values=values)


def _resolve_date_key(date_key: str) -> Tuple[datetime, datetime]:
    """Convert a date key (YYYY, YYYYMM, YYYYMMDD) into start/end datetimes."""
    if len(date_key) not in (4, 6, 8) or not date_key.isdigit():
        raise ValueError("date_key must be numeric in formats YYYY, YYYYMM, or YYYYMMDD")

    year = int(date_key[:4])

    if len(date_key) == 4:
        start = datetime(year, 1, 1)
        end = datetime(year + 1, 1, 1)
    elif len(date_key) == 6:
        month = int(date_key[4:6])
        start = datetime(year, month, 1)
        if month == 12:
            end = datetime(year + 1, 1, 1)
        else:
            end = datetime(year, month + 1, 1)
    else:
        month = int(date_key[4:6])
        day = int(date_key[6:8])
        start = datetime(year, month, day)
        end = start + timedelta(days=1)

    return start, end
