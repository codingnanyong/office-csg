"""Service layer for Banbury Anomaly Detection operations."""

from typing import List, Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session
from app.models.banbury_anomaly_detection import BanburyAnomalyResult
from app.schemas.banbury_anomaly_detection import BanburyAnomalyResultRecord

# Default timezone for naive inputs (PLC timestamp is KST)
DEFAULT_TZ = ZoneInfo("Asia/Seoul")


def _parse_cycle_start_date(date_str: str) -> datetime:
    """Parse various date formats to datetime (cycle_start based)
    
    Supported Formats:
    - yyyy (Example: "2024") -> Start of the year
    - yyyyMM (Example: "202412") -> Start of the month
    - yyyyMMdd (Example: "20241205") -> Start of the day
    - yyyy-MM (Example: "2024-12") -> Start of the month
    - YYYY-MM-DD (Example: "2024-12-05") -> Start of the day
    - YYYY-MM-DD HH:MM:SS (Example: "2024-12-05 14:30:00") -> Exact time
    - YYYY-MM-DD HH:MM (Example: "2024-12-05 14:30") -> Exact time
    
    Args:
        date_str: Date string
    
    Returns:
        datetime object
    
    Raises:
        ValueError: Invalid format
    """
    date_str = date_str.strip()

    # ISO 8601 first (supports timezone offset)
    try:
        iso_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if iso_dt.tzinfo is None:
            iso_dt = iso_dt.replace(tzinfo=DEFAULT_TZ)
        return iso_dt
    except ValueError:
        pass
    
    # yyyyMMdd HH:MM:SS format
    if len(date_str) == 19 and " " in date_str and ":" in date_str:
        try:
            return datetime.strptime(date_str, "%Y%m%d %H:%M:%S").replace(tzinfo=DEFAULT_TZ)
        except ValueError:
            pass
    
    # yyyyMMdd HH:MM format
    if len(date_str) == 16 and " " in date_str and ":" in date_str and "-" not in date_str[:10]:
        try:
            return datetime.strptime(date_str, "%Y%m%d %H:%M").replace(tzinfo=DEFAULT_TZ)
        except ValueError:
            pass
    
    # YYYY-MM-DD HH:MM:SS format
    if len(date_str) == 19 and " " in date_str and date_str.count("-") == 2 and date_str.count(":") == 2:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=DEFAULT_TZ)
        except ValueError:
            pass
    
    # YYYY-MM-DD HH:MM format
    if len(date_str) == 16 and " " in date_str and date_str.count("-") == 2 and date_str.count(":") == 1:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M").replace(tzinfo=DEFAULT_TZ)
        except ValueError:
            pass
    
    # YYYY-MM-DD format
    if len(date_str) == 10 and date_str.count("-") == 2:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=DEFAULT_TZ)
        except ValueError:
            pass
    
    # yyyyMMdd format
    if len(date_str) == 8 and date_str.isdigit():
        try:
            return datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=DEFAULT_TZ)
        except ValueError:
            pass
    
    # yyyy-MM format
    if len(date_str) == 7 and date_str.count("-") == 1:
        try:
            parsed = datetime.strptime(date_str, "%Y-%m")
            return datetime(parsed.year, parsed.month, 1, tzinfo=DEFAULT_TZ)
        except ValueError:
            pass
    
    # yyyyMM format
    if len(date_str) == 6 and date_str.isdigit():
        try:
            parsed = datetime.strptime(date_str, "%Y%m")
            return datetime(parsed.year, parsed.month, 1, tzinfo=DEFAULT_TZ)
        except ValueError:
            pass
    
    # yyyy format
    if len(date_str) == 4 and date_str.isdigit():
        try:
            return datetime(int(date_str), 1, 1, tzinfo=DEFAULT_TZ)
        except ValueError:
            pass
    
    raise ValueError(
        f"Invalid date format. Supported formats: yyyy, yyyyMM, yyyyMMdd, yyyy-MM, YYYY-MM-DD, "
        f"YYYY-MM-DD HH:MM, YYYY-MM-DD HH:MM:SS. Input value: {date_str}"
    )


def _resolve_cycle_start_range(
    start_key: Optional[str], 
    end_key: Optional[str]
) -> tuple:
    """Convert date key string to datetime range (cycle_start based)
    
    Args:
        start_key: Start date key
        end_key: End date key
    
    Returns:
        (start_dt, end_dt) tuple
    """
    start_dt = None
    end_dt = None
    
    if start_key:
        start_dt = _parse_cycle_start_date(start_key)
        # yyyy, yyyyMM, yyyyMMdd, yyyy-MM, YYYY-MM-DD format cases, set the start of the corresponding period
        start_key_stripped = start_key.strip()
        if len(start_key_stripped) == 4:  # yyyy
            start_dt = datetime(start_dt.year, 1, 1, tzinfo=start_dt.tzinfo or DEFAULT_TZ)
        elif len(start_key_stripped) == 6 and start_key_stripped.isdigit():  # yyyyMM
            start_dt = datetime(start_dt.year, start_dt.month, 1, tzinfo=start_dt.tzinfo or DEFAULT_TZ)
        elif len(start_key_stripped) == 7 and "-" in start_key_stripped:  # yyyy-MM
            start_dt = datetime(start_dt.year, start_dt.month, 1, tzinfo=start_dt.tzinfo or DEFAULT_TZ)
        elif len(start_key_stripped) == 8 and start_key_stripped.isdigit():  # yyyyMMdd
            start_dt = datetime(start_dt.year, start_dt.month, start_dt.day, tzinfo=start_dt.tzinfo or DEFAULT_TZ)
        elif len(start_key_stripped) == 10 and start_key_stripped.count("-") == 2:  # YYYY-MM-DD
            start_dt = datetime(start_dt.year, start_dt.month, start_dt.day, tzinfo=start_dt.tzinfo or DEFAULT_TZ)
        # Formats containing time (YYYY-MM-DD HH:MM, YYYY-MM-DD HH:MM:SS, etc.) are used as is
    
    if end_key:
        end_dt = _parse_cycle_start_date(end_key)
        # yyyy, yyyyMM, yyyyMMdd, yyyy-MM, YYYY-MM-DD format cases, set the end of the corresponding period
        end_key_stripped = end_key.strip()
        if len(end_key_stripped) == 4:  # yyyy
            end_dt = datetime(end_dt.year + 1, 1, 1, tzinfo=end_dt.tzinfo or DEFAULT_TZ)
        elif len(end_key_stripped) == 6 and end_key_stripped.isdigit():  # yyyyMM
            if end_dt.month == 12:
                end_dt = datetime(end_dt.year + 1, 1, 1, tzinfo=end_dt.tzinfo or DEFAULT_TZ)
            else:
                end_dt = datetime(end_dt.year, end_dt.month + 1, 1, tzinfo=end_dt.tzinfo or DEFAULT_TZ)
        elif len(end_key_stripped) == 7 and "-" in end_key_stripped:  # yyyy-MM
            if end_dt.month == 12:
                end_dt = datetime(end_dt.year + 1, 1, 1, tzinfo=end_dt.tzinfo or DEFAULT_TZ)
            else:
                end_dt = datetime(end_dt.year, end_dt.month + 1, 1, tzinfo=end_dt.tzinfo or DEFAULT_TZ)
        elif len(end_key_stripped) == 8 and end_key_stripped.isdigit():  # yyyyMMdd
            end_dt = end_dt + timedelta(days=1)
        elif len(end_key_stripped) == 10 and end_key_stripped.count("-") == 2:  # YYYY-MM-DD
            end_dt = end_dt + timedelta(days=1)
        # Formats containing time (YYYY-MM-DD HH:MM, YYYY-MM-DD HH:MM:SS, etc.) are used as is
    
    return start_dt, end_dt


async def fetch_banbury_anomaly_results(
    no: Optional[str] = None,
    shift: Optional[int] = None,
    mode: Optional[str] = None,
    is_anomaly: Optional[bool] = None,
    is_3_stage: Optional[bool] = None,
    result: Optional[bool] = None,
    cycle_start_from: Optional[datetime] = None,
    cycle_start_to: Optional[datetime] = None,
) -> List[BanburyAnomalyResultRecord]:
    """Fetch Banbury Anomaly Result data

    Args:
        no: Filter by number
        shift: Filter by shift
        mode: Filter by mode
        is_anomaly: Filter by anomaly
        is_3_stage: Filter by 3 stage
        result: Filter by result
        cycle_start_from: Filter by cycle start time start range
        cycle_start_to: Filter by cycle start time end range

    Returns:
        Banbury Anomaly Result record list
    """
    # Use montrg database (MONTRG_DATABASE_URL)
    async with get_session(db_alias="montrg") as session:
        query = select(BanburyAnomalyResult)

        # Add filter conditions
        if no:
            query = query.where(BanburyAnomalyResult.no == no)
        if shift is not None:
            query = query.where(BanburyAnomalyResult.shift == shift)
        if mode:
            query = query.where(BanburyAnomalyResult.mode == mode)
        if is_anomaly is not None:
            query = query.where(BanburyAnomalyResult.is_anomaly == is_anomaly)
        if is_3_stage is not None:
            query = query.where(BanburyAnomalyResult.is_3_stage == is_3_stage)
        if result is not None:
            query = query.where(BanburyAnomalyResult.result == result)
        if cycle_start_from:
            query = query.where(BanburyAnomalyResult.cycle_start >= cycle_start_from)
        if cycle_start_to:
            query = query.where(BanburyAnomalyResult.cycle_start <= cycle_start_to)

        # Sort (Latest First)
        query = query.order_by(BanburyAnomalyResult.cycle_start.desc())

        result = await session.execute(query)
        records = result.scalars().all()

        # Convert SQLAlchemy model to Pydantic model
        return [
            BanburyAnomalyResultRecord(
                no=record.no,
                shift=record.shift,
                cycle_start=record.cycle_start,
                cycle_end=record.cycle_end,
                mode=record.mode,
                mix_duration_sec=float(record.mix_duration_sec) if record.mix_duration_sec else 0.0,
                max_temp=float(record.max_temp) if record.max_temp else None,
                is_3_stage=record.is_3_stage,
                is_anomaly=record.is_anomaly,
                anomaly_prob=round(float(record.anomaly_prob) * 100, 2) if record.anomaly_prob else 0.0,
                filtered_num=record.filtered_num,
                peak_count=record.peak_count,
                result=record.result,
            )
            for record in records
        ]

