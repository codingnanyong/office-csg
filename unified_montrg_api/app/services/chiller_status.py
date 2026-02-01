"""Service layer for Chiller Status operations."""

from typing import List, Tuple, Optional, Any, Dict
from collections import defaultdict
from sqlalchemy import select, func, literal, Numeric, text, case, Integer
from sqlalchemy.orm import aliased
from datetime import datetime, timedelta, timezone

from app.core.database import get_session
from app.models.chiller_status import ChillerStatus, ChillerDevice
from app.schemas.chiller_status import (
    ChillerStatusRecord,
    ChillerRunningStatusRecord,
    ChillerAlarmStatusRecord,
    ChillerStatusHistoryRecord,
    ChillerStatusHistoryItem,
)

# Define alarm mapping constants
ALARM_MAP: List[Tuple[int, str]] = [
    (27, 'Flow_SW_Alarm'),
    (28, '#1_Comp_OCR_Alarm'),
    (29, '#2_Comp_OCR_Alarm'),
    (30, '#3_Comp_OCR_Alarm'),
    (31, '#4_Comp_OCR_Alarm'),
    (32, '#1_Fan_OCR_Alarm'),
    (33, '#2_Fan_OCR_Alarm'),
    (34, '#3_Fan_OCR_Alarm'),
    (35, '#4_Fan_OCR_Alarm'),
    (36, '#1_Comp_INT_69_Alarm'),
    (37, '#2_Comp_INT_69_Alarm'),
    (38, '#3_Comp_INT_69_Alarm'),
    (39, '#4_Comp_INT_69_Alarm'),
    (40, '#1_Low_Pressure_Switch'),
    (41, '#2_Low_Pressure_Switch'),
    (42, '#3_Low_Pressure_Switch'),
    (43, '#4_Low_Pressure_Switch'),
    (44, '#1_High_Pressure_Switch'),
    (45, '#2_High_Pressure_Switch'),
    (46, '#3_High_Pressure_Switch'),
    (47, '#4_High_Pressure_Switch'),
]


def _build_alarm_map_values_sql() -> str:
    """Convert alarm mapping to VALUES SQL"""
    values = ', '.join(f"({bit_pos}, '{alarm_name}')" for bit_pos, alarm_name in ALARM_MAP)
    return f"""
        SELECT * FROM (VALUES
            {values}
        ) AS a(bit_pos, alarm_name)
    """


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert to float"""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _format_chiller_name(device_id: str) -> str:
    """Convert device_id to 'Chiller XX' format
    
    Args:
        device_id: Device ID (Example: 'CKP004', 'CKP001')
    
    Returns:
        String in 'Chiller 04', 'Chiller 01' format
    """
    if not device_id:
        return device_id
    # Extract the last 2 characters of device_id and convert to "Chiller XX" format
    chiller_number = device_id[-2:] if len(device_id) >= 2 else device_id
    return f"Chiller {chiller_number}"


def _get_date_format_type(date_str: str) -> str:
    """Return the format type of the date string
    
    Args:
        date_str: Date string
    
    Returns:
        Format type: 'yyyy', 'yyyyMM', 'yyyyMMdd', 'yyyyMMdd HH:MM', 'yyyyMMdd HH:MM:SS', 'unknown'
    """
    date_str = date_str.strip()
    
    if len(date_str) == 19 and " " in date_str and ":" in date_str:
        return "yyyyMMdd HH:MM:SS"
    elif len(date_str) == 16 and " " in date_str and ":" in date_str:
        return "yyyyMMdd HH:MM"
    elif len(date_str) == 8 and date_str.isdigit():
        return "yyyyMMdd"
    elif len(date_str) == 6 and date_str.isdigit():
        return "yyyyMM"
    elif len(date_str) == 4 and date_str.isdigit():
        return "yyyy"
    else:
        return "unknown"


def _parse_date_string(date_str: str) -> datetime:
    """Parse various date formats to datetime
    
    Supported Formats:
    - yyyy (Example: "2024")
    - yyyyMM (Example: "202412")
    - yyyyMMdd (Example: "20241205")
    - yyyyMMdd HH:MM (Example: "20241205 14:30")
    - yyyyMMdd HH:MM:SS (Example: "20241205 14:30:00")
    
    Args:
        date_str: Date string
    
    Returns:
        datetime object
    
    Raises:
        ValueError: Invalid format
    """
    date_str = date_str.strip()
    
    # yyyyMMdd HH:MM:SS format
    if len(date_str) == 19 and " " in date_str and ":" in date_str:
        try:
            return datetime.strptime(date_str, "%Y%m%d %H:%M:%S")
        except ValueError:
            pass
    
    # yyyyMMdd HH:MM format
    if len(date_str) == 16 and " " in date_str and ":" in date_str:
        try:
            return datetime.strptime(date_str, "%Y%m%d %H:%M")
        except ValueError:
            pass
    
    # yyyyMMdd format
    if len(date_str) == 8 and date_str.isdigit():
        try:
            return datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            pass
    
    # yyyyMM format
    if len(date_str) == 6 and date_str.isdigit():
        try:
            return datetime.strptime(date_str, "%Y%m")
        except ValueError:
            pass
    
    # yyyy format
    if len(date_str) == 4 and date_str.isdigit():
        try:
            return datetime.strptime(date_str, "%Y")
        except ValueError:
            pass
    
    raise ValueError(
        f"Invalid date format. Supported formats: yyyy, yyyyMM, yyyyMMdd, yyyyMMdd HH:MM, yyyyMMdd HH:MM:SS. Input value: {date_str}"
    )


def resolve_date_range(start_key: Optional[str], end_key: Optional[str]) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Convert date key string to datetime range
    
    Args:
        start_key: Start date key (yyyy, yyyyMM, yyyyMMdd, yyyyMMdd HH:MM, yyyyMMdd HH:MM:SS)
        end_key: End date key (yyyy, yyyyMM, yyyyMMdd, yyyyMMdd HH:MM, yyyyMMdd HH:MM:SS)
    
    Returns:
        (start_dt, end_dt) tuple
    
    Raises:
        ValueError: start and end formats are different
    """
    # start and end formats are the same
    if start_key and end_key:
        start_format = _get_date_format_type(start_key)
        end_format = _get_date_format_type(end_key)
        
        if start_format != end_format:
            raise ValueError(
                f"start_dt and end_dt formats must be the same. "
                f"start_dt format: {start_format}, end_dt format: {end_format}"
            )
    
    start_dt = None
    end_dt = None
    
    if start_key:
        start_dt = _parse_date_string(start_key)
        # yyyy, yyyyMM, yyyyMMdd format cases, set the start of the corresponding period
        if len(start_key.strip()) == 4:  # yyyy
            start_dt = datetime(start_dt.year, 1, 1)
        elif len(start_key.strip()) == 6:  # yyyyMM
            start_dt = datetime(start_dt.year, start_dt.month, 1)
        elif len(start_key.strip()) == 8 and " " not in start_key:  # yyyyMMdd
            start_dt = datetime(start_dt.year, start_dt.month, start_dt.day)
    
    if end_key:
        end_dt = _parse_date_string(end_key)
        # yyyy, yyyyMM, yyyyMMdd format cases, set the end of the corresponding period
        if len(end_key.strip()) == 4:  # yyyy
            end_dt = datetime(end_dt.year + 1, 1, 1)
        elif len(end_key.strip()) == 6:  # yyyyMM
            if end_dt.month == 12:
                end_dt = datetime(end_dt.year + 1, 1, 1)
            else:
                end_dt = datetime(end_dt.year, end_dt.month + 1, 1)
        elif len(end_key.strip()) == 8 and " " not in end_key:  # yyyyMMdd
            end_dt = end_dt + timedelta(days=1)
        # Formats containing time (YYYY-MM-DD HH:MM, YYYY-MM-DD HH:MM:SS, etc.) are used as is
    
    return start_dt, end_dt


async def fetch_chiller_status() -> List[ChillerStatusRecord]:
    """Fetch the latest data for each device_id in the Status table
    
    Fetch the latest record for each device_id in the Status table using CKP_CHILLER_DATABASE_URL.
    
    Returns:
        ChillerStatus record list
    """
    # Use LATERAL join to find the latest status data for each device_id
    latest_status_subq = (
        select(
            ChillerStatus.upd_dt,
            func.round(ChillerStatus.water_in_temp.cast(Numeric), 1).label("water_in_temp"),
            func.round(ChillerStatus.water_out_temp.cast(Numeric), 1).label("water_out_temp"),
            func.round(ChillerStatus.external_temp.cast(Numeric), 1).label("external_temp"),
            func.round(ChillerStatus.discharge_temp_1.cast(Numeric), 1).label("discharge_temp_1"),
            func.round(ChillerStatus.discharge_temp_2.cast(Numeric), 1).label("discharge_temp_2"),
            func.round(ChillerStatus.discharge_temp_3.cast(Numeric), 1).label("discharge_temp_3"),
            func.round(ChillerStatus.discharge_temp_4.cast(Numeric), 1).label("discharge_temp_4"),
        )
        .where(ChillerStatus.device_id == ChillerDevice.device_id)
        .order_by(ChillerStatus.upd_dt.desc())
        .limit(1)
        .lateral()
    )
    
    latest_s = aliased(latest_status_subq, name="latest_s")
    
    # Main query: Join the latest Status with Device using LATERAL join
    query = (
        select(
            ChillerDevice.device_id,
            latest_s.c.upd_dt,
            latest_s.c.water_in_temp,
            latest_s.c.water_out_temp,
            latest_s.c.external_temp,
            latest_s.c.discharge_temp_1,
            latest_s.c.discharge_temp_2,
            latest_s.c.discharge_temp_3,
            latest_s.c.discharge_temp_4,
        )
        .select_from(ChillerDevice)
        .outerjoin(latest_s, literal(True))
    )
    
    async with get_session(db_alias="ckp_chiller") as session:
        result = await session.execute(query)
        rows = result.all()
        
        return [
            ChillerStatusRecord(
                chiller_name=_format_chiller_name(row[0]),
                upd_dt=row[1],
                water_in_temp=_safe_float(row[2]),
                water_out_temp=_safe_float(row[3]),
                external_temp=_safe_float(row[4]),
                discharge_temp_1=_safe_float(row[5]),
                discharge_temp_2=_safe_float(row[6]),
                discharge_temp_3=_safe_float(row[7]),
                discharge_temp_4=_safe_float(row[8]),
            )
            for row in rows
        ]


async def fetch_chiller_running_status() -> List[ChillerRunningStatusRecord]:
    """Fetch the running status for each device_id in the Status table in the last 5 minutes
    
    Fetch the running status for each device_id in the Status table in the last 5 minutes using CKP_CHILLER_DATABASE_URL.
    
    Returns:
        ChillerRunningStatusRecord record list
    """
    # Use LATERAL join to find the latest status data for each device_id in the last 5 minutes
    running_subq = (
        select(
            func.substring(ChillerStatus.digitals, 26, 1).label("running_char")
        )
        .where(
            ChillerStatus.device_id == ChillerDevice.device_id,
            ChillerStatus.upd_dt >= func.timezone('Asia/Jakarta', func.current_timestamp()) - timedelta(minutes=5)
        )
        .order_by(ChillerStatus.upd_dt.desc())
        .limit(1)
        .lateral()
    )
    
    latest = aliased(running_subq, name="latest")
    running = func.coalesce(latest.c.running_char, literal('0')).label("running")
    
    # Main query: Join the latest running status with Device using LATERAL join
    query = (
        select(
            ChillerDevice.device_id,
            running,
        )
        .select_from(ChillerDevice)
        .outerjoin(latest, literal(True))
    )
    
    async with get_session(db_alias="ckp_chiller") as session:
        result = await session.execute(query)
        rows = result.all()
        
        return [
            ChillerRunningStatusRecord(
                device_id=row[0],
                running=str(row[1]) if row[1] is not None else '0',
            )
            for row in rows
        ]


async def fetch_chiller_alarm_status() -> List[ChillerAlarmStatusRecord]:
    """Fetch the alarm status for each device_id in the Status table
    
    Fetch the alarm status for each device_id in the Status table using CKP_CHILLER_DATABASE_URL.
    
    Returns:
        ChillerAlarmStatusRecord record list
    """
    device_list = (
        select(ChillerDevice.device_id)
        .subquery()
    )
    
    d = device_list.alias('d')
    st = ChillerStatus.__table__.alias('st')
    
    latest_status_subq = (
        select(
            st.c.digitals,
            st.c.upd_dt
        )
        .where(st.c.device_id == d.c.device_id)
        .order_by(st.c.upd_dt.desc())
        .limit(1)
        .lateral()
    )
    
    latest_status = (
        select(
            d.c.device_id,
            latest_status_subq.c.digitals,
            latest_status_subq.c.upd_dt
        )
        .select_from(d)
        .outerjoin(latest_status_subq, literal(True))
        .where(latest_status_subq.c.upd_dt.isnot(None))
        .subquery()
    )
    
    # text() cannot be used as a subquery, so the entire query is written in raw SQL
   
    alarm_map_sql = _build_alarm_map_values_sql().strip()
    
    # 전체 쿼리를 raw SQL로 작성 (ORM과 하이브리드)
    query = text(f"""
        WITH device_list AS (
            SELECT device_id
            FROM device
        ),
        latest_status AS (
            SELECT
                d.device_id,
                st.digitals,
                st.upd_dt
            FROM device_list d
            LEFT JOIN LATERAL (
                SELECT digitals, upd_dt
                FROM status s
                WHERE s.device_id = d.device_id
                ORDER BY upd_dt DESC
                LIMIT 1
            ) st ON TRUE
            WHERE st.upd_dt IS NOT NULL
        ),
        alarm_map AS (
            {alarm_map_sql}
        ),
        alarm_values AS (
            SELECT
                ls.device_id,
                ls.upd_dt,
                am.alarm_name,
                CAST(SUBSTRING(ls.digitals FROM am.bit_pos FOR 1) AS INTEGER) AS status_value
            FROM latest_status ls
            CROSS JOIN alarm_map am
        )
        SELECT
            alarm_name,
            COUNT(*) FILTER (WHERE status_value = 1) AS alarm_count,
            ARRAY_AGG(
                jsonb_build_object(
                    'device_id', device_id,
                    'device_name', device_id,
                    'upd_dt', upd_dt
                )
                ORDER BY device_id
            ) FILTER (WHERE status_value = 1) AS device_info
        FROM alarm_values
        GROUP BY alarm_name
        ORDER BY alarm_name
    """)
    
    async with get_session(db_alias="ckp_chiller") as session:
        result = await session.execute(query)
        rows = result.all()
        
        records = []
        for row in rows:
            alarm_name = row[0]
            alarm_count = row[1] if row[1] is not None else 0
            device_info_raw = row[2] if row[2] is not None else []
            
            # device_info의 각 항목에서 device_name을 매핑
            device_info = []
            for device in device_info_raw:
                device_id = device.get('device_id', '')
                device_info.append({
                    'device_id': device_id,
                    'device_name': _format_chiller_name(device_id),
                    'upd_dt': device.get('upd_dt'),
                })
            
            records.append(
                ChillerAlarmStatusRecord(
                    alarm_name=alarm_name,
                    alarm_count=alarm_count,
                    device_info=device_info,
                )
            )
        
        return records


async def fetch_chiller_status_history(
    device_id: Optional[str] = None,
    hours: int = 6,
) -> List[ChillerStatusHistoryRecord]:
    """Fetch the history for each device_id in the Status table in the last N hours (10 minute bucket)
    
    Fetch the history for each device_id in the Status table in the last N hours (10 minute bucket) using CKP_CHILLER_DATABASE_URL.
    
    Args:
        device_id: device_id to fetch history for (None means all devices)
                   - "CKP004" format or "04" format both supported
        hours: time range to fetch history for (default: 6 hours)
    
    Returns:
        ChillerStatusHistoryRecord record list
    """
    # SQL expression for 10 minute bucket
    # date_trunc('minute', upd_dt AT TIME ZONE 'Asia/Jakarta')
    #   - ((EXTRACT(minute FROM upd_dt AT TIME ZONE 'Asia/Jakarta')::int % 10) * INTERVAL '1 minute')
    
    # create device_id filtering condition
    device_filter = ""
    if device_id:
        # if only number is input (e.g. "04", "4") or all device_id (e.g. "CKP004")
        if device_id.isdigit():
            # if only number is input: filter by RIGHT(device_id, 2)
            device_filter = f"AND RIGHT(device_id, 2) = '{device_id.zfill(2)}'"
        else:
            # if all device_id is input
            device_filter = "AND device_id = :device_id"
    
    query = text(f"""
        SELECT
            device_id,
            (date_trunc('minute', (upd_dt AT TIME ZONE 'Asia/Jakarta'))
                - ((EXTRACT(minute FROM upd_dt AT TIME ZONE 'Asia/Jakarta')::int % 10) * INTERVAL '1 minute'))
                AT TIME ZONE 'Asia/Jakarta'
                AS bucket_time,
            ROUND(AVG(water_in_temp::float8)::numeric, 2)        AS water_in_temp,
            ROUND(AVG(water_out_temp::float8)::numeric, 2)     AS water_out_temp,
            ROUND(AVG(sv_temp::float8)::numeric, 2)             AS sv_temp,
            ROUND(AVG(discharge_temp_1::float8)::numeric, 2)   AS discharge_temp_1,
            ROUND(AVG(discharge_temp_2::float8)::numeric, 2)  AS discharge_temp_2,
            ROUND(AVG(discharge_temp_3::float8)::numeric, 2)   AS discharge_temp_3,
            ROUND(AVG(discharge_temp_4::float8)::numeric, 2)   AS discharge_temp_4
        FROM status
        WHERE upd_dt >= now() - INTERVAL '{hours + 1} hour'
        {device_filter}
        GROUP BY device_id, bucket_time
        ORDER BY device_id, bucket_time
    """)
    
    params = {}
    if device_id and not device_id.isdigit():
        params['device_id'] = device_id
    
    async with get_session(db_alias="ckp_chiller") as session:
        result = await session.execute(query, params)
        rows = result.all()
        
        # group by device_id
        grouped: Dict[str, List[ChillerStatusHistoryItem]] = defaultdict(list)
        for row in rows:
            row_device_id = row[0]
            grouped[row_device_id].append(
                ChillerStatusHistoryItem(
                    bucket_time=row[1],
                    water_in_temp=_safe_float(row[2]),
                    water_out_temp=_safe_float(row[3]),
                    sv_temp=_safe_float(row[4]),
                    discharge_temp_1=_safe_float(row[5]),
                    discharge_temp_2=_safe_float(row[6]),
                    discharge_temp_3=_safe_float(row[7]),
                    discharge_temp_4=_safe_float(row[8]),
                )
            )
        
        # convert to ChillerStatusHistoryRecord list (convert device_id to "Chiller XX" format)
        return [
            ChillerStatusHistoryRecord(
                device_id=_format_chiller_name(row_device_id),
                history=history_items,
            )
            for row_device_id, history_items in sorted(grouped.items())
        ]


async def fetch_chiller_status_history_by_range(
    device_id: Optional[str] = None,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
) -> List[ChillerStatusHistoryRecord]:
    """Fetch the history for each device_id in the Status table in the specified time range (10 minute bucket)
    
    Fetch the history for each device_id in the Status table in the specified time range (10 minute bucket) using CKP_CHILLER_DATABASE_URL.
    
    Args:
        device_id: device_id to fetch history for (None means all devices)
                   - "CKP004" format or "04" format both supported
        start_dt: start time (None means no limit)
        end_dt: end time (None means no limit)
    
    Returns:
        ChillerStatusHistoryRecord record list
    """
    # create WHERE condition dynamically
    where_conditions = []
    params = {}
    
    # create device_id filtering condition (same logic as fetch_chiller_status_history)
    if device_id:
        # if only number is input (e.g. "04", "4") or all device_id (e.g. "CKP004")
        if device_id.isdigit():
            # if only number is input: filter by RIGHT(device_id, 2)
            where_conditions.append(f"RIGHT(device_id, 2) = '{device_id.zfill(2)}'")
        else:
            # if all device_id is input
            where_conditions.append("device_id = :device_id")
            params['device_id'] = device_id
    
    if start_dt:
        # convert start_dt to Asia/Jakarta
        where_conditions.append("(upd_dt AT TIME ZONE 'Asia/Jakarta') >= (CAST(:start_dt AS timestamp without time zone) AT TIME ZONE 'Asia/Jakarta')")
        params['start_dt'] = start_dt
    
    if end_dt:
        # convert end_dt to Asia/Jakarta
        where_conditions.append("(upd_dt AT TIME ZONE 'Asia/Jakarta') < (CAST(:end_dt AS timestamp without time zone) AT TIME ZONE 'Asia/Jakarta')")
        params['end_dt'] = end_dt
    
    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    query = text(f"""
        SELECT
            device_id,
            (date_trunc('minute', (upd_dt AT TIME ZONE 'Asia/Jakarta'))
                - ((EXTRACT(minute FROM upd_dt AT TIME ZONE 'Asia/Jakarta')::int % 10) * INTERVAL '1 minute'))
                AT TIME ZONE 'Asia/Jakarta'
                AS bucket_time,
            ROUND(AVG(water_in_temp::float8)::numeric, 2)        AS water_in_temp,
            ROUND(AVG(water_out_temp::float8)::numeric, 2)     AS water_out_temp,
            ROUND(AVG(sv_temp::float8)::numeric, 2)             AS sv_temp,
            ROUND(AVG(discharge_temp_1::float8)::numeric, 2)   AS discharge_temp_1,
            ROUND(AVG(discharge_temp_2::float8)::numeric, 2)  AS discharge_temp_2,
            ROUND(AVG(discharge_temp_3::float8)::numeric, 2)   AS discharge_temp_3,
            ROUND(AVG(discharge_temp_4::float8)::numeric, 2)   AS discharge_temp_4
        FROM status
        {where_clause}
        GROUP BY device_id, bucket_time
        ORDER BY device_id, bucket_time
    """)
    
    async with get_session(db_alias="ckp_chiller") as session:
        result = await session.execute(query, params)
        rows = result.all()
        
        # group by device_id
        grouped: Dict[str, List[ChillerStatusHistoryItem]] = defaultdict(list)
        for row in rows:
            row_device_id = row[0]
            grouped[row_device_id].append(
                ChillerStatusHistoryItem(
                    bucket_time=row[1],
                    water_in_temp=_safe_float(row[2]),
                    water_out_temp=_safe_float(row[3]),
                    sv_temp=_safe_float(row[4]),
                    discharge_temp_1=_safe_float(row[5]),
                    discharge_temp_2=_safe_float(row[6]),
                    discharge_temp_3=_safe_float(row[7]),
                    discharge_temp_4=_safe_float(row[8]),
                )
            )
        
        # convert to ChillerStatusHistoryRecord list (convert device_id to "Chiller XX" format)
        return [
            ChillerStatusHistoryRecord(
                device_id=_format_chiller_name(row_device_id),
                history=history_items,
            )
            for row_device_id, history_items in sorted(grouped.items())
        ]