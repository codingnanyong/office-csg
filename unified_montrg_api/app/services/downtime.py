"""Service layer for Downtime operations."""

from datetime import date
import asyncio
from typing import List, Optional
from sqlalchemy import text
from app.core.database import get_sync_engine
from app.schemas.downtime import DowntimeRecord


def get_first_day_of_month(d: date) -> date:
    """Return the first day of the month that the date belongs to"""
    return d.replace(day=1)

def _fetch_downtime_sync(
    factory: Optional[str] = None,
    building: Optional[str] = None,
    line: Optional[str] = None,
    date: Optional[date] = None,
    process: Optional[str] = None,
) -> List[DowntimeRecord]:
    """Fetch Downtime data from V_DOWNTIME view using synchronous Oracle(CMMS) connection."""
    # if process parameter is provided, use special query
    if process:
        # convert process code to uppercase and mapping
        process_upper = process.upper()
        process_mapping = {
            'OS': 'Outsole',
            'IP': 'IP',
            'PH': 'Phylon'
        }
        
        if process_upper not in process_mapping:
            return []
        
        line_nm_value = process_mapping[process_upper]
        
        # query for process filtering
        query_str = """
            SELECT 
                FACTORY,
                FACTORY_NM,
                BUILDING,
                BUILDING_NM,
                LINE,
                LINE_NM,
                CASE
                    WHEN LINE_NM = 'Outsole' THEN 'OS'
                    WHEN LINE_NM = 'IP'      THEN 'IP'
                    WHEN LINE_NM = 'Phylon'  THEN 'PH'
                END AS PROCESS,
                "DATE" AS PROCESS_DATE,
                NVL(DOWN_TIME_TARGET, 0) AS DOWN_TIME_TARGET,
                NVL(DOWN_TIME_VALUE, 0) AS DOWN_TIME_VALUE
            FROM ICMMS.V_DOWNTIME
            WHERE LINE_NM = :line_nm
        """
        
        params = {"line_nm": line_nm_value}
        
        # date filter: if date is provided, get the first day of the month that the date belongs to, otherwise get the current month
        # DATE column is DATE type, so pass the first day of the month that the date belongs to (calculated in Python)
        if date:
            # get the first day of the month that the date belongs to (calculated in Python)
            first_day = get_first_day_of_month(date)
            query_str += ' AND "DATE" = :date'
            params["date"] = first_day
        else:
            # default: only current month
            query_str += ' AND "DATE" = TRUNC(SYSDATE, \'MM\')'
        
        query_str += " ORDER BY PROCESS_DATE DESC, FACTORY, BUILDING, LINE"
    else:
        # default query (current month, LINE_NM IN condition fixed)
        query_str = """
            SELECT 
                FACTORY,
                FACTORY_NM,
                BUILDING,
                BUILDING_NM,
                LINE,
                LINE_NM,
                CASE
                    WHEN LINE_NM = 'Outsole' THEN 'OS'
                    WHEN LINE_NM = 'IP'      THEN 'IP'
                    WHEN LINE_NM = 'Phylon'  THEN 'PH'
                END AS PROCESS,
                "DATE" AS PROCESS_DATE,
                NVL(DOWN_TIME_TARGET, 0) AS DOWN_TIME_TARGET,
                NVL(DOWN_TIME_VALUE, 0) AS DOWN_TIME_VALUE
            FROM ICMMS.V_DOWNTIME
            WHERE "DATE" = TRUNC(SYSDATE, 'MM')
              AND LINE_NM IN ('Outsole', 'IP', 'Phylon')
        """
        
        params = {}
        
        # date filter: if date is provided, get the first day of the month that the date belongs to, otherwise get the current month
        # DATE column is DATE type, so pass the first day of the month that the date belongs to (calculated in Python)
        if date:
            # get the first day of the month that the date belongs to (calculated in Python)
            first_day = get_first_day_of_month(date)
            query_str = query_str.replace(
                'WHERE "DATE" = TRUNC(SYSDATE, \'MM\')',
                'WHERE "DATE" = :date'
            )
            params["date"] = first_day
        
        query_str += " ORDER BY PROCESS_DATE DESC, FACTORY, BUILDING, LINE"
    
    query = text(query_str)
    
    engine = get_sync_engine("cmms")
    with engine.connect() as conn:
        result = conn.execute(query, params)
        rows = result.fetchall()
    
    # map the columns safely based on the order (FACTORY, FACTORY_NM, BUILDING, BUILDING_NM, LINE, LINE_NM, PROCESS, PROCESS_DATE, DOWN_TIME_TARGET, DOWN_TIME_VALUE)
    return [
        DowntimeRecord(
            factory=row[0],
            factory_nm=row[1],
            building=row[2],
            building_nm=row[3],
            line=row[4],
            line_nm=row[5],
            process=row[6],
            date=row[7],
            down_time_target=row[8],
            down_time_value=row[9],
        )
        for row in rows
    ]


async def fetch_downtime(
    factory: Optional[str] = None,
    building: Optional[str] = None,
    line: Optional[str] = None,
    date: Optional[date] = None,
    process: Optional[str] = None,
) -> List[DowntimeRecord]:
    """Fetch Downtime data from V_DOWNTIME view using synchronous Oracle(CMMS) connection."""
    return await asyncio.to_thread(
        _fetch_downtime_sync,
        factory,
        building,
        line,
        date,
        process,
    )

