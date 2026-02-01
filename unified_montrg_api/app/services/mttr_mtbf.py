"""Service layer for MTTR/MTBF operations."""

from datetime import date
import asyncio
from typing import List, Optional
from sqlalchemy import text
from app.core.database import get_sync_engine
from app.schemas.mttr_mtbf import MttrMtbfRecord


def get_first_day_of_month_str(d: date) -> str:
    """Return the first day of the month that the date belongs to in YYYYMMDD format"""
    first_day = d.replace(day=1)
    return first_day.strftime("%Y%m%d")


def _fetch_mttr_mtbf_sync(
    date: Optional[date] = None,
    process: Optional[str] = None,
) -> List[MttrMtbfRecord]:
    """Fetch MTTR/MTBF data from V_MTTR_MTBF view using synchronous Oracle(CMMS) connection."""
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
                CASE
                    WHEN LINE_NM = 'Outsole' THEN 'OS'
                    WHEN LINE_NM = 'IP'      THEN 'IP'
                    WHEN LINE_NM = 'Phylon'  THEN 'PH'
                END AS PROCESS,
                "DATE" AS PROCESS_DATE,
                NVL(MTTR_TARGET, 0) AS MTTR_TARGET,
                NVL(MTTR, 0) AS MTTR,
                NVL(MTBF_TARGET, 0) AS MTBF_TARGET,
                NVL(MTBF, 0) AS MTBF
            FROM ICMMS.V_MTTR_MTBF
            WHERE LINE_NM = :line_nm
        """
        
        params = {"line_nm": line_nm_value}
        
        # date filter: if date is provided, get the first day of the month that the date belongs to, otherwise get the current month
        # DATE column is VARCHAR2(8) format, so compare with YYYYMMDD string
        if date:
            # get the first day of the month that the date belongs to in YYYYMMDD format
            date_str = get_first_day_of_month_str(date)
            query_str += ' AND "DATE" = :date'
            params["date"] = date_str
        else:
            # default: only current month (YYYYMMDD format)
            query_str += ' AND "DATE" = TO_CHAR(TRUNC(SYSDATE, \'MM\'), \'YYYYMMDD\')'
        
        query_str += " ORDER BY PROCESS_DATE DESC"
    else:
        # default query (current month, LINE_NM IN condition fixed)
        query_str = """
            SELECT 
                CASE
                    WHEN LINE_NM = 'Outsole' THEN 'OS'
                    WHEN LINE_NM = 'IP'      THEN 'IP'
                    WHEN LINE_NM = 'Phylon'  THEN 'PH'
                END AS PROCESS,
                "DATE" AS PROCESS_DATE,
                NVL(MTTR_TARGET, 0) AS MTTR_TARGET,
                NVL(MTTR, 0) AS MTTR,
                NVL(MTBF_TARGET, 0) AS MTBF_TARGET,
                NVL(MTBF, 0) AS MTBF
            FROM ICMMS.V_MTTR_MTBF
            WHERE "DATE" = TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYYMMDD')
              AND LINE_NM IN ('Outsole', 'IP', 'Phylon')
        """
        
        params = {}
        
        # date filter: if date is provided, get the first day of the month that the date belongs to, otherwise get the current month
        # DATE column is VARCHAR2(8) format, so compare with YYYYMMDD string
        if date:
            # get the first day of the month that the date belongs to in YYYYMMDD format
            date_str = get_first_day_of_month_str(date)
            query_str = query_str.replace(
                'WHERE "DATE" = TO_CHAR(TRUNC(SYSDATE, \'MM\'), \'YYYYMMDD\')',
                'WHERE "DATE" = :date'
            )
            params["date"] = date_str
        
        query_str += " ORDER BY PROCESS_DATE DESC"
    
    query = text(query_str)
    
    engine = get_sync_engine("cmms")
    with engine.connect() as conn:
        result = conn.execute(query, params)
        rows = result.fetchall()
    
    # map the columns safely based on the order (PROCESS, PROCESS_DATE, MTTR_TARGET, MTTR, MTBF_TARGET, MTBF)
    # PROCESS_DATE is VARCHAR2(8) format, so convert to date object
    records = []
    for row in rows:
        # PROCESS_DATE (YYYYMMDD string) to date object
        date_str = str(row[1]) if row[1] else None
        parsed_date = None
        if date_str and len(date_str) == 8:
            try:
                from datetime import datetime
                parsed_date = datetime.strptime(date_str, "%Y%m%d").date()
            except (ValueError, TypeError):
                parsed_date = None
        
        records.append(
            MttrMtbfRecord(
                process=row[0],
                date=parsed_date,
                mttr_target=float(row[2]) if row[2] is not None else None,
                mttr=float(row[3]) if row[3] is not None else None,
                mtbf_target=float(row[4]) if row[4] is not None else None,
                mtbf=float(row[5]) if row[5] is not None else None,
            )
        )
    
    return records


async def fetch_mttr_mtbf(
    date: Optional[date] = None,
    process: Optional[str] = None,
) -> List[MttrMtbfRecord]:
    """Fetch MTTR/MTBF data from V_MTTR_MTBF view using synchronous Oracle(CMMS) connection."""
    return await asyncio.to_thread(
        _fetch_mttr_mtbf_sync,
        date,
        process,
    )

