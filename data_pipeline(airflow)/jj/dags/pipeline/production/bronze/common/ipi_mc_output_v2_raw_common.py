"""공통 함수 모듈 - IPI MC Output V2 Raw"""
import logging
from datetime import datetime, timedelta, timezone
from plugins.hooks.mssql_hook import MSSQLHelper
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
MSSQL_CONN_ID = "ms_ip"
POSTGRES_CONN_ID = "pg_jj_production_dw"
SCHEMA_NAME = "bronze"
TABLE_NAME = "ipi_mc_output_v2_raw"
INDO_TZ = timezone(timedelta(hours=7))
INITIAL_START_DATE = datetime(2023, 1, 1, 0, 0, 0)
DAYS_OFFSET_FOR_INCREMENTAL = 2


# ════════════════════════════════════════════════════════════════
# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════

def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string with microsecond support"""
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


def get_month_end_date(start_date: datetime) -> datetime:
    """Get the last day of the month for a given date"""
    next_month = start_date.replace(day=1) + timedelta(days=32)
    month_end = next_month.replace(day=1) - timedelta(days=1)
    # 23:59:59로 설정
    return month_end.replace(hour=23, minute=59, second=59, microsecond=999999)


def calculate_expected_monthly_loops(start_date: datetime, end_date: datetime) -> int:
    """Calculate expected number of monthly loops"""
    current_date = start_date
    month_count = 0
    
    while current_date < end_date:
        month_end = get_month_end_date(current_date)
        if month_end > end_date:
            month_end = end_date
        current_date = month_end + timedelta(days=1)
        month_count += 1
    
    return month_count


# ════════════════════════════════════════════════════════════════
# 3️⃣ Data Extraction
# ════════════════════════════════════════════════════════════════

def build_extract_sql(start_date: str, end_date: str) -> str:
    """Build SQL query for data extraction"""
    return f'''
        SELECT
            MC_CD, ST_NUM, ST_SIDE, ACT_QTY, HEAT_TIME,
            TEMP_SV, TEMP_PV, RST_YN, RST_YMD,
            UPD_SO_ID, UPD_PRS_QTY, UPD_CNT_QTY, UPD_DTTM
        FROM APSSS.dbo.IPI_MC_OUTPUT_V2
        WHERE RST_YMD BETWEEN '{start_date}' AND '{end_date}'
    '''


def extract_data(mssql: MSSQLHelper, start_date: str, end_date: str) -> tuple:
    """Extract data from MSSQL database"""
    sql = build_extract_sql(start_date, end_date)
    logging.info(f"실행 쿼리: {sql}")
    
    data = mssql.execute_query(sql, task_id="extract_data_task", xcom_key=None)
    
    # Calculate row count from MSSQL result
    if data and isinstance(data, list):
        row_count = len(data)
    elif data and hasattr(data, 'rowcount'):
        row_count = data.rowcount
    else:
        row_count = 0
    
    logging.info(f"{start_date} ~ {end_date} 추출 row 수: {row_count}")
    return data, row_count


# ════════════════════════════════════════════════════════════════
# 4️⃣ Data Loading
# ════════════════════════════════════════════════════════════════

def prepare_insert_data(data: list, extract_time: datetime) -> list:
    """Prepare data for PostgreSQL insertion"""
    # MSSQL 결과가 딕셔너리 형태인지 확인하고 처리
    if data and isinstance(data[0], dict):
        # 딕셔너리 형태인 경우 (MSSQL 결과)
        return [
            (
                row['MC_CD'], row['ST_NUM'], row['ST_SIDE'], row['RST_YMD'],
                row['ACT_QTY'], row['HEAT_TIME'], row['TEMP_SV'], row['TEMP_PV'], row['RST_YN'],
                row['UPD_SO_ID'], row['UPD_PRS_QTY'], row['UPD_CNT_QTY'], row['UPD_DTTM'],
                extract_time
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0], row[1], row[2], row[8],  # MC_CD, ST_NUM, ST_SIDE, RST_YMD
                row[3], row[4], row[5], row[6], row[7],  # ACT_QTY, HEAT_TIME, TEMP_SV, TEMP_PV, RST_YN
                row[9], row[10], row[11], row[12],  # UPD_SO_ID, UPD_PRS_QTY, UPD_CNT_QTY, UPD_DTTM
                extract_time
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "mc_cd", "st_num", "st_side", "rst_ymd",
        "act_qty", "heat_time", "temp_sv", "temp_pv", "rst_yn",
        "upd_so_id", "upd_prs_qty", "upd_cnt_qty", "upd_dttm",
        "etl_extract_time"
    ]


def load_data(
    pg: PostgresHelper, 
    data: list, 
    extract_time: datetime,
    schema_name: str = SCHEMA_NAME,
    table_name: str = TABLE_NAME
) -> None:
    """Load data into PostgreSQL database"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_column_names()
    conflict_columns = ["mc_cd", "st_num", "st_side", "rst_ymd"]
    
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

