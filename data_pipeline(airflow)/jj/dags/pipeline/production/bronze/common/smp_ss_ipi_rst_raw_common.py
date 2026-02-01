"""공통 함수 모듈 - SMP SS IPI RST Raw"""
import logging
from datetime import datetime, timedelta, timezone
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
ORACLE_CONN_ID = "orc_jj_erp"
POSTGRES_CONN_ID = "pg_jj_production_dw"
SCHEMA_NAME = "bronze"
TABLE_NAME = "smp_ss_ipi_rst_raw"
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
            RST_YMD, SO_ID, SER_NO, VERSION_ID, FACTORY,
            RESOURCE_CD, ZONE_CD, IPP_LINE_CD, MACHINE_CD, STATION_CD,
            STATION_TYPE, START_DATE, END_DATE, NET_WRK_SEC, PRS_QTY,
            CNT_QTY, REASON, INJECTOR_CD, MOLD_ID, REMARK,
            UPD_USER, UPD_YMD, MOLD_BAR_KEY, SEARCH_REMARK, SEARCH_DT,
            SEARCH_PROC
        FROM EDIF.SMP_SS_IPI_RST
        WHERE UPD_YMD BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') 
                          AND TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')
    '''


def extract_data(oracle: OracleHelper, start_date: str, end_date: str) -> tuple:
    """Extract data from Oracle database"""
    sql = build_extract_sql(start_date, end_date)
    logging.info(f"실행 쿼리: {sql}")
    
    data = oracle.execute_query(sql, task_id="extract_data_task", xcom_key=None)
    
    # Calculate row count from Oracle result
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
    # Oracle 결과가 딕셔너리 형태인지 확인하고 처리
    if data and isinstance(data[0], dict):
        # 딕셔너리 형태인 경우 (Oracle 결과)
        return [
            (
                row['RST_YMD'], row['SO_ID'], row['SER_NO'], row['VERSION_ID'], row['FACTORY'],
                row['RESOURCE_CD'], row['ZONE_CD'], row['IPP_LINE_CD'], row['MACHINE_CD'], row['STATION_CD'],
                row['STATION_TYPE'], row['START_DATE'], row['END_DATE'], row['NET_WRK_SEC'], row['PRS_QTY'], row['CNT_QTY'], row['REASON'], row['INJECTOR_CD'],
                row['MOLD_ID'], row['REMARK'], row['UPD_USER'], row['UPD_YMD'], row['MOLD_BAR_KEY'], row['SEARCH_REMARK'], row['SEARCH_DT'],
                row['SEARCH_PROC'],
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0], row[1], row[2], row[3], row[4],  # RST_YMD, SO_ID, SER_NO, VERSION_ID, FACTORY
                row[5], row[6], row[7], row[8], row[9], row[10],  # RESOURCE_CD, ZONE_CD, IPP_LINE_CD, MACHINE_CD, STATION_CD, STATION_TYPE
                row[11], row[12], row[13], row[14], row[15], row[16], row[17],  # START_DATE, END_DATE, NET_WRK_SEC, PRS_QTY, CNT_QTY, REASON, INJECTOR_CD
                row[18], row[19], row[20], row[21], row[22], row[23], row[24],  # MOLD_ID, REMARK, UPD_USER, UPD_YMD, MOLD_BAR_KEY, SEARCH_REMARK, SEARCH_DT
                row[25],  # SEARCH_PROC
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "rst_ymd", "so_id", "ser_no", "version_id", "factory",
        "resource_cd", "zone_cd", "ipp_line_cd", "machine_cd", "station_cd",
        "station_type", "start_date", "end_date", "net_wrk_sec", "prs_qty", "cnt_qty", "reason", "injector_cd",
        "mold_id", "remark", "upd_user", "upd_ymd", "mold_bar_key", "search_remark", "search_dt",
        "search_proc",
        "etl_extract_time", "etl_ingest_time"
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
    conflict_columns = ["rst_ymd", "so_id", "ser_no", "start_date"]
    
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

