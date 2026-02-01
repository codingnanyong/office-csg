"""공통 함수 모듈 - MSBP Roll SO Raw"""
import logging
from datetime import datetime, timedelta, timezone
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
ORACLE_CONN_ID = "orc_jj_gmes"
POSTGRES_CONN_ID = "pg_jj_production_dw"
SCHEMA_NAME = "bronze"
TABLE_NAME = "msbp_roll_so_raw"
INDO_TZ = timezone(timedelta(hours=7))
INITIAL_START_DATE = datetime(2020, 1, 1, 0, 0, 0)
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


def clean_string_value(value):
    """Clean string values that may contain commas or special characters"""
    if value is None:
        return None
    if isinstance(value, str):
        # 쉼표를 제거하거나 공백으로 대체
        return value.replace(',', ' ').strip()
    return value


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
            SO_ID, SO_SEQ, MC_CD, MC_SEQ, SHIFT, HH, HH_SEQ, STATUS,
            BATCH_TYPE, BATCH_SIZE, BATCH_QTY, UNID_SEQ, UPD_USER, UPD_YMD,
            DOWNLOAD_YN, UPLOAD_YN, OLD_MC_CD, SOTBL_MCS_CD, SOTBL_INP_USER,
            SOTBL_INP_DT, SOTBL_EXTRA1, SOTBL_EXTRA2, SOTBL_EXTRA3, SEQ,
            SO_CFM_DATE, SO_COLOR_CD, SO_CLEAR_YN, SO_MEMO, INPUT_PROC,
            INPUT_DT, INPUT_MEMO
        FROM LMES.MSBP_ROLL_SO
        WHERE UPD_YMD BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')
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
        # 딕셔너리 형태인 경우 (Oracle 결과) - Oracle 원본 순서에 맞춰 매핑
        return [
            (
                # Oracle 원본 순서 (1-31)
                row['SO_ID'], row['SO_SEQ'], clean_string_value(row['MC_CD']), row['MC_SEQ'], row['SHIFT'],  # 1-5
                row['HH'], row['HH_SEQ'], clean_string_value(row['STATUS']), clean_string_value(row['BATCH_TYPE']), row['BATCH_SIZE'],  # 6-10
                row['BATCH_QTY'], row['UNID_SEQ'], clean_string_value(row['UPD_USER']), row['UPD_YMD'], clean_string_value(row['DOWNLOAD_YN']),  # 11-15
                clean_string_value(row['UPLOAD_YN']), clean_string_value(row['OLD_MC_CD']), clean_string_value(row['SOTBL_MCS_CD']),  # 16-18
                clean_string_value(row['SOTBL_INP_USER']), row['SOTBL_INP_DT'], clean_string_value(row['SOTBL_EXTRA1']),  # 19-21
                clean_string_value(row['SOTBL_EXTRA2']), clean_string_value(row['SOTBL_EXTRA3']), row['SEQ'],  # 22-24
                clean_string_value(row['SO_CFM_DATE']), clean_string_value(row['SO_COLOR_CD']), clean_string_value(row['SO_CLEAR_YN']),  # 25-27
                clean_string_value(row['SO_MEMO']), clean_string_value(row['INPUT_PROC']), row['INPUT_DT'],  # 28-30
                clean_string_value(row['INPUT_MEMO']),  # 31
                # ETL metadata
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 - 컬럼 순서를 DDL과 정확히 매칭
        return [
            (
                # Oracle 원본 순서 (1-31)
                row[0], row[1], clean_string_value(row[2]), row[3], row[4],  # SO_ID, SO_SEQ, MC_CD, MC_SEQ, SHIFT
                row[5], row[6], clean_string_value(row[7]), clean_string_value(row[8]), row[9],  # HH, HH_SEQ, STATUS, BATCH_TYPE, BATCH_SIZE
                row[10], row[11], clean_string_value(row[12]), row[13], clean_string_value(row[14]),  # BATCH_QTY, UNID_SEQ, UPD_USER, UPD_YMD, DOWNLOAD_YN
                clean_string_value(row[15]), clean_string_value(row[16]), clean_string_value(row[17]),  # UPLOAD_YN, OLD_MC_CD, SOTBL_MCS_CD
                clean_string_value(row[18]), row[19], clean_string_value(row[20]),  # SOTBL_INP_USER, SOTBL_INP_DT, SOTBL_EXTRA1
                clean_string_value(row[21]), clean_string_value(row[22]), row[23],  # SOTBL_EXTRA2, SOTBL_EXTRA3, SEQ
                clean_string_value(row[24]), clean_string_value(row[25]), clean_string_value(row[26]),  # SO_CFM_DATE, SO_COLOR_CD, SO_CLEAR_YN
                clean_string_value(row[27]), clean_string_value(row[28]), row[29],  # SO_MEMO, INPUT_PROC, INPUT_DT
                clean_string_value(row[30]),  # INPUT_MEMO
                # ETL metadata
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table (Oracle 원본 순서)"""
    return [
        # Oracle 원본 순서 (1-31)
        "so_id", "so_seq", "mc_cd", "mc_seq", "shift",                    # 1-5
        "hh", "hh_seq", "status", "batch_type", "batch_size",             # 6-10
        "batch_qty", "unid_seq", "upd_user", "upd_ymd", "download_yn",    # 11-15
        "upload_yn", "old_mc_cd", "sotbl_mcs_cd", "sotbl_inp_user",       # 16-19
        "sotbl_inp_dt", "sotbl_extra1", "sotbl_extra2", "sotbl_extra3",   # 20-23
        "seq", "so_cfm_date", "so_color_cd", "so_clear_yn", "so_memo",    # 24-28
        "input_proc", "input_dt", "input_memo",                           # 29-31
        # ETL metadata
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
    conflict_columns = ["so_id", "so_seq"]
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

