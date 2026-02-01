"""공통 함수 모듈 - SMP SS PHP SO"""
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
TABLE_NAME = "sss_php_so_raw"
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
            VERSION_ID, FACTORY, RESOURCE_CD, PCARD_ID, HH,
            START_DATE, END_DATE, MODEL_CD, STYLE_CD, CS_SIZE,
            PRS_QTY, MOLD_CD, MOLD_SIZE_CD, PRS_PER_PRESS, RESOURCE_TYPE,
            STATUS, LINE_CD, ASY_YMD, SO_ID, HH_SEQ,
            SHORT_NAME, IE_ROTATION, WS_ROTATION, PLAN_TYPE, PLAN_SHIFT,
            UPD_USER, UPD_YMD, PLAN_DATE, PLAN_HOUR, SFG_CODE,
            L_PRS_QTY, R_PRS_QTY, CT_MOLD_IE, TALLYSHEET_HH
        FROM LMES.SSS_PHP_SO
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
                row['VERSION_ID'], row['FACTORY'], row['RESOURCE_CD'], row['PCARD_ID'], row['HH'],
                row['START_DATE'], row['END_DATE'], row['MODEL_CD'], row['STYLE_CD'], row['CS_SIZE'],
                row['PRS_QTY'], row['MOLD_CD'], row['MOLD_SIZE_CD'], row['PRS_PER_PRESS'], row['RESOURCE_TYPE'],
                row['STATUS'], row['LINE_CD'], row['ASY_YMD'], row['SO_ID'], row['HH_SEQ'],
                row['SHORT_NAME'], row['IE_ROTATION'], row['WS_ROTATION'], row['PLAN_TYPE'], row['PLAN_SHIFT'],
                row['UPD_USER'], row['UPD_YMD'], row['PLAN_DATE'], row['PLAN_HOUR'], row['SFG_CODE'],
                row['L_PRS_QTY'], row['R_PRS_QTY'], row['CT_MOLD_IE'], row['TALLYSHEET_HH'],
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0], row[1], row[2], row[3], row[4],  # VERSION_ID, FACTORY, RESOURCE_CD, PCARD_ID, HH
                row[5], row[6], row[7], row[8], row[9],  # START_DATE, END_DATE, MODEL_CD, STYLE_CD, CS_SIZE
                row[10], row[11], row[12], row[13], row[14],  # PRS_QTY, MOLD_CD, MOLD_SIZE_CD, PRS_PER_PRESS, RESOURCE_TYPE
                row[15], row[16], row[17], row[18], row[19],  # STATUS, LINE_CD, ASY_YMD, SO_ID, HH_SEQ
                row[20], row[21], row[22], row[23], row[24],  # SHORT_NAME, IE_ROTATION, WS_ROTATION, PLAN_TYPE, PLAN_SHIFT
                row[25], row[26], row[27], row[28], row[29],  # UPD_USER, UPD_YMD, PLAN_DATE, PLAN_HOUR, SFG_CODE
                row[30], row[31], row[32], row[33],  # L_PRS_QTY, R_PRS_QTY, CT_MOLD_IE, TALLYSHEET_HH
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "version_id", "factory", "resource_cd", "pcard_id", "hh",
        "start_date", "end_date", "model_cd", "style_cd", "cs_size",
        "prs_qty", "mold_cd", "mold_size_cd", "prs_per_press", "resource_type",
        "status", "line_cd", "asy_ymd", "so_id", "hh_seq",
        "short_name", "ie_rotation", "ws_rotation", "plan_type", "plan_shift",
        "upd_user", "upd_ymd", "plan_date", "plan_hour", "sfg_code",
        "l_prs_qty", "r_prs_qty", "ct_mold_ie", "tallysheet_hh",
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
    # Primary Key: (VERSION_ID, FACTORY, RESOURCE_CD, PCARD_ID, HH, SO_ID)
    conflict_columns = ["version_id", "factory", "resource_cd", "pcard_id", "hh", "so_id"]
    
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

