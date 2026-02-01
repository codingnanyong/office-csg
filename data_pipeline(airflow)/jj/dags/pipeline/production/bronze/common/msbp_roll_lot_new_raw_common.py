"""공통 함수 모듈 - MSBP Roll Lot New Raw"""
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
TABLE_NAME = "msbp_roll_lot_new_raw"
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
            FACTORY, AREA_CD, BAR_KEY, LOT_NO, LOT_NO_SEQ, LOT_TYPE,
            MCS_CD, MCS_NAME, MCS_COLOR, MCS_COLOR_NAME, MODEL_CD, MODEL_NAME,
            ST_ER, CHECK_ER, OP_CD, YMD, HMS, IO_DIV, WEIGHT, UPD_USER,
            UPD_YMD, DOWNLOAD_YN, UPLOAD_YN, LOCATE_CD, CHECK_CD, CHECK_RS,
            PRESS_OP_CD, PROCESSFLAG, PRODUCTDEFINITIONID, THICKNESS, CLOSE_YN,
            INPUT_PROC, PROC_MEMO, EMP_ID
        FROM LMES.MSBP_ROLL_LOT_NEW
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
                # Oracle 원본 순서 (1-34)
                clean_string_value(row['FACTORY']), clean_string_value(row['AREA_CD']), clean_string_value(row['BAR_KEY']),  # 1-3
                clean_string_value(row['LOT_NO']), row['LOT_NO_SEQ'], clean_string_value(row['LOT_TYPE']),  # 4-6
                clean_string_value(row['MCS_CD']), clean_string_value(row['MCS_NAME']), clean_string_value(row['MCS_COLOR']),  # 7-9
                clean_string_value(row['MCS_COLOR_NAME']), clean_string_value(row['MODEL_CD']), clean_string_value(row['MODEL_NAME']),  # 10-12
                row['ST_ER'], row['CHECK_ER'], clean_string_value(row['OP_CD']),  # 13-15
                clean_string_value(row['YMD']), clean_string_value(row['HMS']), clean_string_value(row['IO_DIV']),  # 16-18
                row['WEIGHT'], clean_string_value(row['UPD_USER']), row['UPD_YMD'],  # 19-21
                clean_string_value(row['DOWNLOAD_YN']), clean_string_value(row['UPLOAD_YN']), clean_string_value(row['LOCATE_CD']),  # 22-24
                clean_string_value(row['CHECK_CD']), clean_string_value(row['CHECK_RS']), clean_string_value(row['PRESS_OP_CD']),  # 25-27
                clean_string_value(row['PROCESSFLAG']), clean_string_value(row['PRODUCTDEFINITIONID']), clean_string_value(row['THICKNESS']),  # 28-30
                clean_string_value(row['CLOSE_YN']), clean_string_value(row['INPUT_PROC']), clean_string_value(row['PROC_MEMO']),  # 31-33
                clean_string_value(row['EMP_ID']),  # 34
                # ETL metadata
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 - 컬럼 순서를 DDL과 정확히 매칭
        return [
            (
                # Oracle 원본 순서 (1-34)
                clean_string_value(row[0]), clean_string_value(row[1]), clean_string_value(row[2]),  # FACTORY, AREA_CD, BAR_KEY
                clean_string_value(row[3]), row[4], clean_string_value(row[5]),  # LOT_NO, LOT_NO_SEQ, LOT_TYPE
                clean_string_value(row[6]), clean_string_value(row[7]), clean_string_value(row[8]),  # MCS_CD, MCS_NAME, MCS_COLOR
                clean_string_value(row[9]), clean_string_value(row[10]), clean_string_value(row[11]),  # MCS_COLOR_NAME, MODEL_CD, MODEL_NAME
                row[12], row[13], clean_string_value(row[14]),  # ST_ER, CHECK_ER, OP_CD
                clean_string_value(row[15]), clean_string_value(row[16]), clean_string_value(row[17]),  # YMD, HMS, IO_DIV
                row[18], clean_string_value(row[19]), row[20],  # WEIGHT, UPD_USER, UPD_YMD
                clean_string_value(row[21]), clean_string_value(row[22]), clean_string_value(row[23]),  # DOWNLOAD_YN, UPLOAD_YN, LOCATE_CD
                clean_string_value(row[24]), clean_string_value(row[25]), clean_string_value(row[26]),  # CHECK_CD, CHECK_RS, PRESS_OP_CD
                clean_string_value(row[27]), clean_string_value(row[28]), clean_string_value(row[29]),  # PROCESSFLAG, PRODUCTDEFINITIONID, THICKNESS
                clean_string_value(row[30]), clean_string_value(row[31]), clean_string_value(row[32]),  # CLOSE_YN, INPUT_PROC, PROC_MEMO
                clean_string_value(row[33]),  # EMP_ID
                # ETL metadata
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table (Oracle 원본 순서)"""
    return [
        # Oracle 원본 순서 (1-34)
        "factory", "area_cd", "bar_key", "lot_no", "lot_no_seq", "lot_type",  # 1-6
        "mcs_cd", "mcs_name", "mcs_color", "mcs_color_name", "model_cd", "model_name",  # 7-12
        "st_er", "check_er", "op_cd", "ymd", "hms", "io_div",  # 13-18
        "weight", "upd_user", "upd_ymd", "download_yn", "upload_yn", "locate_cd",  # 19-24
        "check_cd", "check_rs", "press_op_cd", "processflag", "productdefinitionid", "thickness",  # 25-30
        "close_yn", "input_proc", "proc_memo", "emp_id",  # 31-34
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
    """Load data into PostgreSQL database with deduplication"""
    insert_data = prepare_insert_data(data, extract_time)
    
    # 배치 내 중복 제거 (factory, area_cd, bar_key, op_cd, ymd, hms 기준)
    unique_data = []
    seen_keys = set()
    for row in insert_data:
        key = (row[0], row[1], row[2], row[14], row[15], row[16])  # factory, area_cd, bar_key, op_cd, ymd, hms
        if key not in seen_keys:
            seen_keys.add(key)
            unique_data.append(row)
    
    logging.info(f"📊 Original: {len(insert_data)} rows, After deduplication: {len(unique_data)} rows")
    
    columns = get_column_names()
    conflict_columns = ["factory", "area_cd", "bar_key", "op_cd", "ymd", "hms"]
    pg.insert_data(schema_name, table_name, unique_data, columns, conflict_columns)
    logging.info(f"✅ {len(unique_data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

