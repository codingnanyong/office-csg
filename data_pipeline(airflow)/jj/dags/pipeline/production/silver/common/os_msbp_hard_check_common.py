"""공통 함수 모듈 - OS MSBP Hard Check Silver"""
import logging
from datetime import datetime, timedelta, timezone
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
POSTGRES_CONN_ID = "pg_jj_production_dw"
SCHEMA_NAME = "silver"
TABLE_NAME = "os_msbp_hard_check"
SOURCE_SCHEMA = "bronze"
SOURCE_TABLE = "msbp_hard_check_raw"
INDO_TZ = timezone(timedelta(hours=7))
INITIAL_START_DATE = datetime(2010, 1, 1, 0, 0, 0)
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
# 3️⃣ Data Transformation
# ════════════════════════════════════════════════════════════════

def build_silver_transform_sql(start_date: str, end_date: str) -> str:
    """Build SQL query for silver layer transformation with LAB_COMP_CD = 'OSR' filter"""
    return f'''
        SELECT
            factory, lab_ymd, lab_comp_cd, mcs_no, lab_no, cs_size,
            model_cd, category, color_cd, gen, patch_no, mc_line,
            ex_width, ex_length, hard_2hours, hard_24hours, sg_check,
            weight, volume, result_yn, remarks, status, upd_user,
            upd_ymd, mold_code, slab_test_date, slab_test_time, input_proc,
            etl_extract_time, etl_ingest_time
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE lab_comp_cd = 'OSR'
        AND upd_ymd BETWEEN '{start_date}' AND '{end_date}'
    '''


def extract_silver_data(pg: PostgresHelper, start_date: str, end_date: str) -> tuple:
    """Extract and transform data from bronze to silver"""
    sql = build_silver_transform_sql(start_date, end_date)
    logging.info(f"실행 쿼리: {sql}")
    
    data = pg.execute_query(sql, task_id="extract_silver_data_task", xcom_key=None)
    
    # Calculate row count from PostgreSQL result
    if data and isinstance(data, list):
        row_count = len(data)
    elif data and hasattr(data, 'rowcount'):
        row_count = data.rowcount
    else:
        row_count = 0
    
    logging.info(f"{start_date} ~ {end_date} Silver 변환 row 수: {row_count}")
    return data, row_count


# ════════════════════════════════════════════════════════════════
# 4️⃣ Data Loading
# ════════════════════════════════════════════════════════════════

def prepare_silver_insert_data(data: list, extract_time: datetime) -> list:
    """Prepare data for PostgreSQL insertion"""
    # PostgreSQL 결과가 딕셔너리 형태인지 확인하고 처리
    if data and isinstance(data[0], dict):
        # 딕셔너리 형태인 경우 (PostgreSQL 결과)
        return [
            (
                clean_string_value(row['factory']), clean_string_value(row['lab_ymd']), 
                clean_string_value(row['lab_comp_cd']), clean_string_value(row['mcs_no']), 
                clean_string_value(row['lab_no']), clean_string_value(row['cs_size']),
                clean_string_value(row['model_cd']), clean_string_value(row['category']), 
                clean_string_value(row['color_cd']), clean_string_value(row['gen']),
                clean_string_value(row['patch_no']), clean_string_value(row['mc_line']),
                clean_string_value(row['ex_width']), clean_string_value(row['ex_length']), 
                clean_string_value(row['hard_2hours']), clean_string_value(row['hard_24hours']),
                clean_string_value(row['sg_check']), clean_string_value(row['weight']),
                clean_string_value(row['volume']), clean_string_value(row['result_yn']), 
                clean_string_value(row['remarks']), clean_string_value(row['status']),
                clean_string_value(row['upd_user']), row['upd_ymd'],
                clean_string_value(row['mold_code']), clean_string_value(row['slab_test_date']), 
                clean_string_value(row['slab_test_time']), clean_string_value(row['input_proc']),
                row['etl_extract_time'], row['etl_ingest_time']
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                clean_string_value(row[0]), clean_string_value(row[1]), clean_string_value(row[2]),
                clean_string_value(row[3]), clean_string_value(row[4]), clean_string_value(row[5]),
                clean_string_value(row[6]), clean_string_value(row[7]), clean_string_value(row[8]),
                clean_string_value(row[9]), clean_string_value(row[10]), clean_string_value(row[11]),
                clean_string_value(row[12]), clean_string_value(row[13]), clean_string_value(row[14]),
                clean_string_value(row[15]), clean_string_value(row[16]), clean_string_value(row[17]),
                clean_string_value(row[18]), clean_string_value(row[19]), clean_string_value(row[20]),
                clean_string_value(row[21]), clean_string_value(row[22]), row[23],
                clean_string_value(row[24]), clean_string_value(row[25]), clean_string_value(row[26]),
                clean_string_value(row[27]), row[28], row[29]
            ) for row in data
        ]


def get_silver_column_names() -> list:
    """Get column names for PostgreSQL silver table"""
    return [
        "factory", "lab_ymd", "lab_comp_cd", "mcs_no", "lab_no", "cs_size",
        "model_cd", "category", "color_cd", "gen", "patch_no", "mc_line",
        "ex_width", "ex_length", "hard_2hours", "hard_24hours", "sg_check",
        "weight", "volume", "result_yn", "remarks", "status", "upd_user",
        "upd_ymd", "mold_code", "slab_test_date", "slab_test_time", "input_proc",
        "etl_extract_time", "etl_ingest_time"
    ]


def load_silver_data(
    pg: PostgresHelper, 
    data: list, 
    extract_time: datetime,
    schema_name: str = SCHEMA_NAME,
    table_name: str = TABLE_NAME
) -> None:
    """Load data into PostgreSQL silver database"""
    insert_data = prepare_silver_insert_data(data, extract_time)
    columns = get_silver_column_names()
    conflict_columns = ["factory", "lab_ymd", "lab_comp_cd", "mcs_no", "lab_no", "color_cd"]
    
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted into silver layer.")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

