"""공통 함수 모듈 - OS MSBP Roll Plan Silver"""
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
TABLE_NAME = "os_msbp_roll_plan"
SOURCE_SCHEMA = "bronze"
SOURCE_TABLE = "msbp_roll_plan_raw"
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
    """Build SQL query for silver layer transformation"""
    return f'''
        SELECT
            SO_ID, CFM_DATE, FA_DATE, MCS_CD,
            USAGE_WEIGHT, SHORTAGE_WEIGHT, PLAN_WEIGHT,
            PRS_QTY, BATCH_SIZE, BATCH_QTY, LARGE_QTY,
            UPD_YMD
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE OP_CD = 'OS'
        AND UPD_YMD BETWEEN '{start_date}' AND '{end_date}'
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
                row['so_id'], row['cfm_date'], row['fa_date'], row['mcs_cd'],
                row['usage_weight'], row['shortage_weight'], row['plan_weight'],
                row['prs_qty'], row['batch_size'], row['batch_qty'], row['large_qty'],
                row['upd_ymd'],
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0], row[1], row[2], row[3],  # SO_ID, CFM_DATE, FA_DATE, MCS_CD
                row[4], row[5], row[6],  # USAGE_WEIGHT, SHORTAGE_WEIGHT, PLAN_WEIGHT
                row[7], row[8], row[9], row[10],  # PRS_QTY, BATCH_SIZE, BATCH_QTY, LARGE_QTY
                row[11],  # UPD_YMD
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_silver_column_names() -> list:
    """Get column names for PostgreSQL silver table"""
    return [
        "so_id", "cfm_date", "fa_date", "mcs_cd",
        "usage_weight", "shortage_weight", "plan_weight",
        "prs_qty", "batch_size", "batch_qty", "large_qty",
        "upd_ymd",
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
    conflict_columns = ["so_id"]
    
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted into silver layer (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

