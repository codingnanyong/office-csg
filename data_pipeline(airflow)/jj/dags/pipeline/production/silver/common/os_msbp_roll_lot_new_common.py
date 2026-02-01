"""공통 함수 모듈 - OS MSBP Roll Lot New Silver"""
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
TABLE_NAME = "os_msbp_roll_lot_new"
SOURCE_SCHEMA = "bronze"
LOT_RAW = "msbp_roll_lot_raw"
LOT_NEW_RAW = "msbp_roll_lot_new_raw"
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
        return value.replace(',', ' ').strip()
    return value


def get_month_end_date(start_date: datetime) -> datetime:
    """Get the last day of the month for a given date"""
    next_month = start_date.replace(day=1) + timedelta(days=32)
    month_end = next_month.replace(day=1) - timedelta(days=1)
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
    """Select specified columns from LOT_NEW joined with LOT_RAW on BAR_KEY, date-bounded by LOT_RAW.UPD_YMD"""
    return f'''
        SELECT
            N.BAR_KEY, N.LOT_NO, N.LOT_NO_SEQ, N.LOT_TYPE,
            N.MCS_CD, N.MCS_NAME, N.MCS_COLOR, N.MCS_COLOR_NAME,
            N.MODEL_CD, N.MODEL_NAME,
            N.OP_CD, N.YMD, N.HMS,
            N.WEIGHT, N.UPD_YMD,
            N.LOCATE_CD, N.CHECK_RS, N.THICKNESS
        FROM {SOURCE_SCHEMA}.{LOT_RAW} L
        INNER JOIN {SOURCE_SCHEMA}.{LOT_NEW_RAW} N
            ON L.BAR_KEY = N.BAR_KEY
        WHERE L.OP_CD = 'OS' AND L.UPD_YMD BETWEEN '{start_date}' AND '{end_date}'
    '''


def extract_silver_data(pg: PostgresHelper, start_date: str, end_date: str) -> tuple:
    """Extract and transform data from bronze to silver"""
    sql = build_silver_transform_sql(start_date, end_date)
    logging.info(f"실행 쿼리: {sql}")
    data = pg.execute_query(sql, task_id="extract_silver_lot_new_task", xcom_key=None)
    if data and isinstance(data, list):
        row_count = len(data)
    elif data and hasattr(data, 'rowcount'):
        row_count = data.rowcount
    else:
        row_count = 0
    logging.info(f"{start_date} ~ {end_date} Silver LOT_NEW 변환 row 수: {row_count}")
    return data, row_count


# ════════════════════════════════════════════════════════════════
# 4️⃣ Data Loading
# ════════════════════════════════════════════════════════════════

def prepare_silver_insert_data(data: list, extract_time: datetime) -> list:
    """Prepare data for PostgreSQL insertion"""
    if data and isinstance(data[0], dict):
        return [
            (
                clean_string_value(row['bar_key']), clean_string_value(row['lot_no']), row['lot_no_seq'],
                clean_string_value(row['lot_type']), clean_string_value(row['mcs_cd']), clean_string_value(row['mcs_name']),
                clean_string_value(row['mcs_color']), clean_string_value(row['mcs_color_name']),
                clean_string_value(row['model_cd']), clean_string_value(row['model_name']),
                clean_string_value(row['op_cd']), clean_string_value(row['ymd']), clean_string_value(row['hms']),
                row['weight'], row['upd_ymd'],
                clean_string_value(row['locate_cd']), clean_string_value(row['check_rs']), clean_string_value(row['thickness']),
                extract_time, datetime.utcnow()
            ) for row in data
        ]
    else:
        return [
            (
                clean_string_value(row[0]), clean_string_value(row[1]), row[2],
                clean_string_value(row[3]), clean_string_value(row[4]), clean_string_value(row[5]),
                clean_string_value(row[6]), clean_string_value(row[7]),
                clean_string_value(row[8]), clean_string_value(row[9]),
                clean_string_value(row[10]), clean_string_value(row[11]), clean_string_value(row[12]),
                row[13], row[14],
                clean_string_value(row[15]), clean_string_value(row[16]), clean_string_value(row[17]),
                extract_time, datetime.utcnow()
            ) for row in data
        ]


def get_silver_column_names() -> list:
    """Get column names for PostgreSQL silver table"""
    return [
        "bar_key", "lot_no", "lot_no_seq", "lot_type",
        "mcs_cd", "mcs_name", "mcs_color", "mcs_color_name",
        "model_cd", "model_name",
        "op_cd", "ymd", "hms",
        "weight", "upd_ymd",
        "locate_cd", "check_rs", "thickness",
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
    # conflict_columns 없음
    pg.insert_data(schema_name, table_name, insert_data, columns)
    logging.info(f"✅ {len(data)} rows inserted into silver LOT_NEW.")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

