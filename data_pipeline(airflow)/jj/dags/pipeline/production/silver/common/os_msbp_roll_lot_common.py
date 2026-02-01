"""공통 함수 모듈 - OS MSBP Roll Lot Silver"""
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
TABLE_NAME = "os_msbp_roll_lot"
SOURCE_SCHEMA = "bronze"
LOT_TABLE = "msbp_roll_lot_raw"
PLAN_TABLE = "msbp_roll_plan_raw"
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
    """Build SQL query for silver layer transformation with JOIN"""
    return f'''
        SELECT
            LOT.SO_ID, LOT.SO_SEQ, LOT.MCS_CD, LOT.MCS_COLOR, LOT.STYLE_CD,
            LOT.ROLL_MC, LOT.RECYCLE_LOT_ID, LOT.ROLL_OP_CD, LOT.PREV_OP_CD,
            LOT.NEXT_OP_CD, LOT.STATUS, LOT.BAR_KEY, LOT.LOT_NO, LOT.LOT_NO_SEQ,
            LOT.LOT_YMD, LOT.LOT_HMS, LOT.OUT_YMD, LOT.OUT_HMS, LOT.LOT_WEIGHT,
            LOT.THICKNESS, LOT.THICKNESS_MAX, LOT.THICKNESS_MIN, LOT.THICKNESS_ACT,
            LOT.THICKNESS_ACT_MIN, LOT.THICKNESS_ACT_MAX, LOT.TEMP1, LOT.TEMP2,
            LOT.TEMP3, LOT.UPD_YMD, LOT.PRINT_DT
        FROM {SOURCE_SCHEMA}.{LOT_TABLE} LOT
        JOIN {SOURCE_SCHEMA}.{PLAN_TABLE} PLAN ON LOT.SO_ID = PLAN.SO_ID
        WHERE PLAN.OP_CD = 'OS'
        AND LOT.UPD_YMD BETWEEN '{start_date}' AND '{end_date}'
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
                row['so_id'], row['so_seq'], clean_string_value(row['mcs_cd']),
                clean_string_value(row['mcs_color']), clean_string_value(row['style_cd']),
                clean_string_value(row['roll_mc']), clean_string_value(row['recycle_lot_id']),
                clean_string_value(row['roll_op_cd']), clean_string_value(row['prev_op_cd']),
                clean_string_value(row['next_op_cd']), clean_string_value(row['status']),
                clean_string_value(row['bar_key']), clean_string_value(row['lot_no']),
                clean_string_value(row['lot_no_seq']), clean_string_value(row['lot_ymd']),
                clean_string_value(row['lot_hms']), clean_string_value(row['out_ymd']),
                clean_string_value(row['out_hms']), row['lot_weight'],
                clean_string_value(row['thickness']), row['thickness_max'],
                row['thickness_min'], clean_string_value(row['thickness_act']),
                row['thickness_act_min'], row['thickness_act_max'],
                row['temp1'], row['temp2'], row['temp3'],
                row['upd_ymd'], row['print_dt'],
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0], row[1], clean_string_value(row[2]), clean_string_value(row[3]),
                clean_string_value(row[4]), clean_string_value(row[5]), clean_string_value(row[6]),
                clean_string_value(row[7]), clean_string_value(row[8]), clean_string_value(row[9]),
                clean_string_value(row[10]), clean_string_value(row[11]), clean_string_value(row[12]),
                clean_string_value(row[13]), clean_string_value(row[14]), clean_string_value(row[15]),
                clean_string_value(row[16]), clean_string_value(row[17]), row[18],
                clean_string_value(row[19]), row[20], row[21], clean_string_value(row[22]),
                row[23], row[24], row[25], row[26], row[27],
                row[28], row[29],
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_silver_column_names() -> list:
    """Get column names for PostgreSQL silver table"""
    return [
        "so_id", "so_seq", "mcs_cd", "mcs_color", "style_cd", "roll_mc",
        "recycle_lot_id", "roll_op_cd", "prev_op_cd", "next_op_cd", "status",
        "bar_key", "lot_no", "lot_no_seq", "lot_ymd", "lot_hms", "out_ymd",
        "out_hms", "lot_weight", "thickness", "thickness_max", "thickness_min",
        "thickness_act", "thickness_act_min", "thickness_act_max", "temp1",
        "temp2", "temp3", "upd_ymd", "print_dt",
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
    logging.info(f"✅ {len(data)} rows inserted into silver layer.")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

