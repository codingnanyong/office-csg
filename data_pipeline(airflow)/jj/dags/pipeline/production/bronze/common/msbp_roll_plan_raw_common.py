"""공통 함수 모듈 - MSBP Roll Plan Raw"""
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
TABLE_NAME = "msbp_roll_plan_raw"
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
# 3️⃣ Data Extraction
# ════════════════════════════════════════════════════════════════

def build_extract_sql(start_date: str, end_date: str) -> str:
    """Build SQL query for data extraction"""
    return f'''
        SELECT
            SO_ID, FACTORY, PLANT_CD, AREA_CD, OP_CD, ITEM_CLASS,
            CFM_DATE, FA_DATE, LINE_CD, MCS_CD, MCS_COLOR, ITEM_CD,
            STYLE_CD, MODEL_CD, MODEL_NM, REMAIN_WEIGHT, USAGE_WEIGHT,
            SHORTAGE_WEIGHT, DEFECTIVE_WEIGHT, ETC_WEIGHT, PLAN_WEIGHT,
            LARGE_STOCK, SMALL_STOCK, PRS_QTY, BATCH_SIZE, BATCH_QTY,
            LARGE_QTY, SMALL_QTY, UPD_USER, UPD_YMD, DOWNLOAD_YN,
            UPLOAD_YN, OP_YMD, OSD_TYPE, PLAN_YMD, PRODUCTDEFINITIONID,
            UNID, INPUT_PROC, INPUT_DT, INPUT_MEMO
        FROM LMES.MSBP_ROLL_PLAN
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
        # 딕셔너리 형태인 경우 (Oracle 결과)
        return [
            (
                row['SO_ID'], row['FACTORY'], row['PLANT_CD'], row['AREA_CD'], row['OP_CD'], row['ITEM_CLASS'],
                row['CFM_DATE'], row['FA_DATE'], row['LINE_CD'], row['MCS_CD'], row['MCS_COLOR'], row['ITEM_CD'],
                row['STYLE_CD'], row['MODEL_CD'], row['MODEL_NM'], row['REMAIN_WEIGHT'], row['USAGE_WEIGHT'],
                row['SHORTAGE_WEIGHT'], row['DEFECTIVE_WEIGHT'], row['ETC_WEIGHT'], row['PLAN_WEIGHT'],
                row['LARGE_STOCK'], row['SMALL_STOCK'], row['PRS_QTY'], row['BATCH_SIZE'], row['BATCH_QTY'],
                row['LARGE_QTY'], row['SMALL_QTY'], row['UPD_USER'], row['UPD_YMD'], row['DOWNLOAD_YN'],
                row['UPLOAD_YN'], row['OP_YMD'], row['OSD_TYPE'], row['PLAN_YMD'], row['PRODUCTDEFINITIONID'],
                row['UNID'], row['INPUT_PROC'], row['INPUT_DT'], row['INPUT_MEMO'],
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0], row[1], row[2], row[3], row[4], row[5],  # SO_ID, FACTORY, PLANT_CD, AREA_CD, OP_CD, ITEM_CLASS
                row[6], row[7], row[8], row[9], row[10], row[11],  # CFM_DATE, FA_DATE, LINE_CD, MCS_CD, MCS_COLOR, ITEM_CD
                row[12], row[13], row[14], row[15], row[16], row[17],  # STYLE_CD, MODEL_CD, MODEL_NM, REMAIN_WEIGHT, USAGE_WEIGHT, SHORTAGE_WEIGHT
                row[18], row[19], row[20], row[21], row[22], row[23],  # DEFECTIVE_WEIGHT, ETC_WEIGHT, PLAN_WEIGHT, LARGE_STOCK, SMALL_STOCK, PRS_QTY
                row[24], row[25], row[26], row[27], row[28], row[29],  # BATCH_SIZE, BATCH_QTY, LARGE_QTY, SMALL_QTY, UPD_USER, UPD_YMD
                row[30], row[31], row[32], row[33], row[34], row[35],  # DOWNLOAD_YN, UPLOAD_YN, OP_YMD, OSD_TYPE, PLAN_YMD, PRODUCTDEFINITIONID
                row[36], row[37], row[38], row[39],  # UNID, INPUT_PROC, INPUT_DT, INPUT_MEMO
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "so_id", "factory", "plant_cd", "area_cd", "op_cd", "item_class",
        "cfm_date", "fa_date", "line_cd", "mcs_cd", "mcs_color", "item_cd",
        "style_cd", "model_cd", "model_nm", "remain_weight", "usage_weight",
        "shortage_weight", "defective_weight", "etc_weight", "plan_weight",
        "large_stock", "small_stock", "prs_qty", "batch_size", "batch_qty",
        "large_qty", "small_qty", "upd_user", "upd_ymd", "download_yn",
        "upload_yn", "op_ymd", "osd_type", "plan_ymd", "productdefinitionid",
        "unid", "input_proc", "input_dt", "input_memo",
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
    conflict_columns = ["so_id"]
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

