"""공통 함수 모듈 - SMP SS PHP RST Raw"""
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
TABLE_NAME = "smp_ss_php_rst_raw"
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
            WORK_DATE, HMS, SO_ID, SER_NO, LINE_NO,
            RESOURCE_CD, MACHINE_CD, STATION_CD, SHIFT_CD, STYLE_CD,
            START_DATE, END_DATE, START_HMS, NUMBER_MOLD, TEMP1,
            TEMP2, TEMP3, TEMP4, TEMP5, PROD_QTY,
            PROD_COUNT, REMARKS1, REMARKS2, REMARKS3, REMARKS4,
            SS_ID, MOLD_CD, MOLD_BARCODE, LOAD_YN, UPD_USER,
            UPD_DATE, HEAT_TIME, REFRIGERATE, COLD_TIME, ICE_TIME,
            HEAT_PRESS, HEAT_FLOW, PRESSURE, PRESSURE1, MODEL,
            MOLD_SIZE, MOLD_BAR_KEY, SEARCH_REMARK, SEARCH_DT, SEARCH_PROC
        FROM LMES.SMP_SS_PHP_RST
        WHERE UPD_DATE BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') 
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
                row['WORK_DATE'], row['HMS'], row['SO_ID'], row['SER_NO'], row['LINE_NO'],
                row['RESOURCE_CD'], row['MACHINE_CD'], row['STATION_CD'], row['SHIFT_CD'], row['STYLE_CD'],
                row['START_DATE'], row['END_DATE'], row['START_HMS'], row['NUMBER_MOLD'], row['TEMP1'],
                row['TEMP2'], row['TEMP3'], row['TEMP4'], row['TEMP5'], row['PROD_QTY'],
                row['PROD_COUNT'], row['REMARKS1'], row['REMARKS2'], row['REMARKS3'], row['REMARKS4'],
                row['SS_ID'], row['MOLD_CD'], row['MOLD_BARCODE'], row['LOAD_YN'], row['UPD_USER'],
                row['UPD_DATE'], row['HEAT_TIME'], row['REFRIGERATE'], row['COLD_TIME'], row['ICE_TIME'],
                row['HEAT_PRESS'], row['HEAT_FLOW'], row['PRESSURE'], row['PRESSURE1'], row['MODEL'],
                row['MOLD_SIZE'], row['MOLD_BAR_KEY'], row['SEARCH_REMARK'], row['SEARCH_DT'], row['SEARCH_PROC'],
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0], row[1], row[2], row[3], row[4],  # WORK_DATE, HMS, SO_ID, SER_NO, LINE_NO
                row[5], row[6], row[7], row[8], row[9],  # RESOURCE_CD, MACHINE_CD, STATION_CD, SHIFT_CD, STYLE_CD
                row[10], row[11], row[12], row[13], row[14],  # START_DATE, END_DATE, START_HMS, NUMBER_MOLD, TEMP1
                row[15], row[16], row[17], row[18], row[19],  # TEMP2, TEMP3, TEMP4, TEMP5, PROD_QTY
                row[20], row[21], row[22], row[23], row[24],  # PROD_COUNT, REMARKS1, REMARKS2, REMARKS3, REMARKS4
                row[25], row[26], row[27], row[28], row[29],  # SS_ID, MOLD_CD, MOLD_BARCODE, LOAD_YN, UPD_USER
                row[30], row[31], row[32], row[33], row[34],  # UPD_DATE, HEAT_TIME, REFRIGERATE, COLD_TIME, ICE_TIME
                row[35], row[36], row[37], row[38], row[39],  # HEAT_PRESS, HEAT_FLOW, PRESSURE, PRESSURE1, MODEL
                row[40], row[41], row[42], row[43], row[44],  # MOLD_SIZE, MOLD_BAR_KEY, SEARCH_REMARK, SEARCH_DT, SEARCH_PROC
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "work_date", "hms", "so_id", "ser_no", "line_no",
        "resource_cd", "machine_cd", "station_cd", "shift_cd", "style_cd",
        "start_date", "end_date", "start_hms", "number_mold", "temp1",
        "temp2", "temp3", "temp4", "temp5", "prod_qty",
        "prod_count", "remarks1", "remarks2", "remarks3", "remarks4",
        "ss_id", "mold_cd", "mold_barcode", "load_yn", "upd_user",
        "upd_date", "heat_time", "refrigerate", "cold_time", "ice_time",
        "heat_press", "heat_flow", "pressure", "pressure1", "model",
        "mold_size", "mold_bar_key", "search_remark", "search_dt", "search_proc",
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
    # Primary Key: (WORK_DATE, HMS, SO_ID, SER_NO)
    conflict_columns = ["work_date", "hms", "so_id", "ser_no"]
    
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

