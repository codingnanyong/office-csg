"""공통 함수 모듈 - MSBP Roll Lot Raw"""
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
TABLE_NAME = "msbp_roll_lot_raw"
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
            FACTORY, AREA_CD, OP_CD, SO_ID, SO_SEQ, MCS_CD, MCS_COLOR, MCS_OPTION,
            STYLE_CD, ROLL_MC, RECYCLE_LOT_ID, ROLL_OP_CD, PREV_OP_CD, NEXT_OP_CD,
            PRESS_OP_CD, STATUS, BAR_KEY, LOT_NO, LOT_NO_SEQ, LOT_TYPE, PARENT_LOT,
            CHILD_LOT, IO_DIV, LOCATE_CD, LOT_YMD, LOT_HMS, LOT_YMD_S, LOT_HMS_S,
            LOT_WEIGHT, OUT_YMD, OUT_HMS, OUT_WEIGHT, ST_ER, CHECK_ER, CHECK_CD,
            CHECK_RS, THICKNESS, COUNT1, COUNT2, COUNT3, TIME1, TIME2, TIME3,
            TEMP1, TEMP2, TEMP3, SPEED1, SPEED2, SPEED3, UPD_USER, UPD_YMD,
            DOWNLOAD_YN, UPLOAD_YN, PRINT_DT, INSERT_PROC, MANUAL_NIK, LOT_ID,
            MCS_COLOR1, OUT_HMS_E, OUT_YMD_E, PRODUCTDEFINITIONID, SCRAP_WEIGHT,
            THICKNESS_MAX, THICKNESS_MIN, THICKNESS_ACT, THICKNESS_ACT_MIN,
            THICKNESS_ACT_MAX, PRECURE_WEIGHT
        FROM LMES.MSBP_ROLL_LOT
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
                # Oracle 원본 순서 (1-68)
                clean_string_value(row['FACTORY']), clean_string_value(row['AREA_CD']), clean_string_value(row['OP_CD']), row['SO_ID'], row['SO_SEQ'],  # 1-5
                clean_string_value(row['MCS_CD']), clean_string_value(row['MCS_COLOR']), clean_string_value(row['MCS_OPTION']), clean_string_value(row['STYLE_CD']), clean_string_value(row['ROLL_MC']),  # 6-10
                clean_string_value(row['RECYCLE_LOT_ID']), clean_string_value(row['ROLL_OP_CD']), clean_string_value(row['PREV_OP_CD']), clean_string_value(row['NEXT_OP_CD']), clean_string_value(row['PRESS_OP_CD']),  # 11-15
                clean_string_value(row['STATUS']), clean_string_value(row['BAR_KEY']), clean_string_value(row['LOT_NO']), clean_string_value(row['LOT_NO_SEQ']), clean_string_value(row['LOT_TYPE']),  # 16-20
                clean_string_value(row['PARENT_LOT']), clean_string_value(row['CHILD_LOT']), clean_string_value(row['IO_DIV']), clean_string_value(row['LOCATE_CD']), clean_string_value(row['LOT_YMD']),  # 21-25
                clean_string_value(row['LOT_HMS']), clean_string_value(row['LOT_YMD_S']), clean_string_value(row['LOT_HMS_S']), row['LOT_WEIGHT'], clean_string_value(row['OUT_YMD']),  # 26-30
                clean_string_value(row['OUT_HMS']), row['OUT_WEIGHT'], row['ST_ER'], row['CHECK_ER'], clean_string_value(row['CHECK_CD']),  # 31-35
                clean_string_value(row['CHECK_RS']), clean_string_value(row['THICKNESS']), row['COUNT1'], row['COUNT2'], row['COUNT3'],  # 36-40
                row['TIME1'], row['TIME2'], row['TIME3'], row['TEMP1'], row['TEMP2'],  # 41-45
                row['TEMP3'], row['SPEED1'], row['SPEED2'], row['SPEED3'], clean_string_value(row['UPD_USER']),  # 46-50
                row['UPD_YMD'], clean_string_value(row['DOWNLOAD_YN']), clean_string_value(row['UPLOAD_YN']), row['PRINT_DT'], clean_string_value(row['INSERT_PROC']),  # 51-55
                clean_string_value(row['MANUAL_NIK']), clean_string_value(row['LOT_ID']), clean_string_value(row['MCS_COLOR1']), clean_string_value(row['OUT_HMS_E']), clean_string_value(row['OUT_YMD_E']),  # 56-60
                clean_string_value(row['PRODUCTDEFINITIONID']), row['SCRAP_WEIGHT'], row['THICKNESS_MAX'], row['THICKNESS_MIN'], clean_string_value(row['THICKNESS_ACT']),  # 61-65
                row['THICKNESS_ACT_MIN'], row['THICKNESS_ACT_MAX'], row['PRECURE_WEIGHT'],  # 66-68
                # ETL metadata
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 - 컬럼 순서를 DDL과 정확히 매칭
        return [
            (
                # Primary business columns (1-5)
                clean_string_value(row[0]), clean_string_value(row[1]), clean_string_value(row[2]), row[3], row[4],  # FACTORY, AREA_CD, OP_CD, SO_ID, SO_SEQ
                # MCS and style columns (6-10)
                clean_string_value(row[5]), clean_string_value(row[6]), clean_string_value(row[7]), clean_string_value(row[8]), clean_string_value(row[9]),  # MCS_CD, MCS_COLOR, MCS_OPTION, STYLE_CD, ROLL_MC
                # Operation columns (11-15)
                clean_string_value(row[10]), clean_string_value(row[11]), clean_string_value(row[12]), clean_string_value(row[13]), clean_string_value(row[14]),  # RECYCLE_LOT_ID, ROLL_OP_CD, PREV_OP_CD, NEXT_OP_CD, PRESS_OP_CD
                # Status and lot columns (16-20)
                clean_string_value(row[15]), clean_string_value(row[16]), clean_string_value(row[17]), clean_string_value(row[18]), clean_string_value(row[19]),  # STATUS, BAR_KEY, LOT_NO, LOT_NO_SEQ, LOT_TYPE
                # Lot relationship columns (21-25)
                clean_string_value(row[20]), clean_string_value(row[21]), clean_string_value(row[22]), clean_string_value(row[23]), clean_string_value(row[24]),  # PARENT_LOT, CHILD_LOT, IO_DIV, LOCATE_CD, LOT_YMD
                # Date and time columns (26-30)
                clean_string_value(row[25]), clean_string_value(row[26]), clean_string_value(row[27]), clean_string_value(row[28]), row[29],  # LOT_HMS, LOT_YMD_S, LOT_HMS_S, LOT_WEIGHT, OUT_YMD
                # Output columns (31-35)
                clean_string_value(row[30]), row[31], row[32], row[33], clean_string_value(row[34]),  # OUT_HMS, OUT_WEIGHT, ST_ER, CHECK_ER, CHECK_CD
                # Check and thickness columns (36-40)
                clean_string_value(row[35]), clean_string_value(row[36]), row[37], row[38], row[39],  # CHECK_RS, THICKNESS, COUNT1, COUNT2, COUNT3
                # Time columns (41-45)
                row[40], row[41], row[42], row[43], row[44],  # TIME1, TIME2, TIME3, TEMP1, TEMP2
                # Temperature and speed columns (46-50)
                row[45], row[46], row[47], row[48], clean_string_value(row[49]),  # TEMP3, SPEED1, SPEED2, SPEED3, UPD_USER
                # Update and control columns (51-55)
                row[50], clean_string_value(row[51]), clean_string_value(row[52]), row[53], clean_string_value(row[54]),  # UPD_YMD, DOWNLOAD_YN, UPLOAD_YN, PRINT_DT, INSERT_PROC
                # Additional columns (56-60)
                clean_string_value(row[55]), clean_string_value(row[56]), clean_string_value(row[57]), clean_string_value(row[58]), clean_string_value(row[59]),  # MANUAL_NIK, LOT_ID, MCS_COLOR1, OUT_HMS_E, OUT_YMD_E
                # Product and measurement columns (61-65)
                clean_string_value(row[60]), row[61], row[62], row[63], clean_string_value(row[64]),  # PRODUCTDEFINITIONID, SCRAP_WEIGHT, THICKNESS_MAX, THICKNESS_MIN, THICKNESS_ACT
                # Final measurement columns (66-68)
                row[65], row[66], row[67],  # THICKNESS_ACT_MIN, THICKNESS_ACT_MAX, PRECURE_WEIGHT
                # ETL metadata
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table (Oracle 원본 순서)"""
    return [
        # Oracle 원본 순서 (1-68)
        "factory", "area_cd", "op_cd", "so_id", "so_seq",                    # 1-5
        "mcs_cd", "mcs_color", "mcs_option", "style_cd", "roll_mc",         # 6-10
        "recycle_lot_id", "roll_op_cd", "prev_op_cd", "next_op_cd", "press_op_cd",  # 11-15
        "status", "bar_key", "lot_no", "lot_no_seq", "lot_type",             # 16-20
        "parent_lot", "child_lot", "io_div", "locate_cd", "lot_ymd",        # 21-25
        "lot_hms", "lot_ymd_s", "lot_hms_s", "lot_weight", "out_ymd",       # 26-30
        "out_hms", "out_weight", "st_er", "check_er", "check_cd",           # 31-35
        "check_rs", "thickness", "count1", "count2", "count3",               # 36-40
        "time1", "time2", "time3", "temp1", "temp2",                        # 41-45
        "temp3", "speed1", "speed2", "speed3", "upd_user",                  # 46-50
        "upd_ymd", "download_yn", "upload_yn", "print_dt", "insert_proc",   # 51-55
        "manual_nik", "lot_id", "mcs_color1", "out_hms_e", "out_ymd_e",     # 56-60
        "productdefinitionid", "scrap_weight", "thickness_max", "thickness_min", "thickness_act",  # 61-65
        "thickness_act_min", "thickness_act_max", "precure_weight",         # 66-68
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
    
    # 배치 내 중복 제거 (so_id, so_seq, roll_op_cd 기준)
    unique_data = []
    seen_keys = set()
    for row in insert_data:
        key = (row[3], row[4], row[11])  # so_id, so_seq, roll_op_cd
        if key not in seen_keys:
            seen_keys.add(key)
            unique_data.append(row)
    
    logging.info(f"📊 Original: {len(insert_data)} rows, After deduplication: {len(unique_data)} rows")
    
    columns = get_column_names()
    conflict_columns = ["so_id", "so_seq", "roll_op_cd"]
    pg.insert_data(schema_name, table_name, unique_data, columns, conflict_columns)
    logging.info(f"✅ {len(unique_data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

