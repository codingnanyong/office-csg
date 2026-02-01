"""공통 함수 모듈 - MSBP Hard Check Raw"""
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
TABLE_NAME = "msbp_hard_check_raw"
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
# 3️⃣ Data Extraction
# ════════════════════════════════════════════════════════════════

def build_extract_sql(start_date: str, end_date: str) -> str:
    """Build SQL query for data extraction"""
    return f'''
        SELECT
            FACTORY, LAB_YMD, LAB_COMP_CD, MCS_NO, LAB_NO, CS_SIZE,
            MODEL_CD, CATEGORY, COLOR_CD, GEN, PATCH_NO, MC_LINE,
            EX_WIDTH, EX_LENGTH, HARD_2HOURS, HARD_24HOURS, SG_CHECK,
            WEIGHT, VOLUME, RESULT_YN, REMARKS, STATUS, UPD_USER,
            UPD_YMD, MOLD_CODE, SLAB_TEST_DATE, SLAB_TEST_TIME, INPUT_PROC
        FROM LMES.MSBP_HARD_CHECK
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
                # Oracle 원본 순서 (1-28)
                clean_string_value(row['FACTORY']), clean_string_value(row['LAB_YMD']), clean_string_value(row['LAB_COMP_CD']),  # 1-3
                clean_string_value(row['MCS_NO']), clean_string_value(row['LAB_NO']), clean_string_value(row['CS_SIZE']),  # 4-6
                clean_string_value(row['MODEL_CD']), clean_string_value(row['CATEGORY']), clean_string_value(row['COLOR_CD']),  # 7-9
                clean_string_value(row['GEN']), clean_string_value(row['PATCH_NO']), clean_string_value(row['MC_LINE']),  # 10-12
                clean_string_value(row['EX_WIDTH']), clean_string_value(row['EX_LENGTH']), clean_string_value(row['HARD_2HOURS']),  # 13-15
                clean_string_value(row['HARD_24HOURS']), clean_string_value(row['SG_CHECK']), clean_string_value(row['WEIGHT']),  # 16-18
                clean_string_value(row['VOLUME']), clean_string_value(row['RESULT_YN']), clean_string_value(row['REMARKS']),  # 19-21
                clean_string_value(row['STATUS']), clean_string_value(row['UPD_USER']), row['UPD_YMD'],  # 22-24
                clean_string_value(row['MOLD_CODE']), clean_string_value(row['SLAB_TEST_DATE']), clean_string_value(row['SLAB_TEST_TIME']),  # 25-27
                clean_string_value(row['INPUT_PROC']),  # 28
                # ETL metadata
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 - 컬럼 순서를 DDL과 정확히 매칭
        return [
            (
                # Oracle 원본 순서 (1-28)
                clean_string_value(row[0]), clean_string_value(row[1]), clean_string_value(row[2]),  # FACTORY, LAB_YMD, LAB_COMP_CD
                clean_string_value(row[3]), clean_string_value(row[4]), clean_string_value(row[5]),  # MCS_NO, LAB_NO, CS_SIZE
                clean_string_value(row[6]), clean_string_value(row[7]), clean_string_value(row[8]),  # MODEL_CD, CATEGORY, COLOR_CD
                clean_string_value(row[9]), clean_string_value(row[10]), clean_string_value(row[11]),  # GEN, PATCH_NO, MC_LINE
                clean_string_value(row[12]), clean_string_value(row[13]), clean_string_value(row[14]),  # EX_WIDTH, EX_LENGTH, HARD_2HOURS
                clean_string_value(row[15]), clean_string_value(row[16]), clean_string_value(row[17]),  # HARD_24HOURS, SG_CHECK, WEIGHT
                clean_string_value(row[18]), clean_string_value(row[19]), clean_string_value(row[20]),  # VOLUME, RESULT_YN, REMARKS
                clean_string_value(row[21]), clean_string_value(row[22]), row[23],  # STATUS, UPD_USER, UPD_YMD
                clean_string_value(row[24]), clean_string_value(row[25]), clean_string_value(row[26]),  # MOLD_CODE, SLAB_TEST_DATE, SLAB_TEST_TIME
                clean_string_value(row[27]),  # INPUT_PROC
                # ETL metadata
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table (Oracle 원본 순서)"""
    return [
        # Oracle 원본 순서 (1-28)
        "factory", "lab_ymd", "lab_comp_cd", "mcs_no", "lab_no", "cs_size",  # 1-6
        "model_cd", "category", "color_cd", "gen", "patch_no", "mc_line",  # 7-12
        "ex_width", "ex_length", "hard_2hours", "hard_24hours", "sg_check",  # 13-17
        "weight", "volume", "result_yn", "remarks", "status", "upd_user",  # 18-23
        "upd_ymd", "mold_code", "slab_test_date", "slab_test_time", "input_proc",  # 24-28
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
    conflict_columns = ["factory", "lab_ymd", "lab_comp_cd", "mcs_no", "lab_no", "color_cd"]
    
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")


# ════════════════════════════════════════════════════════════════
# 6️⃣ Single Date Processing
# ════════════════════════════════════════════════════════════════

def process_single_date(
    target_date: str,
    oracle_conn_id: str = ORACLE_CONN_ID,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    schema_name: str = SCHEMA_NAME,
    table_name: str = TABLE_NAME
) -> dict:
    """단일 날짜 데이터 처리 (추출 + 적재)"""
    oracle = OracleHelper(conn_id=oracle_conn_id)
    pg = PostgresHelper(conn_id=postgres_conn_id)
    
    start_date = datetime.strptime(target_date, '%Y-%m-%d').replace(tzinfo=INDO_TZ)
    end_date = start_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"📅 데이터 수집 시작: {start_str} ~ {end_str}")
    
    # 데이터 추출 및 적재
    data, extracted_count = extract_data(oracle, start_str, end_str)
    
    if extracted_count > 0:
        extract_time = datetime.utcnow()
        load_data(pg, data, extract_time, schema_name, table_name)
        logging.info(f"✅ 데이터 수집 완료: 추출 {extracted_count}건")
        
        return {
            "status": "success",
            "date": target_date,
            "extracted_count": extracted_count,
            "start_time": start_str,
            "end_time": end_str,
            "extract_time": extract_time.isoformat()
        }
    else:
        logging.info(f"⚠️ 수집할 데이터가 없습니다: {start_str} ~ {end_str}")
        
        return {
            "status": "success",
            "date": target_date,
            "extracted_count": 0,
            "start_time": start_str,
            "end_time": end_str,
            "message": "수집할 데이터가 없음"
        }

