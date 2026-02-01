"""공통 함수 모듈 - BAS Defective Raw"""
import logging
from datetime import datetime, timedelta, timezone
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
ORACLE_CONN_ID = "orc_jj_cmms"
POSTGRES_CONN_ID = "pg_jj_maintenance_dw"
SCHEMA_NAME = "bronze"
TABLE_NAME = "bas_deffective_raw"
INDO_TZ = timezone(timedelta(hours=7))


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
            COMPANY_CD, DEFE_CD, DEFE_NM_EN, DEFE_NM_VI, DEFE_TYPE,
            HIGH1_CD, HIGH2_CD, HIGH3_CD, HIGH4_CD, USE_YN,
            SORT_NO, REMARK, REG_USER, REG_IP, REG_DATE,
            UPD_USER, UPD_IP, UPD_DATE, WERKS
        FROM ICMMS.BAS_DEFECTIVE
        WHERE UPD_DATE BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')
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
                row['COMPANY_CD'], row['DEFE_CD'], row['DEFE_NM_EN'], row['DEFE_NM_VI'], row['DEFE_TYPE'],
                row['HIGH1_CD'], row['HIGH2_CD'], row['HIGH3_CD'], row['HIGH4_CD'], row['USE_YN'],
                row['SORT_NO'], row['REMARK'], row['REG_USER'], row['REG_IP'], row['REG_DATE'],
                row['UPD_USER'], row['UPD_IP'], row['UPD_DATE'], row['WERKS'],
                extract_time
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0], row[1], row[2], row[3], row[4],  # COMPANY_CD, DEFE_CD, DEFE_NM_EN, DEFE_NM_VI, DEFE_TYPE
                row[5], row[6], row[7], row[8], row[9],  # HIGH1_CD, HIGH2_CD, HIGH3_CD, HIGH4_CD, USE_YN
                row[10], row[11], row[12], row[13], row[14],  # SORT_NO, REMARK, REG_USER, REG_IP, REG_DATE
                row[15], row[16], row[17], row[18],  # UPD_USER, UPD_IP, UPD_DATE, WERKS
                extract_time
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "company_cd", "defe_cd", "defe_nm_en", "defe_nm_vi", "defe_type",
        "high1_cd", "high2_cd", "high3_cd", "high4_cd", "use_yn",
        "sort_no", "remark", "reg_user", "reg_ip", "reg_date",
        "upd_user", "upd_ip", "upd_date", "werks",
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
    conflict_columns = ["company_cd", "defe_cd", "defe_nm_en", "defe_nm_vi", "defe_type"]
    
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(variable_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(variable_key, end_extract_time)
    logging.info(f"📌 Variable `{variable_key}` Update: {end_extract_time}")


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
    data, row_count = extract_data(oracle, start_str, end_str)
    
    if row_count > 0:
        extract_time = datetime.utcnow()
        load_data(pg, data, extract_time, schema_name, table_name)
        logging.info(f"✅ 데이터 수집 완료: {row_count} rows")
        
        return {
            "status": "success",
            "date": target_date,
            "rows_processed": row_count,
            "start_time": start_str,
            "end_time": end_str,
            "extract_time": extract_time.isoformat()
        }
    else:
        logging.info(f"⚠️ 수집할 데이터가 없습니다: {start_str} ~ {end_str}")
        
        return {
            "status": "success",
            "date": target_date,
            "rows_processed": 0,
            "start_time": start_str,
            "end_time": end_str,
            "message": "수집할 데이터가 없음"
        }

