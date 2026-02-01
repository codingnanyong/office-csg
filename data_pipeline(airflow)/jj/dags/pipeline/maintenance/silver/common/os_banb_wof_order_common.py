"""공통 함수 모듈 - OS BANB WOF Order Silver"""
import logging
from datetime import datetime, timedelta, timezone
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
POSTGRES_CONN_ID = "pg_jj_maintenance_dw"
SOURCE_SCHEMA = "bronze"
SOURCE_TABLE = "wof_order_raw"
TARGET_SCHEMA = "silver"
TARGET_TABLE = "os_banb_wof_order"
INDO_TZ = timezone(timedelta(hours=7))

# Filter Conditions
TARGET_MACH_IDS = ['3110COP00009', '3110COP00001', '3110COP00015']


# ════════════════════════════════════════════════════════════════
# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════

def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string with microsecond support"""
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


# ════════════════════════════════════════════════════════════════
# 3️⃣ Data Extraction (Bronze → Transform)
# ════════════════════════════════════════════════════════════════

def build_extract_sql(start_date: str = None, end_date: str = None) -> str:
    """Build SQL query for extracting and transforming data from Bronze
    - Bronze의 upd_date 기준 증분 수집 (수정된 데이터)
    - upd_date IS NULL 항상 수집 (한 번도 수정 안 된 데이터)
    - 특정 설비(MACH_ID) 필터링
    
    Args:
        start_date: 시작 날짜 (incremental용, None이면 전체 조회)
        end_date: 종료 날짜 (incremental용, None이면 전체 조회)
    """
    mach_id_list = "', '".join(TARGET_MACH_IDS)
    
    base_sql = f'''
        SELECT 
            wo_date,
            wo_yymm,
            wo_orgn,
            wo_no,
            mach_id,
            defe_date,
            defe_cd,
            defe_content,
            defe_cd1,
            defe_content1,
            defe_cd2,
            defe_content2,
            defe_cd3,
            defe_content3,
            defe_cd4,
            defe_content4,
            solu_date,
            solu_cd,
            solu_content,
            etl_extract_time
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE mach_id IN ('{mach_id_list}')
    '''
    
    if start_date and end_date:
        # Incremental: date range filter
        base_sql += f'''
          AND (
              upd_date BETWEEN '{start_date}'::timestamp AND '{end_date}'::timestamp
              OR upd_date IS NULL
          )
        '''
    
    base_sql += '''
        ORDER BY wo_date, wo_yymm, wo_orgn, wo_no
    '''
    
    return base_sql


def extract_and_transform_data(
    postgres: PostgresHelper, 
    start_date: str = None, 
    end_date: str = None
) -> tuple:
    """Extract data from Bronze and transform"""
    sql = build_extract_sql(start_date, end_date)
    
    if start_date and end_date:
        logging.info("🔍 Bronze 데이터 추출 및 변환 시작 (Incremental)")
        logging.info(f"기간: {start_date} ~ {end_date}")
    else:
        logging.info("🔍 Bronze 전체 데이터 추출 및 변환 시작 (Backfill)")
    
    logging.info(f"대상 설비: {TARGET_MACH_IDS}")
    logging.info(f"실행 쿼리:\n{sql}")
    
    try:
        with postgres.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
            row_count = len(data) if data else 0
            
            logging.info(f"✅ 추출 완료: {row_count:,} rows")
            return data, row_count
            
    except Exception as e:
        logging.error(f"❌ 데이터 추출 실패: {str(e)}")
        raise


# ════════════════════════════════════════════════════════════════
# 4️⃣ Data Loading (Silver)
# ════════════════════════════════════════════════════════════════

def prepare_insert_data(data: list, ingest_time: datetime) -> list:
    """Prepare data for Silver layer insertion"""
    if not data:
        return []
    
    # Add etl_ingest_time to each row
    return [
        (*row, ingest_time)
        for row in data
    ]


def get_column_names() -> list:
    """Get column names for Silver table"""
    return [
        'wo_date', 'wo_yymm', 'wo_orgn', 'wo_no', 'mach_id',
        'defe_date', 'defe_cd', 'defe_content',
        'defe_cd1', 'defe_content1', 'defe_cd2', 'defe_content2',
        'defe_cd3', 'defe_content3', 'defe_cd4', 'defe_content4',
        'solu_date', 'solu_cd', 'solu_content',
        'etl_extract_time', 'etl_ingest_time'
    ]


def load_data_to_silver(
    postgres: PostgresHelper, 
    data: list,
    schema_name: str = TARGET_SCHEMA,
    table_name: str = TARGET_TABLE
) -> int:
    """Load data to Silver layer using PostgresHelper"""
    if not data:
        logging.warning("⚠️ 적재할 데이터가 없습니다.")
        return 0
    
    columns = get_column_names()
    conflict_columns = ['wo_yymm', 'wo_orgn', 'wo_no']
    
    logging.info(f"📦 Silver 레이어 적재 시작: {len(data):,} rows")
    
    try:
        postgres.insert_data(
            schema_name=schema_name,
            table_name=table_name,
            data=data,
            columns=columns,
            conflict_columns=conflict_columns,
            chunk_size=1000
        )
        
        loaded_count = len(data)
        logging.info(f"✅ Silver 적재 완료: {loaded_count:,} rows")
        return loaded_count
        
    except Exception as e:
        logging.error(f"❌ Silver 적재 실패: {str(e)}")
        raise


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(variable_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(variable_key, end_extract_time)
    logging.info(f"📌 Variable `{variable_key}` Update: {end_extract_time}")

