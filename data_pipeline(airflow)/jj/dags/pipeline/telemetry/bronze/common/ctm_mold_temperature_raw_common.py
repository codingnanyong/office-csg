"""
CTM Mold Temperature Raw Common Functions
=========================================
공통 함수 및 설정을 모아둔 모듈
"""

import logging
from datetime import datetime, timedelta, timezone
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from plugins.hooks.mssql_hook import MSSQLHelper
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────

# Database Configuration
INCREMENT_KEY = "last_extract_time_ctm_mold_temperature_raw"
TABLE_NAME = "ctm_mold_temperature_raw"
SCHEMA_NAME = "bronze"

# Connection IDs
SOURCE_POSTGRES_CONN_ID = "ms_ctm_edge"
TARGET_POSTGRES_CONN_ID = "pg_jj_telemetry_dw"

# Date Configuration
INDO_TZ = timezone(timedelta(hours=7))
INITIAL_START_DATE = datetime(2020, 1, 1, 0, 0, 0)
DAYS_OFFSET_FOR_INCREMENTAL = 2


# ────────────────────────────────────────────────────────────────
# Utility Functions
# ────────────────────────────────────────────────────────────────
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


# ────────────────────────────────────────────────────────────────
# Data Extraction
# ────────────────────────────────────────────────────────────────
def build_extract_sql(start_date: str, end_date: str) -> str:
    """Build SQL query for CTM mold temperature data extraction"""
    # rst_ymd가 문자열 형태이므로 직접 비교
    return f'''
        SELECT 
            mc_cd,
            hot_1_1, hot_1_2, hot_1_3, hot_1_4, hot_1_5,
            hot_2_1, hot_2_2, hot_2_3, hot_2_4, hot_2_5,
            hot_3_1, hot_3_2, hot_3_3, hot_3_4, hot_3_5,
            hot_4_1, hot_4_2, hot_4_3, hot_4_4, hot_4_5,
            hot_5_1, hot_5_2, hot_5_3, hot_5_4, hot_5_5,
            cool_1, cool_2, cool_3, cool_4, cool_5,
            rst_ymd
        FROM dbo.mold_temperature
        WHERE rst_ymd >= '{start_date}' 
          AND rst_ymd <= '{end_date}'
        ORDER BY rst_ymd
    '''


def extract_data(mssql: MSSQLHelper, start_date: str, end_date: str) -> tuple:
    """Extract data from CTM SQL Server database"""
    sql = build_extract_sql(start_date, end_date)
    logging.info(f"실행 쿼리: {sql}")
    
    data = mssql.execute_query(sql, task_id="extract_data_task", xcom_key=None)
    
    # Calculate row count
    if data and isinstance(data, list):
        row_count = len(data)
    else:
        row_count = 0
    
    logging.info(f"{start_date} ~ {end_date} 추출 row 수: {row_count}")
    return data, row_count


# ────────────────────────────────────────────────────────────────
# Data Loading
# ────────────────────────────────────────────────────────────────
def prepare_insert_data(data: list, extract_time: datetime) -> list:
    """Prepare data for PostgreSQL insertion"""
    # 딕셔너리 형태인지 확인하고 처리
    if data and isinstance(data[0], dict):
        # 딕셔너리 형태인 경우 (PostgreSQL 결과)
        return [
            (
                row['mc_cd'],
                row['hot_1_1'], row['hot_1_2'], row['hot_1_3'], row['hot_1_4'], row['hot_1_5'],
                row['hot_2_1'], row['hot_2_2'], row['hot_2_3'], row['hot_2_4'], row['hot_2_5'],
                row['hot_3_1'], row['hot_3_2'], row['hot_3_3'], row['hot_3_4'], row['hot_3_5'],
                row['hot_4_1'], row['hot_4_2'], row['hot_4_3'], row['hot_4_4'], row['hot_4_5'],
                row['hot_5_1'], row['hot_5_2'], row['hot_5_3'], row['hot_5_4'], row['hot_5_5'],
                row['cool_1'], row['cool_2'], row['cool_3'], row['cool_4'], row['cool_5'],
                row['rst_ymd'],
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0],  # mc_cd
                row[1], row[2], row[3], row[4], row[5],  # hot_1_1~hot_1_5
                row[6], row[7], row[8], row[9], row[10],  # hot_2_1~hot_2_5
                row[11], row[12], row[13], row[14], row[15],  # hot_3_1~hot_3_5
                row[16], row[17], row[18], row[19], row[20],  # hot_4_1~hot_4_5
                row[21], row[22], row[23], row[24], row[25],  # hot_5_1~hot_5_5
                row[26], row[27], row[28], row[29], row[30],  # cool_1~cool_5
                row[31],  # rst_ymd
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "mc_cd",
        "hot_1_1", "hot_1_2", "hot_1_3", "hot_1_4", "hot_1_5",
        "hot_2_1", "hot_2_2", "hot_2_3", "hot_2_4", "hot_2_5",
        "hot_3_1", "hot_3_2", "hot_3_3", "hot_3_4", "hot_3_5",
        "hot_4_1", "hot_4_2", "hot_4_3", "hot_4_4", "hot_4_5",
        "hot_5_1", "hot_5_2", "hot_5_3", "hot_5_4", "hot_5_5",
        "cool_1", "cool_2", "cool_3", "cool_4", "cool_5",
        "rst_ymd", "etl_extract_time", "etl_ingest_time"
    ]


def load_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load data into PostgreSQL database"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_column_names()
    conflict_columns = ["mc_cd", "rst_ymd"]
    
    pg.insert_data(SCHEMA_NAME, TABLE_NAME, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ────────────────────────────────────────────────────────────────
# Variable Management
# ────────────────────────────────────────────────────────────────
def update_variable(end_extract_time: str) -> None:
    """Update Airflow variable with last extract time (always 23:59:59)"""
    # end_extract_time을 datetime으로 파싱하여 23:59:59로 설정
    parsed_time = parse_datetime(end_extract_time)
    formatted_time = parsed_time.replace(hour=23, minute=59, second=59, microsecond=999999).strftime("%Y-%m-%d %H:%M:%S")
    
    Variable.set(INCREMENT_KEY, formatted_time)
    logging.info(f"📌 Variable `{INCREMENT_KEY}` Update: {formatted_time}")


# ────────────────────────────────────────────────────────────────
# Incremental Logic
# ────────────────────────────────────────────────────────────────
def daily_incremental_collection_task(**kwargs) -> dict:
    """매일 최신 데이터만 수집하는 태스크"""
    # 매일 전일 데이터만 수집
    last_extract_time_str = Variable.get(INCREMENT_KEY, default_var=None)
    
    if last_extract_time_str:
        # 마지막 추출 시간을 파싱
        last_extract_time = parse_datetime(last_extract_time_str)
        
        # 마지막 추출 시간의 다음 날 00:00:00부터 23:59:59까지
        start_date = last_extract_time.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        end_date = start_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        logging.info(f"마지막 추출 시간: {last_extract_time_str}")
        logging.info(f"다음 날 데이터 수집: {start_date.strftime('%Y-%m-%d')}")
    else:
        # Variable이 없으면 어제 데이터 수집
        yesterday = datetime.now(INDO_TZ) - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        logging.info(f"Variable이 없어서 어제 데이터 수집: {yesterday.strftime('%Y-%m-%d')}")
    
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"📅 데이터 수집 시작: {start_str} ~ {end_str}")
    logging.info(f"📊 처리 날짜: {start_date.strftime('%Y-%m-%d')}")
    
    try:
        source_mssql = MSSQLHelper(conn_id=SOURCE_POSTGRES_CONN_ID)
        target_pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
        
        # 데이터 추출 및 적재
        data, row_count = extract_data(source_mssql, start_str, end_str)
        
        if row_count > 0:
            extract_time = datetime.utcnow()
            load_data(target_pg, data, extract_time)
            logging.info(f"✅ 데이터 수집 완료: {row_count} rows")
            
            # Variable 업데이트
            update_variable(end_str)
            
            return {
                "status": "daily_incremental_completed",
                "date": start_date.strftime("%Y-%m-%d"),
                "rows_processed": row_count,
                "start_time": start_str,
                "end_time": end_str,
                "extract_time": extract_time.isoformat()
            }
        else:
            logging.info(f"⚠️ 수집할 데이터가 없습니다: {start_str} ~ {end_str}")
            
            # Variable 업데이트 (데이터가 없어도 시간은 업데이트)
            update_variable(end_str)
            
            return {
                "status": "daily_incremental_completed_no_data",
                "date": start_date.strftime("%Y-%m-%d"),
                "rows_processed": 0,
                "start_time": start_str,
                "end_time": end_str,
                "message": "수집할 데이터가 없음"
            }
    except Exception as e:
        # 연결 실패 등 네트워크 오류인 경우 Skip 처리
        error_str = str(e)
        error_lower = error_str.lower()
        is_connection_error = (
            "connection" in error_lower or
            "timeout" in error_lower or
            "timed out" in error_lower or
            "connection reset" in error_lower or
            "reset by peer" in error_lower or
            "errno" in error_lower or
            "network" in error_lower or
            "could not connect" in error_lower or
            "unable to connect" in error_lower or
            "login failed" in error_lower or
            "server is not found" in error_lower
        )
        
        if is_connection_error:
            logging.warning(f"⚠️ 연결 실패: {error_str} - 태스크 Skip")
            # Skip 전에 Variable 업데이트 (연결 실패해도 시간은 업데이트하여 다음 실행 시 올바른 시점부터 재시도)
            try:
                update_variable(end_str)
                logging.info(f"✅ Variable '{INCREMENT_KEY}' 업데이트 (연결 실패로 Skip): {end_str}")
            except Exception as var_err:
                logging.warning(f"⚠️ Variable 업데이트 실패 (무시): {var_err}")
            
            skip_msg = (
                f"⏭️ CTM Mold Temperature ETL 중 연결 불가 - 태스크 Skip\n"
                f"원인: {error_str}\n"
                f"설명: 소스 또는 타겟 데이터베이스 연결이 불가능합니다.\n"
                f"      Variable은 업데이트되었으므로 다음 실행 시 재시도됩니다."
            )
            logging.warning(skip_msg)
            raise AirflowSkipException(skip_msg) from e
        
        # 그 외 오류는 그대로 raise
        logging.error(f"❌ CTM Mold Temperature ETL 실패: {e}")
        raise


# ────────────────────────────────────────────────────────────────
# Backfill Logic
# ────────────────────────────────────────────────────────────────
def process_monthly_batch(
    source_mssql: MSSQLHelper, 
    target_pg: PostgresHelper, 
    start_date: datetime, 
    end_date: datetime,
    loop_count: int,
    expected_loops: int
) -> dict:
    """Process a single monthly batch"""
    logging.info(f"🔄 루프 {loop_count}/{expected_loops} 시작")
    
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"배치 처리 중: {start_str} ~ {end_str}")
    
    data, row_count = extract_data(source_mssql, start_str, end_str)
    
    if row_count > 0:
        extract_time = datetime.utcnow()
        load_data(target_pg, data, extract_time)
        logging.info(f"✅ 배치 완료: {start_str} ~ {end_str} ({row_count} rows)")
    else:
        logging.info(f"배치에 데이터 없음: {start_str} ~ {end_str}")
    
    update_variable(end_str)
    
    return {
        "loop": loop_count,
        "start": start_str,
        "end": end_str,
        "row_count": row_count,
        "batch_size_days": (end_date - start_date).days,
        "month": start_date.strftime("%Y-%m")
    }


def backfill_monthly_batch_task(**kwargs) -> dict:
    """Main backfill task for monthly batch processing"""
    source_mssql = MSSQLHelper(conn_id=SOURCE_POSTGRES_CONN_ID)
    target_pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    # Get start date from variable or use initial date
    last_extract_time = Variable.get(INCREMENT_KEY, default_var=None)
    if not last_extract_time:
        start_date = INITIAL_START_DATE
        logging.info(f"초기 시작 날짜 사용: {start_date}")
    else:
        start_date = parse_datetime(last_extract_time)
        logging.info(f"이전 진행 지점 사용: {start_date}")
    
    # Set timezone and calculate end date
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=INDO_TZ)
    
    # Backfill도 incremental과 동일하게 현재 시간에서 2일 전까지만 처리
    end_date = datetime.now(INDO_TZ).replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)
    
    # Calculate expected loops
    expected_loops = calculate_expected_monthly_loops(start_date, end_date)
    
    # Log backfill information
    logging.info(f"Backfill 시작: {start_date} ~ {end_date}")
    logging.info(f"배치 크기: 월별 (각 월의 실제 일수에 맞춤)")
    logging.info(f"예상 루프 횟수: {expected_loops}회 (월별)")
    logging.info(f"⚠️ 현재 시간에서 {DAYS_OFFSET_FOR_INCREMENTAL}일 전으로 설정 (incremental DAG 시작점)")
    
    # Process monthly batches
    results = []
    total_processed = 0
    loop_count = 0
    current_date = start_date
    
    while current_date < end_date:
        loop_count += 1
        
        # 월 시작일을 00:00:00으로 설정
        month_start = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # Calculate month end date
        month_end = get_month_end_date(current_date)
        if month_end > end_date:
            month_end = end_date
        
        # Process batch
        batch_result = process_monthly_batch(
            source_mssql, target_pg, month_start, month_end, loop_count, expected_loops
        )
        
        results.append(batch_result)
        total_processed += batch_result["row_count"]
        
        # Move to next month
        current_date = month_end + timedelta(days=1)
    
    # Log completion
    logging.info(f"🎉 Backfill 완료! 총 {loop_count}회 루프, {total_processed}개 rows 수집")
    if results:
        logging.info(f"처리 기간: {results[0]['start']} ~ {results[-1]['end']}")
    
    return {
        "status": "backfill_completed",
        "total_loops": loop_count,
        "total_batches": len(results),
        "total_rows": total_processed,
        "results": results
    }

