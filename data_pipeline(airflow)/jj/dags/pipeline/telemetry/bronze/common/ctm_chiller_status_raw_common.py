"""
CTM Chiller Status Raw Common Functions
=======================================
공통 함수 및 설정을 모아둔 모듈
"""

import logging
from datetime import datetime, timedelta, timezone
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────

# Database Configuration
INCREMENT_KEY = "last_extract_time_ctm_chiller_status_raw"
TABLE_NAME = "ctm_chiller_status_raw"
SCHEMA_NAME = "bronze"

# Connection IDs
SOURCE_POSTGRES_CONN_ID = "pg_ckp_chiller"
TARGET_POSTGRES_CONN_ID = "pg_jj_telemetry_dw"

# Date Configuration
INDO_TZ = timezone(timedelta(hours=7))
DAYS_OFFSET_FOR_INCREMENTAL = 2
INITIAL_START_DATE = datetime(2025, 8, 1, 0, 0, 0)


# ────────────────────────────────────────────────────────────────
# Utility Functions
# ────────────────────────────────────────────────────────────────
def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string with microsecond support"""
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


def get_week_end_date(start_date: datetime) -> datetime:
    """Get the end of the week (7 days later) for a given date"""
    week_end = start_date + timedelta(days=6)
    # 23:59:59로 설정
    return week_end.replace(hour=23, minute=59, second=59, microsecond=999999)


def calculate_expected_weekly_loops(start_date: datetime, end_date: datetime) -> int:
    """Calculate expected number of weekly loops"""
    current_date = start_date
    week_count = 0
    
    while current_date < end_date:
        week_end = get_week_end_date(current_date)
        if week_end > end_date:
            week_end = end_date
        current_date = week_end + timedelta(days=1)
        week_count += 1
    
    return week_count


# ────────────────────────────────────────────────────────────────
# Data Extraction
# ────────────────────────────────────────────────────────────────
def build_extract_sql(start_date: str, end_date: str) -> str:
    """Build SQL query for CTM chiller status data extraction"""
    return f'''
        SELECT 
            device_id,
            water_in_temp,
            water_out_temp,
            external_temp,
            discharge_temp_1,
            discharge_temp_2,
            discharge_temp_3,
            discharge_temp_4,
            sv_temp,
            digitals,
            upd_dt
        FROM public.status
        WHERE upd_dt >= '{start_date}' 
          AND upd_dt <= '{end_date}'
        ORDER BY upd_dt
    '''


def extract_data(pg: PostgresHelper, start_date: str, end_date: str) -> tuple:
    """Extract data from CTM PostgreSQL database"""
    sql = build_extract_sql(start_date, end_date)
    logging.info(f"실행 쿼리: {sql}")
    
    data = pg.execute_query(sql, task_id="extract_data_task", xcom_key=None)
    
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
                row['device_id'],
                row['water_in_temp'],
                row['water_out_temp'],
                row['external_temp'],
                row['discharge_temp_1'],
                row['discharge_temp_2'],
                row['discharge_temp_3'],
                row['discharge_temp_4'],
                row['sv_temp'],
                row['digitals'],
                row['upd_dt'],
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0],   # device_id
                row[1],   # water_in_temp
                row[2],   # water_out_temp
                row[3],   # external_temp
                row[4],   # discharge_temp_1
                row[5],   # discharge_temp_2
                row[6],   # discharge_temp_3
                row[7],   # discharge_temp_4
                row[8],   # sv_temp
                row[9],   # digitals
                row[10],  # upd_dt
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "device_id",
        "water_in_temp",
        "water_out_temp",
        "external_temp",
        "discharge_temp_1",
        "discharge_temp_2",
        "discharge_temp_3",
        "discharge_temp_4",
        "sv_temp",
        "digitals",
        "upd_dt",
        "etl_extract_time",
        "etl_ingest_time"
    ]


def load_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_column_names()
    conflict_columns = ["device_id", "upd_dt"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, TABLE_NAME, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows upserted (device_id, upd_dt 기준).")


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
    # 마지막 추출 시간을 기준으로 다음 날 데이터 수집
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
        source_pg = PostgresHelper(conn_id=SOURCE_POSTGRES_CONN_ID)
        target_pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
        
        # 데이터 추출 및 적재
        data, row_count = extract_data(source_pg, start_str, end_str)
        
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
            "unable to connect" in error_lower
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
                f"⏭️ CTM Chiller Status ETL 중 연결 불가 - 태스크 Skip\n"
                f"원인: {error_str}\n"
                f"설명: 소스 또는 타겟 데이터베이스 연결이 불가능합니다.\n"
                f"      Variable은 업데이트되었으므로 다음 실행 시 재시도됩니다."
            )
            logging.warning(skip_msg)
            raise AirflowSkipException(skip_msg) from e
        
        # 그 외 오류는 그대로 raise
        logging.error(f"❌ CTM Chiller Status ETL 실패: {e}")
        raise


# ────────────────────────────────────────────────────────────────
# Backfill Logic
# ────────────────────────────────────────────────────────────────
def process_weekly_batch(
    source_pg: PostgresHelper, 
    target_pg: PostgresHelper, 
    start_date: datetime, 
    end_date: datetime,
    loop_count: int,
    expected_loops: int
) -> dict:
    """Process a single weekly batch"""
    logging.info(f"🔄 주별 루프 {loop_count}/{expected_loops} 시작")
    
    week_start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    week_end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"주별 배치 처리 중: {week_start_str} ~ {week_end_str}")
    
    # 주별 데이터 처리
    data, row_count = extract_data(source_pg, week_start_str, week_end_str)
    
    if row_count > 0:
        extract_time = datetime.utcnow()
        load_data(target_pg, data, extract_time)
        logging.info(f"✅ 주별 배치 완료: {week_start_str} ~ {week_end_str} ({row_count} rows)")
    else:
        logging.info(f"주별 배치에 데이터 없음: {week_start_str} ~ {week_end_str}")
    
    # 주별 배치 완료 후 Variable 업데이트
    update_variable(week_end_str)
    
    return {
        "loop": loop_count,
        "start": week_start_str,
        "end": week_end_str,
        "row_count": row_count,
        "batch_size_days": (end_date - start_date).days,
        "week": start_date.strftime("%Y-W%U")
    }


def backfill_weekly_batch_task(**kwargs) -> dict:
    """Main backfill task for weekly batch processing"""
    source_pg = PostgresHelper(conn_id=SOURCE_POSTGRES_CONN_ID)
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
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # Calculate expected loops
    expected_loops = calculate_expected_weekly_loops(start_date, end_date)
    
    # Log backfill information
    logging.info(f"Backfill 시작: {start_date} ~ {end_date}")
    logging.info(f"배치 크기: 주별 (7일 단위)")
    logging.info(f"예상 루프 횟수: {expected_loops}회 (주별)")
    logging.info(f"⚠️ 현재 시간에서 {DAYS_OFFSET_FOR_INCREMENTAL}일 전으로 설정 (incremental DAG 시작점)")
    
    # Process weekly batches
    results = []
    total_processed = 0
    loop_count = 0
    current_date = start_date
    
    while current_date < end_date:
        loop_count += 1
        
        # 주 시작일을 00:00:00으로 설정
        week_start = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Calculate week end date
        week_end = get_week_end_date(current_date)
        if week_end > end_date:
            week_end = end_date
        
        # Process batch
        batch_result = process_weekly_batch(
            source_pg, target_pg, week_start, week_end, loop_count, expected_loops
        )
        
        results.append(batch_result)
        total_processed += batch_result["row_count"]
        
        # Move to next week
        current_date = week_end + timedelta(days=1)
    
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

