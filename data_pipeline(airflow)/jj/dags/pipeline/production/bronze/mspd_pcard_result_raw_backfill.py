"""
MSPD PCARD Result Raw Backfill DAG
===================================
Oracle LMES.MSPD_PCARD_RESULT 테이블의 과거 데이터를 월별 배치로 수집하는 DAG

Source: Oracle LMES.MSPD_PCARD_RESULT
Target: PostgreSQL bronze.mspd_pcard_result_raw
Execution: Manual trigger only (@once)
"""

import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.production.bronze.common.mspd_pcard_result_raw_common import (
    parse_datetime,
    get_month_end_date,
    calculate_expected_monthly_loops,
    extract_data,
    load_data,
    update_variable,
    _normalize_to_0630,
    INDO_TZ,
    INITIAL_START_DATE,
    DAYS_OFFSET_FOR_INCREMENTAL,
    ORACLE_CONN_ID,
    POSTGRES_CONN_ID
)

# ────────────────────────────────────────────────────────────────
# 1️⃣ Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)
}

# Database Configuration
INCREMENT_KEY = "last_extract_time_mspd_pcard_result_raw"

# ────────────────────────────────────────────────────────────────
# 2️⃣ Main Backfill Logic
# ────────────────────────────────────────────────────────────────
def process_monthly_batch(
    oracle: OracleHelper, 
    pg: PostgresHelper, 
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
    
    data, row_count = extract_data(oracle, start_str, end_str)
    
    if row_count > 0:
        extract_time = datetime.utcnow()
        load_data(pg, data, extract_time)
        logging.info(f"✅ 배치 완료: {start_str} ~ {end_str} ({row_count} rows)")
    else:
        logging.info(f"배치에 데이터 없음: {start_str} ~ {end_str}")
    
    update_variable(INCREMENT_KEY, end_str)
    
    return {
        "loop": loop_count,
        "start": start_str,
        "end": end_str,
        "row_count": row_count,
        "batch_size_days": (end_date - start_date).days,
        "month": start_date.strftime("%Y-%m")
    }

def backfill_monthly_batch_task(**kwargs) -> dict:
    """Main backfill task for monthly batch processing (06:30 기준)"""
    oracle = OracleHelper(conn_id=ORACLE_CONN_ID)
    pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
    
    # Get start date from variable or use initial date
    last_extract_time = Variable.get(INCREMENT_KEY, default_var=None)
    if not last_extract_time:
        start_date = _normalize_to_0630(INITIAL_START_DATE)
        logging.info(f"초기 시작 날짜 사용 (06:30 기준): {start_date}")
    else:
        start_date = parse_datetime(last_extract_time)
        start_date = _normalize_to_0630(start_date)
        logging.info(f"이전 진행 지점 사용 (06:30 기준): {start_date}")
    
    # Set timezone
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=INDO_TZ)
    
    # End date: 현재 시간에서 DAYS_OFFSET_FOR_INCREMENTAL일 전의 06:30
    end_date = _normalize_to_0630(
        datetime.now(INDO_TZ) - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)
    )
    
    # Calculate expected loops
    expected_loops = calculate_expected_monthly_loops(start_date, end_date)
    
    # Log backfill information
    logging.info(f"Backfill 시작 (06:30 기준): {start_date} ~ {end_date}")
    logging.info(f"배치 크기: 월별 (각 월의 실제 일수에 맞춤, 06:30 기준)")
    logging.info(f"예상 루프 횟수: {expected_loops}회 (월별)")
    logging.info(f"⚠️ 현재 시간에서 {DAYS_OFFSET_FOR_INCREMENTAL}일 전 06:30으로 설정 (incremental DAG 시작점)")
    
    # Process monthly batches
    results = []
    total_processed = 0
    loop_count = 0
    current_date = start_date
    
    while current_date < end_date:
        loop_count += 1
        
        # Calculate month end date (다음달 1일 06:30)
        month_end = get_month_end_date(current_date)
        if month_end > end_date:
            month_end = end_date
        
        # Ensure start date is 06:30 for the month
        batch_start_date = _normalize_to_0630(current_date)
        
        # Process batch
        batch_result = process_monthly_batch(
            oracle, pg, batch_start_date, month_end, loop_count, expected_loops
        )
        
        results.append(batch_result)
        total_processed += batch_result["row_count"]
        
        # Move to next month (month_end는 이미 다음달 1일 06:30이므로 그대로 사용)
        current_date = month_end
    
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

# ────────────────────────────────────────────────────────────────
# 3️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="mspd_pcard_result_raw_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "raw", "bronze layer", "backfill", "production", "monthly"]
) as dag:
    
    backfill_monthly_batch = PythonOperator(
        task_id="backfill_monthly_batch_task",
        python_callable=backfill_monthly_batch_task,
        provide_context=True,
    )
    
    backfill_monthly_batch

