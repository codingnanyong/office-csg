import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.production.silver.common.os_msbp_roll_plan_common import (
    parse_datetime,
    get_month_end_date,
    calculate_expected_monthly_loops,
    extract_silver_data,
    load_silver_data,
    update_variable,
    INDO_TZ,
    INITIAL_START_DATE,
    DAYS_OFFSET_FOR_INCREMENTAL,
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
INCREMENT_KEY = "last_extract_time_silver_os_msbp_roll_plan"

# ────────────────────────────────────────────────────────────────
# 2️⃣ Main Backfill Logic
# ────────────────────────────────────────────────────────────────
def process_silver_monthly_batch(
    pg: PostgresHelper, 
    start_date: datetime, 
    end_date: datetime,
    loop_count: int,
    expected_loops: int
) -> dict:
    """Process a single monthly batch for silver layer"""
    logging.info(f"🔄 Silver 루프 {loop_count}/{expected_loops} 시작")
    
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"Silver 배치 처리 중: {start_str} ~ {end_str}")
    
    data, row_count = extract_silver_data(pg, start_str, end_str)
    
    if row_count > 0:
        extract_time = datetime.utcnow()
        load_silver_data(pg, data, extract_time)
        logging.info(f"✅ Silver 배치 완료: {start_str} ~ {end_str} ({row_count} rows)")
    else:
        logging.info(f"Silver 배치에 데이터 없음: {start_str} ~ {end_str}")
    
    update_variable(INCREMENT_KEY, end_str)
    
    return {
        "loop": loop_count,
        "start": start_str,
        "end": end_str,
        "row_count": row_count,
        "batch_size_days": (end_date - start_date).days,
        "month": start_date.strftime("%Y-%m")
    }

def backfill_silver_monthly_batch_task(**kwargs) -> dict:
    """Main backfill task for silver layer monthly batch processing"""
    pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
    
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
    
    # Ensure start date is 00:00:00
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    end_date = datetime.now(INDO_TZ).replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)
    
    # Calculate expected loops
    expected_loops = calculate_expected_monthly_loops(start_date, end_date)
    
    # Log backfill information
    logging.info(f"Silver Backfill 시작: {start_date} ~ {end_date}")
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
        
        # Calculate month end date
        month_end = get_month_end_date(current_date)
        if month_end > end_date:
            month_end = end_date
        
        # Ensure start date is 00:00:00 for the month
        batch_start_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Process batch
        batch_result = process_silver_monthly_batch(
            pg, batch_start_date, month_end, loop_count, expected_loops
        )
        
        results.append(batch_result)
        total_processed += batch_result["row_count"]
        
        # Move to next month (start of next month)
        if month_end.month == 12:
            current_date = month_end.replace(year=month_end.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            current_date = month_end.replace(month=month_end.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Log completion
    logging.info(f"🎉 Silver Backfill 완료! 총 {loop_count}회 루프, {total_processed}개 rows 수집")
    if results:
        logging.info(f"처리 기간: {results[0]['start']} ~ {results[-1]['end']}")
    
    return {
        "status": "silver_backfill_completed",
        "total_loops": loop_count,
        "total_batches": len(results),
        "total_rows": total_processed,
        "results": results
    }

# ────────────────────────────────────────────────────────────────
# 3️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="os_msbp_roll_plan_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ","silver", "production", "backfill", "monthly", "OS"]
) as dag:
    
    backfill_silver_monthly_batch = PythonOperator(
        task_id="backfill_silver_monthly_batch_task",
        python_callable=backfill_silver_monthly_batch_task,
        provide_context=True,
    )
    
    backfill_silver_monthly_batch
