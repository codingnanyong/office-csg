"""BAS Defective Raw Backfill DAG (Oracle → Bronze)"""
import logging
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from dags.pipeline.maintenance.bronze.common.bas_deffective_raw_common import (
    process_single_date,
    update_variable,
    get_month_end_date,
    calculate_expected_monthly_loops,
    parse_datetime,
    INDO_TZ,
    ORACLE_CONN_ID,
    POSTGRES_CONN_ID,
    SCHEMA_NAME,
    TABLE_NAME
)


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)
}

INCREMENT_KEY = "last_extract_time_bas_deffective_raw"
INITIAL_START_DATE = datetime(2020, 1, 1, 0, 0, 0)
DAYS_OFFSET_FOR_INCREMENTAL = 2


# ════════════════════════════════════════════════════════════════
# 2️⃣ Main Backfill Logic
# ════════════════════════════════════════════════════════════════

def backfill_monthly_batch_task(**kwargs) -> dict:
    """Main backfill task for monthly batch processing"""
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
        
        # Calculate month end date
        month_end = get_month_end_date(current_date)
        if month_end > end_date:
            month_end = end_date
        
        # Process batch (단일 날짜 처리 함수를 사용하여 월별 일수만큼 반복)
        # 월별 처리: 매일 처리하는 방식으로 변경
        temp_date = current_date
        month_results = []
        
        while temp_date <= month_end:
            date_str = temp_date.strftime('%Y-%m-%d')
            logging.info(f"🔄 루프 {loop_count}/{expected_loops} - 날짜: {date_str}")
            
            try:
                result = process_single_date(
                    date_str,
                    ORACLE_CONN_ID,
                    POSTGRES_CONN_ID,
                    SCHEMA_NAME,
                    TABLE_NAME
                )
                
                if result.get('status') == 'success':
                    month_results.append(result)
                    total_processed += result.get('rows_processed', 0)
                    # Variable 업데이트
                    update_variable(INCREMENT_KEY, result.get('end_time'))
                else:
                    logging.error(f"❌ [{date_str}] 처리 실패: {result.get('message', 'Unknown error')}")
                    raise Exception(f"Processing failed for {date_str}")
            
            except Exception as e:
                logging.error(f"❌ [{date_str}] 예외 발생: {e}")
                raise
            
            temp_date += timedelta(days=1)
        
        # 월별 요약
        month_row_count = sum(r.get('rows_processed', 0) for r in month_results)
        results.append({
            "loop": loop_count,
            "start": current_date.strftime("%Y-%m-%d"),
            "end": month_end.strftime("%Y-%m-%d"),
            "row_count": month_row_count,
            "batch_size_days": (month_end - current_date).days + 1,
            "month": current_date.strftime("%Y-%m")
        })
        
        logging.info(f"✅ 월별 배치 완료: {current_date.strftime('%Y-%m')} ({month_row_count} rows)")
        
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


# ════════════════════════════════════════════════════════════════
# 3️⃣ DAG Definition
# ════════════════════════════════════════════════════════════════

with DAG(
    dag_id="bas_deffective_raw_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "raw", "bronze layer", "backfill", "maintenance", "monthly"],
) as dag:
    
    backfill_monthly_batch = PythonOperator(
        task_id="backfill_monthly_batch_task",
        python_callable=backfill_monthly_batch_task,
    )
    
    backfill_monthly_batch
