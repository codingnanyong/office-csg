"""WOF Order Raw Backfill DAG (Oracle → Bronze)"""
import logging
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from dags.pipeline.maintenance.bronze.common.wof_order_raw_common import (
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

INCREMENT_KEY = "last_extract_time_wof_order_raw"
INITIAL_START_DATE = datetime(2015, 1, 1, 0, 0, 0)
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
    
    # Set timezone and normalize start date to 00:00:00
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=INDO_TZ)
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Calculate end date (2 days ago, 23:59:59)
    end_date = (datetime.now(INDO_TZ) - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)).replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    
    # Calculate expected loops
    expected_loops = calculate_expected_monthly_loops(start_date, end_date)
    
    # Log backfill information
    logging.info(f"Backfill 시작: {start_date} ~ {end_date}")
    logging.info(f"배치 크기: 월별 (각 월의 실제 일수에 맞춤)")
    logging.info(f"예상 루프 횟수: {expected_loops}회 (월별)")
    logging.info(f"⚠️ 현재 시간에서 {DAYS_OFFSET_FOR_INCREMENTAL}일 전으로 설정 (incremental DAG 시작점)")
    
    # Process monthly batches
    results = []
    total_extracted = 0
    total_loaded = 0
    loop_count = 0
    current_date = start_date
    
    while current_date < end_date:
        loop_count += 1
        
        # Calculate month end date
        month_end = get_month_end_date(current_date)
        if month_end > end_date:
            month_end = end_date
        
        logging.info(f"🔄 루프 {loop_count}/{expected_loops} 시작: {current_date.strftime('%Y-%m-%d')} ~ {month_end.strftime('%Y-%m-%d')}")
        
        # Process batch (단일 날짜 처리 함수를 사용하여 월별 일수만큼 반복)
        temp_date = current_date
        month_results = []
        
        while temp_date <= month_end:
            date_str = temp_date.strftime('%Y-%m-%d')
            
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
                    total_extracted += result.get('extracted_count', 0)
                    total_loaded += result.get('loaded_count', 0)
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
        month_extracted = sum(r.get('extracted_count', 0) for r in month_results)
        month_loaded = sum(r.get('loaded_count', 0) for r in month_results)
        results.append({
            "loop": loop_count,
            "start": current_date.strftime("%Y-%m-%d"),
            "end": month_end.strftime("%Y-%m-%d"),
            "extracted_count": month_extracted,
            "loaded_count": month_loaded,
            "batch_size_days": (month_end - current_date).days + 1,
            "month": current_date.strftime("%Y-%m")
        })
        
        logging.info(f"✅ 월별 배치 완료: {current_date.strftime('%Y-%m')} (추출 {month_extracted}건, 적재 {month_loaded}건)")
        
        # Move to next month (next day 00:00:00)
        current_date = (month_end + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Final summary
    logging.info(f"✅ Backfill 완료: 총 {loop_count}개 월 처리, 추출 {total_extracted}건, 적재 {total_loaded}건")
    logging.info(f"📊 월별 처리 결과: {[r['month'] + ':' + str(r['loaded_count']) + '건' for r in results]}")
    
    return {
        "status": "backfill_completed",
        "total_loops": loop_count,
        "expected_loops": expected_loops,
        "total_extracted": total_extracted,
        "total_loaded": total_loaded,
        "monthly_results": results,
        "final_end_date": end_date.strftime("%Y-%m-%d %H:%M:%S")
    }


# ════════════════════════════════════════════════════════════════
# 3️⃣ DAG Definition
# ════════════════════════════════════════════════════════════════

with DAG(
    dag_id='wof_order_raw_backfill',
    default_args=DEFAULT_ARGS,
    description='Work Order Raw Data Backfill (Monthly Batch)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['JJ', 'maintenance', 'bronze layer', 'backfill', 'work_order'],
) as dag:
    
    backfill_task = PythonOperator(
        task_id='backfill_monthly_batch',
        python_callable=backfill_monthly_batch_task,
    )
    
    backfill_task
