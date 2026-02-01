"""IPI Temperature Matching Model Backfill DAG (Silver → Gold)
양품 및 불량 데이터와 온도 데이터를 매칭하여 Gold 레이어에 적재하는 Backfill DAG
Model: Temperature_matching_model
"""
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from dags.pipeline.quality.gold.common.ipi_temperature_matching_common import process_single_date


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=3),
}

BACKFILL_VARIABLE_KEY = "ipi_temperature_matching_last_date"
INITIAL_START_DATE = "2025-01-01"


# ════════════════════════════════════════════════════════════════
# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════

def get_backfill_date_range(**context):
    """Backfill 처리할 날짜 범위 가져오기"""
    now_utc = datetime.utcnow()
    today_minus_2 = (now_utc - timedelta(days=2)).date()
    initial_date = datetime.strptime(INITIAL_START_DATE, '%Y-%m-%d').date()
    
    last_date_str = None
    try:
        last_date_str = Variable.get(BACKFILL_VARIABLE_KEY, default_var=None)
    except Exception:
        pass
    
    start_date = (datetime.strptime(last_date_str, '%Y-%m-%d').date() + timedelta(days=1)) if last_date_str else initial_date
    
    if start_date > today_minus_2:
        logging.info(f"✅ Backfill 완료. 처리할 날짜가 없습니다. (start: {start_date}, max: {today_minus_2})")
        return None, None
    
    logging.info(f"📋 Backfill 처리 범위: {start_date} ~ {today_minus_2} ({(today_minus_2 - start_date).days + 1}일)")
    return start_date, today_minus_2


# ════════════════════════════════════════════════════════════════
# 3️⃣ Main Backfill Logic
# ════════════════════════════════════════════════════════════════

def run_temperature_matching_backfill(**context):
    """Backfill 메인 함수: 2025-01-01부터 -2일 전까지 순차 처리"""
    start_date_obj, end_date_obj = get_backfill_date_range(**context)
    
    if start_date_obj is None or end_date_obj is None:
        return {"status": "success", "message": "No dates to process"}
    
    current_date = start_date_obj
    total_processed = 0
    total_main_rows = 0
    total_detail_rows = 0
    
    while current_date <= end_date_obj:
        date_str = current_date.strftime('%Y-%m-%d')
        logging.info(f"📅 [{date_str}] 처리 시작...")
        
        try:
            result = process_single_date(date_str)
            
            if result.get('status') == 'success':
                total_processed += result.get('rows_processed', 0)
                total_main_rows += result.get('main_rows', 0)
                total_detail_rows += result.get('detail_rows', 0)
                Variable.set(BACKFILL_VARIABLE_KEY, date_str)
                logging.info(f"✅ [{date_str}] 완료, Variable 업데이트: {date_str}")
            else:
                logging.error(f"❌ [{date_str}] 실패: {result.get('error')}")
                raise Exception(f"Processing failed for {date_str}: {result.get('error')}")
        
        except Exception as e:
            logging.error(f"❌ [{date_str}] 예외 발생: {e}")
            raise
        
        current_date += timedelta(days=1)
    
    logging.info(f"✅ Backfill 전체 완료: {total_processed:,} rows, 메인={total_main_rows:,}, 상세={total_detail_rows:,}")
    return {
        "status": "success",
        "total_rows_processed": total_processed,
        "total_main_rows": total_main_rows,
        "total_detail_rows": total_detail_rows,
        "processed_date_range": f"{start_date_obj} ~ {end_date_obj}"
    }


# ════════════════════════════════════════════════════════════════
# 4️⃣ DAG Definition
# ════════════════════════════════════════════════════════════════

with DAG(
    dag_id="ipi_temperature_matching_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "IP", "Quality", "Gold layer", "Backfill", "Temperature_matching_model"],
) as dag:
    
    run_backfill_task = PythonOperator(
        task_id="run_temperature_matching_backfill",
        python_callable=run_temperature_matching_backfill,
    )
    
    run_backfill_task
