"""MCH Machine Raw Incremental DAG (Oracle → Bronze)"""
import logging
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from dags.pipeline.maintenance.bronze.common.mch_machine_raw_common import (
    process_single_date,
    update_variable,
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

INCREMENT_KEY = "last_extract_time_mch_machine_raw"

# ════════════════════════════════════════════════════════════════
# 2️⃣ Main ETL Logic
# ════════════════════════════════════════════════════════════════

def incremental_extract_task(**kwargs) -> dict:
    """Incremental extract task for daily processing"""
    # Get last extract time from variable
    last_extract_time = Variable.get(INCREMENT_KEY, default_var=None)
    if not last_extract_time:
        raise ValueError(f"Variable '{INCREMENT_KEY}' not found. Please run backfill DAG first.")
    
    # Parse last extract time (23:59:59) and convert to next day 00:00:00
    start_date = parse_datetime(last_extract_time)
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=INDO_TZ)
    # Next day 00:00:00
    start_date = (start_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Calculate end date (전일 23:59:59)
    end_date = (datetime.now(INDO_TZ) - timedelta(days=1)).replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    
    # Check if start_date exceeds end_date
    if start_date >= end_date:
        logging.info(f"⚠️ 처리할 데이터 없음: start_date({start_date}) >= end_date({end_date})")
        return {
            "start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": end_date.strftime("%Y-%m-%d %H:%M:%S"),
            "extracted_count": 0,
            "loaded_count": 0,
            "extract_time": datetime.now(INDO_TZ).strftime("%Y-%m-%d %H:%M:%S")
        }
    
    # 단일 날짜 처리 함수를 사용하여 처리 (일별 반복)
    results = []
    total_extracted = 0
    total_loaded = 0
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        logging.info(f"📅 [{date_str}] 처리 시작...")
        
        try:
            result = process_single_date(
                date_str,
                ORACLE_CONN_ID,
                POSTGRES_CONN_ID,
                SCHEMA_NAME,
                TABLE_NAME
            )
            
            if result.get('status') == 'success':
                total_extracted += result.get('extracted_count', 0)
                total_loaded += result.get('loaded_count', 0)
                # Variable 업데이트
                update_variable(INCREMENT_KEY, result.get('end_time'))
                logging.info(f"✅ [{date_str}] 완료, Variable 업데이트")
            else:
                logging.error(f"❌ [{date_str}] 처리 실패: {result.get('message', 'Unknown error')}")
                raise Exception(f"Processing failed for {date_str}")
        
        except Exception as e:
            logging.error(f"❌ [{date_str}] 예외 발생: {e}")
            raise
        
        current_date += timedelta(days=1)
    
    logging.info(f"✅ Incremental 완료: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    logging.info(f"📊 추출: {total_extracted}건, 적재: {total_loaded}건")
    
    return {
        "start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": end_date.strftime("%Y-%m-%d %H:%M:%S"),
        "extracted_count": total_extracted,
        "loaded_count": total_loaded,
        "extract_time": datetime.now(INDO_TZ).strftime("%Y-%m-%d %H:%M:%S")
    }


# ════════════════════════════════════════════════════════════════
# 3️⃣ DAG Definition
# ════════════════════════════════════════════════════════════════

with DAG(
    dag_id='mch_machine_raw_incremental',
    default_args=DEFAULT_ARGS,
    description='Machine Master Raw Data Incremental (Daily)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['JJ', 'maintenance', 'bronze layer', 'incremental', 'machine', 'master'],
) as dag:
    
    incremental_task = PythonOperator(
        task_id='incremental_extract',
        python_callable=incremental_extract_task,
    )
    
    incremental_task
