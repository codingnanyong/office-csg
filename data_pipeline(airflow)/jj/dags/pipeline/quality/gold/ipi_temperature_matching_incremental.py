"""IPI Temperature Matching Model Incremental DAG (Silver → Gold)
양품 및 불량 데이터와 온도 데이터를 매칭하여 Gold 레이어에 적재하는 DAG
Model: Temperature_matching_model
"""
import logging
from datetime import datetime, timedelta
from typing import Tuple
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

INCREMENT_KEY = "ipi_temperature_matching_last_date"


# ════════════════════════════════════════════════════════════════
# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════

def get_processing_time_range(**context) -> Tuple[str, str]:
    """Airflow Variable에서 처리 시간 범위 가져오기 (Incremental: 1일치)"""
    last_date_str = None
    try:
        last_date_str = Variable.get(INCREMENT_KEY, default_var=None)
    except Exception:
        pass
    
    now_utc = datetime.utcnow()
    today_minus_1 = (now_utc - timedelta(days=1)).date()
    
    if last_date_str:
        try:
            last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
            target_date = last_date + timedelta(days=1)
        except Exception:
            target_date = today_minus_1
    else:
        target_date = today_minus_1
    
    if target_date > today_minus_1:
        logging.info(f"✅ 최신 상태입니다. 처리할 날짜가 없습니다. (target: {target_date}, max: {today_minus_1})")
        return None, None
    
    date_str = target_date.strftime('%Y-%m-%d')
    logging.info(f"📋 처리 날짜 범위 (Incremental): {date_str} (1일치, 최대: {today_minus_1})")
    return date_str, date_str


# ════════════════════════════════════════════════════════════════
# 3️⃣ Main ETL Logic
# ════════════════════════════════════════════════════════════════

def run_temperature_matching(**context) -> dict:
    """메인 ETL 함수 (증분 처리)"""
    start_date, end_date = get_processing_time_range(**context)
    
    if start_date is None or end_date is None:
        logging.info("✅ 처리할 날짜가 없습니다. (이미 최신 상태)")
        return {"status": "success", "rows_processed": 0, "rows_inserted": 0, "message": "Already up to date", "processed_date": None}
    
    try:
        result = process_single_date(start_date)
        
        if result.get('status') == 'success':
            Variable.set(INCREMENT_KEY, start_date)
            logging.info(f"✅ Variable `{INCREMENT_KEY}` 업데이트: {start_date}")
        
        return result
        
    except Exception as e:
        logging.error(f"❌ Temperature Matching 실패: {str(e)}", exc_info=True)
        return {"status": "failed", "error": str(e)}


# ════════════════════════════════════════════════════════════════
# 4️⃣ DAG Definition
# ════════════════════════════════════════════════════════════════

with DAG(
    dag_id="ipi_temperature_matching_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "IP", "Quality", "Gold layer", "Incremental", "Temperature_matching_model"],
) as dag:
    
    temperature_matching_task = PythonOperator(
        task_id="run_temperature_matching",
        python_callable=run_temperature_matching,
    )
    
    temperature_matching_task

