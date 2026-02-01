"""BAS Defective Raw Incremental DAG (Oracle → Bronze)"""
import logging
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from dags.pipeline.maintenance.bronze.common.bas_deffective_raw_common import (
    process_single_date,
    update_variable,
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


# ════════════════════════════════════════════════════════════════
# 2️⃣ Main ETL Logic
# ════════════════════════════════════════════════════════════════

def daily_incremental_collection_task(**kwargs) -> dict:
    """매일 최신 데이터만 수집하는 태스크"""
    # 마지막 추출 시간을 기준으로 다음 날 데이터 수집
    last_extract_time_str = Variable.get(INCREMENT_KEY, default_var=None)
    
    if last_extract_time_str:
        from dags.pipeline.maintenance.bronze.common.bas_deffective_raw_common import parse_datetime
        
        # 마지막 추출 시간을 파싱
        last_extract_time = parse_datetime(last_extract_time_str)
        
        # 마지막 추출 시간의 다음 날 00:00:00부터 23:59:59까지
        start_date = last_extract_time.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        target_date_str = start_date.strftime('%Y-%m-%d')
        
        logging.info(f"마지막 추출 시간: {last_extract_time_str}")
        logging.info(f"다음 날 데이터 수집: {target_date_str}")
    else:
        # Variable이 없으면 어제 데이터 수집
        yesterday = datetime.now(INDO_TZ) - timedelta(days=1)
        target_date_str = yesterday.strftime('%Y-%m-%d')
        logging.info(f"Variable이 없어서 어제 데이터 수집: {target_date_str}")
    
    logging.info(f"📊 처리 날짜: {target_date_str}")
    
    # 단일 날짜 처리
    result = process_single_date(
        target_date_str,
        ORACLE_CONN_ID,
        POSTGRES_CONN_ID,
        SCHEMA_NAME,
        TABLE_NAME
    )
    
    # Variable 업데이트 (성공 여부와 관계없이 시간 업데이트)
    if result.get('status') == 'success':
        end_str = result.get('end_time')
        update_variable(INCREMENT_KEY, end_str)
    
    return result


# ════════════════════════════════════════════════════════════════
# 3️⃣ DAG Definition
# ════════════════════════════════════════════════════════════════

with DAG(
    dag_id="bas_deffective_raw_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "raw", "bronze layer", "incremental", "maintenance", "daily"],
) as dag:
    
    daily_collection = PythonOperator(
        task_id="daily_incremental_collection",
        python_callable=daily_incremental_collection_task,
    )
    
    daily_collection
