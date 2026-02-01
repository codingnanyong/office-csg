"""
CTM Chiller Status Raw Incremental DAG
======================================
냉각기 상태 데이터를 일별로 수집하는 Incremental DAG

Source: PostgreSQL pg_ckp_chiller (public.status)
Target: PostgreSQL bronze.ctm_chiller_status_raw
Execution: Daily schedule (@daily)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.telemetry.bronze.common.ctm_chiller_status_raw_common import daily_incremental_collection_task


# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)
}


# ────────────────────────────────────────────────────────────────
# DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ctm_chiller_status_raw_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",  # 매일 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["CTM","raw", "bronze layer", "incremental", "telemetry", "daily", "chiller", "status"]
) as dag:
    
    daily_collection = PythonOperator(
        task_id="daily_incremental_collection",
        python_callable=daily_incremental_collection_task,
        provide_context=True,
    )
    
    daily_collection
