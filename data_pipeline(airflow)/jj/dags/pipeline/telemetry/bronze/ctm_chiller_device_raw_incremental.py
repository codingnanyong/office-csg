"""
CTM Chiller Device Raw Incremental DAG
=======================================
냉각기 장비 데이터를 매일 전체 수집하는 Incremental DAG

Source: PostgreSQL pg_ckp_chiller (public.device)
Target: PostgreSQL bronze.ctm_chiller_device_raw
Execution: Daily schedule (@daily)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.telemetry.bronze.common.ctm_chiller_device_raw_common import ctm_chiller_device_etl_task


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
    dag_id="ctm_chiller_device_raw_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",  # 매일 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["CTM","raw", "bronze layer", "incremental", "telemetry", "daily", "chiller"]
) as dag:
    
    ctm_chiller_device_etl = PythonOperator(
        task_id="ctm_chiller_device_etl_task",
        python_callable=ctm_chiller_device_etl_task,
        provide_context=True,
    )
    
    ctm_chiller_device_etl
