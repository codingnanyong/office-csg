"""
CTM Chiller Status Raw Backfill DAG
====================================
냉각기 상태 데이터를 주별로 수집하는 Backfill DAG

Source: PostgreSQL pg_ckp_chiller (public.status)
Target: PostgreSQL bronze.ctm_chiller_status_raw
Execution: Manual trigger only
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.telemetry.bronze.common.ctm_chiller_status_raw_common import backfill_weekly_batch_task

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
    dag_id="ctm_chiller_status_raw_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["CTM","raw", "bronze layer", "backfill", "telemetry", "weekly", "chiller", "status"]
) as dag:
    
    backfill_weekly_batch = PythonOperator(
        task_id="backfill_weekly_batch_task",
        python_callable=backfill_weekly_batch_task,
        provide_context=True,
    )
    
    backfill_weekly_batch
