"""
CTM Mold Temperature Raw Backfill DAG
======================================
금형 온도 데이터를 월별로 수집하는 Backfill DAG

Source: SQL Server ms_ctm_edge (dbo.mold_temperature)
Target: PostgreSQL bronze.ctm_mold_temperature_raw
Execution: Manual trigger only
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.telemetry.bronze.common.ctm_mold_temperature_raw_common import backfill_monthly_batch_task


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
    dag_id="ctm_mold_temperature_raw_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["CTM","raw", "bronze layer", "backfill", "telemetry", "monthly"]
) as dag:
    
    backfill_monthly_batch = PythonOperator(
        task_id="backfill_monthly_batch_task",
        python_callable=backfill_monthly_batch_task,
        provide_context=True,
    )
    
    backfill_monthly_batch
