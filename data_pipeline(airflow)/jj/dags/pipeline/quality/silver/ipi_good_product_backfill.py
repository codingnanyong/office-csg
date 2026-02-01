"""
IP Good Product Backfill DAG (Bronze → Silver)
===============================================
과거 데이터를 쌓는 Backfill DAG (-2일 전까지)

Source Tables:
- bronze.ipi_mc_output_v2_raw (pg_jj_production_dw)
- bronze.smp_ss_ipi_rst_raw (pg_jj_production_dw)
- bronze.mspq_in_osnd_bt_raw (pg_jj_quality_dw)

Target: silver.ip_good_product (pg_jj_quality_dw)
Execution: Manual trigger only
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.quality.silver.common.ipi_good_product_common import backfill_daily_batch_task


# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2),
}

# ────────────────────────────────────────────────────────────────
# DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id='ipi_good_product_backfill',
    default_args=DEFAULT_ARGS,
    description='IPI Good Product Backfill - 과거 데이터 적재 (-2일 전까지)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['JJ', 'quality', 'IP', 'Silver Layer', 'backfill', 'IPI'],
    max_active_runs=1,
) as dag:
    
    backfill_daily_batch = PythonOperator(
        task_id="backfill_daily_batch_task",
        python_callable=backfill_daily_batch_task,
        provide_context=True,
    )
    
    backfill_daily_batch
