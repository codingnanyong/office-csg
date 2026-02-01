"""
IP Good Product Incremental DAG (Bronze → Silver)
==================================================
전일 데이터를 쌓는 Incremental DAG

Source Tables:
- bronze.ipi_mc_output_v2_raw (pg_jj_production_dw)
- bronze.smp_ss_ipi_rst_raw (pg_jj_production_dw)
- bronze.mspq_in_osnd_bt_raw (pg_jj_quality_dw)

Target: silver.ip_good_product (pg_jj_quality_dw)
Execution: Daily schedule (@daily)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.quality.silver.common.ipi_good_product_common import incremental_ip_good_product

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
    dag_id='ipi_good_product_incremental',
    default_args=DEFAULT_ARGS,
    description='IPI Good Product Incremental - 전일 데이터 적재',
    schedule_interval=None,  # 매일 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['JJ', 'quality', 'IP', 'Silver Layer', 'incremental', 'IPI'],
    max_active_runs=1,
) as dag:
    
    incremental_task = PythonOperator(
        task_id='ipi_good_product_incremental',
        python_callable=incremental_ip_good_product,
        provide_context=True,
    )
    
    incremental_task
