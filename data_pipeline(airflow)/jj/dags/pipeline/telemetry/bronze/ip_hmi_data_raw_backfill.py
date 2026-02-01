"""
IP HMI Data Raw Backfill DAG
=============================
센서 데이터를 시간별로 수집하는 Backfill DAG

Source: MySQL (maria_ip_04, maria_ip_12, maria_ip_20, maria_ip_34, maria_ip_37)
Target: PostgreSQL bronze.ip_hmi_data_raw (TimescaleDB)
Execution: Manual trigger only
"""

import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.telemetry.bronze.common.ip_hmi_data_raw_common import (
    create_backfill_task,
    IP_MACHINE_NO
)


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
    dag_id="ip_hmi_data_raw_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["CKP","IP", "raw", "bronze layer", "backfill", "telemetry", "sensors", "hourly"]
) as dag:
    
    # Start task
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("🚀 센서 데이터 Backfill 시작"),
    )
    
    # Machine-specific tasks (parallel execution) - 동적 생성
    machine_tasks = []
    for idx, machine_no in enumerate(IP_MACHINE_NO):
        task = PythonOperator(
            task_id=f"backfill_machine_{machine_no}",
            python_callable=create_backfill_task(machine_no, idx),
            provide_context=True,
        )
        machine_tasks.append(task)
    
    # End task
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("🎉 센서 데이터 Backfill 완료"),
    )
    
    # Task dependencies
    start >> machine_tasks >> end
