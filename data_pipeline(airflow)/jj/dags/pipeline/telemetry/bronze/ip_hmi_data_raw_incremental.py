"""
IP HMI Data Raw Incremental DAG
===============================
센서 데이터를 시간별로 수집하는 Incremental DAG

Source: MySQL (maria_ip_04, maria_ip_12, maria_ip_20, maria_ip_34, maria_ip_37)
Target: PostgreSQL bronze.ip_hmi_data_raw (TimescaleDB)
Execution: Hourly schedule (@hourly)
"""

import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.telemetry.bronze.common.ip_hmi_data_raw_common import (
    create_incremental_task,
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
    dag_id="ip_hmi_data_raw_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",  # 매시간 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["CKP","IP", "raw", "bronze layer", "incremental", "telemetry", "sensors", "hourly"]
) as dag:
    
    # Start task
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("🚀 센서 데이터 Incremental 수집 시작"),
    )
    
    # Machine-specific tasks (parallel execution) - 동적 생성
    machine_tasks = []
    for idx, machine_no in enumerate(IP_MACHINE_NO):
        task = PythonOperator(
            task_id=f"incremental_machine_{machine_no}",
            python_callable=create_incremental_task(machine_no, idx),
            provide_context=True,
        )
        machine_tasks.append(task)
    
    # End task
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("🎉 센서 데이터 Incremental 수집 완료"),
    )
    
    # Task dependencies
    start >> machine_tasks >> end
