"""
PH CTM HMI Data DAG
===================
HMI 데이터를 시간별로 수집하는 Backfill 및 Incremental DAG

Source: MySQL (maria_ph_01)
Target: PostgreSQL bronze.ph_ctm_hmi_data
Execution: Manual trigger
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.telemetry.bronze.common.ph_ctm_hmi_data_srv import (
    process_backfill,
    run_incremental,
    EQUIPMENTS
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
# DAG Definition - Backfill
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ph_ctm_hmi_data_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "PH", "CTM", "HMI", "backfill", "bronze layer"],
) as backfill_dag:
    for eq in EQUIPMENTS:
        PythonOperator(
            task_id=f"ph_ctm_hmi_data_backfill_{eq['equipment_id']}",
            python_callable=process_backfill,
            op_kwargs={
                "equipment_id": eq["equipment_id"],
                "conn_id": eq["conn_id"],
                "var_key": eq["var_key"],
                "equipment": eq["equipment"],
                "machine_id": eq["machine_id"],
            },
            provide_context=True,
        )


# ────────────────────────────────────────────────────────────────
# DAG Definition - Incremental
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ph_ctm_hmi_data_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "PH", "CTM", "HMI", "incremental", "bronze layer"],
) as incremental_dag:
    for eq in EQUIPMENTS:
        PythonOperator(
            task_id=f"ph_ctm_hmi_data_incremental_{eq['equipment_id']}",
            python_callable=run_incremental,
            op_kwargs={
                "equipment_id": eq["equipment_id"],
                "conn_id": eq["conn_id"],
                "var_key": eq["var_key"],
                "equipment": eq["equipment"],
                "machine_id": eq["machine_id"],
            },
            provide_context=True,
        )
