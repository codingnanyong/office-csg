"""
OS Banbury HMI Alarm Backfill DAG
==================================
알람 데이터를 시간별로 수집하는 Backfill DAG

Source: MySQL (maria_jj_os_banb_1, maria_jj_os_banb_3)
Target: PostgreSQL bronze.os_banb_hmi_alarm_data, bronze.os_banb_hmi_alarm_stat
Execution: Manual trigger only
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.telemetry.bronze.common.os_banb_hmi_alarm_common import (
    process_backfill_data,
    process_backfill_stat,
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
    'sla': timedelta(hours=2),
}


# ────────────────────────────────────────────────────────────────
# DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="os_banb_hmi_alarm_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "OS", "Banbury", "HMI", "alarm", "backfill", "bronze layer"],
):
    for eq in EQUIPMENTS:
        PythonOperator(
            task_id=f"os_banb_hmi_alarm_data_backfill_eq{eq['equipment_id']}",
            python_callable=process_backfill_data,
            op_kwargs={
                "equipment_id": eq["equipment_id"],
                "conn_id": eq["conn_id"],
                "var_key": eq["var_key_data"],
                "equipment_value": eq["equipment_value"],
            },
            provide_context=True,
        )

        PythonOperator(
            task_id=f"os_banb_hmi_alarm_stat_backfill_eq{eq['equipment_id']}",
            python_callable=process_backfill_stat,
            op_kwargs={
                "equipment_id": eq["equipment_id"],
                "conn_id": eq["conn_id"],
                "var_key": eq["var_key_stat"],
                "equipment_value": eq["equipment_value"],
            },
            provide_context=True,
        )
