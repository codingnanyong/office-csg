import sys
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────────────────
# 1️⃣ 설정 (Config & Logging)
# ────────────────────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)
}

logger = logging.getLogger(__name__)

CONN_ID_v1 = "pg_hq_iot_workshop"
CONN_ID_v2 = "pg_fdw_v2_hq"
SCHEMA_NAME = "services"
TABLE_NAME = "sensor"

postgres_helper_v1 = PostgresHelper(CONN_ID_v1)
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)

SQL_QUERY ="""
    select sensor_id ,device_id ,
        case when sensor_id ='TEMPIOT-A001' then '3D Printer Room'
            when sensor_id in ('TEMPIOT-A002','TEMPIOT-A003') then '나염실'
        else mach_id end ,
        company_cd ,"name" ,addr ,descn 
    from public.sensor 
"""
# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ SQL 쿼리 정의 (Query Definitions)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hq_iot_sensor_master_update",
    default_args=default_args,
    description="HQ Sensor Master Update",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["HQ", "IoT","Master"],
) as dag:
    
    check_task = PythonOperator(
        task_id=f"check_{TABLE_NAME}_table",
        python_callable=postgres_helper_v2.check_table,
          op_args=[SCHEMA_NAME, TABLE_NAME],
    )

    clean_task = PythonOperator(
        task_id=f"clean_{TABLE_NAME}_table",
        python_callable=postgres_helper_v2.clean_table,
        op_args=[SCHEMA_NAME, TABLE_NAME],
        trigger_rule="all_success",
    )

    fetch_task = PythonOperator(
        task_id=f"fetch_{TABLE_NAME}_data",
        python_callable=postgres_helper_v1.execute_query,
        op_args=[SQL_QUERY, f"fetch_{TABLE_NAME}_task", f"{TABLE_NAME}_data"],
        trigger_rule="all_success",
    )

    insert_task = PythonOperator(
        task_id=f"insert_{TABLE_NAME}_data",
        python_callable=lambda ti, tn=TABLE_NAME: postgres_helper_v2.insert_data(
            SCHEMA_NAME,tn,
            [tuple(row) for row in (ti.xcom_pull(task_ids=f"fetch_{tn}_data", key=f"{tn}_data") or [])]),
            trigger_rule="all_success",
    )

    check_task >> clean_task >> fetch_task >> insert_task