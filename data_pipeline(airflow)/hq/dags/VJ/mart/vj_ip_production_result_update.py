import sys
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────────────────
# 1️⃣ 설정 (Config & Logging)
# ────────────────────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
logger = logging.getLogger(__name__)

CONN_ID_v1 = "pg_fdw_v1_vj"
CONN_ID_v2 = "pg_fdw_v2_vj"

postgres_helper_v1 = PostgresHelper(CONN_ID_v1)
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)

SCHEMA_NAME = "services"
SQL_QUERIES ={
    "ip_performacne":
    '''
        SELECT map.zone AS zone, 
			   map.cmms_machine_id AS mach_id, 
			   mes.rst_ymd AS rst_ymd, 
			   SUM(mes.prs_qty) AS total_qty
        FROM sephiroth.smp_ss_ipp_rst AS mes
        JOIN services.mes_cmms_mapping AS map ON mes.resource_cd = map.mes_machine_id
        GROUP BY map.zone, map.cmms_machine_id, mes.rst_ymd
    '''
}

# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ DAG 정의 (DAG Definition & Task Dependencies)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="vj_ip_production_result_update",
    default_args=default_args,
    description="VJ IP Production Result",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["VJ","IP","Production"]
) as dag:
    
    parallel_tasks = []
    for table_name, sql_query in SQL_QUERIES.items():
        current_table = table_name

        check_task = PythonOperator(
            task_id=f"check_{current_table}_table",
            python_callable=postgres_helper_v2.check_table,
            op_args=[SCHEMA_NAME, current_table],
        )

        clean_task = PythonOperator(
            task_id=f"clean_{current_table}_table",
            python_callable=postgres_helper_v2.clean_table,
            op_args=[SCHEMA_NAME, current_table],
            trigger_rule="all_success",
        )

        fetch_task = PythonOperator(
            task_id=f"fetch_{current_table}_data",
            python_callable=postgres_helper_v1.execute_query,
            op_args=[sql_query, f"fetch_{current_table}_task", f"{current_table}_data"],
            trigger_rule="all_success",
        )

        insert_task = PythonOperator(
            task_id=f"insert_{table_name}_data",
            python_callable=lambda ti, tn=table_name: postgres_helper_v2.insert_data(
                SCHEMA_NAME,tn,
                [tuple(row) for row in (ti.xcom_pull(task_ids=f"fetch_{tn}_data", key=f"{tn}_data") or [])]
            ),
            trigger_rule="all_success",
        )

        check_task >> clean_task >> fetch_task >> insert_task
        parallel_tasks.append(insert_task)

    trigger_monthly_use = TriggerDagRunOperator(
        task_id='trigger_srv_monthlyuse',
        trigger_dag_id='vj_cmms_sparepart_monthly_usage',
    )

    parallel_tasks >> trigger_monthly_use