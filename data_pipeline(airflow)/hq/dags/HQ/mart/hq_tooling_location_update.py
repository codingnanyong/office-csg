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
}

logger = logging.getLogger(__name__)

CONN_ID_v1 = "pg_fdw_v1_hq"  
CONN_ID_v2 = "pg_fdw_v2_hq"  

SCHEMA_NAME = "services"  
TABLE_NAME = "tooling"

postgres_helper_v1 = PostgresHelper(CONN_ID_v1)  
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)  

# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ SQL 쿼리 정의 (Query Definitions) 
# ────────────────────────────────────────────────────────────────────────────
SQL_TOOLING = """
    SELECT up.purc_no,up.uptl_no ,up.parent_uptl_no,
	   up.part_cd AS part_cd ,up.part_name AS part_name ,up.process_cd AS process_cd ,up.process_name AS process_name ,
	   up.tool_cd AS tool_cd ,up.tool_name AS tool_name ,up.tool_size AS tool_size ,
	   up.status , up.loc_cd,mloc.loc_name AS loc_name
    FROM pcc.pcc_tool_uptl as up
    LEFT JOIN (SELECT mloc.factory, mloc.loc_cd, mloc.op_cd, mloc.loc_name
            FROM pcc.pcc_mst_tooling_loc AS mloc
            INNER JOIN (SELECT factory, loc_cd, MAX(loc_seq) AS max_loc_seq
                        FROM pcc.pcc_mst_tooling_loc
                        GROUP BY factory, loc_cd) AS max_mloc ON mloc.factory = max_mloc.factory
                AND mloc.loc_cd = max_mloc.loc_cd
                AND mloc.loc_seq = max_mloc.max_loc_seq) AS mloc ON up.loc_cd = mloc.loc_cd
    WHERE (purc_no,uptl_no,upd_ymd ) IN (SELECT purc_no,uptl_no ,MAX(upd_ymd) as upd_ymd
                                        FROM pcc.pcc_tool_uptl 
                                        WHERE upd_ymd is not null
                                        group by purc_no,uptl_no) 
"""
# ────────────────────────────────────────────────────────────────────────────
# 3️⃣ DAG 정의 (DAG Definition & Task Dependencies)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hq_tooling_location_update",
    default_args=default_args,
    description="HQ Tooling location",
    schedule_interval = "@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["HQ","Tooling"]
) as dag:
    
    task_check_table = PythonOperator(
        task_id="check_table",
        python_callable=postgres_helper_v2.check_table,
        op_args=[SCHEMA_NAME, TABLE_NAME],
    )

    task_clean_table = PythonOperator(
        task_id="clean_table",
        python_callable=postgres_helper_v2.clean_table,
        op_args=[SCHEMA_NAME, TABLE_NAME],
    )

    task_fetch_tooling = PythonOperator(
        task_id="fetch_tooling", 
        python_callable=postgres_helper_v1.execute_query,
        op_args=[SQL_TOOLING, "fetch_tooling", "tooling_data"],  
        provide_context=True,
    )

    task_insert_tooling = PythonOperator(
        task_id="insert_tooling",
        python_callable=lambda **kwargs: postgres_helper_v2.insert_data(
            SCHEMA_NAME, TABLE_NAME,
            [tuple(row) for row in (kwargs["ti"].xcom_pull(task_ids="fetch_tooling", key="tooling_data") or [])]
        ),
        provide_context=True, 
    )

    task_check_table >> task_clean_table >> task_fetch_tooling >> task_insert_tooling