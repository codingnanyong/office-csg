import sys
import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
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
TABLE_NAME_WORKSHEET = "ws"

postgres_helper_v1 = PostgresHelper(CONN_ID_v1)
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)
# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ SQL 쿼리 정의 (Query Definitions)
# ────────────────────────────────────────────────────────────────────────────
SQL_PLAN_WORKSHEET ="""
    SELECT DISTINCT ON (pl.factory, pl.ws_no) pl.factory AS factory,pl.ws_no AS ws_no
    FROM pcc.pcc_plan_opcd pl
    WHERE pl.op_ymd = TO_CHAR(NOW(), 'YYYYMMDD')
      AND pl.op_chk = 'Y'
    ORDER BY pl.factory, pl.ws_no, pl.seq DESC;
"""
SQL_PROD_WORKSHEET ="""
    SELECT pr.factory AS factory, pr.ws_no AS ws_no
    FROM pcc.pcc_prod_scan pr
    WHERE pr.prod_ymd = TO_CHAR(NOW(), 'YYYYMMDD');
"""
SQL_RTLS_WORKSHEET ="""
    SELECT 'DS' AS factory,RIGHT(at.owner_tag_name, 20) AS wsno 
    FROM rtls.at_tag_xy at
    WHERE (tagid,event_dt) in (SELECT tagid ,MAX(event_dt) FROM rtls.at_tag_xy WHERE tagid IS NOT NULL GROUP BY tagid)
	    and RIGHT(at.owner_tag_name, 20) is not null and RIGHT(at.owner_tag_name, 20) like 'WS%';
"""
# ────────────────────────────────────────────────────────────────────────────
# 3️⃣ 데이터 병합 및 정리 (Merge)
# ────────────────────────────────────────────────────────────────────────────
def merge_results(**kwargs):
    ti = kwargs['ti']

    plan_data = ti.xcom_pull(task_ids='fetch_plan_worksheet', key='plan_data') or []
    prod_data = ti.xcom_pull(task_ids='fetch_prod_worksheet', key='prod_data') or []
    rtls_data = ti.xcom_pull(task_ids='fetch_rtls_worksheet', key='rtls_data') or []

    if not any([plan_data, prod_data, rtls_data]):
        logger.warning("⚠️ Warning: All data sources are empty!")

    df_plan = pd.DataFrame(plan_data, columns=["factory", "ws_no"])
    df_prod = pd.DataFrame(prod_data, columns=["factory", "ws_no"])
    df_rtls = pd.DataFrame(rtls_data, columns=["factory", "ws_no"])

    df_merged = pd.concat([df_plan, df_prod, df_rtls]).drop_duplicates().reset_index(drop=True)

    print("📊 Merged DataFrame:")
    print(df_merged)

    merged_records = df_merged.to_dict(orient='records')
    ti.xcom_push(key='records', value=merged_records)

    logger.info(f"✅ Merged {len(merged_records)} records into XCom.")
    return merged_records

# ────────────────────────────────────────────────────────────────────────────
# 4️⃣ DAG 정의 (DAG Definition & Task Dependencies)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hq_worksheet_generate",
    default_args=default_args,
    description="HQ WorkSheet",
    schedule= "@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["HQ","WorkSheet", "PCC"]
) as dag:
    
    task_check_table = PythonOperator(
        task_id="check_table",
        python_callable=postgres_helper_v2.check_table,
        op_args=[SCHEMA_NAME, TABLE_NAME_WORKSHEET],
    )

    task_clean_table = PythonOperator(
        task_id="clean_table",
        python_callable=postgres_helper_v2.clean_table,
        op_args=[SCHEMA_NAME, TABLE_NAME_WORKSHEET],
    )

    task_fetch_plan_worksheet = PythonOperator(
        task_id="fetch_plan_worksheet",
        python_callable=postgres_helper_v1.execute_query,
        op_args=[SQL_PLAN_WORKSHEET, "fetch_plan_worksheet", "plan_data"],
    )

    task_fetch_prod_worksheet = PythonOperator(
        task_id="fetch_prod_worksheet",
        python_callable=postgres_helper_v1.execute_query,
        op_args=[SQL_PROD_WORKSHEET, "fetch_prod_worksheet", "prod_data"],
    )

    task_fetch_rtls_worksheet = PythonOperator(
        task_id="fetch_rtls_worksheet",
        python_callable=postgres_helper_v1.execute_query,
        op_args=[SQL_RTLS_WORKSHEET, "fetch_rtls_worksheet", "rtls_data"],
    )

    task_merge_worksheet_data = PythonOperator(
        task_id="merge_worksheet_data",
        python_callable=merge_results
    )

    task_insert_results = PythonOperator(
        task_id="insert_results",
        python_callable=lambda **kwargs: postgres_helper_v2.insert_data(
            SCHEMA_NAME, TABLE_NAME_WORKSHEET,
            [tuple(row.values()) for row in (kwargs['ti'].xcom_pull(task_ids='merge_worksheet_data', key='records') or [])]
        )
    )

    trigger_worksheet_pcc_data = TriggerDagRunOperator(
        task_id='trigger_worksheet_pcc_data',
        trigger_dag_id='hq_worksheet_pcc_data_ingest',
    )

    trigger_worksheet_rtls_data = TriggerDagRunOperator(
        task_id='trigger_worksheet_rtls_data',
        trigger_dag_id='hq_worksheet_rtls_data_ingest',
    )
    (
        task_check_table >> task_clean_table
        >> [task_fetch_plan_worksheet, task_fetch_prod_worksheet, task_fetch_rtls_worksheet]
        >> task_merge_worksheet_data >> task_insert_results
        >> [trigger_worksheet_pcc_data, trigger_worksheet_rtls_data]
    )