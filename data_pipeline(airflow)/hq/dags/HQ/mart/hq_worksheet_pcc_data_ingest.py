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
postgres_helper_v1 = PostgresHelper(CONN_ID_v1)
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)

SCHEMA_NAME = "services"
SQL_WS = "SELECT ws_no FROM services.ws"
TABLES = {
    "ws_detail": 
    """
        SELECT bom.ws_no, bom.dpa, bom.bom_id, bom.st_cd, bom.sub_st_cd,
            bom.season_cd, bom.category, bom.dev_name, bom.style_cd,
            bom.sample_ets, bom.sample_qty, bom.sample_size, bom.prod_factory,
            bom.gender, bom.td, bom.model_id, bom.pcc_pm, bom.rework_yn,
            bom.dev_style_id, bom.dev_colorway_id, bom.dev_style_number
        FROM pcc.pcc_bom_head bom
        JOIN (SELECT factory, ws_no, MAX(upd_ymd) AS latest_upd_ymd
            FROM pcc.pcc_bom_head
            GROUP BY factory, ws_no) lbom
        ON bom.factory = lbom.factory AND bom.ws_no = lbom.ws_no AND bom.upd_ymd = lbom.latest_upd_ymd
        WHERE bom.factory = 'DS' AND bom.ws_no IN {};
    """,
    "ws_his" : 
    """
        SELECT pl.ws_no, pl.op_cd, pl.op_ymd as plan_date, pl.op_qty as plan_qty,
           pr.prod_ymd as prod_date, pr.prod_time as prod_time,  ROUND(pr.prod_qty, 2) AS prod_qty,
           CASE
	        WHEN pr.prod_status = 'Y' THEN 'I'
	        WHEN pr.prod_status = 'C' THEN 'O'
	        WHEN pl.op_chk = 'N' AND pr.prod_status IS NULL THEN 'X'
	        WHEN pr.prod_status IS NULL THEN 'N'
            END AS "status"
        FROM pcc.pcc_plan_opcd pl
            JOIN (
                SELECT factory, ws_no, op_cd, MAX(seq) AS max_seq
                FROM pcc.pcc_plan_opcd
                WHERE factory = 'DS'
                GROUP BY factory, ws_no, op_cd
            ) lpl
            ON pl.ws_no = lpl.ws_no AND pl.op_cd = lpl.op_cd AND pl.seq = lpl.max_seq
            LEFT JOIN (
                SELECT DISTINCT ON (factory, ws_no, op_cd, prod_status)
                    factory, ws_no, op_cd, prod_status, prod_ymd, prod_time, prod_qty
                FROM pcc.pcc_prod_scan
                WHERE factory = 'DS'
                ORDER BY factory, ws_no, op_cd, prod_status, prod_seq DESC
            ) pr
            ON pl.ws_no = pr.ws_no AND pl.op_cd = pr.op_cd
            WHERE pl.ws_no IN {};
    """,
    "ws_status": 
    """
        SELECT pl.ws_no, pl.op_cd, dt.pcc_pm AS pm, dt.dev_name AS model, dt.season_cd AS season_cd,
            dt.bom_id AS bom_id, dt.style_cd AS style_cd, dt.dev_colorway_id AS dev_colorway_id,
            pl.op_ymd AS plan_date, pr.prod_ymd AS prod_date,
            CASE WHEN pr.prod_status = 'C' THEN 'O' WHEN pr.prod_status = 'Y' THEN 'I' ELSE 'N' END AS "status", pr.prod_qty AS prod_qty
        FROM pcc.pcc_plan_opcd AS pl
        LEFT JOIN (SELECT DISTINCT ON (factory, ws_no, op_cd, prod_status) factory, ws_no, op_cd, prod_ymd, prod_status,prod_qty
            FROM pcc.pcc_prod_scan 
            ORDER BY factory, ws_no, op_cd, prod_status, prod_seq DESC) AS pr ON pl.factory = pr.factory AND pl.ws_no = pr.ws_no AND pl.op_cd = pr.op_cd
        LEFT JOIN (SELECT bom.factory, bom.ws_no, bom.bom_id, bom.season_cd, bom.dev_name, bom.style_cd, bom.model_id, bom.pcc_pm, bom.dev_colorway_id
            FROM pcc.pcc_bom_head bom
            JOIN (SELECT factory, ws_no, MAX(upd_ymd) AS latest_upd_ymd
                FROM pcc.pcc_bom_head
                GROUP BY factory, ws_no) lbom ON bom.factory = lbom.factory AND bom.ws_no = lbom.ws_no AND bom.upd_ymd = lbom.latest_upd_ymd
            ) AS dt ON pl.factory = dt.factory AND pl.ws_no = dt.ws_no
        WHERE pl.op_chk = 'Y' AND pl.factory = 'DS'
            AND (pl.factory, pl.ws_no, pl.op_cd, pl.seq) IN (
                SELECT factory, ws_no, op_cd, MAX(seq)
                FROM pcc.pcc_plan_opcd
                WHERE factory = 'DS'
                GROUP BY factory, ws_no, op_cd)
            AND pl.ws_no IN {};
    """
}
# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ DAG 정의 (DAG Definition & Task Dependencies)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hq_worksheet_pcc_data_ingest",
    default_args=default_args,
    description="HQ PCC WorkSheet Data Ingest",
    schedule_interval=None,  
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["HQ","WorkSheet", "PCC"]
) as dag:
    
    def fetch_task_common(table_name, sql_query, **kwargs):
        ws_list = kwargs['ti'].xcom_pull(key='ws_data') or []
        if not ws_list:
            logger.warning(f"No ws_no found for {table_name}")
            return

        flattened_data = tuple(item[0] for item in ws_list)
        formatted_query = sql_query.format(flattened_data)
        logger.info(f"Executing {table_name} query: {formatted_query}")

        data = postgres_helper_v1.execute_query(
            formatted_query, task_id=f"fetch_{table_name}_data", xcom_key=f"{table_name}_data", ti=kwargs['ti']
        )
        logger.info(f"✅ {table_name} records fetched: {len(data) if data else 0}")

    task_fetch_ws = PythonOperator(
        task_id="fetch_ws",
        python_callable=postgres_helper_v2.execute_query,
        op_args=[SQL_WS, "fetch_ws", "ws_data"],
    )

    parallel_tasks = []
    for table_name, sql_query in TABLES.items():

        check_task = PythonOperator(
            task_id=f"check_{table_name}",
            python_callable=postgres_helper_v2.check_table,
            op_args=[SCHEMA_NAME, table_name],
        )

        clean_task = PythonOperator(
            task_id=f"clean_{table_name}",
            python_callable=postgres_helper_v2.clean_table,
            op_args=[SCHEMA_NAME, table_name],
            trigger_rule="all_success",
        )

        fetch_task = PythonOperator(
            task_id=f"fetch_{table_name}",
            python_callable=fetch_task_common,
            op_args=[table_name, sql_query],
            trigger_rule="all_success",
        )

        insert_task = PythonOperator(
            task_id=f"insert_{table_name}",
            python_callable=lambda ti, tn=table_name: postgres_helper_v2.insert_data(
                SCHEMA_NAME,tn,
                [tuple(row) for row in (ti.xcom_pull(task_ids=f"fetch_{tn}", key=f"{tn}_data") or [])]
            ),
            trigger_rule="all_success",
        )

        task_fetch_ws >> check_task >> clean_task >> fetch_task >> insert_task
        parallel_tasks.append(insert_task)
