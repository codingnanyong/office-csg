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
TABLE_NAME = "dailystatus"

postgres_helper_v1 = PostgresHelper(CONN_ID_v1)
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)

# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ SQL 쿼리 정의 (Query Definitions)
# ────────────────────────────────────────────────────────────────────────────
SQL_MASTER_DATA = """
    SELECT factory, op_cd, op_name, op_local_name 
    FROM pcc.pcc_mst_opcd
    WHERE factory = 'DS'
    ORDER BY factory, sort_no;
"""

SQL_PLAN_DATA = """
    SELECT a.factory, a.op_cd, count(*) as plcnt, sum(qty) as plqty
    FROM (
        SELECT DISTINCT pl.factory, pl.ws_no, pl.op_cd, opl.op_qty as qty
        FROM (
            SELECT subpl.factory, subpl.ws_no, subpl.op_cd
            FROM pcc.pcc_plan_opcd AS subpl
            WHERE (factory, ws_no, op_cd, seq) IN (
                SELECT p.factory, p.ws_no, p.op_cd, MAX(p.seq)
                FROM pcc.pcc_plan_opcd AS p
                GROUP BY p.factory, p.ws_no, p.op_cd
            )
            AND subpl.op_chk = 'Y' 
            AND subpl.op_ymd = TO_CHAR(NOW(), 'YYYYMMDD')
            UNION 
            SELECT subpr.factory, subpr.ws_no, subpr.op_cd
            FROM pcc.pcc_prod_scan AS subpr
            WHERE (factory, ws_no, op_cd, prod_seq) IN (
                SELECT p.factory, p.ws_no, p.op_cd, MAX(p.prod_seq)
                FROM pcc.pcc_prod_scan AS p
                WHERE p.prod_ymd = TO_CHAR(NOW(), 'YYYYMMDD')
                GROUP BY p.factory, p.ws_no, p.op_cd
            )
        ) AS pl
        LEFT JOIN pcc.pcc_plan_opcd AS opl 
        ON pl.factory = opl.factory AND pl.ws_no = opl.ws_no AND pl.op_cd = opl.op_cd
    ) AS a
    GROUP BY a.factory, a.op_cd;
"""

SQL_PRODUCTION_DATA = """
    SELECT factory, op_cd, COUNT(*) AS prodcnt, ROUND(SUM(prod_qty)) AS prodqty
    FROM pcc.pcc_prod_scan AS pr
    WHERE (factory, ws_no, op_cd, prod_seq) IN (
        SELECT p.factory, p.ws_no, p.op_cd, MAX(p.prod_seq)
        FROM pcc.pcc_prod_scan AS p
        WHERE p.prod_ymd = TO_CHAR(NOW(), 'YYYYMMDD')
        GROUP BY p.factory, p.ws_no, p.op_cd
    )
    AND prod_status = 'C'
    GROUP BY factory, op_cd;
"""

# ────────────────────────────────────────────────────────────────────────────
# 3️⃣ 데이터 병합 및 정리 (Merge & Clean Data)
# ────────────────────────────────────────────────────────────────────────────
def merge_results(**kwargs):
    ti = kwargs['ti']

    master_data = ti.xcom_pull(task_ids='fetch_master_data', key='master_data') or []
    plan_data = ti.xcom_pull(task_ids='fetch_plan_data', key='plan_data') or []
    production_data = ti.xcom_pull(task_ids='fetch_production_data', key='production_data') or []

    if not master_data:
        logger.warning("❌ Warning: Master Data is empty!")
    if not plan_data:
        logger.warning("❌ Warning: Plan Data is empty!")
    if not production_data:
        logger.warning("❌ Warning: Production Data is empty!")

    plan_dict = { (str(row[0]).strip(), str(row[1]).strip()): (row[2] or 0, row[3] or 0.0) for row in plan_data }
    production_dict = { (str(row[0]).strip(), str(row[1]).strip()): (row[2] or 0, row[3] or 0.0) for row in production_data }

    cleaned_results = [
        (
            str(factory).strip(),
            str(op_cd).strip(),
            str(op_name or "").strip(),
            str(op_local_name or "").strip(),
            int(plan_dict.get((factory, op_cd), (0, 0.0))[0]),  # plancnt
            float(plan_dict.get((factory, op_cd), (0, 0.0))[1]),  # planqty
            int(production_dict.get((factory, op_cd), (0, 0.0))[0]),  # prodcnt
            float(production_dict.get((factory, op_cd), (0, 0.0))[1]),  # prodqty
            round(
                (float(production_dict.get((factory, op_cd), (0, 0.0))[1]) / float(plan_dict.get((factory, op_cd), (0, 1))[1])) * 100, 2
            ) if float(plan_dict.get((factory, op_cd), (0, 1))[1]) > 0 else 0  # rate
        )
        for factory, op_cd, op_name, op_local_name in master_data
    ]

    logger.info(f"✅ Final Merged & Cleaned Results (Sample): {cleaned_results[:5]} ... (Total: {len(cleaned_results)})")
    ti.xcom_push(key='final_result', value=cleaned_results)

# ────────────────────────────────────────────────────────────────────────────
# 4️⃣ DAG 정의 (DAG Definition & Task Dependencies)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hq_production_daily_status_update",
    default_args=default_args,
    description="HQ Daily Status",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["HQ", "Production"]
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

    task_fetch_master_data = PythonOperator(
        task_id="fetch_master_data",
        python_callable=postgres_helper_v1.execute_query,
        op_args=[SQL_MASTER_DATA, "fetch_master_data", "master_data"],
    )

    task_fetch_plan_data = PythonOperator(
        task_id="fetch_plan_data",
        python_callable=postgres_helper_v1.execute_query,
        op_args=[SQL_PLAN_DATA, "fetch_plan_data", "plan_data"],
    )

    task_fetch_production_data = PythonOperator(
        task_id="fetch_production_data",
        python_callable=postgres_helper_v1.execute_query,
        op_args=[SQL_PRODUCTION_DATA, "fetch_production_data", "production_data"],
    )

    task_merge_results = PythonOperator(
        task_id="merge_results",
        python_callable=merge_results,
    )

    task_insert_results = PythonOperator(
        task_id="insert_results",
        python_callable=lambda **kwargs: postgres_helper_v2.insert_data(
            SCHEMA_NAME, TABLE_NAME,
            [tuple(row) for row in (kwargs["ti"].xcom_pull(task_ids="merge_results", key="final_result") or [])]
        )
    )

    (
        task_check_table
        >> task_clean_table
        >> [task_fetch_master_data, task_fetch_plan_data, task_fetch_production_data]
        >> task_merge_results
        >> task_insert_results
    )