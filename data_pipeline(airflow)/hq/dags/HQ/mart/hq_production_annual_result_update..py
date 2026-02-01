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
TABLE_NAME = "annual"

postgres_helper_v1 = PostgresHelper(CONN_ID_v1)
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)

# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ SQL 쿼리 정의 (Query Definitions)
# ────────────────────────────────────────────────────────────────────────────
SQL_SUMMARY_DATA = """
    WITH latest_bom AS (
        SELECT DISTINCT ON (factory, ws_no) 
            factory, 
            ws_no, 
            sample_qty, 
            sample_ets,
            upd_ymd
        FROM pcc.pcc_bom_head
        ORDER BY factory, ws_no, upd_ymd DESC  
    ),
    yearly_bom AS (
        SELECT 
            SUBSTRING(ws_no FROM 3 FOR 4)::INTEGER AS year,  
            factory,
            sample_qty,
            sample_ets
        FROM latest_bom
    ),
    filtered_bom AS (
        SELECT *
        FROM yearly_bom
        WHERE sample_ets::TEXT BETWEEN 
            TO_CHAR(MAKE_DATE(year, 1, 1), 'YYYYMMDD') 
            AND TO_CHAR(MAKE_DATE(year, 12, 31), 'YYYYMMDD')
    ),
    latest_prod_scan AS (
        SELECT DISTINCT ON (ws_no) 
            ws_no, 
            factory,
            TO_CHAR(prod_ymd::DATE, 'YYYY')::INTEGER AS year,
            SUM(prod_qty) AS total_prod_qty  
        FROM pcc.pcc_prod_scan
        WHERE prod_status = 'C'
        GROUP BY ws_no, factory, prod_ymd
        ORDER BY ws_no, prod_ymd::DATE DESC  
    ),
    plan_summary AS (
        SELECT 
            year, 
            factory, 
            COUNT(*) AS plan_cnt, 
            ROUND(SUM(sample_qty)) AS plan_total_qty
        FROM filtered_bom
        GROUP BY year, factory
    ),
    prod_summary AS (
        SELECT 
            year, 
            factory, 
            COUNT(*) AS prod_cnt,
            SUM(total_prod_qty) AS prod_total_qty  
        FROM latest_prod_scan
        GROUP BY year, factory
    )
    SELECT 
        COALESCE(p.year, s.year) AS year,
        COALESCE(p.factory, s.factory) AS factory,
        COALESCE(p.plan_cnt, 0) AS plan_cnt,
        COALESCE(CAST(p.plan_total_qty AS INTEGER), 0) AS plan_total_qty,  -- NUMERIC → INTEGER 변환
        COALESCE(s.prod_cnt, 0) AS prod_cnt,
        COALESCE(CAST(ROUND(s.prod_total_qty, 0) AS INTEGER), 0) AS prod_total_qty  -- NUMERIC → INTEGER 변환
    FROM plan_summary p
    FULL OUTER JOIN prod_summary s 
    ON p.year = s.year AND p.factory = s.factory
    WHERE COALESCE(p.factory, s.factory) = 'DS'  -- NULL 방지
    ORDER BY year, factory;
    """
# ────────────────────────────────────────────────────────────────────────────
# 3️⃣ DAG 정의 (DAG Definition & Task Dependencies)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hq_production_annual_result_update",
    default_args=default_args,
    description="HQ Facatory Annual Summary ETL Daily",
    schedule= "@daily",
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

    task_fetch_annual_data = PythonOperator(
        task_id="fetch_annual_data",
        python_callable=postgres_helper_v1.execute_query,
        op_args=[SQL_SUMMARY_DATA, "fetch_annual_data", "annual_data"],
    )

    task_insert_results = PythonOperator(
        task_id="insert_results",
        python_callable=lambda **kwargs: postgres_helper_v2.insert_data(
            SCHEMA_NAME, TABLE_NAME,
            [tuple(row) for row in (kwargs["ti"].xcom_pull(task_ids="fetch_annual_data", key="annual_data") or [])]
        )
    )

    (
        task_check_table
        >> task_clean_table
        >> task_fetch_annual_data
        >> task_insert_results
    )