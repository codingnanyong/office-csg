"""
Edge Test - OS Banbury HMI Eq1 query check
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.telemetry.bronze.common.os_banb_hmi_data_common import (
    build_extract_sql,
    EQUIPMENTS,
    INDO_TZ,
)
from plugins.hooks.mysql_hook import MySQLHelper


def check_os_banb_hmi_eq1_query(**_context):
    eq = EQUIPMENTS[0]
    logging.info(
        "Eq info: equipment_id=%s, equipment_value=%s, conn_id=%s",
        eq.get("equipment_id"),
        eq.get("equipment_value"),
        eq.get("conn_id"),
    )
    mysql = MySQLHelper(conn_id=eq["conn_id"])

    end_dt = datetime.now(INDO_TZ).replace(microsecond=0)
    start_dt = end_dt - timedelta(hours=1)

    base_sql = build_extract_sql(start_dt, end_dt, eq["equipment_value"])
    sql = f"SELECT * FROM ({base_sql}) AS t LIMIT 1"

    logging.info("Testing query for Eq%s (%s ~ %s)", eq["equipment_id"], start_dt, end_dt)
    logging.info("SQL:\n%s", sql)

    t0 = datetime.utcnow()
    try:
        rows = mysql.execute_query(sql, task_id="edge_test_os_banb_eq1_query", xcom_key=None)
    except Exception as e:
        elapsed = (datetime.utcnow() - t0).total_seconds()
        logging.error("Query failed after %.2fs: %s", elapsed, str(e))
        raise
    elapsed = (datetime.utcnow() - t0).total_seconds()

    row_count = len(rows) if rows is not None else 0
    logging.info("Query OK. Rows returned: %s (%.2fs)", row_count, elapsed)
    if rows:
        logging.info("First row: %s", rows[0])


with DAG(
    dag_id="edge_test_hmi_query",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["edge", "test", "hmi"],
) as dag:
    PythonOperator( 
        task_id="check_os_banb_hmi_eq1_query",
        python_callable=check_os_banb_hmi_eq1_query,
    )
