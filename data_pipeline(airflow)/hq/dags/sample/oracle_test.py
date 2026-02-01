import os
import logging
import oracledb
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def test_oracle():
    logging.info("📂 Environmental Information")
    logging.info(f"LD_LIBRARY_PATH: {os.environ.get('LD_LIBRARY_PATH')}")
    logging.info(f"instantclient_23_7 exists: {os.path.exists('/opt/oracle/instantclient_23_7')}")
    logging.info(f"libclntsh.so exists: {os.path.exists('/opt/oracle/instantclient_23_7/libclntsh.so')}")

    try:
        logging.info("🔌 Oracle Client Initialize Start")
        oracledb.init_oracle_client(lib_dir="/opt/oracle/instantclient_23_7")
        logging.info("✅ Oracle Client Initialize Sucess")

        conn = oracledb.connect(
            user="LMES",
            password="{ORACLE_PASSWORD}",
            dsn="{ORACLE_HOST}/{ORACLE_SID}"
        )
        logging.info("✅ Oracle Connection Sucess")

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM LMES.MSPQ_EX_OSND_DETECT_LINE meodl WHERE ROWNUM = 1")
        result = cursor.fetchall()
        logging.info(f"✅ Query Result: {result}")

    except Exception as e:
        logging.error(f"❌ Error Occurred: {str(e)}", exc_info=True)
        raise

with DAG(
    dag_id="oracle_con_test",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["oracle","test"]
) as dag:

    task_oracle = PythonOperator(
        task_id="test_conn",
        python_callable=test_oracle
    )

    task_oracle
