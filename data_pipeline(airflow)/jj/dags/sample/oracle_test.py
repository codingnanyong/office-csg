import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.oracle_hook import OracleHelper

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def test_oracle_with_helper(**kwargs):
    try:
        with OracleHelper(conn_id="orc_jj_cmms") as helper:
            logging.info("🔍 Testing Oracle connection and query")

            if not helper.check_table("ICMMS", "BAS_DEFECTIVE"):
                raise Exception("Table does not exist")

            result = helper.execute_query(
                sql="SELECT * FROM ICMMS.BAS_DEFECTIVE WHERE ROWNUM = 1",
                task_id="test_oracle_with_helper",
                xcom_key="oracle_result",
                **kwargs
            )
            logging.info(f"✅ Query Result: {result}")

    except Exception as e:
        logging.error(f"❌ Error occurred during OracleHelper test: {str(e)}", exc_info=True)
        raise

with DAG(
    dag_id="oracle_con_test",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["oracle", "test"]
) as dag:

    task_oracle = PythonOperator(
        task_id="test_conn",
        python_callable=test_oracle_with_helper,
        provide_context=True
    )
