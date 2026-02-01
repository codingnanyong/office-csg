import sys
import logging
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
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

CONN_ID_v1 = "pg_hq_iot_workshop"  
CONN_ID_v2 = "pg_fdw_v1_iot"  

EXTRACT_SCHEMA_NAME = "public"
LOAD_SCHEMA_NAME = "workshop"  
TARGET_TABLE_NAME = "temperature"

postgres_helper_v1 = PostgresHelper(CONN_ID_v1)  
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)  

# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ 마지막 적재 시간 가져오기 (Airflow Variables 사용)
# ────────────────────────────────────────────────────────────────────────────
def get_last_ingested_time():
    try:
        last_ingested_time = Variable.get("temperature_raw_last_ingested_time", default_var="2024-06-26 00:00:00")
        logger.info(f"✅ Retrieved last_ingested_time from Airflow Variables: {last_ingested_time}")
    except Exception as e:
        last_ingested_time = "2025-07-01 00:00:00"
        logger.warning(f"🚀 Variable not found. Using default: {last_ingested_time}")

    return last_ingested_time

# ────────────────────────────────────────────────────────────────────────────
# 3️⃣ 증분 데이터 조회 및 XCom 저장 (데이터 없을 때 기본값 저장)
# ────────────────────────────────────────────────────────────────────────────
def fetch_incremental_temperature(**kwargs):

    ti = kwargs["ti"]
    last_ingested_time = get_last_ingested_time()

    # last_ingested_time을 파싱하고 30분을 더해서 조회
    # 시간대 정보를 유지하면서 계산
    if '+' in last_ingested_time or 'Z' in last_ingested_time:
        if 'Z' in last_ingested_time:
            last_time = datetime.fromisoformat(last_ingested_time.replace('Z', '+00:00'))
        else:
            last_time = datetime.fromisoformat(last_ingested_time)
    else:
        last_time = datetime.fromisoformat(last_ingested_time + '+09:00')
    
    # 조회 시작 시간은 last_ingested_time, 종료 시간은 30분 후
    start_time = last_time
    end_time = last_time + timedelta(minutes=30)
    
    logger.info(f"📥 Fetching incremental temperature data from `{EXTRACT_SCHEMA_NAME}.temperature` since {start_time.strftime('%Y-%m-%d %H:%M:%S%z')} to {end_time.strftime('%Y-%m-%d %H:%M:%S%z')}...")

    query = f"""
        SELECT ymd, hmsf, sensor_id, device_id, capture_dt, t1, t2, t3
        FROM {EXTRACT_SCHEMA_NAME}.temperature
        WHERE capture_dt >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AT TIME ZONE 'Asia/Seoul'
        AND capture_dt < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}' AT TIME ZONE 'Asia/Seoul'
        ORDER BY capture_dt ASC;
    """

    records = postgres_helper_v1.execute_query(query, task_id="fetch_temperature", xcom_key="temperature_data", **kwargs)

    if records:
        current_time = datetime.now().isoformat()
        
        processed_records = [
            (ymd, hmsf, sensor_id, device_id, capture_dt.isoformat(), t1, t2, t3, 
             'N',  # upload_yn
             None,  # upload_dt
             current_time,  # extract_time - 현재 task 동작 시점
             None   # transform_time
             # load_time은 DEFAULT CURRENT_TIMESTAMP이므로 자동으로 설정됨
            ) 
            for ymd, hmsf, sensor_id, device_id, capture_dt, t1, t2, t3 in records
        ]

        logger.info(f"✅ Retrieved {len(records)} records from `{EXTRACT_SCHEMA_NAME}.temperature`.")

        next_start_time = end_time

        logger.info(f"✅ Storing last_ingested_time in XCom: {next_start_time}")
        ti.xcom_push(key="incremental_temperature_data", value=processed_records)
        ti.xcom_push(key="last_ingested_time", value=str(next_start_time))
    else:
        logger.warning("⚠️ No new incremental data found.")
        ti.xcom_push(key="incremental_temperature_data", value=[])  
        ti.xcom_push(key="last_ingested_time", value=last_ingested_time)

# ────────────────────────────────────────────────────────────────────────────
# 4️⃣ last_ingested_time Airflow Variable 업데이트
# ────────────────────────────────────────────────────────────────────────────
def update_last_ingested_time(**kwargs):
    ti = kwargs["ti"]
    latest_time = ti.xcom_pull(task_ids="fetch_temperature", key="last_ingested_time")

    if not latest_time:
        logger.warning("⚠️ No update for last_ingested_time in Airflow Variables.")
        return

    logger.info(f"✅ Updating last_ingested_time in Airflow Variables: {latest_time}")

    try:
        Variable.set("temperature_raw_last_ingested_time", latest_time)
        logger.info("✅ Successfully updated last_ingested_time in Airflow Variables.")
    except Exception as e:
        logger.error(f"❌ Failed to update last_ingested_time: {str(e)}")
        
# ────────────────────────────────────────────────────────────────────────────
# 5️⃣ DAG 정의 (DAG Definition & Task Dependencies)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hq_iot_temperature_ingest_raw",
    default_args=default_args,
    description="HD IoT Temperature Raw Data Ingest Every 30 Minutes",
    schedule_interval="*/30 * * * *",  
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["HQ","IoT","Raw"],
) as dag:

    task_fetch_temperature = PythonOperator(
        task_id="fetch_temperature",
        python_callable=fetch_incremental_temperature,
        trigger_rule="all_success",
    )

    task_insert_temperature = PythonOperator(
        task_id="insert_temperature_data",
        python_callable=lambda ti: postgres_helper_v2.insert_data(
            LOAD_SCHEMA_NAME,
            TARGET_TABLE_NAME,
            [tuple(row) for row in (ti.xcom_pull(task_ids="fetch_temperature", key="incremental_temperature_data") or [])],
            conflict_columns=None  # Raw 데이터는 중복 제거 없이 모든 데이터 저장
        ),
        trigger_rule="all_success",
    )

    task_update_last_ingested_time = PythonOperator(
        task_id="update_last_ingested_time",
        python_callable=update_last_ingested_time,
        trigger_rule="all_success",
    )

    trigger_iot_temperature_ingest = TriggerDagRunOperator(
        task_id="trigger_iot_temperature_ingest",
        trigger_dag_id="hq_iot_temperature_ingest",
    )

    task_fetch_temperature >> task_insert_temperature >> task_update_last_ingested_time >> trigger_iot_temperature_ingest
