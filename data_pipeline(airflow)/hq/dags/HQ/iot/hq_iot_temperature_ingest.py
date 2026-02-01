import sys
import logging
import pendulum
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

CONN_ID_v1 = "pg_fdw_v1_iot"  
CONN_ID_v2 = "pg_fdw_v2_hq"  

EXTRACT_SCHEMA_NAME = "workshop"
LOAD_SCHEMA_NAME = "services"  
TARGET_TABLE_NAME = "temperature"

postgres_helper_v1 = PostgresHelper(CONN_ID_v1)  
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)  

# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ 마지막 적재 시간 가져오기 (기본값 설정)
# ────────────────────────────────────────────────────────────────────────────
def get_last_ingested_time():
    query = """
        SELECT last_ingested 
        FROM services.last_ingested_time
        WHERE table_name = 'temperature';
    """
    result = postgres_helper_v2.execute_query(query, task_id="get_last_ingested_time", xcom_key=None)

    if result and result[0][0]:
        last_ingested_time = result[0][0]
        logger.info(f"✅ Retrieved last_ingested_time from DB: {last_ingested_time}")
    else:
        last_ingested_time = "2024-06-26 00:00:00"
        logger.warning(f"🚀 No record found. Using default: {last_ingested_time}")

    return last_ingested_time

# ────────────────────────────────────────────────────────────────────────────
# 3️⃣ 증분 데이터 조회 및 XCom 저장 (데이터 없을 때 기본값 저장)
# ────────────────────────────────────────────────────────────────────────────
def fetch_incremental_temperature(**kwargs):

    ti = kwargs["ti"]
    last_ingested_time = get_last_ingested_time()

    logger.info(f"📥 Fetching incremental temperature data from `{EXTRACT_SCHEMA_NAME}.temperature` since {last_ingested_time}...")

    query = f"""
        WITH ranked_data AS (
            SELECT subt.ymd, subt.hmsf, subt.capture_dt, subt.sensor_id, subt.device_id, subt.t1, subt.t2,
                ROW_NUMBER() OVER (
                    PARTITION BY subt.sensor_id,
                    DATE_TRUNC('hour', subt.capture_dt) + 
                    CASE 
                        WHEN EXTRACT(MINUTE FROM subt.capture_dt) <= 0 THEN INTERVAL '0 minutes'  -- 00분 포함
                        WHEN EXTRACT(MINUTE FROM subt.capture_dt) < 30 THEN INTERVAL '0 minutes'
                        ELSE INTERVAL '30 minutes'
                    END
                    ORDER BY subt.capture_dt ASC
                ) AS rn
            FROM workshop.temperature AS subt
            WHERE subt.capture_dt >= '{last_ingested_time}'::TIMESTAMP
            AND subt.capture_dt < ('{last_ingested_time}'::TIMESTAMP + INTERVAL '30 minutes')
        )
        SELECT ymd, hmsf, sensor_id, device_id,capture_dt, t1, t2
        FROM ranked_data
        WHERE rn = 1
        ORDER BY sensor_id, capture_dt ASC;

    """

    records = postgres_helper_v1.execute_query(query, task_id="fetch_temperature", xcom_key="temperature_data", **kwargs)

    if records:
        processed_records = [
            (ymd, hmsf, sensor_id, device_id, capture_dt.isoformat(), t1, t2) 
            for ymd, hmsf, sensor_id, device_id, capture_dt, t1, t2 in records
        ]

        logger.info(f"✅ Retrieved {len(records)} records from `{EXTRACT_SCHEMA_NAME}.temperature`.")

        # last_ingested_time에 30분을 더한 값을 다음 시작 시간으로 설정
        if isinstance(last_ingested_time, str):
            next_start_time = datetime.fromisoformat(last_ingested_time.replace('Z', '+00:00')) + timedelta(minutes=30)
        else:
            next_start_time = last_ingested_time + timedelta(minutes=30)

        logger.info(f"✅ Storing last_ingested_time in XCom: {next_start_time}")
        ti.xcom_push(key="incremental_temperature_data", value=processed_records)
        ti.xcom_push(key="last_ingested_time", value=str(next_start_time))
    else:
        logger.warning("⚠️ No new incremental data found.")
        ti.xcom_push(key="incremental_temperature_data", value=[])  
        ti.xcom_push(key="last_ingested_time", value=last_ingested_time)  

# ────────────────────────────────────────────────────────────────────────────
# 4️⃣ last_ingested_time 테이블 업데이트
# ────────────────────────────────────────────────────────────────────────────
def update_last_ingested_time(**kwargs):
    ti = kwargs["ti"]
    latest_time = ti.xcom_pull(task_ids="fetch_temperature", key="last_ingested_time")

    if not latest_time:
        logger.warning("⚠️ No update for last_ingested_time in DB.")
        return

    logger.info(f"✅ Updating last_ingested_time in DB: {latest_time}")

    update_query = """
    UPDATE services.last_ingested_time
    SET last_ingested = %s
    WHERE table_name = 'temperature';
    """

    try:
        postgres_helper_v2.execute_update(update_query, task_id="update_last_ingested_time", parameters=(latest_time,))
        logger.info("✅ Successfully updated last_ingested_time in DB.")
    except Exception as e:
        logger.error(f"❌ Failed to update last_ingested_time: {str(e)}")
        
# ────────────────────────────────────────────────────────────────────────────
# 5️⃣ DAG 정의 (DAG Definition & Task Dependencies)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hq_iot_temperature_ingest",
    default_args=default_args,
    description="HD IoT Temperature Ingest Every 30 Minutes",
    schedule_interval=None,  
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["HQ","IoT"],
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
            conflict_columns=["sensor_id", "capture_dt"] if TARGET_TABLE_NAME == "temperature" else []
        ),
        trigger_rule="all_success",
    )

    task_update_last_ingested_time = PythonOperator(
        task_id="update_last_ingested_time",
        python_callable=update_last_ingested_time,
        trigger_rule="all_success",
    )

    task_fetch_temperature >> task_insert_temperature >> task_update_last_ingested_time
