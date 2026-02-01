import sys
import logging
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper
from plugins.hooks.mysql_hook import MySQLHelper

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

CONN_ID_v1 = "mdb_hq_rtls"  
CONN_ID_v2 = "pg_fdw_v1_hq"

EXTRACT_SCHEMA_NAME = "dskorea"
LOAD_SCHEMA_NAME = "rtls"  
TARGET_TABLE_NAME = "at_tag_xy"

mysql_helper = MySQLHelper(CONN_ID_v1)
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)  

# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ 마지막 적재 시간 가져오기 (Airflow Variables 사용)
# ────────────────────────────────────────────────────────────────────────────
def get_last_ingested_time():
    try:
        last_ingested_time = Variable.get("epaper_raw_last_ingested_time", default_var="2024-06-26 00:00:00")
        logger.info(f"✅ Retrieved last_ingested_time from Airflow Variables: {last_ingested_time}")
    except Exception as e:
        last_ingested_time = "2025-04-24 00:00:00"
        logger.warning(f"🚀 Variable not found. Using default: {last_ingested_time}")

    return last_ingested_time

# ────────────────────────────────────────────────────────────────────────────
# 3️⃣ 증분 데이터 조회 및 XCom 저장 (데이터 없을 때 기본값 저장)
# ────────────────────────────────────────────────────────────────────────────
def fetch_incremental_epaper(**kwargs):
    ti = kwargs["ti"]
    last_ingested_time = ti.xcom_pull(task_ids="get_last_ingested_time", key="last_ingested_time")
    logger.info(f"📥 Fetching incremental epaper data from `{EXTRACT_SCHEMA_NAME}.at_tag_xy` since {last_ingested_time}...")    

    query = f"""
        SELECT tagid, pos_ts, event_Id, OWNER_TAG_NAME, position_xy, zone_id, zone, zone_before, coordi_sys, send_yn, IN_DATE, MOD_DATE
        FROM {EXTRACT_SCHEMA_NAME}.at_tag_xy
        WHERE IN_DATE >= '{last_ingested_time}'
        ORDER BY IN_DATE ASC;
    """

    records = mysql_helper.execute_query(query, task_id="fetch_epaper", xcom_key="epaper_data", **kwargs)

    if records:
        # 현재 시간을 추출 시점으로 설정
        current_time = datetime.now().isoformat()
        
        processed_records = [
            (tagid, 
             datetime.fromtimestamp(pos_ts / 1000).isoformat() if pos_ts else None,  # pos_ts를 timestamp로 변환 (밀리초 → 초)
             event_Id, OWNER_TAG_NAME, 
             position_xy.replace(',', '/') if position_xy and position_xy != 'N/A' else position_xy,  # position_xy 형식 변환: 17.83,87.53 → 17.83/87.53
             zone_id, zone, zone_before, coordi_sys, send_yn, IN_DATE.isoformat() if IN_DATE else None, MOD_DATE.isoformat() if MOD_DATE else None,
             current_time,  # extract_time - 현재 task 동작 시점
             None   # transform_time
             # load_time은 DEFAULT CURRENT_TIMESTAMP이므로 자동으로 설정됨
            ) 
            for tagid, pos_ts, event_Id, OWNER_TAG_NAME, position_xy, zone_id, zone, zone_before, coordi_sys, send_yn, IN_DATE, MOD_DATE in records
        ]

        logger.info(f"✅ Retrieved {len(records)} records from `{EXTRACT_SCHEMA_NAME}.at_tag_xy`.")
        ti.xcom_push(key="incremental_epaper_data", value=processed_records)
    else:
        logger.warning("⚠️ No new epaper data found.")
        ti.xcom_push(key="incremental_epaper_data", value=[])  

# ────────────────────────────────────────────────────────────────────────────
# 4️⃣ 마지막 적재 시간 업데이트
# ────────────────────────────────────────────────────────────────────────────
def update_last_ingested_time(**kwargs):
    ti = kwargs["ti"]
    latest_time = datetime.now().isoformat()
    logger.info(f"✅ Updating last_ingested_time in Airflow Variables: {latest_time}")
    Variable.set("epaper_raw_last_ingested_time", latest_time)
    logger.info("✅ Successfully updated last_ingested_time in Airflow Variables.")

# ────────────────────────────────────────────────────────────────────────────
# 5️⃣ DAG 정의 (DAG Definition & Task Dependencies)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hq_rtls_epaper_raw",
    default_args=default_args,
    description="HQ RTLS E-Paper Raw Data Ingest Every Minute",
    schedule_interval="*/5 * * * *",  
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["HQ","RTLS","Raw"],
) as dag:

    task_get_last_ingested_time = PythonOperator(
        task_id="get_last_ingested_time",
        python_callable=get_last_ingested_time,
        provide_context=True,
    )

    task_fetch_epaper = PythonOperator(
        task_id="fetch_epaper",
        python_callable=fetch_incremental_epaper,
        provide_context=True,
    )

    task_insert_epaper = PythonOperator(
        task_id="insert_epaper_data",   
        python_callable=lambda ti: postgres_helper_v2.insert_data(
            LOAD_SCHEMA_NAME,
            TARGET_TABLE_NAME,
            [tuple(row) for row in (ti.xcom_pull(task_ids="fetch_epaper", key="incremental_epaper_data") or [])],
            conflict_columns=None  # Raw 데이터는 중복 제거 없이 모든 데이터 저장
        ),
        trigger_rule="all_success",
    )

    task_update_last_ingested_time = PythonOperator(
        task_id="update_last_ingested_time",
        python_callable=update_last_ingested_time,
        provide_context=True,
    )

    task_get_last_ingested_time >> task_fetch_epaper >> task_insert_epaper >> task_update_last_ingested_time