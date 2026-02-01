"""
OS BANB Machine Master Silver Layer Incremental ETL DAG (Bronze → Silver)
==========================================================================
Bronze 레이어에서 Silver 레이어로 특정 설비의 Machine Master 데이터를 전처리하여 적재

Source: bronze.mch_machine_raw
Target: silver.os_banb_mch_machine
Filter: MACH_ID IN ('3110COP00009', '3110COP00001', '3110COP00015')
Schedule: Daily at 3 AM
"""

import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.maintenance.silver.common.os_banb_mch_machine_common import (
    parse_datetime,
    extract_and_transform_data,
    prepare_insert_data,
    load_data_to_silver,
    update_variable,
    INDO_TZ,
    POSTGRES_CONN_ID,
    TARGET_MACH_IDS
)

# ────────────────────────────────────────────────────────────────
# 1️⃣ Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)
}

# Database Configuration
INCREMENT_KEY = "last_extract_time_os_banb_mch_machine"

# ────────────────────────────────────────────────────────────────
# 2️⃣ Main Processing Functions
# ────────────────────────────────────────────────────────────────
def incremental_silver_etl_task(**kwargs) -> dict:
    """Main incremental Silver ETL task"""
    postgres = PostgresHelper(conn_id=POSTGRES_CONN_ID)
    
    # Get last extract time from variable
    last_extract_time = Variable.get(INCREMENT_KEY, default_var=None)
    if not last_extract_time:
        # First run: use a default start date
        start_date = datetime(2015, 1, 1, 0, 0, 0, tzinfo=INDO_TZ)
        logging.info(f"⚠️ Variable 없음. 초기 시작 날짜 사용: {start_date}")
    else:
        start_date = parse_datetime(last_extract_time)
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=INDO_TZ)
        logging.info(f"이전 추출 시간: {start_date}")
    
    # Normalize start_date to next day 00:00:00
    start_date = (start_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Calculate end date (전일 23:59:59)
    end_date = (datetime.now(INDO_TZ) - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
    
    # Check if start_date exceeds end_date
    if start_date >= end_date:
        logging.info(f"⚠️ 처리할 데이터 없음: start_date({start_date}) >= end_date({end_date})")
        return {
            "start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": end_date.strftime("%Y-%m-%d %H:%M:%S"),
            "extracted_count": 0,
            "loaded_count": 0,
            "target_mach_ids": TARGET_MACH_IDS
        }
    
    # Extract and transform data from Bronze
    extract_time = datetime.now(INDO_TZ)
    data, extracted_count = extract_and_transform_data(
        postgres, 
        start_date.strftime("%Y-%m-%d %H:%M:%S"),
        end_date.strftime("%Y-%m-%d %H:%M:%S")
    )
    
    # Load data to Silver if any exists
    loaded_count = 0
    if extracted_count > 0:
        ingest_time = datetime.now(INDO_TZ)
        prepared_data = prepare_insert_data(data, extract_time, ingest_time)
        loaded_count = load_data_to_silver(postgres, prepared_data)
    
    # Update variable for next run (전일 23:59:59)
    update_variable(INCREMENT_KEY, end_date.strftime("%Y-%m-%d %H:%M:%S"))
    
    logging.info(f"✅ Silver ETL 완료")
    logging.info(f"📊 추출: {extracted_count:,}건, 적재: {loaded_count:,}건")
    logging.info(f"🕐 다음 시작 시간: {end_date}")
    
    return {
        "start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": end_date.strftime("%Y-%m-%d %H:%M:%S"),
        "extracted_count": extracted_count,
        "loaded_count": loaded_count,
        "target_mach_ids": TARGET_MACH_IDS
    }

# ────────────────────────────────────────────────────────────────
# 6️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
dag = DAG(
    'os_banb_mch_machine_incremental',
    default_args=DEFAULT_ARGS,
    description='OS BANB Machine Master Silver Layer Incremental ETL (Bronze → Silver)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['JJ', 'maintenance', 'silver layer', 'incremental', 'machine', 'OS', 'Banbury']
)

# Task definition
silver_etl_task = PythonOperator(
    task_id='incremental_silver_etl',
    python_callable=incremental_silver_etl_task,
    dag=dag
)
