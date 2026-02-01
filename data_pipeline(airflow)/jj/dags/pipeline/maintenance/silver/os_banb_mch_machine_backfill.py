"""
OS BANB Machine Master Silver Layer Backfill DAG (Bronze → Silver)
==================================================================
Bronze 레이어에서 Silver 레이어로 특정 설비의 Machine Master 데이터를 전처리하여 초기 적재

Source: bronze.mch_machine_raw
Target: silver.os_banb_mch_machine
Filter: MACH_ID IN ('3110COP00009', '3110COP00001', '3110COP00015')
Schedule: Manual trigger only
"""

import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.maintenance.silver.common.os_banb_mch_machine_common import (
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
def backfill_silver_etl_task(**kwargs) -> dict:
    """Main backfill Silver ETL task"""
    postgres = PostgresHelper(conn_id=POSTGRES_CONN_ID)
    
    start_time = datetime.now(INDO_TZ)
    logging.info("="*80)
    logging.info("🚀 OS BANB Machine Master Silver Backfill 시작")
    logging.info(f"시작 시간: {start_time}")
    logging.info(f"대상 설비: {TARGET_MACH_IDS}")
    logging.info("="*80)
    
    # Extract and transform all data from Bronze
    extract_time = datetime.now(INDO_TZ)
    data, extracted_count = extract_and_transform_data(postgres)
    
    # Load data to Silver if any exists
    loaded_count = 0
    if extracted_count > 0:
        ingest_time = datetime.now(INDO_TZ)
        prepared_data = prepare_insert_data(data, extract_time, ingest_time)
        loaded_count = load_data_to_silver(postgres, prepared_data)
    
    # Set initial variable for incremental DAG (전일 23:59:59)
    end_time = (datetime.now(INDO_TZ) - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
    update_variable(INCREMENT_KEY, end_time.strftime("%Y-%m-%d %H:%M:%S"))
    
    elapsed_time = (datetime.now(INDO_TZ) - start_time).total_seconds()
    
    logging.info("="*80)
    logging.info(f"✅ Backfill 완료")
    logging.info(f"📊 추출: {extracted_count:,}건, 적재: {loaded_count:,}건")
    logging.info(f"⏱️ 소요 시간: {elapsed_time:.2f}초")
    logging.info(f"🕐 Variable 설정: {end_time}")
    logging.info("="*80)
    
    return {
        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "extracted_count": extracted_count,
        "loaded_count": loaded_count,
        "elapsed_seconds": elapsed_time,
        "target_mach_ids": TARGET_MACH_IDS
    }

# ────────────────────────────────────────────────────────────────
# 6️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
dag = DAG(
    'os_banb_mch_machine_backfill',
    default_args=DEFAULT_ARGS,
    description='OS BANB Machine Master Silver Layer Backfill (Bronze → Silver)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['JJ', 'maintenance', 'silver layer', 'backfill', 'machine', 'OS', 'Banbury']
)

# Task definition
backfill_task = PythonOperator(
    task_id='backfill_silver_etl',
    python_callable=backfill_silver_etl_task,
    dag=dag
)