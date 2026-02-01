"""
MSPD PCARD Result Raw Incremental DAG
======================================
Oracle LMES.MSPD_PCARD_RESULT 테이블의 증분 데이터를 PostgreSQL로 수집하는 DAG

Source: Oracle LMES.MSPD_PCARD_RESULT
Target: PostgreSQL bronze.mspd_pcard_result_raw
Execution: Daily incremental collection
"""

import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.production.bronze.common.mspd_pcard_result_raw_common import (
    parse_datetime,
    extract_data,
    load_data,
    update_variable,
    _normalize_to_0630,
    INDO_TZ,
    ORACLE_CONN_ID,
    POSTGRES_CONN_ID
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
INCREMENT_KEY = "last_extract_time_mspd_pcard_result_raw"

# ────────────────────────────────────────────────────────────────
# 2️⃣ Daily Incremental Collection
# ────────────────────────────────────────────────────────────────
def daily_incremental_collection_task(**kwargs) -> dict:
    """매일 최신 데이터만 수집하는 태스크 (06:30 기준)"""
    oracle = OracleHelper(conn_id=ORACLE_CONN_ID)
    pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
    
    # 마지막 추출 시간을 기준으로 다음 날 데이터 수집 (06:30 기준)
    last_extract_time_str = Variable.get(INCREMENT_KEY, default_var=None)
    
    if last_extract_time_str:
        # 마지막 추출 시간을 파싱
        last_extract_time = parse_datetime(last_extract_time_str)
        
        # 마지막 처리 시간을 06:30으로 정규화하고, 다음날 06:30까지 처리
        start_date = _normalize_to_0630(last_extract_time)
        end_date = _normalize_to_0630(last_extract_time + timedelta(days=1))
        
        logging.info(f"마지막 추출 시간: {last_extract_time_str}")
        logging.info(f"다음 날 데이터 수집 (06:30 기준): {start_date.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        # Variable이 없으면 어제 06:30부터 오늘 06:30까지 데이터 수집
        yesterday = datetime.now(INDO_TZ) - timedelta(days=1)
        start_date = _normalize_to_0630(yesterday)
        end_date = _normalize_to_0630(yesterday + timedelta(days=1))
        logging.info(f"Variable이 없어서 어제 06:30부터 오늘 06:30까지 데이터 수집: {start_date.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"📅 데이터 수집 시작: {start_str} ~ {end_str}")
    logging.info(f"📊 처리 날짜: {start_date.strftime('%Y-%m-%d')}")
    
    # 데이터 추출 및 적재
    data, row_count = extract_data(oracle, start_str, end_str)
    
    if row_count > 0:
        extract_time = datetime.utcnow()
        load_data(pg, data, extract_time)
        logging.info(f"✅ 데이터 수집 완료: {row_count} rows")
        
        # Variable 업데이트
        update_variable(INCREMENT_KEY, end_str)
        
        return {
            "status": "daily_incremental_completed",
            "date": start_date.strftime("%Y-%m-%d"),
            "rows_processed": row_count,
            "start_time": start_str,
            "end_time": end_str,
            "extract_time": extract_time.isoformat()
        }
    else:
        logging.info(f"⚠️ 수집할 데이터가 없습니다: {start_str} ~ {end_str}")
        
        # Variable 업데이트 (데이터가 없어도 시간은 업데이트)
        update_variable(INCREMENT_KEY, end_str)
        
        return {
            "status": "daily_incremental_completed_no_data",
            "date": start_date.strftime("%Y-%m-%d"),
            "rows_processed": 0,
            "start_time": start_str,
            "end_time": end_str,
            "message": "수집할 데이터가 없음"
        }

# ────────────────────────────────────────────────────────────────
# 3️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="mspd_pcard_result_raw_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",  # TODO: 스케줄 설정 필요 (예: "0 1 * * *" - 매일 01:00)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "raw", "bronze layer", "incremental", "production", "daily"]
) as dag:
    
    daily_collection = PythonOperator(
        task_id="daily_incremental_collection",
        python_callable=daily_incremental_collection_task,
        provide_context=True,
    )
    
    daily_collection

