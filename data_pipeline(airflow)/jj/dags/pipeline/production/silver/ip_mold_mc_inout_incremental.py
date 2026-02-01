"""
IP Mold MC In/Out Daily Silver Incremental DAG

이 DAG는 매일 실행되어 어제의 IP Mold MC In/Out 데이터를 Bronze에서 Silver 레이어로 변환합니다.

실행 스케줄: 매일 00:10 UTC (한국시간 09:10)
처리 데이터: 어제 00:00:00 ~ 23:59:59 데이터
데이터 소스: bronze.jmm_mold_mc_inout_raw (WH_ID = 'IP')
데이터 대상: silver.ip_mold_mc_inout

주요 특징:
- 매일 어제 데이터를 안정적으로 처리
- 중복 데이터 자동 제거
- 에러 발생 시 재시도 (1회)
- SLA: 2시간
"""

import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.production.silver.common.ip_mold_mc_inout_common import (
    parse_datetime,
    extract_silver_data,
    load_silver_data,
    update_variable,
    INDO_TZ,
    POSTGRES_CONN_ID,
    SILVER_SCHEMA,
    SILVER_TABLE_NAME
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
INCREMENT_KEY = "last_extract_time_silver_ip_mold_mc_inout"  # Daily 실행 시 마지막 처리 날짜 저장

# ────────────────────────────────────────────────────────────────
# 2️⃣ Daily Incremental Collection
# ────────────────────────────────────────────────────────────────
def daily_silver_incremental_task(**kwargs) -> dict:
    """매일 어제 데이터를 Silver로 변환하는 태스크 (Daily 실행)"""
    try:
        pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
        
        # 항상 어제 데이터를 처리 (Daily 실행 기준)
        today_kst = datetime.now(INDO_TZ)
        yesterday = today_kst - timedelta(days=1)
        
        # 어제 00:00:00 ~ 23:59:59 데이터 처리
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        logging.info(f"📅 Daily Silver 데이터 처리 시작")
        logging.info(f"📊 처리 날짜: {start_str} (어제 데이터)")
        logging.info(f"⏰ 처리 시간 범위: {start_date.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"🌏 현재 시간 (KST): {today_kst.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Bronze에서 Silver로 데이터 변환 및 적재
        data, row_count = extract_silver_data(pg, start_str, end_str)
        
        if row_count > 0:
            extract_time = datetime.utcnow()
            load_silver_data(pg, data, extract_time, silver_schema=SILVER_SCHEMA, silver_table_name=SILVER_TABLE_NAME, remove_duplicates=True)
            logging.info(f"✅ Daily Silver 데이터 처리 완료: {row_count} rows")
            
            # Variable 업데이트 (마지막 처리 날짜 기록)
            update_variable(INCREMENT_KEY, end_str)
            
            return {
                "status": "daily_silver_incremental_completed",
                "date": start_date.strftime("%Y-%m-%d"),
                "rows_processed": row_count,
                "start_time": start_str,
                "end_time": end_str,
                "extract_time": extract_time.isoformat(),
                "execution_date": today_kst.strftime("%Y-%m-%d %H:%M:%S")
            }
        else:
            logging.info(f"⚠️ 처리할 데이터가 없습니다: {start_str}")
            
            # Variable 업데이트 (데이터가 없어도 날짜는 업데이트)
            update_variable(INCREMENT_KEY, end_str)
            
            return {
                "status": "daily_silver_incremental_completed_no_data",
                "date": start_date.strftime("%Y-%m-%d"),
                "rows_processed": 0,
                "start_time": start_str,
                "end_time": end_str,
                "message": "처리할 데이터가 없음",
                "execution_date": today_kst.strftime("%Y-%m-%d %H:%M:%S")
            }
            
    except Exception as e:
        logging.error(f"❌ Daily Silver 데이터 처리 중 오류 발생: {str(e)}")
        raise e

# ────────────────────────────────────────────────────────────────
# 3️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ip_mold_mc_inout_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # 매일 00:10 UTC (한국시간 09:10) 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,  # 동시 실행 방지
    description="IP Mold MC In/Out 데이터를 매일 Silver 레이어로 변환하는 DAG",
    tags=["CKP","IP", "clean", "silver layer", "incremental", "production", "daily", "IP", "mold"]
) as dag:
    
    daily_silver_incremental = PythonOperator(
        task_id="daily_silver_incremental",
        python_callable=daily_silver_incremental_task,
        provide_context=True,
    )
    
    daily_silver_incremental
