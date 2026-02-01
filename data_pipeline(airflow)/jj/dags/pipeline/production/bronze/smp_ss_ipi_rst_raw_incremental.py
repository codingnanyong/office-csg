import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.production.bronze.common.smp_ss_ipi_rst_raw_common import (
    parse_datetime,
    extract_data,
    load_data,
    update_variable,
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
INCREMENT_KEY = "last_extract_time_smp_ss_ipi_rst_raw"

# ────────────────────────────────────────────────────────────────
# 2️⃣ Daily Incremental Collection
# ────────────────────────────────────────────────────────────────
def daily_incremental_collection_task(**kwargs) -> dict:
    """06:30 ~ 다음날 06:30 스케줄로 데이터 수집하는 태스크"""
    oracle = OracleHelper(conn_id=ORACLE_CONN_ID)
    pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
    
    # 마지막 추출 시간을 기준으로 다음 기간 데이터 수집
    last_extract_time_str = Variable.get(INCREMENT_KEY, default_var=None)
    
    if last_extract_time_str:
        # 마지막 추출 시간을 파싱
        last_extract_time = parse_datetime(last_extract_time_str)
        
        # 기존 스케줄(23:59:59로 끝나는 경우)에서 새 스케줄(06:30)로 전환하는 경우
        if last_extract_time.hour == 23 and last_extract_time.minute == 59 and last_extract_time.second == 59:
            # 다음날 00:00:00부터 그 다음날 06:30:00까지 (첫 번째 전환)
            start_date = last_extract_time.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            end_date = start_date.replace(hour=6, minute=30, second=0, microsecond=0) + timedelta(days=1)
            logging.info(f"🔄 스케줄 전환: 마지막 추출 시간 {last_extract_time_str} → 새 스케줄 적용")
            logging.info(f"📅 첫 전환 수집 기간: {start_date.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            # 정상 스케줄: 마지막 추출 시간의 다음 06:30:01부터 다음날 06:30:00까지
            # 마지막 추출 시간이 06:30:00이면, 그 날짜의 06:30:01부터 시작
            if last_extract_time.hour == 6 and last_extract_time.minute == 30 and last_extract_time.second == 0:
                # 같은 날짜의 06:30:01부터 다음날 06:30:00까지
                start_date = last_extract_time.replace(second=1, microsecond=0)
                end_date = start_date.replace(hour=6, minute=30, second=0, microsecond=0) + timedelta(days=1)
            else:
                # 마지막 추출 시간의 날짜 기준으로 다음 06:30:01 설정
                base_date = last_extract_time.replace(hour=6, minute=30, second=1, microsecond=0)
                # 마지막 추출 시간이 이미 06:30:01 이후라면 다음날로
                if (last_extract_time.hour > 6) or (last_extract_time.hour == 6 and last_extract_time.minute > 30) or (last_extract_time.hour == 6 and last_extract_time.minute == 30 and last_extract_time.second >= 1):
                    start_date = base_date + timedelta(days=1)
                else:
                    start_date = base_date
                
                # 종료 시간: 시작 날짜의 다음날 06:30:00
                end_date = start_date.replace(hour=6, minute=30, second=0, microsecond=0) + timedelta(days=1)
            
            logging.info(f"마지막 추출 시간: {last_extract_time_str}")
            logging.info(f"📅 수집 기간: {start_date.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        # Variable이 없으면 어제 06:30:00부터 오늘 06:30:00까지 수집
        yesterday = datetime.now(INDO_TZ) - timedelta(days=1)
        start_date = yesterday.replace(hour=6, minute=30, second=0, microsecond=0)
        end_date = datetime.now(INDO_TZ).replace(hour=6, minute=30, second=0, microsecond=0)
        logging.info(f"Variable이 없어서 어제 06:30부터 오늘 06:30까지 수집: {start_date.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    
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
        
        # Variable 업데이트 (종료 시간을 06:30:00으로 설정)
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
        
        # Variable 업데이트 (데이터가 없어도 시간은 업데이트, 종료 시간을 06:30:00으로 설정)
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
    dag_id="smp_ss_ipi_rst_raw_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # 매일 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ","raw", "IP","IPI", "bronze layer", "incremental", "production"]
) as dag:
    
    daily_collection = PythonOperator(
        task_id="daily_incremental_collection",
        python_callable=daily_incremental_collection_task,
        provide_context=True,
    )
    
    daily_collection
