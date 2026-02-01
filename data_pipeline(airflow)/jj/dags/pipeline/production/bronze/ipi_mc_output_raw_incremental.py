import logging
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.mssql_hook import MSSQLHelper
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.production.bronze.common.ipi_mc_output_raw_common import (
    parse_datetime,
    extract_data,
    load_data,
    update_variable,
    INDO_TZ,
    MSSQL_CONN_ID,
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
INCREMENT_KEY = "last_extract_time_ipi_mc_output_raw"

# ────────────────────────────────────────────────────────────────
# 2️⃣ Daily Incremental Collection
# ────────────────────────────────────────────────────────────────
def daily_incremental_collection_task(**kwargs) -> dict:
    """매일 최신 데이터만 수집하는 태스크"""
    # 마지막 추출 시간을 기준으로 다음 날 데이터 수집
    last_extract_time_str = Variable.get(INCREMENT_KEY, default_var=None)
    
    if last_extract_time_str:
        # 마지막 추출 시간을 파싱
        last_extract_time = parse_datetime(last_extract_time_str)
        
        # 마지막 추출 시간의 다음 날 00:00:00부터 23:59:59까지
        start_date = last_extract_time.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        end_date = start_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        logging.info(f"마지막 추출 시간: {last_extract_time_str}")
        logging.info(f"다음 날 데이터 수집: {start_date.strftime('%Y-%m-%d')}")
    else:
        # Variable이 없으면 어제 데이터 수집
        yesterday = datetime.now(INDO_TZ) - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        logging.info(f"Variable이 없어서 어제 데이터 수집: {yesterday.strftime('%Y-%m-%d')}")
    
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"📅 데이터 수집 시작: {start_str} ~ {end_str}")
    logging.info(f"📊 처리 날짜: {start_date.strftime('%Y-%m-%d')}")
    
    try:
        mssql = MSSQLHelper(conn_id=MSSQL_CONN_ID)
        pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
        
        # 데이터 추출 및 적재
        data, row_count = extract_data(mssql, start_str, end_str)
        
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
    except Exception as e:
        # 연결 실패 등 네트워크 오류인 경우 Skip 처리
        error_str = str(e)
        error_lower = error_str.lower()
        is_connection_error = (
            "connection" in error_lower or
            "timeout" in error_lower or
            "timed out" in error_lower or
            "connection reset" in error_lower or
            "reset by peer" in error_lower or
            "errno" in error_lower or
            "network" in error_lower or
            "could not connect" in error_lower or
            "unable to connect" in error_lower or
            "login failed" in error_lower or
            "server is not found" in error_lower
        )
        
        if is_connection_error:
            logging.warning(f"⚠️ 연결 실패: {error_str} - 태스크 Skip")
            # Skip 전에 Variable 업데이트 (연결 실패해도 시간은 업데이트하여 다음 실행 시 올바른 시점부터 재시도)
            try:
                update_variable(INCREMENT_KEY, end_str)
                logging.info(f"✅ Variable '{INCREMENT_KEY}' 업데이트 (연결 실패로 Skip): {end_str}")
            except Exception as var_err:
                logging.warning(f"⚠️ Variable 업데이트 실패 (무시): {var_err}")
            
            skip_msg = (
                f"⏭️ IPI MC Output ETL 중 연결 불가 - 태스크 Skip\n"
                f"원인: {error_str}\n"
                f"설명: 소스 또는 타겟 데이터베이스 연결이 불가능합니다.\n"
                f"      Variable은 업데이트되었으므로 다음 실행 시 재시도됩니다."
            )
            logging.warning(skip_msg)
            raise AirflowSkipException(skip_msg) from e
        
        # 그 외 오류는 그대로 raise
        logging.error(f"❌ IPI MC Output ETL 실패: {e}")
        raise

# ────────────────────────────────────────────────────────────────
# 3️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ipi_mc_output_raw_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # 매일 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["CKP","IP","raw", "bronze layer", "incremental", "production"]
) as dag:
    
    daily_collection = PythonOperator(
        task_id="daily_incremental_collection",
        python_callable=daily_incremental_collection_task,
        provide_context=True,
    )
    
    daily_collection
