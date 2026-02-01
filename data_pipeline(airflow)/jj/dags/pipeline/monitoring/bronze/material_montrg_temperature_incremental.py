import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.monitoring.bronze.common.material_montrg_temperature_common import (
    get_company_start_date,
    extract_data,
    load_data,
    update_variable,
    INDO_TZ,
    TARGET_POSTGRES_CONN_ID,
    COMPANIES
)

# ────────────────────────────────────────────────────────────────
# 1️⃣ Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=1)
}

# ────────────────────────────────────────────────────────────────
# 2️⃣ Incremental Collection
# ────────────────────────────────────────────────────────────────

def process_company_incremental(company_cd: str, increment_key: str, source_conn_id: str) -> dict:
    """Process incremental collection for a single company"""
    
    # Get company-specific start time (use incremental mode with initial_date=True)
    start_time = get_company_start_date(increment_key, company_cd, initial_date=True)
    
    # Normalize start time to hour boundary
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=INDO_TZ)
    start_time = start_time.replace(minute=0, second=0, microsecond=0)

    current_time_indo = datetime.now(INDO_TZ)

    # Ensure we are not trying to collect a future window
    if start_time >= current_time_indo:
        start_time = (current_time_indo - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    end_time = (start_time + timedelta(hours=1)) - timedelta(seconds=1)

    # If the target window is still in progress, shift to the previous hour
    if current_time_indo <= end_time:
        start_time = (current_time_indo - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        end_time = (start_time + timedelta(hours=1)) - timedelta(seconds=1)

    if start_time > end_time:
        logging.info(f"⚠️ {company_cd} - 처리할 데이터 없음: start_time({start_time}) > end_time({end_time})")
        update_variable(increment_key, end_time.strftime("%Y-%m-%d %H:%M:%S"))
        return {
            "status": "skipped_no_data",
            "company_cd": company_cd,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "extracted_count": 0,
            "loaded_count": 0
        }

    start_str = start_time.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_time.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"📅 {company_cd} 데이터 수집:")
    logging.info(f"  - 현재 인도네시아 시간: {current_time_indo.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"  - 수집 범위: {start_str} ~ {end_str}")
    
    # 데이터베이스 연결
    try:
        source_pg = PostgresHelper(conn_id=source_conn_id)
        target_pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    except Exception as e:
        logging.error(f"❌ {company_cd} - 데이터베이스 연결 실패: {str(e)}")
        # 연결 실패해도 Variable 업데이트
        update_variable(increment_key, end_str)
        return {
            "status": "connection_failed",
            "company_cd": company_cd,
            "error": str(e),
            "message": f"{company_cd} 데이터베이스 연결 실패 - 서버가 꺼져있을 수 있습니다",
            "variable_updated": True
        }
    
    # 데이터 추출
    try:
        data, row_count = extract_data(source_pg, start_str, end_str, company_cd)
    except Exception as e:
        logging.error(f"❌ {company_cd} - 데이터 추출 실패: {str(e)}")
        # 추출 실패해도 Variable 업데이트
        update_variable(increment_key, end_str)
        return {
            "status": "extraction_failed",
            "company_cd": company_cd,
            "error": str(e),
            "message": f"{company_cd} 데이터 추출 실패 - 데이터베이스 연결 문제일 수 있습니다",
            "variable_updated": True
        }
    
    if row_count > 0:
        try:
            extract_time = datetime.utcnow()
            loaded_rows = load_data(target_pg, data, extract_time, company_cd)
            logging.info(f"✅ {company_cd} 데이터 수집 완료: {loaded_rows} rows")
            
            # Variable 업데이트
            update_variable(increment_key, end_str)
        except Exception as e:
            logging.error(f"❌ {company_cd} - 데이터 로딩 실패: {str(e)}")
            return {
                "status": "loading_failed",
                "company_cd": company_cd,
                "error": str(e),
                "message": f"{company_cd} 데이터 로딩 실패 - 타겟 데이터베이스 연결 문제일 수 있습니다"
            }
        
        return {
            "status": "incremental_completed",
            "company_cd": company_cd,
            "start_time": start_str,
            "end_time": end_str,
            "rows_processed": loaded_rows,
            "extract_time": extract_time.isoformat()
        }
    else:
        logging.info(f"⚠️ {company_cd} 수집할 데이터가 없습니다: {start_str} ~ {end_str}")
        
        # Variable 업데이트 (데이터가 없어도 시간은 업데이트)
        update_variable(increment_key, end_str)
        
        return {
            "status": "incremental_completed_no_data",
            "company_cd": company_cd,
            "start_time": start_str,
            "end_time": end_str,
            "rows_processed": 0,
            "message": "수집할 데이터가 없음"
        }

# ────────────────────────────────────────────────────────────────
# 6️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="material_warehouse_temperature_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",  # 매시간 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "Material", "Warehouse", "Temperature", "Incremental"]
) as dag:
    
    # Start task
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("🚀 Temperature Incremental 시작"),
    )
    
    # Company-specific tasks (parallel execution) - 동적 생성
    incremental_tasks = []
    for company in COMPANIES:
        task = PythonOperator(
            task_id=f"incremental_{company['code'].lower()}",
            python_callable=lambda comp=company, **kwargs: process_company_incremental(
                comp['code'], comp['increment_key'], comp['source_conn_id']
            ),
            provide_context=True,
        )
        incremental_tasks.append(task)
    
    # End task
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("🎉 Temperature Incremental 완료"),
    )
    
    # Task dependencies - All companies run in parallel
    start >> incremental_tasks >> end
