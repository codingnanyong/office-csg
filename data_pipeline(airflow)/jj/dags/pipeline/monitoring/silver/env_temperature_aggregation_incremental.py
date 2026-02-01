"""Temperature Aggregation Incremental DAG (10분 단위 집계)

Source: public.temperature
Target: silver.temperature_aggregated

10분 단위로 temperature 데이터를 집계하여 저장합니다.
"""

import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags.pipeline.monitoring.silver.common.env_temperature_aggregation_common import (
    get_incremental_date_range,
    process_aggregation,
    update_variable,
    INDO_TZ
)

# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ────────────────────────────────────────────────────────────────
# Task Functions
# ────────────────────────────────────────────────────────────────

def get_date_range(**context) -> dict | None:
    """날짜 범위 계산"""
    return get_incremental_date_range()


def run_aggregation(**context) -> dict:
    """집계 처리 실행"""
    date_range = context['ti'].xcom_pull(task_ids='get_date_range')
    
    if not date_range:
        logging.info("⚠️ 처리할 데이터 없음")
        return {"status": "skipped", "message": "No data to process"}
    
    start_date = date_range["start_date"]
    end_date = date_range["end_date"]
    
    result = process_aggregation(start_date, end_date)
    
    # 성공 시 Variable 업데이트 (실제 적재된 마지막 시간 사용)
    if result.get("status") == "success":
        # 실제 적재된 마지막 시간이 있으면 그것을 사용, 없으면 end_date 사용
        actual_last_time = result.get("actual_last_time")
        if actual_last_time:
            update_variable(actual_last_time)
        else:
            update_variable(end_date)
    
    return result


# ────────────────────────────────────────────────────────────────
# DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="env_temperature_aggregation_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",  # 매 10분마다 실행
    start_date=datetime(2025, 1, 1, tzinfo=INDO_TZ),
    catchup=False,
    tags=["JJ", "Monitoring", "Temperature", "Aggregation", "Incremental"]
) as dag:
    
    # Start task
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("🚀 Temperature Aggregation Incremental 시작"),
    )
    
    # Get date range
    get_date_range_task = PythonOperator(
        task_id="get_date_range",
        python_callable=get_date_range,
    )
    
    # Run aggregation
    run_aggregation_task = PythonOperator(
        task_id="run_aggregation",
        python_callable=run_aggregation,
    )
    
    # End task
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("🎉 Temperature Aggregation Incremental 완료"),
    )
    
    # Task dependencies
    start >> get_date_range_task >> run_aggregation_task >> end
