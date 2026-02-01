"""Banbury Productivity DAG (교대별 Roll 계획/실적 집계)

Source: Oracle GMES (msbp_roll_plan, msbp_roll_so, msbp_roll_lot)
Target: MariaDB ccs_rtf.banbury_productivity (maria_jj_os_banb_3)

Oracle에서 교대별 Roll 계획/실적 데이터를 집계하여 MariaDB에 저장합니다.
Incremental과 Backfill 모드를 모두 포함합니다.
"""

import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from dags.pipeline.monitoring.silver.common.banbury_productivity_srv import (
    process_roll_shift_summary,
    get_realtime_date_range,
    get_backfill_date_range,
)

# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=24),  # 태스크 최대 실행 시간 24시간
}

# ────────────────────────────────────────────────────────────────
# Realtime DAG Task Functions
# ────────────────────────────────────────────────────────────────

def get_realtime_date_range_task(**context) -> dict | None:
    """실시간 날짜 범위 계산 (당일)"""
    return get_realtime_date_range()


def run_roll_shift_summary_realtime(**context) -> dict:
    """실시간 집계 처리 실행 (당일 데이터 업데이트)"""
    date_range = context['ti'].xcom_pull(task_ids='get_realtime_date_range')
    
    if not date_range:
        logging.info("⚠️ 처리할 데이터 없음")
        return {"status": "skipped", "message": "No data to process"}
    
    v_p_date_1 = date_range["v_p_date_1"]
    v_p_date_2 = date_range["v_p_date_2"]
    process_date = date_range["process_date"]
    
    result = process_roll_shift_summary(v_p_date_1, v_p_date_2)
    
    # 실시간은 당일 데이터를 계속 업데이트 (ON DUPLICATE KEY UPDATE 사용)
    # 다음날이 되면 process_date가 바뀌므로 자동으로 INSERT 발생
    
    return result


# ────────────────────────────────────────────────────────────────
# Backfill DAG Task Functions
# ────────────────────────────────────────────────────────────────

def get_backfill_date_range_task(**context) -> dict | None:
    """백필 날짜 범위 계산"""
    return get_backfill_date_range()


def run_roll_shift_summary_backfill(**context) -> dict:
    """백필 집계 처리 실행 (전일까지 여러 날짜 처리)"""
    date_range = context['ti'].xcom_pull(task_ids='get_backfill_date_range')
    
    if not date_range:
        logging.info("⚠️ 처리할 데이터 없음")
        return {"status": "skipped", "message": "No data to process"}
    
    date_pairs = date_range["date_pairs"]
    backfill_end_date = date_range["backfill_end_date"]
    
    # 각 날짜별로 처리 (DATE_1은 전일, DATE_2는 당일)
    total_processed = 0
    for date_pair in date_pairs:
        v_p_date_1 = date_pair["date_1"]
        v_p_date_2 = date_pair["date_2"]
        process_date = date_pair["process_date"]
        
        logging.info(f"📅 백필 처리 중: DATE_1={v_p_date_1} (전일, 1교대), DATE_2={v_p_date_2} (당일, 2/3교대)")
        
        result = process_roll_shift_summary(v_p_date_1, v_p_date_2)
        
        if result.get("status") == "success":
            total_processed += result.get("rows_processed", 0)
        else:
            logging.error(f"❌ 날짜 {process_date} 처리 실패: {result.get('error', 'Unknown error')}")
            # 한 날짜 실패해도 다음 날짜는 계속 처리
    
    # Variable 없이 매번 전체 처리하므로 업데이트 불필요
    
    return {
        "status": "success",
        "total_rows_processed": total_processed,
        "dates_processed": len(date_pairs),
        "end_date": backfill_end_date
    }


# ────────────────────────────────────────────────────────────────
# Realtime DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="banbury_productivity_realtime",
    default_args=DEFAULT_ARGS,
    description="Banbury Productivity Realtime - Oracle에서 교대별 Roll 계획/실적 집계 (실시간, 당일 데이터 업데이트)",
    schedule_interval="*/5 * * * *",  # 매 5분마다 실행 (당일 데이터 실시간 업데이트)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "Monitoring", "Banbury", "Productivity", "Realtime"],
) as realtime_dag:
    
    # Start task
    start_rt = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("🚀 Banbury Productivity Realtime 시작"),
    )
    
    # Get realtime date range
    get_realtime_date_range_task = PythonOperator(
        task_id="get_realtime_date_range",
        python_callable=get_realtime_date_range_task,
    )
    
    # Run roll shift summary realtime
    run_summary_realtime_task = PythonOperator(
        task_id="run_roll_shift_summary_realtime",
        python_callable=run_roll_shift_summary_realtime,
    )
    
    # End task
    end_rt = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("🎉 Banbury Productivity Realtime 완료"),
    )
    
    # Task dependencies
    start_rt >> get_realtime_date_range_task >> run_summary_realtime_task >> end_rt


# ────────────────────────────────────────────────────────────────
# Backfill DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="banbury_productivity_backfill",
    default_args=DEFAULT_ARGS,
    description="Banbury Productivity Backfill - Oracle에서 교대별 Roll 계획/실적 집계 (백필)",
    schedule_interval=None,  # 수동 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "Monitoring", "Banbury", "Productivity", "Backfill"],
) as backfill_dag:
    
    # Start task
    start_bf = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("🚀 Banbury Productivity Backfill 시작"),
    )
    
    # Get backfill date range
    get_backfill_date_range_task = PythonOperator(
        task_id="get_backfill_date_range",
        python_callable=get_backfill_date_range_task,
    )
    
    # Run roll shift summary backfill
    run_summary_backfill_task = PythonOperator(
        task_id="run_roll_shift_summary_backfill",
        python_callable=run_roll_shift_summary_backfill,
        execution_timeout=timedelta(hours=24),  # 백필은 많은 날짜를 처리하므로 24시간 타임아웃
    )
    
    # End task
    end_bf = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("🎉 Banbury Productivity Backfill 완료"),
    )
    
    # Task dependencies
    start_bf >> get_backfill_date_range_task >> run_summary_backfill_task >> end_bf
