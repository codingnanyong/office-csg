"""
Plate Temperature Anomaly Detection DAG - Backfill
==================================================
Anomaly Transformer를 사용한 플레이트 온도 이상치 탐지 및 제거 DAG (과거 데이터 일괄 처리)

Source: PostgreSQL bronze.ip_hmi_data_raw (Airflow에서 이미 수집된 데이터)
Target: PostgreSQL silver.ipi_anomaly_transformer_result (이상치 제거 후 정제된 데이터)
Execution: Manual trigger only

처리 방식:
- Variable 'ipi_anomaly_transformer_last_date' 기반으로 시작점 결정
- Variable이 없으면 INITIAL_START_DATE부터 시작
- today - DAYS_OFFSET_FOR_INCREMENTAL일까지 일별 배치로 처리
- 처리 완료 후 Variable 업데이트
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.quality.silver.common.ipi_anomaly_transformer_common import backfill_daily_batch_task


# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2),
}


# ────────────────────────────────────────────────────────────────
# DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ipi_anomaly_transformer_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",  # 수동 실행만
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "IP", "Quality", "Anomaly Transformer", "Silver layer", "Backfill"],
) as dag:
    
    backfill_task = PythonOperator(
        task_id="backfill_daily_batch",
        python_callable=backfill_daily_batch_task,
        provide_context=True,
    )
    
    backfill_task
