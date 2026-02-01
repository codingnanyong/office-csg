"""
Plate Temperature Anomaly Detection DAG - Incremental
======================================================
Anomaly Transformer를 사용한 플레이트 온도 이상치 탐지 및 제거 DAG (증분 처리)

Source: PostgreSQL bronze.ip_hmi_data_raw (Airflow에서 이미 수집된 데이터)
Target: PostgreSQL silver.ipi_anomaly_transformer_result (이상치 제거 후 정제된 데이터)
Execution: Scheduled or manual trigger

처리 방식:
- Airflow Variable 'ipi_anomaly_transformer_last_date' 기반으로 마지막 처리일 다음날부터 처리
- Variable이 없으면 전일(today-1) 처리
- 처리 완료 후 Variable 업데이트
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.quality.silver.common.ipi_anomaly_transformer_common import run_anomaly_transformer


# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=4),
}


# ────────────────────────────────────────────────────────────────
# DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ipi_anomaly_transformer_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # 수동 실행 또는 필요시 스케줄 설정 (예: "0 2 * * *" - 매일 02:00 UTC)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "IP", "Quality", "Anomaly Transformer", "Silver layer", "Incremental"],
) as dag:
    
    anomaly_transformer_task = PythonOperator(
        task_id="run_ipi_anomaly_transformer",
        python_callable=run_anomaly_transformer,
        provide_context=True,
    )
    
    anomaly_transformer_task
