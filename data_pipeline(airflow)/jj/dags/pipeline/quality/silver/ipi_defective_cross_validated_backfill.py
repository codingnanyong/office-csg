"""
IPI Defective Cross Validated Backfill DAG (Quality Layer IPI Defective Cross Validated)
===========================================================
과거 데이터를 쌓는 Backfill DAG

Source Tables:
- bronze.mspq_in_osnd_bt_raw - OSND 데이터
- silver.ip_mold_mc_inout - MMS 데이터 (Production Layer MMS 데이터)
- silver.ipi_defective_time_corrected - IPI Defective Time Corrected 데이터 (IPI Time Correction DAG 결과 데이터)

Target: silver.ipi_defective_cross_validated - IPI Defective Cross Validated 데이터 (Quality Layer IPI Defective Cross Validated 데이터)
Execution: Manual trigger only

로직:
1. MMS 데이터 보정 (겹침 제거)
2. OSND 데이터와 Station 매칭
3. IPI Defective Time Corrected와 Cross Check (IPI Time Correction DAG 결과 데이터와 OSND 데이터 비교)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.quality.silver.common.ipi_defective_cross_validated_common import backfill_daily_batch


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
    dag_id="ipi_defective_cross_validated_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # 수동 실행만
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    description="IPI Defective Cross Validated - Backfill DAG (IPI_DMS 데이터 보정)",
    tags=["JJ", "quality", "Silver Layer", "backfill", "IPI", "IP"]
) as dag:
    
    backfill_daily_batch_task = PythonOperator(
        task_id="backfill_daily_batch",
        python_callable=backfill_daily_batch,
        provide_context=True,
    )
    
    backfill_daily_batch_task
