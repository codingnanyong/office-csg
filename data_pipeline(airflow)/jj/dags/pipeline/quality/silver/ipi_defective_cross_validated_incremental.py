"""
IPI Defective Cross Validated Incremental DAG (Quality Layer IPI Defective Cross Validated)
===========================================================
전일 데이터를 쌓는 Incremental DAG

Source Tables:
- bronze.mspq_in_osnd_bt_raw - OSND 데이터
- silver.ip_mold_mc_inout - MMS 데이터 (Production Layer MMS 데이터)
- silver.ipi_defective_time_corrected - IPI Defective Time Corrected 데이터 (IPI Time Correction DAG 결과 데이터)

Target: silver.ipi_defective_cross_validated - IPI Defective Cross Validated 데이터 (Quality Layer IPI Defective Cross Validated 데이터)
Execution: Daily schedule (@daily)

로직:
1. MMS 데이터 보정 (겹침 제거)
2. OSND 데이터와 Station 매칭
3. IPI Defective Time Corrected와 Cross Check (IPI Time Correction DAG 결과 데이터와 OSND 데이터 비교)
"""


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.quality.silver.common.ipi_defective_cross_validated_common import incremental_ipi_defective_cross_validated


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
    dag_id="ipi_defective_cross_validated_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # 매일 자동 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    description="IPI Defective Cross Validated - Incremental DAG (전일 데이터 처리)",
    tags=["JJ", "quality", "Silver Layer", "incremental", "IPI", "IP"]
) as dag:
    
    incremental_task = PythonOperator(
        task_id="ipi_defective_cross_validated_incremental",
        python_callable=incremental_ipi_defective_cross_validated,
        provide_context=True,
    )
    
    incremental_task

