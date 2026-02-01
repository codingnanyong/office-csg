import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone

# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=3)
}

# Date Configuration
INDO_TZ = timezone(timedelta(hours=7))

# ════════════════════════════════════════════════════════════════
# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════
def log_pipeline_start(**kwargs) -> dict:
    """로그 파이프라인 시작"""
    execution_date = kwargs['execution_date']
    logging.info(f"🚀 MSBP Roll Pipeline 시작: {execution_date}")
    
    return {
        "status": "pipeline_started",
        "execution_date": execution_date.isoformat(),
        "message": "MSBP Roll Pipeline 시작됨"
    }

def log_pipeline_completion(**kwargs) -> dict:
    """로그 파이프라인 완료"""
    execution_date = kwargs['execution_date']
    logging.info(f"✅ MSBP Roll Pipeline 완료: {execution_date}")
    
    return {
        "status": "pipeline_completed",
        "execution_date": execution_date.isoformat(),
        "message": "MSBP Roll Pipeline 완료됨"
    }

# ════════════════════════════════════════════════════════════════
# 3️⃣ DAG Definition
# ════════════════════════════════════════════════════════════════
with DAG(
    dag_id="production_os_orchestration",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",  # 매일 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="APS & MES OS 데이터 파이프라인 - Bronze → Silver 순차 처리",
    tags=["JJ", "orchestration", "production", "daily", "OS"]
) as dag:
    
    # ════════════════════════════════════════════════════════════════
    # Start Task
    # ════════════════════════════════════════════════════════════════
    start_task = PythonOperator(
        task_id="pipeline_start",
        python_callable=log_pipeline_start,
        provide_context=True,
    )
    
    # ════════════════════════════════════════════════════════════════
    # Bronze Layer - 병렬 실행
    # ════════════════════════════════════════════════════════════════
    # Bronze DAG들을 병렬로 트리거
    trigger_plan_bronze = TriggerDagRunOperator(
        task_id="trigger_plan_bronze_incremental",
        trigger_dag_id="msbp_roll_plan_raw_incremental",
        wait_for_completion=True,
        poke_interval=30,
        conf={"triggered_by": "main_pipeline", "phase": "incremental"}
    )
    
    trigger_lot_bronze = TriggerDagRunOperator(
        task_id="trigger_lot_bronze_incremental",
        trigger_dag_id="msbp_roll_lot_raw_incremental",
        wait_for_completion=True,
        poke_interval=30,
        conf={"triggered_by": "main_pipeline", "phase": "incremental"}
    )
    
    trigger_so_bronze = TriggerDagRunOperator(
        task_id="trigger_so_bronze_incremental",
        trigger_dag_id="msbp_roll_so_raw_incremental",
        wait_for_completion=True,
        poke_interval=30,
        conf={"triggered_by": "main_pipeline", "phase": "incremental"}
    )
    
    trigger_lot_new_bronze = TriggerDagRunOperator(
        task_id="trigger_lot_new_bronze_incremental",
        trigger_dag_id="msbp_roll_lot_new_raw_incremental",
        wait_for_completion=True,
        poke_interval=30,
        conf={"triggered_by": "main_pipeline", "phase": "incremental"}
    )
    
    trigger_hard_check_bronze = TriggerDagRunOperator(
        task_id="trigger_hard_check_bronze_incremental",
        trigger_dag_id="msbp_hard_check_raw_incremental",
        wait_for_completion=True,
        poke_interval=30,
        conf={"triggered_by": "main_pipeline", "phase": "incremental"}
    )
    
    # ════════════════════════════════════════════════════════════════
    # Bronze Layer 완료 대기
    # ════════════════════════════════════════════════════════════════
    bronze_complete = DummyOperator(
        task_id="bronze_layer_complete"
    )
    
    # ════════════════════════════════════════════════════════════════
    # Silver Layer - 병렬 실행
    # ════════════════════════════════════════════════════════════════
    # Silver DAG들을 병렬로 트리거
    trigger_plan_silver = TriggerDagRunOperator(
        task_id="trigger_plan_silver_incremental",
        trigger_dag_id="os_msbp_roll_plan_incremental",
        wait_for_completion=True,
        poke_interval=30,
        conf={"triggered_by": "main_pipeline"}
    )
    
    trigger_lot_silver = TriggerDagRunOperator(
        task_id="trigger_lot_silver_incremental",
        trigger_dag_id="os_msbp_roll_lot_incremental",
        wait_for_completion=True,
        poke_interval=30,
        conf={"triggered_by": "main_pipeline"}
    )
    
    trigger_hard_check_silver = TriggerDagRunOperator(
        task_id="trigger_hard_check_silver_incremental",
        trigger_dag_id="os_msbp_hard_check_incremental",
        wait_for_completion=True,
        poke_interval=30,
        conf={"triggered_by": "main_pipeline"}
    )
    
    trigger_so_silver = TriggerDagRunOperator(
        task_id="trigger_so_silver_incremental",
        trigger_dag_id="os_msbp_roll_so_incremental",
        wait_for_completion=True,
        poke_interval=30,
        conf={"triggered_by": "main_pipeline"}
    )

    trigger_lot_new_silver = TriggerDagRunOperator(
        task_id="trigger_lot_new_silver_incremental",
        trigger_dag_id="os_msbp_roll_lot_new_incremental",
        wait_for_completion=True,
        poke_interval=30,
        conf={"triggered_by": "main_pipeline"}
    )
    
    # ════════════════════════════════════════════════════════════════
    # End Task
    # ════════════════════════════════════════════════════════════════
    end_task = PythonOperator(
        task_id="pipeline_end",
        python_callable=log_pipeline_completion,
        provide_context=True,
    )
    
    # ════════════════════════════════════════════════════════════════
    # Dependencies 설정
    # ════════════════════════════════════════════════════════════════
    # 파이프라인 흐름:
    # start → [incremental_bronze] → bronze_complete → [silver] → end
    
    start_task >> [trigger_plan_bronze, trigger_lot_bronze, trigger_so_bronze, trigger_lot_new_bronze, trigger_hard_check_bronze] >> bronze_complete
    bronze_complete >> [trigger_plan_silver, trigger_lot_silver, trigger_hard_check_silver, trigger_so_silver, trigger_lot_new_silver] >> end_task
    
    # SO Silver DAG가 생성되면 아래 주석을 해제하고 위의 의존성을 수정
    # bronze_complete >> [trigger_plan_silver, trigger_lot_silver, trigger_so_silver]
    # [trigger_plan_silver, trigger_lot_silver, trigger_so_silver] >> end_task
