"""Plan Prod OP CD Orchestration DAG - SSS IPP SO, SSS PHP SO, SMP SS PHP RST Incremental Collection"""
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)
}

# ════════════════════════════════════════════════════════════════
# 2️⃣ Helper Functions
# ════════════════════════════════════════════════════════════════

def log_pipeline_start(**kwargs) -> dict:
    """파이프라인 시작 로깅"""
    from airflow.models import Variable
    from datetime import datetime, timezone, timedelta
    
    execution_date = kwargs.get('execution_date') or kwargs.get('data_interval_start')
    
    # 각 DAG의 마지막 수집 시점 확인
    ipp_so_last_time = Variable.get("last_extract_time_sss_ipp_so", default_var=None)
    php_so_last_time = Variable.get("last_extract_time_sss_php_so", default_var=None)
    rst_last_time = Variable.get("last_extract_time_smp_ss_php_rst", default_var=None)
    
    # 목표 종료 시점 계산 (오늘 06:30:00 인도네시아 시간)
    if execution_date:
        if execution_date.tzinfo is None:
            execution_date_utc = execution_date.replace(tzinfo=timezone.utc)
        else:
            execution_date_utc = execution_date.astimezone(timezone.utc)
        
        INDO_TZ = timezone(timedelta(hours=7))
        execution_date_indo = execution_date_utc.astimezone(INDO_TZ)
        target_end_date = execution_date_indo.replace(hour=6, minute=30, second=0, microsecond=0)
        
        logging.info(f"🚀 Plan Prod OP CD Orchestration 시작: {execution_date}")
        logging.info(f"📅 목표 종료 시점 (인도네시아): {target_end_date.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"📌 SSS IPP SO 마지막 수집 시점: {ipp_so_last_time}")
        logging.info(f"📌 SSS PHP SO 마지막 수집 시점: {php_so_last_time}")
        logging.info(f"📌 SMP SS PHP RST 마지막 수집 시점: {rst_last_time}")
        
        # 이미 수집 완료된 경우 경고
        if ipp_so_last_time:
            from dags.pipeline.production.bronze.common.sss_ipp_so_common import parse_datetime
            try:
                last_time = parse_datetime(ipp_so_last_time)
                if last_time.tzinfo is None:
                    last_time = last_time.replace(tzinfo=INDO_TZ)
                if last_time >= target_end_date:
                    logging.warning(
                        f"⚠️ SSS IPP SO는 이미 수집 완료된 상태입니다 "
                        f"(마지막: {ipp_so_last_time}, 목표: {target_end_date.strftime('%Y-%m-%d %H:%M:%S')})"
                    )
            except Exception:
                pass
        
        if php_so_last_time:
            from dags.pipeline.production.bronze.common.sss_php_so_common import parse_datetime
            try:
                last_time = parse_datetime(php_so_last_time)
                if last_time.tzinfo is None:
                    last_time = last_time.replace(tzinfo=INDO_TZ)
                if last_time >= target_end_date:
                    logging.warning(
                        f"⚠️ SSS PHP SO는 이미 수집 완료된 상태입니다 "
                        f"(마지막: {php_so_last_time}, 목표: {target_end_date.strftime('%Y-%m-%d %H:%M:%S')})"
                    )
            except Exception:
                pass

        if rst_last_time:
            from dags.pipeline.production.bronze.common.smp_ss_php_rst_raw_common import parse_datetime
            try:
                last_time = parse_datetime(rst_last_time)
                if last_time.tzinfo is None:
                    last_time = last_time.replace(tzinfo=INDO_TZ)
                if last_time >= target_end_date:
                    logging.warning(
                        f"⚠️ SMP SS PHP RST는 이미 수집 완료된 상태입니다 "
                        f"(마지막: {rst_last_time}, 목표: {target_end_date.strftime('%Y-%m-%d %H:%M:%S')})"
                    )
            except Exception:
                pass
    else:
        logging.info(f"🚀 Plan Prod OP CD Orchestration 시작: {execution_date}")
        logging.info(f"📌 SSS IPP SO 마지막 수집 시점: {ipp_so_last_time}")
        logging.info(f"📌 SSS PHP SO 마지막 수집 시점: {php_so_last_time}")
        logging.info(f"📌 SMP SS PHP RST 마지막 수집 시점: {rst_last_time}")
    
    return {
        "status": "started",
        "execution_date": str(execution_date),
        "sss_ipp_so_last_time": ipp_so_last_time,
        "sss_php_so_last_time": php_so_last_time,
        "smp_ss_php_rst_last_time": rst_last_time,
        "message": "Plan OP CD Orchestration 시작"
    }

def log_pipeline_completion(**kwargs) -> dict:
    """파이프라인 완료 로깅 (Plan Prod OP CD)"""
    from airflow.models import Variable
    
    execution_date = kwargs.get('execution_date') or kwargs.get('data_interval_start')
    
    # 각 DAG의 마지막 수집 시점 확인
    ipp_so_last_time = Variable.get("last_extract_time_sss_ipp_so", default_var=None)
    php_so_last_time = Variable.get("last_extract_time_sss_php_so", default_var=None)
    rst_last_time = Variable.get("last_extract_time_smp_ss_php_rst", default_var=None)
    
    logging.info(f"🎉 Plan Prod OP CD Orchestration 완료: {execution_date}")
    logging.info(f"📌 SSS IPP SO 마지막 수집 시점: {ipp_so_last_time}")
    logging.info(f"📌 SSS PHP SO 마지막 수집 시점: {php_so_last_time}")
    logging.info(f"📌 SMP SS PHP RST 마지막 수집 시점: {rst_last_time}")
    
    return {
        "status": "completed",
        "execution_date": str(execution_date),
        "sss_ipp_so_last_time": ipp_so_last_time,
        "sss_php_so_last_time": php_so_last_time,
        "smp_ss_php_rst_last_time": rst_last_time,
        "message": "Plan Prod OP CD Orchestration 완료"
    }

# ════════════════════════════════════════════════════════════════
# 3️⃣ DAG Definition
# ════════════════════════════════════════════════════════════════

with DAG(
    dag_id="plan_prod_op_cd_orchestration",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",  # 매일 UTC 00:00:00 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="Plan/Prod OP CD 데이터 수집 파이프라인 - SSS IPP SO, SSS PHP SO, SMP SS PHP RST Incremental",
    tags=["JJ", "orchestration", "production", "daily", "Plan", "Prod", "OP", "CD"]
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
    # Bronze Layer - Incremental Collection (병렬 실행)
    # ════════════════════════════════════════════════════════════════
    trigger_sss_ipp_so = TriggerDagRunOperator(
        task_id="trigger_sss_ipp_so_incremental",
        trigger_dag_id="sss_ipp_so_incremental",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
        conf={"triggered_by": "plan_prod_op_cd_orchestration", "phase": "incremental"}
    )
    
    trigger_sss_php_so = TriggerDagRunOperator(
        task_id="trigger_sss_php_so_incremental",
        trigger_dag_id="sss_php_so_incremental",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
        conf={"triggered_by": "plan_prod_op_cd_orchestration", "phase": "incremental"}
    )

    trigger_smp_ss_php_rst = TriggerDagRunOperator(
        task_id="trigger_smp_ss_php_rst_raw_incremental",
        trigger_dag_id="smp_ss_php_rst_raw_incremental",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
        conf={"triggered_by": "plan_prod_op_cd_orchestration", "phase": "incremental"}
    )
    
    # ════════════════════════════════════════════════════════════════
    # Synchronization Point
    # ════════════════════════════════════════════════════════════════
    bronze_end = EmptyOperator(
        task_id="bronze_end",
        trigger_rule='all_done'  # 모든 트리거 완료 후 실행 (성공/실패 무관)
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
    # Task Dependencies
    # ════════════════════════════════════════════════════════════════
    start_task >> [trigger_sss_ipp_so, trigger_sss_php_so, trigger_smp_ss_php_rst] >> bronze_end >> end_task

