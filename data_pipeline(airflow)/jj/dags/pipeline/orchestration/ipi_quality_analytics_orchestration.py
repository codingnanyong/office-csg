"""
IPI Quality Analytics Orchestration DAG
=======================================
IPI Quality 관련 DAG들을 순차적으로 실행하는 오케스트레이션 DAG

Pipeline Flow:
1. Bronze Layer (Raw Data) - 병렬 실행
   - ipi_mc_output_raw_incremental
   - ipi_mc_output_v2_raw_incremental
   - smp_ss_ipi_rst_raw_incremental
   - mspq_in_osnd_bt_ipi_raw_incremental
2. Bronze End (Synchronization Point)
3. Silver Layer 1 - 병렬 실행 (서로 독립적)
   - ip_mold_mc_inout_incremental (MMS 데이터)
   - ipi_good_product_incremental (A: 양품 데이터)
   - ipi_defective_time_correction_incremental (B_1: 불량 시간 보정)
   - ipi_anomaly_transformer_incremental (C_1: 온도 이상치 제거, bronze.ip_hmi_data_raw 직접 사용)
4. Silver Layer 1 End (Synchronization Point)
5. Silver Layer 2 - 순차 실행 (B_1 결과 필요)
   - ipi_defective_cross_validated_incremental (B_2: OSND와 IPI DMS 크로스 체크)
     * 의존성: B_1(ipi_defective_time_correction) 완료 필요
6. Silver Layer 2 End (Synchronization Point)
7. Gold Layer - 순차 실행 (A, B_2, C_1 결과 모두 필요)
   - ipi_temperature_matching_incremental (D: 최종 온도 매칭)
     * 의존성: A(ipi_good_product), B_2(ipi_defective_cross_validated), C_1(ipi_anomaly_transformer) 완료 필요
8. End

Schedule: Daily at 00:10 UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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


# ════════════════════════════════════════════════════════════════
# 2️⃣ DAG Definition
# ════════════════════════════════════════════════════════════════

dag = DAG(
    dag_id='ipi_quality_analytics_orchestration',
    default_args=DEFAULT_ARGS,
    description='IPI Quality Analytics Complete Pipeline (Bronze → Silver)',
    schedule_interval="10 0 * * *", 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['JJ', 'quality', 'orchestration', 'pipeline', 'IP', 'IPI']
)


# ════════════════════════════════════════════════════════════════
# 3️⃣ Task Definitions
# ════════════════════════════════════════════════════════════════

# Start task
start = DummyOperator(
    task_id='start',
    dag=dag
)

# Bronze Layer - Raw Data Collection (병렬 실행)
ipi_mc_output = TriggerDagRunOperator(
    task_id='trigger_ipi_mc_output_raw_incremental',
    trigger_dag_id='ipi_mc_output_raw_incremental',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

ipi_mc_output_v2 = TriggerDagRunOperator(
    task_id='trigger_ipi_mc_output_v2_raw_incremental',
    trigger_dag_id='ipi_mc_output_v2_raw_incremental',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

smp_ipi_rst = TriggerDagRunOperator(
    task_id='trigger_smp_ss_ipi_rst_raw_incremental',
    trigger_dag_id='smp_ss_ipi_rst_raw_incremental',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

mspq_in_osnd_bt_ipi = TriggerDagRunOperator(
    task_id='trigger_mspq_in_osnd_bt_ipi_raw_incremental',
    trigger_dag_id='mspq_in_osnd_bt_ipi_raw_incremental',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Bronze End - Synchronization point
bronze_end = DummyOperator(
    task_id='bronze_end',
    dag=dag
)

# Silver Layer 1 - Processed Data (병렬 실행)
ip_mold_inout = TriggerDagRunOperator(
    task_id='trigger_ip_mold_mc_inout_incremental',
    trigger_dag_id='ip_mold_mc_inout_incremental',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

ipi_good_product = TriggerDagRunOperator(
    task_id='trigger_ipi_good_product_incremental',
    trigger_dag_id='ipi_good_product_incremental',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

ipi_defective_time_correction = TriggerDagRunOperator(
    task_id='trigger_ipi_defective_time_correction_incremental',
    trigger_dag_id='ipi_defective_time_correction_incremental',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Silver Layer 1 - Anomaly Detection (병렬 실행, bronze.ip_hmi_data_raw 직접 사용)
ipi_anomaly_transformer = TriggerDagRunOperator(
    task_id='trigger_ipi_anomaly_transformer_incremental',
    trigger_dag_id='ipi_anomaly_transformer_incremental',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Silver Layer 1 End - Synchronization point
silver_1_end = DummyOperator(
    task_id='silver_1_end',
    dag=dag
)

# Silver Layer 2 - Cross Validated (순차 실행)
ipi_cross_validated = TriggerDagRunOperator(
    task_id='trigger_ipi_defective_cross_validated_incremental',
    trigger_dag_id='ipi_defective_cross_validated_incremental',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

silver_2_end = DummyOperator(
    task_id='silver_2_end',
    dag=dag
)

# Gold Layer - Temperature Matching (순차 실행)
ipi_temperature_matching = TriggerDagRunOperator(
    task_id='trigger_ipi_temperature_matching_incremental',
    trigger_dag_id='ipi_temperature_matching_incremental',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# End task
end = DummyOperator(
    task_id='end',
    dag=dag
)


# ════════════════════════════════════════════════════════════════
# 4️⃣ Task Dependencies
# ════════════════════════════════════════════════════════════════

# Start → Bronze Layer (병렬 실행)
start >> [
    ipi_mc_output,
    ipi_mc_output_v2,
    smp_ipi_rst,
    mspq_in_osnd_bt_ipi
] >> bronze_end

# Bronze End → Silver Layer 1 (병렬 실행)
# A, B_1, C_1은 서로 독립적이므로 병렬 실행 가능
# ipi_anomaly_transformer는 bronze.ip_hmi_data_raw를 직접 사용하므로 병렬 실행 가능
bronze_end >> [
    ip_mold_inout,
    ipi_good_product,  # A
    ipi_defective_time_correction,  # B_1
    ipi_anomaly_transformer  # C_1
] >> silver_1_end

# Silver Layer 1 End → Silver Layer 2 (B_2는 B_1 완료 후 실행 필요)
# B_2는 B_1의 결과를 사용하므로 B_1 완료 후 실행
silver_1_end >> ipi_cross_validated >> silver_2_end

# Silver Layer 2 End → Gold Layer (D는 A, B_2, C_1 모두 완료 후 실행 필요)
# D는 A, B_2, C_1의 결과를 모두 사용하므로 모두 완료 후 실행
silver_2_end >> ipi_temperature_matching >> end
