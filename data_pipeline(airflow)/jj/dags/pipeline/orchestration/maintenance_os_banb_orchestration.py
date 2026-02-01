"""
Maintenance OS BANB Pipeline DAG
================================
전체 유지보수 파이프라인을 순차적으로 실행하는 메인 DAG

Pipeline Flow:
1. Bronze Layer (Raw Data)
   - wof_order_raw_incremental (Work Order)
   - mch_machine_raw_incremental (Machine Master)
2. Bronze End (Dummy Task)
3. Silver Layer (Processed Data)
   - os_banb_mch_machine_incremental (Machine Master)
   - os_banb_wof_order_incremental (Work Order)
4. End

Schedule: Daily at 3 AM
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
    'maintenance_os_banb_orchestration',
    default_args=DEFAULT_ARGS,
    description='Maintenance OS BANB Complete Pipeline (Bronze → Silver)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['JJ', 'maintenance', 'orchestration', 'pipeline', 'OS', 'Banbury']
)

# ════════════════════════════════════════════════════════════════
# 3️⃣ Task Definitions
# ════════════════════════════════════════════════════════════════

# Start task
start = DummyOperator(
    task_id='start',
    dag=dag
)

# Bronze Layer - Raw Data Collection
wof_order_raw_incremental = TriggerDagRunOperator(
    task_id='trigger_wof_order_raw_incremental',
    trigger_dag_id='wof_order_raw_incremental',
    wait_for_completion=True,
    dag=dag
)

mch_machine_raw_incremental = TriggerDagRunOperator(
    task_id='trigger_mch_machine_raw_incremental',
    trigger_dag_id='mch_machine_raw_incremental',
    wait_for_completion=True,
    dag=dag
)

# Bronze End - Synchronization point
bronze_end = DummyOperator(
    task_id='bronze_end',
    dag=dag
)

# Silver Layer - Processed Data
os_banb_mch_machine_incremental = TriggerDagRunOperator(
    task_id='trigger_os_banb_mch_machine_incremental',
    trigger_dag_id='os_banb_mch_machine_incremental',
    wait_for_completion=True,
    dag=dag
)

os_banb_wof_order_incremental = TriggerDagRunOperator(
    task_id='trigger_os_banb_wof_order_incremental',
    trigger_dag_id='os_banb_wof_order_incremental',
    wait_for_completion=True,
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
# Start
start >> [wof_order_raw_incremental, mch_machine_raw_incremental]

# Bronze Layer (Parallel execution)
[wof_order_raw_incremental, mch_machine_raw_incremental] >> bronze_end

# Silver Layer (Sequential execution after Bronze completion)
bronze_end >> [os_banb_mch_machine_incremental , os_banb_wof_order_incremental] >> end