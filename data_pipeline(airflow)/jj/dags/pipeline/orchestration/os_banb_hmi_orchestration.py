import logging
import threading
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from plugins.hooks.mysql_hook import MySQLHelper


def _test_connection_with_timeout(mysql, timeout_seconds, result):
    """타임아웃이 있는 연결 테스트"""
    try:
        with mysql.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        result['success'] = True
    except Exception as e:
        result['success'] = False
        result['error'] = str(e)


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2),
}


INCREMENTAL_DAGS = [
    "os_banb_hmi_alarm_incremental",
    "os_banb_hmi_maintenance_incremental",
    "os_banb_hmi_data_incremental",
    "os_banb_hmi_sensor_incremental",
]

# 공통 연결 (상시 켜져 있지 않은 연결)
COMMON_CONNECTIONS = ["maria_jj_os_banb_1", "maria_jj_os_banb_3"]


def check_connection_available(conn_id: str, timeout_seconds: int = 5) -> bool:
    """연결 가능 여부 확인 (상시 켜져 있지 않은 연결용, 타임아웃 설정)"""
    try:
        mysql = MySQLHelper(conn_id=conn_id)
        result = {'success': False, 'error': None}
        
        # 별도 스레드에서 연결 테스트
        thread = threading.Thread(target=_test_connection_with_timeout, args=(mysql, timeout_seconds, result))
        thread.daemon = True
        thread.start()
        thread.join(timeout=timeout_seconds)
        
        if thread.is_alive():
            # 타임아웃 발생
            logging.warning(f"⚠️ 연결 타임아웃: {conn_id} ({timeout_seconds}초 초과)")
            return False
        
        if result['success']:
            logging.info(f"✅ 연결 확인 성공: {conn_id}")
            return True
        else:
            logging.warning(f"⚠️ 연결 불가: {conn_id} - {result.get('error', 'Unknown error')}")
            return False
    except Exception as e:
        logging.warning(f"⚠️ 연결 실패: {conn_id} - {str(e)}")
        return False


def check_connections_available(**context):
    """공통 연결 상태 확인 (하나라도 연결 가능하면 True)"""
    available = any(check_connection_available(conn_id) for conn_id in COMMON_CONNECTIONS)
    if available:
        logging.info("✅ 연결 가능하여 모든 DAG를 트리거합니다")
    else:
        logging.warning("⚠️ 모든 연결 불가하여 모든 DAG를 스킵합니다")
    return available


with DAG(
    dag_id="os_banb_hmi_orchestration",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "OS", "Banbury", "HMI", "orchestration", "incremental"],
):
    start = EmptyOperator(task_id="start")
    
    # 공통 연결 확인 (하나만 확인)
    check_connections = ShortCircuitOperator(
        task_id="check_connections",
        python_callable=check_connections_available,
    )
    
    done = EmptyOperator(
        task_id="done",
        trigger_rule='all_done'  # 모든 트리거 완료 후 실행 (성공/실패 무관)
    )

    # 연결 가능하면 모든 DAG 트리거
    triggers = []
    for dag_id in INCREMENTAL_DAGS:
        trigger_task = TriggerDagRunOperator(
            task_id=f"trigger__{dag_id}",
            trigger_dag_id=dag_id,
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=30,
            execution_date="{{ ds }}",
        )
        triggers.append(trigger_task)
    
    start >> check_connections >> triggers >> done
