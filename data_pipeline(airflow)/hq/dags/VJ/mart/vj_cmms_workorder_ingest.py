import sys
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────────────────
# 1️⃣ DAG 기본 설정
# ────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

logger = logging.getLogger(__name__)

CONN_ID_V1 = "pg_fdw_v1_vj"  
CONN_ID_V2 = "pg_fdw_v2_vj"  

SOURCE_SCHEMA = "cmms"
TARGET_SCHEMA = "services"
TARGET_TABLE = "cmms_workorders"

postgres_helper_v1 = PostgresHelper(CONN_ID_V1)  
postgres_helper_v2 = PostgresHelper(CONN_ID_V2)  

# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ 마지막 증분 실행 시간 조회
# ────────────────────────────────────────────────────────────────────────────
def get_last_ingested_date():
    query = """
        SELECT last_ingested FROM services.last_ingested_time
        WHERE table_name = 'cmms_workorders';
    """
    result = postgres_helper_v2.execute_query(query, task_id="get_last_ingested_date", xcom_key=None)

    if result and result[0][0]:
        last_ingested_wo_date = result[0][0]
        logger.info(f"✅ Retrieved last_ingested_wo_date from DB: {last_ingested_wo_date}")
    else:
        last_ingested_wo_date = datetime(2018, 12, 1, 0, 0, 0)
        logger.warning(f"🚀 No record found. Using default: {last_ingested_wo_date}")

    next_ingested_wo_date = last_ingested_wo_date + timedelta(days=1)

    current_timestamp = datetime.now()
    if next_ingested_wo_date > current_timestamp:
        next_ingested_wo_date = current_timestamp
        logger.info(f"⚠️ next_ingested_wo_date exceeds current time, adjusted to: {next_ingested_wo_date}")

    return last_ingested_wo_date, next_ingested_wo_date

# ────────────────────────────────────────────────────────────────────────────
# 3️⃣ 증분 데이터 조회 및 XCom 저장
# ────────────────────────────────────────────────────────────────────────────
def fetch_incremental_work_orders(**kwargs):
    ti = kwargs["ti"]
    last_ingested_wo_date, next_ingested_wo_date = get_last_ingested_date()

    logger.info(f"📥 Fetching incremental work orders for request_date >= {last_ingested_wo_date} and < {next_ingested_wo_date}...")

    query = f"""
    SELECT DISTINCT ON (wf.company_cd, wf.wo_yymm, COALESCE(wf.wo_orgn, 'UNKNOWN'), wf.wo_no) 
        wf.company_cd, mp.zone, wf.mach_id, wf.wo_type, wf.wo_yymm,
        COALESCE(wf.wo_orgn, 'UNKNOWN') AS wo_orgn,
        wf.wo_no, wf.wo_class, wf.work_type, wf.wo_status,
        wf.request_date, wf.wo_date, wf.problem_date, wf.defe_date, wf.solu_date, def.defe_nm_en
    FROM {SOURCE_SCHEMA}.wof_order AS wf
    LEFT JOIN services.mes_cmms_mapping AS mp 
        ON wf.mach_id = mp.cmms_machine_id
    LEFT JOIN {SOURCE_SCHEMA}.bas_defective AS def 
        ON wf.company_cd = def.company_cd AND wf.defe_cd = def.defe_cd
    WHERE wf.request_date >= '{last_ingested_wo_date.strftime("%Y-%m-%d %H:%M:%S")}' 
      AND wf.request_date < '{next_ingested_wo_date.strftime("%Y-%m-%d %H:%M:%S")}'
    ORDER BY wf.company_cd, wf.wo_yymm,COALESCE(wf.wo_orgn, 'UNKNOWN'), wf.wo_no, wf.upd_date DESC;
    """

    records = postgres_helper_v1.execute_query(query, task_id="fetch_work_orders", xcom_key="work_order_data", **kwargs)

    if records:
        processed_records = [tuple(record) for record in records]
        logger.info(f"✅ Retrieved {len(records)} work order records.")
        logger.info(f"✅ Storing next_ingested_wo_date in XCom: {next_ingested_wo_date}")

        ti.xcom_push(key="incremental_work_order_data", value=processed_records)
        ti.xcom_push(key="last_ingested_wo_date", value=next_ingested_wo_date.strftime("%Y-%m-%d %H:%M:%S"))
    else:
        logger.warning("⚠️ No new incremental work orders found.")
        ti.xcom_push(key="incremental_work_order_data", value=[])
        ti.xcom_push(key="last_ingested_wo_date", value=next_ingested_wo_date.strftime("%Y-%m-%d %H:%M:%S"))

# ────────────────────────────────────────────────────────────────────────────
# 4️⃣ 증분 데이터 적재
# ────────────────────────────────────────────────────────────────────────────
def insert_work_order_data(**kwargs):
    ti = kwargs["ti"]
    work_order_data = ti.xcom_pull(task_ids="fetch_work_orders", key="incremental_work_order_data")

    if not work_order_data:
        logger.info("⚠️ No new data to insert into work_order_summary.")
        return

    postgres_helper_v2.insert_data(
        TARGET_SCHEMA, TARGET_TABLE, 
        [tuple(row) for row in work_order_data],
        conflict_columns = ["wo_yymm","wo_orgn","wo_no"] if TARGET_TABLE == "cmms_workorders" else None
    )

    logger.info(f"✅ Inserted {len(work_order_data)} records into {TARGET_TABLE}.")

# ────────────────────────────────────────────────────────────────────────────
# 5️⃣ last_ingested 테이블 업데이트
# ────────────────────────────────────────────────────────────────────────────
def update_last_ingested(**kwargs):
    ti = kwargs["ti"]
    latest_wo_date_str = ti.xcom_pull(task_ids="fetch_work_orders", key="last_ingested_wo_date")

    if not latest_wo_date_str:
        logger.warning("⚠️ No update for last_ingested_wo_date in DB.")
        return

    latest_ingested_timestamp = datetime.strptime(latest_wo_date_str, "%Y-%m-%d %H:%M:%S")
    logger.info(f"✅ Updating last_ingested_wo_date in DB: {latest_ingested_timestamp}")

    update_query = """
    UPDATE services.last_ingested_time
    SET last_ingested = %s
    WHERE table_name = 'cmms_workorders';
    """

    try:
        postgres_helper_v2.execute_update(update_query, task_id="update_last_ingested_wo_date", parameters=(latest_ingested_timestamp,))
        logger.info("✅ Successfully updated last_ingested_wo_date in DB as timestamp.")
    except Exception as e:
        logger.error(f"❌ Failed to update last_ingested_wo_date: {str(e)}")

# ────────────────────────────────────────────────────────────────────────────
# 6️⃣ DAG 정의
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="vj_cmms_workorder_ingests",
    default_args=default_args,
    description="VJ CMMS Work Order Ingest" ,
    # schedule_interval="*/10 * * * *",  
    schedule_interval="@hourly",  
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["VJ","CMMS","WorkOrder"],
) as dag:

    task_fetch_work_orders = PythonOperator(
        task_id="fetch_work_orders",
        python_callable=fetch_incremental_work_orders,
        trigger_rule="all_success",
    )

    task_insert_work_orders = PythonOperator(
        task_id="insert_work_orders",
        python_callable=insert_work_order_data,
        trigger_rule="all_success",
    )

    task_update_last_ingested = PythonOperator(
        task_id="update_last_ingested",
        python_callable=update_last_ingested,
        trigger_rule="all_success",
    )

    task_fetch_work_orders >> task_insert_work_orders >> task_update_last_ingested
