import sys
import logging
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────────────────
# 1️⃣ 설정 (Config & Logging)
# ────────────────────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger = logging.getLogger(__name__)

CONN_ID_v1 = "pg_fdw_v1_iot"  
CONN_ID_v2 = "pg_fdw_v2_hq"  

EXTRACT_SCHEMA_NAME = "workshop"
LOAD_SCHEMA_NAME = "services"  
TARGET_TABLE_NAME = "temperature"

postgres_helper_v1 = PostgresHelper(CONN_ID_v1)  
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)  

# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ 백필 모드: 과거부터 현재까지 모든 데이터 처리
# ────────────────────────────────────────────────────────────────────────────
def get_last_ingested_time():
    query = """
        SELECT last_ingested 
        FROM services.last_ingested_time
        WHERE table_name = 'temperature';
    """
    result = postgres_helper_v2.execute_query(query, task_id="get_last_ingested_time", xcom_key=None)

    if result and result[0][0]:
        last_ingested_time = result[0][0]
        logger.info(f"✅ Retrieved last_ingested_time from DB: {last_ingested_time}")
    else:
        last_ingested_time = "2024-06-26 00:00:00"
        logger.warning(f"🚀 No record found. Using default: {last_ingested_time}")

    return last_ingested_time

def fetch_backfill_temperature(**kwargs):
    """백필 모드: 30분씩 반복해서 모든 과거 데이터 처리"""
    ti = kwargs["ti"]
    all_processed_records = []
    batch_count = 0
    
    logger.info("🔄 Starting BACKFILL mode - processing all historical data in 30-minute batches")
    
    while True:
        batch_count += 1
        last_ingested_time = get_last_ingested_time()
        
        # 현재 시간과 비교 (최대 현재 시간까지만 처리)
        current_time = datetime.now()
        if isinstance(last_ingested_time, str):
            last_time = datetime.fromisoformat(last_ingested_time.replace('Z', '+00:00'))
        else:
            last_time = last_ingested_time
            
        if last_time >= current_time:
            logger.info(f"✅ BACKFILL COMPLETED! Processed {batch_count-1} batches")
            break
            
        logger.info(f"📥 Processing batch #{batch_count} from {last_ingested_time}...")

        query = f"""
            WITH ranked_data AS (
                SELECT subt.ymd, subt.hmsf, subt.capture_dt, subt.sensor_id, subt.device_id, subt.t1, subt.t2,
                    ROW_NUMBER() OVER (
                        PARTITION BY subt.sensor_id,
                        DATE_TRUNC('hour', subt.capture_dt) + 
                        CASE 
                            WHEN EXTRACT(MINUTE FROM subt.capture_dt) <= 0 THEN INTERVAL '0 minutes'
                            WHEN EXTRACT(MINUTE FROM subt.capture_dt) < 30 THEN INTERVAL '0 minutes'
                            ELSE INTERVAL '30 minutes'
                        END
                        ORDER BY subt.capture_dt ASC
                    ) AS rn
                FROM workshop.temperature AS subt
                WHERE subt.capture_dt >= '{last_ingested_time}'::TIMESTAMP
                AND subt.capture_dt < ('{last_ingested_time}'::TIMESTAMP + INTERVAL '30 minutes')
            )
            SELECT ymd, hmsf, sensor_id, device_id, capture_dt, t1, t2
            FROM ranked_data
            WHERE rn = 1
            ORDER BY sensor_id, capture_dt ASC;
        """

        records = postgres_helper_v1.execute_query(query, task_id="fetch_temperature", xcom_key=None)

        if records:
            processed_records = [
                (ymd, hmsf, sensor_id, device_id, capture_dt.isoformat(), t1, t2) 
                for ymd, hmsf, sensor_id, device_id, capture_dt, t1, t2 in records
            ]
            
            # 즉시 데이터 삽입
            postgres_helper_v2.insert_data(
                LOAD_SCHEMA_NAME,
                TARGET_TABLE_NAME,
                [tuple(row) for row in processed_records],
                conflict_columns=["sensor_id", "capture_dt"]
            )
            
            # last_ingested_time 업데이트
            if isinstance(last_ingested_time, str):
                next_start_time = datetime.fromisoformat(last_ingested_time.replace('Z', '+00:00')) + timedelta(minutes=30)
            else:
                next_start_time = last_ingested_time + timedelta(minutes=30)
                
            update_query = """
                UPDATE services.last_ingested_time
                SET last_ingested = %s
                WHERE table_name = 'temperature';
            """
            postgres_helper_v2.execute_update(update_query, task_id="update_last_ingested_time", parameters=(str(next_start_time),))
            
            logger.info(f"✅ Batch #{batch_count}: Processed {len(records)} records, updated to {next_start_time}")
            all_processed_records.extend(processed_records)
            
        else:
            logger.warning(f"⚠️ No data found for batch #{batch_count}, moving to next batch")
            # 데이터가 없어도 시간을 진행
            if isinstance(last_ingested_time, str):
                next_start_time = datetime.fromisoformat(last_ingested_time.replace('Z', '+00:00')) + timedelta(minutes=30)
            else:
                next_start_time = last_ingested_time + timedelta(minutes=30)
                
            update_query = """
                UPDATE services.last_ingested_time
                SET last_ingested = %s
                WHERE table_name = 'temperature';
            """
            postgres_helper_v2.execute_update(update_query, task_id="update_last_ingested_time", parameters=(str(next_start_time),))
            
        # 너무 많은 배치 방지 (안전장치)
        if batch_count > 10000:  # 최대 10000 배치 (약 208일)
            logger.warning("⚠️ Maximum batch limit reached, stopping backfill")
            break
    
    logger.info(f"🎉 BACKFILL COMPLETED! Total records processed: {len(all_processed_records)}")
    ti.xcom_push(key="backfill_summary", value=f"Processed {len(all_processed_records)} records in {batch_count} batches")

# ────────────────────────────────────────────────────────────────────────────
# 3️⃣ DAG 정의 (BACKFILL용)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hq_iot_temperature_backfill",
    default_args=default_args,
    description="HD IoT Temperature BACKFILL - Process ALL historical data",
    schedule_interval=None,  
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["HQ","IoT","BACKFILL"],
    max_active_runs=1,  # 동시 실행 방지
) as dag:

    task_backfill_temperature = PythonOperator(
        task_id="backfill_temperature",
        python_callable=fetch_backfill_temperature,
        trigger_rule="all_success",
        execution_timeout=timedelta(hours=6),  # 최대 6시간
    )
