"""
IP Defect Code Initial ETL DAG (Bronze → Silver)
=================================================
Bronze 레이어에서 Silver 레이어로 데이터를 전처리하여 초기 적재하는 DAG
plant_cd='3120', op_cd='IPI' 조건으로 필터링

Source: bronze.mspq_op_defect_raw
Target: silver.ip_defect_code
Execution: Manual trigger only (@once)
"""

import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────
# 1️⃣ Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=30)
}

# Database Configuration
SOURCE_SCHEMA = "bronze"
SOURCE_TABLE = "mspq_op_defect_raw"
TARGET_SCHEMA = "silver"
TARGET_TABLE = "ip_defect_code"

# Connection IDs
POSTGRES_CONN_ID = "pg_jj_quality_dw"

# Filter Conditions
PLANT_CD = "3120"
OP_CD = "IPI"

# ────────────────────────────────────────────────────────────────
# 2️⃣ Data Extraction (Bronze → Transform)
# ────────────────────────────────────────────────────────────────
def build_extract_sql() -> str:
    """Build SQL query for extracting and transforming data from Bronze"""
    return f'''
        SELECT 
            modr.plant_cd,
            modr.op_cd,
            modr.defect_cd,
            modr.osnd_type,
            modr.matrix_type,
            modr.defect_ko_name AS defect_name,
            modr.etl_extract_time,
            now() AS etl_ingest_time
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} modr
        WHERE modr.plant_cd = '{PLANT_CD}'
          AND modr.op_cd = '{OP_CD}'
        ORDER BY modr.defect_cd ASC
    '''

def extract_and_transform_data(postgres: PostgresHelper) -> tuple:
    """Extract data from Bronze and transform"""
    sql = build_extract_sql()
    logging.info("🔍 Bronze 데이터 추출 및 변환 시작")
    logging.info(f"조건: plant_cd='{PLANT_CD}', op_cd='{OP_CD}'")
    logging.info(f"실행 쿼리:\n{sql}")
    
    try:
        with postgres.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
            row_count = len(data) if data else 0
            
            logging.info(f"✅ 추출 완료: {row_count:,} rows")
            return data, row_count
            
    except Exception as e:
        logging.error(f"❌ 데이터 추출 실패: {str(e)}")
        raise

# ────────────────────────────────────────────────────────────────
# 3️⃣ Data Loading (Silver)
# ────────────────────────────────────────────────────────────────
def prepare_insert_data(data: list) -> list:
    """Prepare data for Silver layer insertion"""
    if not data:
        return []
    
    # Data is already in tuple format from cursor.fetchall()
    return data

def get_column_names() -> list:
    """Get column names for Silver table"""
    return [
        "plant_cd", "op_cd", "defect_cd",
        "osnd_type", "matrix_type", "defect_name",
        "etl_extract_time", "etl_ingest_time"
    ]

def load_data(postgres: PostgresHelper, data: list) -> int:
    """Load data to Silver layer"""
    if not data:
        logging.warning("⚠️ 적재할 데이터가 없습니다")
        return 0
    
    total_rows = len(data)
    logging.info(f"📦 Silver 레이어 적재 시작: 총 {total_rows:,} rows")
    
    try:
        columns = get_column_names()
        conflict_columns = ["plant_cd", "op_cd", "defect_cd"]
        
        # PostgresHelper의 insert_data 메서드 사용
        postgres.insert_data(
            schema_name=TARGET_SCHEMA,
            table_name=TARGET_TABLE,
            data=data,
            columns=columns,
            conflict_columns=conflict_columns,
            chunk_size=1000
        )
        
        logging.info(f"🎉 Silver 레이어 적재 완료: {total_rows:,} rows")
        return total_rows
        
    except Exception as e:
        logging.error(f"❌ 데이터 적재 실패: {str(e)}")
        raise

# ────────────────────────────────────────────────────────────────
# 4️⃣ Main ETL Task
# ────────────────────────────────────────────────────────────────
def bronze_to_silver_etl(**kwargs):
    """
    Main ETL function: Bronze → Silver 데이터 전처리 및 적재
    1회 실행용
    """
    start_time = datetime.now()
    logging.info(f"{'='*60}")
    logging.info(f"🚀 IP Defect Code ETL 시작 (Bronze → Silver)")
    logging.info(f"{'='*60}")
    logging.info(f"📅 Start Time: {start_time}")
    logging.info(f"🏭 Filter: plant_cd='{PLANT_CD}', op_cd='{OP_CD}'")
    
    try:
        postgres = PostgresHelper(conn_id=POSTGRES_CONN_ID)
        
        # 1️⃣ Check Source Table
        logging.info("\n" + "─"*60)
        logging.info("1️⃣ Source 테이블 확인 중...")
        logging.info("─"*60)
        
        if not postgres.check_table(SOURCE_SCHEMA, SOURCE_TABLE):
            raise Exception(f"❌ Source 테이블이 존재하지 않습니다: {SOURCE_SCHEMA}.{SOURCE_TABLE}")
        
        # 2️⃣ Check Target Table
        logging.info("\n" + "─"*60)
        logging.info("2️⃣ Target 테이블 확인 중...")
        logging.info("─"*60)
        
        if not postgres.check_table(TARGET_SCHEMA, TARGET_TABLE):
            raise Exception(f"❌ Target 테이블이 존재하지 않습니다: {TARGET_SCHEMA}.{TARGET_TABLE}")
        
        # 3️⃣ Extract & Transform from Bronze
        logging.info("\n" + "─"*60)
        logging.info("3️⃣ Bronze 데이터 추출 및 변환 중...")
        logging.info("─"*60)
        
        data, extract_count = extract_and_transform_data(postgres)
        
        if not data or extract_count == 0:
            logging.warning("⚠️ 추출된 데이터가 없습니다. 작업 종료.")
            return {
                "status": "success",
                "message": "No data to process",
                "extracted": 0,
                "loaded": 0
            }
        
        # 4️⃣ Prepare data
        logging.info("\n" + "─"*60)
        logging.info("4️⃣ 데이터 준비 중...")
        logging.info("─"*60)
        
        prepared_data = prepare_insert_data(data)
        logging.info(f"✅ 준비 완료: {len(prepared_data):,} rows")
        
        # 5️⃣ Load to Silver
        logging.info("\n" + "─"*60)
        logging.info("5️⃣ Silver 레이어 적재 중...")
        logging.info("─"*60)
        
        loaded_count = load_data(postgres, prepared_data)
        
        # 6️⃣ Summary
        duration = datetime.now() - start_time
        logging.info("\n" + "="*60)
        logging.info("✅ ETL 완료")
        logging.info("="*60)
        logging.info(f"📊 Source: {SOURCE_SCHEMA}.{SOURCE_TABLE}")
        logging.info(f"📊 Target: {TARGET_SCHEMA}.{TARGET_TABLE}")
        logging.info(f"📊 Filter: plant_cd='{PLANT_CD}', op_cd='{OP_CD}'")
        logging.info(f"📊 추출: {extract_count:,} rows")
        logging.info(f"📊 적재: {loaded_count:,} rows")
        logging.info(f"⏱️  소요 시간: {duration}")
        logging.info("="*60)
        
        return {
            "status": "success",
            "source": f"{SOURCE_SCHEMA}.{SOURCE_TABLE}",
            "target": f"{TARGET_SCHEMA}.{TARGET_TABLE}",
            "filter": f"plant_cd='{PLANT_CD}', op_cd='{OP_CD}'",
            "extracted": extract_count,
            "loaded": loaded_count,
            "duration": str(duration)
        }
        
    except Exception as e:
        logging.error(f"\n{'='*60}")
        logging.error(f"❌ ETL 실패: {str(e)}")
        logging.error(f"{'='*60}")
        raise

# ────────────────────────────────────────────────────────────────
# 5️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id='ip_defect_code_init',
    default_args=DEFAULT_ARGS,
    description='IP Defect Code 초기 적재 (Silver Layer) - plant_cd=3120, op_cd=IPI',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['JJ', 'quality','IP', 'silver', 'silver layer', 'init'],
    max_active_runs=1,
) as dag:
    
    etl_task = PythonOperator(
        task_id='ip_defect_code_init',
        python_callable=bronze_to_silver_etl,
        provide_context=True,
    )
    
    etl_task

