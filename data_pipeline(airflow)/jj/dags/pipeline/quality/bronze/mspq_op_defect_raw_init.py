"""
MSPQ_OP_DEFECT Initial Copy DAG
================================
Oracle LMES.MSPQ_OP_DEFECT 테이블의 전체 데이터를 PostgreSQL로 초기 복사하는 DAG
1회 실행용 - Variable 사용 없음

Source: Oracle LMES.MSPQ_OP_DEFECT
Target: PostgreSQL bronze.mspq_op_defect_raw
Execution: Manual trigger only (@once)
"""

import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────
# 1️⃣ Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=1)
}

# Database Configuration
SCHEMA_NAME = "bronze"
TABLE_NAME = "mspq_op_defect_raw"

# Connection IDs
ORACLE_CONN_ID = "orc_jj_gmes"
POSTGRES_CONN_ID = "pg_jj_quality_dw"

# Batch Configuration
BATCH_SIZE = 5000  # 한 번에 처리할 로우 수

# ────────────────────────────────────────────────────────────────
# 2️⃣ Data Extraction
# ────────────────────────────────────────────────────────────────
def build_extract_sql() -> str:
    """Build SQL query for full data extraction"""
    return '''
        SELECT
            PLANT_CD, OP_CD, DEFECT_CD, OSND_TYPE, MATRIX_TYPE,
            DEFECT_KO_NAME, DEFECT_EN_NAME, DEFECT_BH_NAME, DEFECT_VN_NAME, 
            DEFECT_CN_NAME, DEFECT_TM_NAME,
            REWORK_YN, HI_YN, OI_YN, USE_YN,
            NIKE_DEFECT_CD, MNGT_01_CD, MNGT_02_CD, MNGT_03_CD,
            MEMO, CREATOR, CREATE_DT, CREATE_PC,
            UPDATER, UPDATE_DT, UPDATE_PC,
            HI_IMAGE_TYPE, OI_IMAGE_TYPE
        FROM LMES.MSPQ_OP_DEFECT
        ORDER BY PLANT_CD, OP_CD, DEFECT_CD
    '''

def extract_data(oracle: OracleHelper) -> tuple:
    """Extract all data from Oracle database"""
    sql = build_extract_sql()
    logging.info("🔍 전체 데이터 추출 시작")
    logging.info(f"실행 쿼리: {sql}")
    
    data = oracle.execute_query(sql, task_id="extract_full_data", xcom_key=None)
    
    # Calculate row count from Oracle result
    if data and isinstance(data, list):
        row_count = len(data)
    elif data and hasattr(data, 'rowcount'):
        row_count = data.rowcount
    else:
        row_count = 0
    
    logging.info(f"✅ 추출 완료: {row_count:,} rows")
    return data, row_count

# ────────────────────────────────────────────────────────────────
# 3️⃣ Data Loading
# ────────────────────────────────────────────────────────────────
def prepare_insert_data(data: list, extract_time: datetime) -> list:
    """Prepare data for PostgreSQL insertion"""
    if not data:
        return []
    
    # Oracle 결과가 딕셔너리 형태인지 확인하고 처리
    if isinstance(data[0], dict):
        # 딕셔너리 형태인 경우 (OracleHelper 결과)
        return [
            (
                row['PLANT_CD'], row['OP_CD'], row['DEFECT_CD'], 
                row['OSND_TYPE'], row['MATRIX_TYPE'],
                row['DEFECT_KO_NAME'], row['DEFECT_EN_NAME'], row['DEFECT_BH_NAME'], 
                row['DEFECT_VN_NAME'], row['DEFECT_CN_NAME'], row['DEFECT_TM_NAME'],
                row['REWORK_YN'], row['HI_YN'], row['OI_YN'], row['USE_YN'],
                row['NIKE_DEFECT_CD'], row['MNGT_01_CD'], row['MNGT_02_CD'], row['MNGT_03_CD'],
                row['MEMO'], row['CREATOR'], row['CREATE_DT'], row['CREATE_PC'],
                row['UPDATER'], row['UPDATE_DT'], row['UPDATE_PC'],
                row['HI_IMAGE_TYPE'], row['OI_IMAGE_TYPE'],
                extract_time, extract_time  # etl_extract_time, etl_ingest_time
            ) for row in data
        ]
    else:
        # 튜플/리스트 형태인 경우
        return [
            (
                row[0], row[1], row[2], row[3], row[4],
                row[5], row[6], row[7], row[8], row[9], row[10],
                row[11], row[12], row[13], row[14],
                row[15], row[16], row[17], row[18],
                row[19], row[20], row[21], row[22],
                row[23], row[24], row[25],
                row[26], row[27],
                extract_time, extract_time
            ) for row in data
        ]

def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "plant_cd", "op_cd", "defect_cd", "osnd_type", "matrix_type",
        "defect_ko_name", "defect_en_name", "defect_bh_name", "defect_vn_name", 
        "defect_cn_name", "defect_tm_name",
        "rework_yn", "hi_yn", "oi_yn", "use_yn",
        "nike_defect_cd", "mngt_01_cd", "mngt_02_cd", "mngt_03_cd",
        "memo", "creator", "create_dt", "create_pc",
        "updater", "update_dt", "update_pc",
        "hi_image_type", "oi_image_type",
        "etl_extract_time", "etl_ingest_time"
    ]

def load_data(postgres: PostgresHelper, data: list) -> int:
    """Load data to PostgreSQL using insert_data method"""
    if not data:
        logging.warning("⚠️ 적재할 데이터가 없습니다")
        return 0
    
    total_rows = len(data)
    logging.info(f"📦 데이터 적재 시작: 총 {total_rows:,} rows")
    
    try:
        columns = get_column_names()
        conflict_columns = ["plant_cd", "op_cd", "defect_cd"]
        
        # PostgresHelper의 insert_data 메서드 사용
        postgres.insert_data(
            schema_name=SCHEMA_NAME,
            table_name=TABLE_NAME,
            data=data,
            columns=columns,
            conflict_columns=conflict_columns,
            chunk_size=BATCH_SIZE
        )
        
        logging.info(f"🎉 전체 적재 완료: {total_rows:,} rows")
        return total_rows
        
    except Exception as e:
        logging.error(f"❌ 데이터 적재 실패: {str(e)}")
        raise

# ────────────────────────────────────────────────────────────────
# 4️⃣ Main ETL Task
# ────────────────────────────────────────────────────────────────
def full_copy_etl(**kwargs):
    """
    Main ETL function for initial copy from Oracle to PostgreSQL
    1회 실행용 - Variable 미사용
    """
    extract_time = datetime.now()
    logging.info(f"{'='*60}")
    logging.info(f"🚀 MSPQ_OP_DEFECT 초기 복사 시작 (1회 실행)")
    logging.info(f"{'='*60}")
    logging.info(f"📅 Extract Time: {extract_time}")
    
    try:
        # 1️⃣ Extract from Oracle
        logging.info("\n" + "─"*60)
        logging.info("1️⃣ Oracle 데이터 추출 중...")
        logging.info("─"*60)
        
        oracle = OracleHelper(conn_id=ORACLE_CONN_ID)
        
        # 테이블 존재 확인
        if not oracle.check_table("LMES", "MSPQ_OP_DEFECT"):
            raise Exception("❌ Oracle 테이블이 존재하지 않습니다: LMES.MSPQ_OP_DEFECT")
        
        data, extract_count = extract_data(oracle)
        
        if not data or extract_count == 0:
            logging.warning("⚠️ 추출된 데이터가 없습니다. 작업 종료.")
            return {
                "status": "success",
                "message": "No data to process",
                "extracted": 0,
                "loaded": 0
            }
        
        # 2️⃣ Transform (Prepare data)
        logging.info("\n" + "─"*60)
        logging.info("2️⃣ 데이터 변환 중...")
        logging.info("─"*60)
        
        prepared_data = prepare_insert_data(data, extract_time)
        logging.info(f"✅ 변환 완료: {len(prepared_data):,} rows")
        
        # 3️⃣ Load to PostgreSQL
        logging.info("\n" + "─"*60)
        logging.info("3️⃣ PostgreSQL 적재 중...")
        logging.info("─"*60)
        
        postgres = PostgresHelper(conn_id=POSTGRES_CONN_ID)
        
        # 테이블 존재 확인
        if not postgres.check_table(SCHEMA_NAME, TABLE_NAME):
            raise Exception(f"❌ PostgreSQL 테이블이 존재하지 않습니다: {SCHEMA_NAME}.{TABLE_NAME}")
        
        loaded_count = load_data(postgres, prepared_data)
        
        # 4️⃣ Summary
        logging.info("\n" + "="*60)
        logging.info("✅ ETL 완료")
        logging.info("="*60)
        logging.info(f"📊 추출: {extract_count:,} rows")
        logging.info(f"📊 적재: {loaded_count:,} rows")
        logging.info(f"⏱️  소요 시간: {datetime.now() - extract_time}")
        logging.info("="*60)
        
        return {
            "status": "success",
            "extract_time": extract_time.isoformat(),
            "extracted": extract_count,
            "loaded": loaded_count,
            "duration": str(datetime.now() - extract_time)
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
    dag_id='mspq_op_defect_raw_init',
    default_args=DEFAULT_ARGS,
    description='LMES.MSPQ_OP_DEFECT 전체 데이터 초기 복사 (Oracle → PostgreSQL) - 1회 실행',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['JJ', 'quality', 'bronze layer', 'raw', 'init', 'master'],
    max_active_runs=1,
) as dag:
    
    init_copy_task = PythonOperator(
        task_id='init_copy_mspq_op_defect',
        python_callable=full_copy_etl,
        provide_context=True,
    )
    
    init_copy_task

