"""
CTM Chiller Device Raw Common Functions
========================================
공통 함수 및 설정을 모아둔 모듈
"""

import logging
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────

# Database Configuration
TABLE_NAME = "ctm_chiller_device_raw"
SCHEMA_NAME = "bronze"

# Connection IDs
SOURCE_POSTGRES_CONN_ID = "pg_ckp_chiller"
TARGET_POSTGRES_CONN_ID = "pg_jj_telemetry_dw"

# ────────────────────────────────────────────────────────────────
# Data Extraction
# ────────────────────────────────────────────────────────────────
def build_extract_sql() -> str:
    """Build SQL query for CTM chiller device data extraction"""
    # 조건 없이 전체 데이터 추출, CHILLER 장비만 필터링
    return '''
        SELECT 
            device_id,
            company_cd,
            device_kind,
            device_name,
            descn,
            ip_addr,
            st_num,
            reg_addr,
            reg_num,
            data_type,
            building_cd,
            floor_cd,
            line_cd,
            mline_cd,
            op_cd,
            upd_dt
        FROM public.device
        ORDER BY device_id
    '''


def extract_data(pg: PostgresHelper) -> tuple:
    """Extract data from CTM PostgreSQL database"""
    sql = build_extract_sql()
    logging.info(f"실행 쿼리: {sql}")
    
    data = pg.execute_query(sql, task_id="extract_data_task", xcom_key=None)
    
    # Calculate row count
    if data and isinstance(data, list):
        row_count = len(data)
    else:
        row_count = 0
    
    logging.info(f"CHILLER 장비 데이터 추출 row 수: {row_count}")
    return data, row_count


# ────────────────────────────────────────────────────────────────
# Data Loading
# ────────────────────────────────────────────────────────────────
def prepare_insert_data(data: list, extract_time: datetime) -> list:
    """Prepare data for PostgreSQL insertion"""
    # 딕셔너리 형태인지 확인하고 처리
    if data and isinstance(data[0], dict):
        # 딕셔너리 형태인 경우 (PostgreSQL 결과)
        return [
            (
                row['device_id'],
                row['company_cd'],
                row['device_kind'],
                row['device_name'],
                row['descn'],
                row['ip_addr'],
                row['st_num'],
                row['reg_addr'],
                row['reg_num'],
                row['data_type'],
                row['building_cd'],
                row['floor_cd'],
                row['line_cd'],
                row['mline_cd'],
                row['op_cd'],
                row['upd_dt'],
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0],   # device_id
                row[1],   # company_cd
                row[2],   # device_kind
                row[3],   # device_name
                row[4],   # descn
                row[5],   # ip_addr
                row[6],   # st_num
                row[7],   # reg_addr
                row[8],   # reg_num
                row[9],   # data_type
                row[10],  # building_cd
                row[11],  # floor_cd
                row[12],  # line_cd
                row[13],  # mline_cd
                row[14],  # op_cd
                row[15],  # upd_dt
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "device_id",
        "company_cd",
        "device_kind",
        "device_name",
        "descn",
        "ip_addr",
        "st_num",
        "reg_addr",
        "reg_num",
        "data_type",
        "building_cd",
        "floor_cd",
        "line_cd",
        "mline_cd",
        "op_cd",
        "upd_dt",
        "etl_extract_time",
        "etl_ingest_time"
    ]


def load_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_column_names()
    conflict_columns = ["device_id"]  # device_id 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, TABLE_NAME, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows upserted (device_id 기준).")


# ────────────────────────────────────────────────────────────────
# ETL Logic
# ────────────────────────────────────────────────────────────────
def ctm_chiller_device_etl_task(**kwargs) -> dict:
    """CTM chiller device 데이터 ETL 태스크"""
    try:
        source_pg = PostgresHelper(conn_id=SOURCE_POSTGRES_CONN_ID)
        target_pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
        
        logging.info("🔄 CTM Chiller Device ETL 시작")
        
        # 데이터 추출
        data, row_count = extract_data(source_pg)
        
        if row_count > 0:
            extract_time = datetime.utcnow()
            load_data(target_pg, data, extract_time)
            logging.info(f"✅ ETL 완료: {row_count} rows 처리")
            
            return {
                "status": "etl_completed",
                "rows_processed": row_count,
                "extract_time": extract_time.isoformat(),
                "message": "CTM chiller device 데이터 ETL 완료"
            }
        else:
            logging.info("⚠️ 처리할 데이터가 없습니다.")
            
            return {
                "status": "etl_completed_no_data",
                "rows_processed": 0,
                "message": "처리할 데이터가 없음"
            }
    except Exception as e:
        # 연결 실패 등 네트워크 오류인 경우 Skip 처리
        error_str = str(e)
        error_lower = error_str.lower()
        is_connection_error = (
            "connection" in error_lower or
            "timeout" in error_lower or
            "timed out" in error_lower or
            "connection reset" in error_lower or
            "reset by peer" in error_lower or
            "errno" in error_lower or
            "network" in error_lower or
            "could not connect" in error_lower or
            "unable to connect" in error_lower
        )
        
        if is_connection_error:
            logging.warning(f"⚠️ 연결 실패: {error_str} - 태스크 Skip")
            skip_msg = (
                f"⏭️ CTM Chiller Device ETL 중 연결 불가 - 태스크 Skip\n"
                f"원인: {error_str}\n"
                f"설명: 소스 또는 타겟 데이터베이스 연결이 불가능합니다.\n"
                f"      다음 실행 시 자동으로 재시도됩니다."
            )
            logging.warning(skip_msg)
            raise AirflowSkipException(skip_msg) from e
        
        # 그 외 오류는 그대로 raise
        logging.error(f"❌ CTM Chiller Device ETL 실패: {e}")
        raise

