"""공통 함수 모듈 - IP Mold MC In/Out Silver"""
import logging
from datetime import datetime, timedelta, timezone
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
POSTGRES_CONN_ID = "pg_jj_production_dw"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
BRONZE_TABLE_NAME = "jmm_mold_mc_inout_raw"
SILVER_TABLE_NAME = "ip_mold_mc_inout"
INDO_TZ = timezone(timedelta(hours=7))
INITIAL_START_DATE = datetime(2014, 10, 1, 0, 0, 0)
DAYS_OFFSET_FOR_INCREMENTAL = 2


# ════════════════════════════════════════════════════════════════
# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════

def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string with multiple format support"""
    formats = [
        "%Y-%m-%d %H:%M:%S.%f",  # 2024-01-01 12:30:45.123456
        "%Y-%m-%d %H:%M:%S",     # 2024-01-01 12:30:45
        "%Y-%m-%d"               # 2024-01-01
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse datetime string: {dt_str}")


def get_month_end_date(start_date: datetime) -> datetime:
    """Get the last day of the month for a given date"""
    next_month = start_date.replace(day=1) + timedelta(days=32)
    month_end = next_month.replace(day=1) - timedelta(days=1)
    # 23:59:59로 설정
    return month_end.replace(hour=23, minute=59, second=59, microsecond=999999)


def calculate_expected_monthly_loops(start_date: datetime, end_date: datetime) -> int:
    """Calculate expected number of monthly loops"""
    current_date = start_date
    month_count = 0
    
    while current_date < end_date:
        month_end = get_month_end_date(current_date)
        if month_end > end_date:
            month_end = end_date
        current_date = month_end + timedelta(days=1)
        month_count += 1
    
    return month_count


# ════════════════════════════════════════════════════════════════
# 3️⃣ Data Transformation
# ════════════════════════════════════════════════════════════════

def build_silver_transform_sql(start_date: str, end_date: str) -> str:
    """Build SQL query for Silver layer transformation"""
    # Convert YYYY-MM-DD to YYYYMMDD format
    start_date_formatted = start_date.replace('-', '')
    end_date_formatted = end_date.replace('-', '')
    
    return f'''
        SELECT 
            WH_ID AS "workshop",
            CASE 
                WHEN WH_ID = 'PL' THEN 
                    LPAD(SUBSTR(MC_NO, 3, 2), 3, '0') || '_MC' || 
                    LPAD(SUBSTR(MC_TABLE, 3, 1), 3, '0') || '_' ||
                    CASE 
                        WHEN SUBSTR(MC_TABLE, 1, 3) = 'ST6' THEN 'P_' 
                        ELSE SUBSTR(MC_TABLE, 4, 1) || '_' 
                    END ||
                    LPAD(
                        CASE 
                            WHEN SUBSTR(MC_TABLE, 1, 3) = 'ST6' THEN 
                                (SUBSTR(MC_TABLE, 5, 1)::INTEGER + 
                                CASE 
                                    WHEN SUBSTR(MC_TABLE, 4, 1) = 'C' THEN 0 
                                    ELSE 4 
                                END)::TEXT
                            ELSE SUBSTR(MC_TABLE, 5, 1)
                        END, 
                        3, '0'
                    )
                WHEN WH_ID = 'IP' THEN 
                    'IPI-' || MC_NO || '-' || SUBSTR(MC_TABLE, 3, 2) || '-' || SUBSTR(MC_TABLE, 5, 1) 
                WHEN WH_ID = 'CP' THEN 
                    MC_LINE || '_' || MC_NO || '_' || MC_TABLE
                WHEN WH_ID = 'OS' THEN 
                    SUBSTR(MC_LINE, 3, 2) || '-' || MC_NO 
            END AS "machine",
            MOLD_ID AS "mold_id",
            MOLD_REMOVE_DATE AS "mold_remove_date",
            MOLD_REMOVE_TIME AS "mold_remove_time",
            MOLD_INPUT_DATE AS "mold_input_date",
            MOLD_INPUT_TIME AS "mold_input_time"
        FROM {BRONZE_SCHEMA}.{BRONZE_TABLE_NAME}
        WHERE 
            WH_ID = 'IP'
            AND (MOLD_REMOVE_DATE >= '{start_date_formatted}' AND MOLD_REMOVE_DATE <= '{end_date_formatted}' OR MOLD_REMOVE_DATE IS NULL)
    '''


def extract_silver_data(pg: PostgresHelper, start_date: str, end_date: str) -> tuple:
    """Extract transformed data from Bronze layer"""
    sql = build_silver_transform_sql(start_date, end_date)
    logging.info(f"실행 쿼리: {sql}")
    
    data = pg.execute_query(sql, task_id="extract_silver_data_task", xcom_key=None)
    
    # Calculate row count from PostgreSQL result
    if data and isinstance(data, list):
        row_count = len(data)
    elif data and hasattr(data, 'rowcount'):
        row_count = data.rowcount
    else:
        row_count = 0
    
    logging.info(f"{start_date} ~ {end_date} Silver 변환 row 수: {row_count}")
    return data, row_count


# ════════════════════════════════════════════════════════════════
# 4️⃣ Data Loading
# ════════════════════════════════════════════════════════════════

def prepare_silver_insert_data(data: list, extract_time: datetime) -> list:
    """Prepare data for Silver layer insertion"""
    # PostgreSQL 결과가 딕셔너리 형태인지 확인하고 처리
    if data and isinstance(data[0], dict):
        # 딕셔너리 형태인 경우 (PostgreSQL 결과)
        return [
            (
                row['workshop'], row['machine'], row['mold_id'],
                row['mold_remove_date'], row['mold_remove_time'],
                row['mold_input_date'], row['mold_input_time'],
                extract_time,  # Silver ETL extract time
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0], row[1], row[2],  # workshop, machine, mold_id
                row[3], row[4],  # mold_remove_date, mold_remove_time
                row[5], row[6],  # mold_input_date, mold_input_time
                extract_time,    # Silver ETL extract time
            ) for row in data
        ]


def get_silver_column_names() -> list:
    """Get column names for Silver table"""
    return [
        "workshop", "machine", "mold_id",
        "mold_remove_date", "mold_remove_time",
        "mold_input_date", "mold_input_time",
        "etl_extract_time", "etl_ingest_time"
    ]


def load_silver_data(
    pg: PostgresHelper, 
    data: list, 
    extract_time: datetime,
    bronze_schema: str = BRONZE_SCHEMA,
    silver_schema: str = SILVER_SCHEMA,
    silver_table_name: str = SILVER_TABLE_NAME,
    remove_duplicates: bool = False
) -> None:
    """Load data into Silver layer"""
    insert_data = prepare_silver_insert_data(data, extract_time)
    
    # Remove duplicates within the same batch if requested (for incremental)
    if remove_duplicates:
        unique_data = []
        seen_keys = set()
        conflict_columns = ["workshop", "machine", "mold_id", "mold_remove_date", "mold_input_date"]
        
        for row in insert_data:
            # Create a unique key from conflict columns
            key = (row[0], row[1], row[2], row[3], row[5])  # workshop, machine, mold_id, mold_remove_date, mold_input_date
            if key not in seen_keys:
                unique_data.append(row)
                seen_keys.add(key)
            else:
                logging.warning(f"⚠️ 중복 데이터 제거: {key}")
        
        if len(unique_data) != len(insert_data):
            logging.info(f"🔄 중복 제거: {len(insert_data)} → {len(unique_data)} rows")
        
        insert_data = unique_data
    
    columns = get_silver_column_names()
    conflict_columns = ["workshop", "machine", "mold_id", "mold_remove_date", "mold_input_date"]
    
    chunk_size = 100 if remove_duplicates else None
    pg.insert_data(silver_schema, silver_table_name, insert_data, columns, conflict_columns, chunk_size=chunk_size)
    logging.info(f"✅ {len(insert_data)} rows inserted to Silver layer (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

