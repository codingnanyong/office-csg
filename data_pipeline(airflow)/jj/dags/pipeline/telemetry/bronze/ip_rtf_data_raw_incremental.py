import logging   
import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from plugins.hooks.mysql_hook import MySQLHelper
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────
# 1️⃣ Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)
}

# Database Configuration - 머신별 독립적인 Variable 사용
INCREMENT_KEY_20 = "last_extract_time_ip_rtf_data_raw_20"
INCREMENT_KEY_34 = "last_extract_time_ip_rtf_data_raw_34"
TABLE_NAME = "ip_rtf_data_raw"
SCHEMA_NAME = "bronze"

# Connection IDs
IP_CONN_DB = ["maria_ip_20", "maria_ip_34"]
POSTGRES_CONN_ID = "pg_jj_telemetry_dw"  # TimescaleDB 대상

# Date Configuration
INDO_TZ = timezone(timedelta(hours=7))
HOURS_OFFSET_FOR_INCREMENTAL = 1  # 1시간 전 데이터까지 (안전 마진)

# 센서 설정
IP_MACHINE_NO = ["20", "34"]  # 센서 존 번호 (DAG 파라미터로 변경 가능)

# ────────────────────────────────────────────────────────────────
# 2️⃣ Utility Functions
# ────────────────────────────────────────────────────────────────
def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string with microsecond support"""
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

# ────────────────────────────────────────────────────────────────
# 3️⃣ Data Extraction
# ────────────────────────────────────────────────────────────────
def build_extract_sql(start_date: str, end_date: str) -> str:
    """Build SQL query for sensor data extraction"""
    return f'''
        SELECT
            SeqNo, PID, RxDate, Pvalue
        FROM rtf_data
        WHERE RxDate BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY RxDate
    '''

def extract_data(mysql: MySQLHelper, start_date: str, end_date: str) -> tuple:
    """Extract sensor data from MySQL database"""
    sql = build_extract_sql(start_date, end_date)
    logging.info(f"실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, task_id="extract_data_task", xcom_key=None)
    
    # Calculate row count from MySQL result
    if data and (isinstance(data, list) or isinstance(data, tuple)) and len(data) > 0:
        row_count = len(data)
        logging.info(f"{start_date} ~ {end_date} 추출 row 수: {row_count}")
    else:
        row_count = 0
        logging.info(f"{start_date} ~ {end_date} 추출 row 수: {row_count} (데이터 없음)")
    
    return data, row_count

# ────────────────────────────────────────────────────────────────
# 4️⃣ Data Loading
# ────────────────────────────────────────────────────────────────
def prepare_insert_data(data: list, extract_time: datetime, machine_no: str) -> list:
    """Prepare sensor data for PostgreSQL insertion"""
    machine_no_with_prefix = f"MCA{machine_no}"
    
    if data and isinstance(data[0], dict):
        return [
            (
                machine_no_with_prefix,
                row['SeqNo'],
                row['PID'],
                row['RxDate'],
                row['Pvalue'],
                extract_time
            ) for row in data
        ]
    else:
        return [
            (
                machine_no_with_prefix,
                row[0],
                row[1],
                row[2],
                row[3],
                extract_time
            ) for row in data
        ]

def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "machine_no",
        "seqno",
        "pid", 
        "rxdate",
        "pvalue",
        "etl_extract_time"
    ]

def load_data(pg: PostgresHelper, data: list, extract_time: datetime, machine_no: str) -> None:
    """Load sensor data into PostgreSQL TimescaleDB"""
    insert_data = prepare_insert_data(data, extract_time, machine_no)
    columns = get_column_names()
    conflict_columns = ["machine_no", "seqno", "rxdate"]
    
    pg.insert_data(SCHEMA_NAME, TABLE_NAME, insert_data, columns, conflict_columns, chunk_size=10000)
    logging.info(f"✅ {len(data):,} rows inserted (duplicates ignored) for machine {machine_no}.")

# ────────────────────────────────────────────────────────────────
# 5️⃣ Variable Management
# ────────────────────────────────────────────────────────────────
def get_machine_variables(machine_no: str) -> dict:
    """Get machine-specific extract times from Variable"""
    try:
        if machine_no == "20":
            variables_str = Variable.get(INCREMENT_KEY_20, default_var="{}")
        else:
            variables_str = Variable.get(INCREMENT_KEY_34, default_var="{}")
        return json.loads(variables_str)
    except Exception as e:
        logging.warning(f"Variable 파싱 실패, 빈 딕셔너리 사용: {e}")
        return {}

def update_machine_variable(machine_no: str, end_extract_time: str) -> None:
    """Update machine-specific extract time in JSON Variable"""

    try:
        # Get current variables for this machine
        variables = get_machine_variables(machine_no)
        
        # Update specific machine
        mca_machine_no = f"MCA{machine_no}"
        variables[mca_machine_no] = end_extract_time
        
        # Save back to machine-specific Variable
        if machine_no == "20":
            variable_key = INCREMENT_KEY_20
        else:
            variable_key = INCREMENT_KEY_34
            
        try:
            Variable.set(variable_key, json.dumps(variables))
        except Exception as e:
            # 기존 값이 있는 경우 업데이트
            if "already exists" in str(e):
                # Variable을 삭제하고 다시 생성
                try:
                    Variable.delete(variable_key)
                except:
                    pass  # 삭제 실패해도 무시
                Variable.set(variable_key, json.dumps(variables))
            else:
                raise e
        
        logging.info(f"📌 Machine {machine_no} Variable Update: {end_extract_time}")
        logging.info(f"📌 Machine {machine_no} Variable 상태: {variables}")
    except Exception as e:
        logging.error(f"❌ Variable 업데이트 실패: {e}")

def get_machine_last_extract_time(machine_no: str) -> str:
    """Get last extract time for specific machine"""
    variables = get_machine_variables(machine_no)
    mca_machine_no = f"MCA{machine_no}"
    return variables.get(mca_machine_no, None)

# ────────────────────────────────────────────────────────────────
# 6️⃣ Hourly Incremental Collection
# ────────────────────────────────────────────────────────────────
def process_hourly_incremental_for_machine(
    mysql: MySQLHelper, 
    pg: PostgresHelper, 
    start_date: datetime, 
    end_date: datetime,
    machine_no: str
) -> dict:
    """Process hourly incremental collection for specific machine"""
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"📅 센서 데이터 수집 시작: {start_str} ~ {end_str} (Machine: {machine_no})")
    
    # 데이터 추출 및 적재
    data, row_count = extract_data(mysql, start_str, end_str)
    
    if row_count > 0:
        extract_time = datetime.utcnow()
        load_data(pg, data, extract_time, machine_no)
        logging.info(f"✅ 센서 데이터 수집 완료: {row_count} rows (Machine: {machine_no})")
        
        # Update machine-specific variable
        update_machine_variable(machine_no, end_str)
        
        return {
            "machine_no": machine_no,
            "rows_processed": row_count,
            "start_time": start_str,
            "end_time": end_str,
            "extract_time": extract_time.isoformat(),
            "status": "completed"
        }
    else:
        logging.info(f"⚠️ 수집할 센서 데이터가 없습니다: {start_str} ~ {end_str} (Machine: {machine_no})")
        
        # Update variable even if no data (to track progress)
        update_machine_variable(machine_no, end_str)
        
        return {
            "machine_no": machine_no,
            "rows_processed": 0,
            "start_time": start_str,
            "end_time": end_str,
            "status": "no_data"
        }

def incremental_machine_20_task(**kwargs) -> dict:
    """Incremental task for Machine 20"""
    return process_machine_incremental("20", 0)

def incremental_machine_34_task(**kwargs) -> dict:
    """Incremental task for Machine 34"""
    return process_machine_incremental("34", 1)

def process_machine_incremental(machine_no: str, machine_index: int) -> dict:
    """Process hourly incremental collection for specific machine"""
    logging.info(f"🔄 Machine {machine_no} 처리 시작")
    
    # Get machine-specific last extract time
    last_extract_time_str = get_machine_last_extract_time(machine_no)
    
    # 최대 허용 시간: 현재 시간 - 1시간 (안전 마진)
    max_allowed_time = datetime.now(INDO_TZ) - timedelta(hours=1)
    max_allowed_time = max_allowed_time.replace(minute=0, second=0, microsecond=0)
    
    if last_extract_time_str:
        # 마지막 추출 시간을 파싱
        last_extract_time = parse_datetime(last_extract_time_str)
        
        # 마지막 추출 시간의 다음 시간부터 1시간 동안
        start_date = last_extract_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        # 시간대 정보 추가
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=INDO_TZ)
        end_date = start_date.replace(minute=59, second=59, microsecond=999999)
        
        # 🔒 안전 제약: 최대 허용 시간을 초과하지 않도록 제한
        if start_date > max_allowed_time:
            logging.warning(f"⚠️ Machine {machine_no} 요청 시간이 안전 마진을 초과합니다!")
            logging.warning(f"   요청 시간: {start_date.strftime('%Y-%m-%d %H:00')}")
            logging.warning(f"   최대 허용: {max_allowed_time.strftime('%Y-%m-%d %H:00')}")
            logging.warning(f"   Machine {machine_no} 데이터 수집을 건너뜁니다.")
            return {
                "machine_no": machine_no,
                "rows_processed": 0,
                "status": "skipped_safety_margin",
                "reason": f"Requested time exceeds safety margin (max: {max_allowed_time.strftime('%Y-%m-%d %H:00')})"
            }
        
        logging.info(f"Machine {machine_no} 마지막 추출 시간: {last_extract_time_str}")
        logging.info(f"Machine {machine_no} 다음 시간 센서 데이터 수집: {start_date.strftime('%Y-%m-%d %H:00')}")
    else:
        # Variable이 없으면 1시간 전 데이터 수집 (안전 마진 적용)
        one_hour_ago = datetime.now(INDO_TZ) - timedelta(hours=1)
        start_date = one_hour_ago.replace(minute=0, second=0, microsecond=0)
        end_date = start_date.replace(minute=59, second=59, microsecond=999999)
        logging.info(f"Machine {machine_no} Variable이 없어서 1시간 전 센서 데이터 수집: {start_date.strftime('%Y-%m-%d %H:00')}")
    
    # 시간대 변환: UTC -> KST
    start_date_kst = start_date + timedelta(hours=9)
    end_date_kst = end_date + timedelta(hours=9)
    
    start_str = start_date_kst.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date_kst.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"📅 Machine {machine_no} 센서 데이터 수집 시작: {start_str} ~ {end_str}")
    
    # Process this machine
    conn_id = IP_CONN_DB[machine_index]
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
    
    try:
        machine_result = process_hourly_incremental_for_machine(
            mysql, pg, start_date, end_date, machine_no
        )
        logging.info(f"✅ Machine {machine_no} 완료: {machine_result['rows_processed']} rows")
        return machine_result
    except Exception as e:
        logging.error(f"❌ Machine {machine_no} 처리 실패: {str(e)}")
        return {
            "machine_no": machine_no,
            "rows_processed": 0,
            "status": "failed",
            "error": str(e)
        }

# ────────────────────────────────────────────────────────────────
# 7️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ip_rtf_data_raw_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",  # 매시간 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["CKP", "raw", "bronze layer", "incremental", "telemetry", "sensors", "hourly"]
) as dag:
    
    # Start task
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("🚀 센서 데이터 Incremental 수집 시작"),
    )
    
    # Machine-specific tasks (parallel execution)
    incremental_machine_20 = PythonOperator(
        task_id="incremental_machine_20",
        python_callable=incremental_machine_20_task,
        provide_context=True,
    )
    
    incremental_machine_34 = PythonOperator(
        task_id="incremental_machine_34",
        python_callable=incremental_machine_34_task,
        provide_context=True,
    )
    
    # End task
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("🎉 센서 데이터 Incremental 수집 완료"),
    )
    
    # Task dependencies
    start >> [incremental_machine_20, incremental_machine_34] >> end
