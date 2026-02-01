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

# Date Configuration - 최근 데이터가 있는 날짜로 변경
INITIAL_START_DATE = datetime(2024, 8, 21, 0, 0, 0)
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

def get_hour_end_date(start_date: datetime) -> datetime:
    """Get the end of the hour for a given date"""
    # 시간을 정규화해서 해당 시간의 마지막 초로 설정 (예: 21:00:00 -> 21:59:59)
    normalized_hour = start_date.replace(minute=0, second=0, microsecond=0)
    return normalized_hour.replace(minute=59, second=59, microsecond=999999)

def calculate_expected_hourly_loops(start_date: datetime, end_date: datetime) -> int:
    """Calculate expected number of hourly loops"""
    current_date = start_date
    hour_count = 0
    
    while current_date < end_date:
        hour_end = get_hour_end_date(current_date)
        if hour_end > end_date:
            hour_end = end_date
        current_date = hour_end + timedelta(hours=1)
        hour_count += 1
    
    return hour_count

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
    
    # 디버깅을 위한 로그 추가
    logging.info(f"🔍 DEBUG: data type: {type(data)}, data value: {data is not None}")
    if data is not None:
        logging.info(f"🔍 DEBUG: data length: {len(data) if isinstance(data, list) else 'not a list'}")
    
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
    # MCA 접두사 추가
    machine_no_with_prefix = f"MCA{machine_no}"
    
    if data and isinstance(data[0], dict):
        # 딕셔너리 형태인 경우 (MySQL 결과)
        return [
            (
                machine_no_with_prefix,  # machine_no (MCA + 센서 존 번호)
                row['SeqNo'],
                row['PID'],
                row['RxDate'],
                row['Pvalue'],
                extract_time
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우
        return [
            (
                machine_no_with_prefix,  # machine_no (MCA + 센서 존 번호)
                row[0],  # SeqNo
                row[1],  # PID
                row[2],  # RxDate
                row[3],  # Pvalue
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
    conflict_columns = ["machine_no", "seqno", "rxdate"]  # TimescaleDB PK 기준
    
    pg.insert_data(SCHEMA_NAME, TABLE_NAME, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored) for machine {machine_no}.")

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
        
        # Update specific machine with MCA prefix
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

def get_machine_start_date(machine_no: str) -> datetime:
    """Get start date for specific machine"""
    variables = get_machine_variables(machine_no)
    
    # Check for MCA prefix in variables
    mca_machine_no = f"MCA{machine_no}"
    if mca_machine_no in variables:
        last_time = parse_datetime(variables[mca_machine_no])
        logging.info(f"Machine {machine_no} 이전 진행 지점 사용: {last_time}")
        return last_time
    else:
        logging.info(f"Machine {machine_no} 초기 시작 날짜 사용: {INITIAL_START_DATE}")
        return INITIAL_START_DATE

# ────────────────────────────────────────────────────────────────
# 6️⃣ Main Backfill Logic
# ────────────────────────────────────────────────────────────────
def process_hourly_batch_for_machine(
    mysql: MySQLHelper, 
    pg: PostgresHelper, 
    start_date: datetime, 
    end_date: datetime,
    machine_no: str,
    loop_count: int,
    expected_loops: int
) -> dict:
    """Process a single hourly batch for specific machine"""
    logging.info(f"🔄 루프 {loop_count}/{expected_loops} 시작 (Machine: {machine_no})")
    
    # 시간대 변환: UTC -> KST (MySQL은 KST 시간으로 저장됨)
    # UTC + 9시간 = KST (정상적인 시간대 변환)
    start_date_kst = start_date + timedelta(hours=9)
    end_date_kst = end_date + timedelta(hours=9)
    
    start_str = start_date_kst.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date_kst.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"시간별 배치 처리 중: {start_str} ~ {end_str} (Machine: {machine_no})")
    logging.info(f"🔍 시간대 변환: UTC {start_date.strftime('%Y-%m-%d %H:%M:%S')} -> 조정된 시간 {start_str}")
    
    data, row_count = extract_data(mysql, start_str, end_str)
    
    if row_count > 0:
        extract_time = datetime.utcnow()
        load_data(pg, data, extract_time, machine_no)
        logging.info(f"✅ 시간별 배치 완료: {start_str} ~ {end_str} ({row_count:,} rows) for machine {machine_no}")
    else:
        logging.info(f"시간별 배치에 데이터 없음: {start_str} ~ {end_str} (Machine: {machine_no})")
    
    # Update machine-specific variable
    update_machine_variable(machine_no, end_str)
    
    return {
        "loop": loop_count,
        "machine_no": machine_no,
        "start": start_str,
        "end": end_str,
        "row_count": row_count,
        "batch_size_hours": (end_date - start_date).total_seconds() / 3600,
        "datetime": start_date.strftime("%Y-%m-%d %H:00")
    }

def backfill_machine_20_task(**kwargs) -> dict:
    """Backfill task for Machine 20"""
    return process_machine_backfill("20", 0)

def backfill_machine_34_task(**kwargs) -> dict:
    """Backfill task for Machine 34"""
    return process_machine_backfill("34", 1)

def process_machine_backfill(machine_no: str, machine_index: int) -> dict:
    """Process backfill for a single machine"""
    # Calculate end date (common for all machines) - 안전 마진 적용
    end_date = datetime.now(INDO_TZ).replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(hours=1)  # 최소 1시간 안전 마진
    
    logging.info(f"🔄 Machine {machine_no} 처리 시작")
    
    # Get machine-specific start date
    start_date = get_machine_start_date(machine_no)
    
    # Set timezone
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=INDO_TZ)
    
    # Calculate expected loops for this machine
    expected_loops = calculate_expected_hourly_loops(start_date, end_date)
    
    logging.info(f"Machine {machine_no} 시작: {start_date} ~ {end_date}")
    logging.info(f"Machine {machine_no} 예상 루프: {expected_loops}회 (시간별)")
    
    # Process hourly batches for this machine
    machine_results = []
    loop_count = 0
    current_date = start_date
    
    while current_date < end_date:
        loop_count += 1
        
        # Calculate hour end date
        hour_end = get_hour_end_date(current_date)
        if hour_end > end_date:
            hour_end = end_date
        
        # Process this hour for this machine
        conn_id = IP_CONN_DB[machine_index]
        mysql = MySQLHelper(conn_id=conn_id)
        pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
        
        # 시간을 정규화 (00:00:00 형태로)
        normalized_start = current_date.replace(minute=0, second=0, microsecond=0)
        normalized_end = hour_end.replace(minute=59, second=59, microsecond=999999)
        
        try:
            batch_result = process_hourly_batch_for_machine(
                mysql, pg, normalized_start, normalized_end, machine_no, loop_count, expected_loops
            )
            machine_results.append(batch_result)
            logging.info(f"✅ Machine {machine_no} 루프 {loop_count} 완료: {batch_result['row_count']:,} rows")
        except Exception as e:
            logging.error(f"❌ Machine {machine_no} 루프 {loop_count} 실패: {str(e)}")
        
        # Move to next hour
        current_date = hour_end + timedelta(hours=1)
    
    total_rows = sum([r['row_count'] for r in machine_results])
    logging.info(f"🎉 Machine {machine_no} 완료! {len(machine_results)}회 루프, {total_rows:,}개 rows")
    
    return {
        "status": "backfill_completed",
        "machine_no": machine_no,
        "total_batches": len(machine_results),
        "total_rows": sum([r['row_count'] for r in machine_results]),
        "results": machine_results
    }

# ────────────────────────────────────────────────────────────────
# 7️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ip_rtf_data_raw_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["CKP", "raw", "bronze layer", "backfill", "telemetry", "sensors", "hourly"]
) as dag:
    
    # Start task
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("🚀 센서 데이터 Backfill 시작"),
    )
    
    # Machine-specific tasks (parallel execution)
    backfill_machine_20 = PythonOperator(
        task_id="backfill_machine_20",
        python_callable=backfill_machine_20_task,
        provide_context=True,
    )
    
    backfill_machine_34 = PythonOperator(
        task_id="backfill_machine_34", 
        python_callable=backfill_machine_34_task,
        provide_context=True,
    )
    
    # End task
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("🎉 센서 데이터 Backfill 완료"),
    )
    
    # Task dependencies
    start >> [backfill_machine_20, backfill_machine_34] >> end
