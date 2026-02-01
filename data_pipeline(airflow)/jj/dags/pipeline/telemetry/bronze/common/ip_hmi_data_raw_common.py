"""
IP HMI Data Raw Common Functions
=================================
공통 함수 및 설정을 모아둔 모듈
"""

import logging
import json
import time
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
from plugins.hooks.mysql_hook import MySQLHelper
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────

# Database Configuration - 머신별 독립적인 Variable 사용
TABLE_NAME = "ip_hmi_data_raw"
SCHEMA_NAME = "bronze"

# Connection IDs (설비 번호 순서대로 정렬)
IP_CONN_DB = ["maria_ip_04", "maria_ip_12", "maria_ip_20", "maria_ip_34", "maria_ip_37"]
POSTGRES_CONN_ID = "pg_jj_telemetry_dw"  # TimescaleDB 대상

# Date Configuration
INDO_TZ = timezone(timedelta(hours=7))
HOURS_OFFSET_FOR_INCREMENTAL = 1  # 1시간 전 데이터까지 (안전 마진)

# 센서 설정 (설비 번호 순서대로 정렬)
IP_MACHINE_NO = ["04", "12", "20", "34", "37"]  # 센서 존 번호

# Backfill Configuration
INITIAL_START_DATE = datetime(2024, 8, 21, 0, 0, 0)

# Query Timeout Configuration
QUERY_TIMEOUT_SECONDS = 600  # 쿼리 타임아웃: 10분 (600초)
IP04_CHUNK_MINUTES = 10  # IP04 쿼리 지연 대응: 10분 단위 분할


# ────────────────────────────────────────────────────────────────
# Utility Functions
# ────────────────────────────────────────────────────────────────
def get_increment_key(machine_no: str) -> str:
    """Get increment key for specific machine"""
    return f"last_extract_time_ip_hmi_data_raw_{machine_no}"


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


def _iter_time_windows(start_dt: datetime, end_dt: datetime, chunk_minutes: int):
    """Iterate time windows between start/end with inclusive boundaries."""
    cur = start_dt
    while cur <= end_dt:
        window_end = cur + timedelta(minutes=chunk_minutes) - timedelta(seconds=1)
        if window_end > end_dt:
            window_end = end_dt
        yield cur, window_end
        cur = window_end + timedelta(seconds=1)


# ────────────────────────────────────────────────────────────────
# Data Extraction
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
    """Extract sensor data from MySQL database
    
    쿼리 실행 시간이 10분을 초과하면 TimeoutError를 발생시킵니다.
    """
    sql = build_extract_sql(start_date, end_date)
    logging.info(f"실행 쿼리: {sql}")
    
    # 쿼리 실행 시작 시간 기록
    query_start_time = time.time()
    
    try:
        data = mysql.execute_query(sql, task_id="extract_data_task", xcom_key=None)
    except Exception as e:
        # 쿼리 실행 시간 확인 (10분 초과 시 추가 정보 제공)
        elapsed_time = time.time() - query_start_time
        if elapsed_time > QUERY_TIMEOUT_SECONDS:
            timeout_minutes = QUERY_TIMEOUT_SECONDS / 60
            error_msg = f"쿼리 실행 시간이 {timeout_minutes}분({QUERY_TIMEOUT_SECONDS}초)을 초과하여 중단되었습니다 (경과 시간: {elapsed_time:.1f}초)"
            logging.error(f"⏱️ {error_msg}")
            raise TimeoutError(error_msg) from e
        raise
    
    # 쿼리 실행 시간 확인 (10분 초과 시 예외 발생)
    elapsed_time = time.time() - query_start_time
    if elapsed_time > QUERY_TIMEOUT_SECONDS:
        timeout_minutes = QUERY_TIMEOUT_SECONDS / 60
        error_msg = f"쿼리 실행 시간이 {timeout_minutes}분({QUERY_TIMEOUT_SECONDS}초)을 초과하여 중단되었습니다 (경과 시간: {elapsed_time:.1f}초)"
        logging.error(f"⏱️ {error_msg}")
        raise TimeoutError(error_msg)
    
    # Calculate row count from MySQL result
    if data and (isinstance(data, list) or isinstance(data, tuple)) and len(data) > 0:
        row_count = len(data)
        logging.info(f"{start_date} ~ {end_date} 추출 row 수: {row_count} (실행 시간: {elapsed_time:.1f}초)")
        logging.info("샘플 row: %s", data[0])
    else:
        row_count = 0
        logging.info(f"{start_date} ~ {end_date} 추출 row 수: {row_count} (데이터 없음, 실행 시간: {elapsed_time:.1f}초)")
    
    return data, row_count


# ────────────────────────────────────────────────────────────────
# Data Loading
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
# Variable Management
# ────────────────────────────────────────────────────────────────
def get_machine_variables(machine_no: str) -> dict:
    """Get machine-specific extract times from Variable"""
    try:
        increment_key = get_increment_key(machine_no)
        variables_str = Variable.get(increment_key, default_var="{}")
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
        variable_key = get_increment_key(machine_no)
            
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


def get_machine_start_date(machine_no: str) -> datetime:
    """Get start date for specific machine"""
    variables = get_machine_variables(machine_no)
    
    # Check for MCA prefix in variables
    mca_machine_no = f"MCA{machine_no}"
    if mca_machine_no in variables:
        # Variable에 저장된 시간은 KST 시간 (MySQL 시간)
        last_time_str = variables[mca_machine_no]
        last_time = parse_datetime(last_time_str)
        # KST (UTC+9)로 설정
        if last_time.tzinfo is None:
            kst_tz = timezone(timedelta(hours=9))
            last_time = last_time.replace(tzinfo=kst_tz)
        # INDO_TZ (UTC+7)로 변환
        last_time_indo = last_time.astimezone(INDO_TZ)
        logging.info(f"Machine {machine_no} 이전 진행 지점 사용 (KST -> INDO_TZ): {last_time} -> {last_time_indo}")
        return last_time_indo
    else:
        logging.info(f"Machine {machine_no} 초기 시작 날짜 사용: {INITIAL_START_DATE}")
        return INITIAL_START_DATE


# ────────────────────────────────────────────────────────────────
# Hourly Incremental Collection
# ────────────────────────────────────────────────────────────────
def process_hourly_incremental_for_machine(
    mysql: MySQLHelper, 
    pg: PostgresHelper, 
    start_date: datetime, 
    end_date: datetime,
    machine_no: str
) -> dict:
    """Process hourly incremental collection for specific machine"""
    # start_date와 end_date는 이미 KST 시간대로 전달됨
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"📅 센서 데이터 수집 시작: {start_str} ~ {end_str} (Machine: {machine_no})")

    chunk_minutes = IP04_CHUNK_MINUTES if machine_no == "04" else 60
    total_rows = 0
    last_extract_time = None

    for window_start, window_end in _iter_time_windows(start_date, end_date, chunk_minutes):
        w_start_str = window_start.strftime("%Y-%m-%d %H:%M:%S")
        w_end_str = window_end.strftime("%Y-%m-%d %H:%M:%S")
        logging.info(
            f"🧩 분할 수집({chunk_minutes}분, Machine {machine_no}): {w_start_str} ~ {w_end_str}"
        )

        try:
            data, row_count = extract_data(mysql, w_start_str, w_end_str)
        except Exception as e:
            error_msg = str(e)
            logging.warning(f"⚠️ MySQL 연결/쿼리 실패 (Machine {machine_no}): {error_msg} - 스킵합니다")
            return {
                "machine_no": machine_no,
                "rows_processed": 0,
                "start_time": start_str,
                "end_time": end_str,
                "status": "failed",
                "error": error_msg
            }

        if row_count > 0:
            extract_time = datetime.utcnow()
            load_data(pg, data, extract_time, machine_no)
            total_rows += row_count
            last_extract_time = extract_time
            logging.info(f"✅ 분할 수집 완료: {row_count} rows (Machine: {machine_no})")
        else:
            logging.info(f"⚠️ 분할 수집 데이터 없음: {w_start_str} ~ {w_end_str} (Machine: {machine_no})")

    # Update machine-specific variable (모든 분할 처리 완료 후)
    update_machine_variable(machine_no, end_str)

    if total_rows > 0:
        return {
            "machine_no": machine_no,
            "rows_processed": total_rows,
            "start_time": start_str,
            "end_time": end_str,
            "extract_time": last_extract_time.isoformat() if last_extract_time else None,
            "status": "completed"
        }

    return {
        "machine_no": machine_no,
        "rows_processed": 0,
        "start_time": start_str,
        "end_time": end_str,
        "status": "no_data"
    }


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
        # Variable에 저장된 시간은 KST 시간 (MySQL 시간)
        last_extract_time = parse_datetime(last_extract_time_str)
        # KST (UTC+9)로 설정
        if last_extract_time.tzinfo is None:
            kst_tz = timezone(timedelta(hours=9))
            last_extract_time = last_extract_time.replace(tzinfo=kst_tz)
        # INDO_TZ (UTC+7)로 변환
        last_extract_time_indo = last_extract_time.astimezone(INDO_TZ)
        
        # 마지막 추출 시간의 다음 시간부터 1시간 동안
        start_date = last_extract_time_indo.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
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
    
    # 시간대 변환: INDO_TZ (UTC+7) -> KST (UTC+9) = +2시간
    # MySQL은 KST 시간으로 저장됨
    start_date_kst = start_date.astimezone(timezone(timedelta(hours=9)))  # INDO_TZ -> KST
    end_date_kst = end_date.astimezone(timezone(timedelta(hours=9)))  # INDO_TZ -> KST
    
    start_str = start_date_kst.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date_kst.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"📅 Machine {machine_no} 센서 데이터 수집 시작: {start_str} ~ {end_str}")
    logging.info(f"🔍 시간대 변환: INDO_TZ {start_date.strftime('%Y-%m-%d %H:%M:%S %Z')} -> KST {start_str}")
    
    # Process this machine
    conn_id = IP_CONN_DB[machine_index]
    try:
        mysql = MySQLHelper(conn_id=conn_id)
        pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
    except Exception as e:
        logging.warning(f"⚠️ 연결 실패 (Machine {machine_no}): {str(e)} - 스킵합니다")
        return {
            "machine_no": machine_no,
            "rows_processed": 0,
            "status": "skipped",
            "reason": "connection_failed",
            "error": str(e)
        }
    
    try:
        machine_result = process_hourly_incremental_for_machine(
            mysql, pg, start_date_kst, end_date_kst, machine_no
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


def create_incremental_task(machine_no: str, machine_index: int):
    """Create incremental task for specific machine"""
    def incremental_task(**kwargs) -> dict:
        return process_machine_incremental(machine_no, machine_index)
    return incremental_task


# ────────────────────────────────────────────────────────────────
# Backfill Logic
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
    
    # 시간대 변환: INDO_TZ (UTC+7) -> KST (UTC+9) = +2시간
    # MySQL은 KST 시간으로 저장됨
    start_date_kst = start_date.astimezone(timezone(timedelta(hours=9)))  # INDO_TZ -> KST
    end_date_kst = end_date.astimezone(timezone(timedelta(hours=9)))  # INDO_TZ -> KST
    
    start_str = start_date_kst.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date_kst.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"시간별 배치 처리 중: {start_str} ~ {end_str} (Machine: {machine_no})")
    logging.info(f"🔍 시간대 변환: INDO_TZ {start_date.strftime('%Y-%m-%d %H:%M:%S %Z')} -> KST {start_str}")
    
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
        
        # 시간을 정규화 (00:00:00 형태로)
        normalized_start = current_date.replace(minute=0, second=0, microsecond=0)
        normalized_end = hour_end.replace(minute=59, second=59, microsecond=999999)
        
        try:
            # 연결 시도
            mysql = MySQLHelper(conn_id=conn_id)
            pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
        except Exception as e:
            logging.warning(f"⚠️ 연결 실패 (Machine {machine_no}, 루프 {loop_count}): {str(e)} - 스킵합니다")
            # 연결 실패 시에도 Variable 업데이트는 진행 (진행 상황 추적)
            end_str = normalized_end.astimezone(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")
            update_machine_variable(machine_no, end_str)
            continue
        
        try:
            batch_result = process_hourly_batch_for_machine(
                mysql, pg, normalized_start, normalized_end, machine_no, loop_count, expected_loops
            )
            machine_results.append(batch_result)
            logging.info(f"✅ Machine {machine_no} 루프 {loop_count} 완료: {batch_result['row_count']:,} rows")
        except Exception as e:
            error_msg = str(e)
            logging.warning(f"⚠️ Machine {machine_no} 루프 {loop_count} 실패: {error_msg} - 스킵합니다")
            # 연결 실패나 쿼리 실패 시에도 Variable 업데이트는 진행
            end_str = normalized_end.astimezone(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")
            update_machine_variable(machine_no, end_str)
        
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


def create_backfill_task(machine_no: str, machine_index: int):
    """Create backfill task for specific machine"""
    def backfill_task(**kwargs) -> dict:
        return process_machine_backfill(machine_no, machine_index)
    return backfill_task

