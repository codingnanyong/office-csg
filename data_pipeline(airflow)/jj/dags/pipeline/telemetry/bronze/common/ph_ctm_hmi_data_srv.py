"""
PH CTM HMI Data Common Functions
==================================
공통 함수 및 설정을 모아둔 모듈
"""

import logging
import threading
from datetime import datetime, timedelta, timezone
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from plugins.hooks.mysql_hook import MySQLHelper
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────

INDO_TZ = timezone(timedelta(hours=7))
EQUIPMENTS = [
    {"equipment_id": "ph01", "equipment": 100, "machine_id": "PH01", "conn_id": "maria_ph_01", "var_key": "last_extract_time_ph_ctm_hmi_data_ph01"},
]
TARGET_POSTGRES_CONN_ID = "pg_jj_telemetry_dw"
SCHEMA_NAME = "bronze"
TABLE_NAME = "ph_ctm_hmi_data"
HOURS_OFFSET_FOR_INCREMENTAL = 1
DEFAULT_MARKER_HOURS_BACK = 2
INITIAL_START_DATE = datetime(2025, 9, 24, 0, 0, 0)


# ────────────────────────────────────────────────────────────────
# Utility Functions
# ────────────────────────────────────────────────────────────────
def _test_connection_quick(mysql, timeout_seconds, result):
    """타임아웃이 있는 빠른 연결 테스트"""
    try:
        with mysql.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        result['success'] = True
    except Exception as e:
        result['success'] = False
        result['error'] = str(e)


def check_mysql_connection_quick(mysql, conn_id: str, timeout_seconds: int = 5) -> bool:
    """빠른 연결 확인 (타임아웃 설정)"""
    result = {'success': False, 'error': None}
    thread = threading.Thread(target=_test_connection_quick, args=(mysql, timeout_seconds, result))
    thread.daemon = True
    thread.start()
    thread.join(timeout=timeout_seconds)
    
    if thread.is_alive():
        logging.warning(f"⚠️ 연결 타임아웃: {conn_id} ({timeout_seconds}초 초과)")
        return False
    
    if result['success']:
        return True
    else:
        logging.warning(f"⚠️ 연결 불가: {conn_id} - {result.get('error', 'Unknown error')}")
        return False


def _eod(dt: datetime) -> datetime:
    """end of day: 23:59:59.999999"""
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def _eoh(dt: datetime) -> datetime:
    """end of hour: hh:59:59"""
    return dt.replace(minute=59, second=59, microsecond=999999)


def _get_default_marker() -> datetime:
    """Get default marker for incremental processing"""
    base = (datetime.now(INDO_TZ) - timedelta(hours=DEFAULT_MARKER_HOURS_BACK)).astimezone(INDO_TZ)
    return _eoh(base)


# ────────────────────────────────────────────────────────────────
# Data Extraction
# ────────────────────────────────────────────────────────────────
def build_extract_sql(start_dt: datetime, end_dt: datetime, equipment: int, machine_id: str) -> str:
    """Build SQL query for HMI data extraction"""
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    return f"""
        SELECT 
            0 AS factory,
            {equipment} AS equipment,
            '{machine_id}' AS machine_id,
            d.SeqNo AS seq_no,
            d.PID AS pid,
            d.RxDate AS rx_date,
            d.PValue AS p_value,
            d.RxDate_Year AS rxdate_year,
            d.RxDate_Month AS rxdate_month,
            d.RxDate_Day AS rxdate_day
        FROM rtf_data d
        WHERE d.RxDate >= '{start_str}' AND d.RxDate <= '{end_str}'
        ORDER BY d.RxDate
    """


# ────────────────────────────────────────────────────────────────
# Data Loading
# ────────────────────────────────────────────────────────────────
def prepare_rows(rows: list, extract_time: datetime) -> list:
    """Prepare rows for PostgreSQL insertion"""
    out = []
    for r in rows:
        out.append(tuple(list(r) + [extract_time, datetime.utcnow()]))
    return out

# ────────────────────────────────────────────────────────────────
# Incremental Logic
# ────────────────────────────────────────────────────────────────
def run_incremental(equipment_id: str, conn_id: str, var_key: str, equipment: int, machine_id: str, **context):
    """Run incremental collection for specific equipment"""
    # hard_end 계산 (Skip된 경우 Variable 업데이트용)
    hard_cap = (datetime.now(INDO_TZ) - timedelta(hours=HOURS_OFFSET_FOR_INCREMENTAL)).astimezone(INDO_TZ)
    hard_cap = _eoh(hard_cap)
    
    try:
        mysql = MySQLHelper(conn_id=conn_id)
        pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
        # 실제 연결 테스트 (타임아웃 방지)
        if not check_mysql_connection_quick(mysql, conn_id, timeout_seconds=5):
            logging.warning(f"⚠️ 연결 불가 (Eq{equipment_id}, {conn_id}) - 스킵합니다")
            # Skip된 경우에도 Variable 업데이트 (현재 시간 - 1시간까지 처리하려고 했던 마커)
            Variable.set(var_key, hard_cap.strftime("%Y-%m-%d %H:%M:%S"))
            logging.info(f"✅ [{equipment_id}] Variable '{var_key}' 업데이트 (Skip된 경우): {hard_cap.strftime('%Y-%m-%d %H:%M:%S')}")
            skip_msg = f"⏭️ 연결 불가 (Eq{equipment_id}, {conn_id}) - 태스크 Skip"
            raise AirflowSkipException(skip_msg)
    except Exception as e:
        # AirflowSkipException은 그대로 전파
        if isinstance(e, AirflowSkipException):
            raise
        logging.warning(f"⚠️ 연결 실패 (Eq{equipment_id}): {str(e)} - 스킵합니다")
        # Skip된 경우에도 Variable 업데이트 (현재 시간 - 1시간까지 처리하려고 했던 마커)
        Variable.set(var_key, hard_cap.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info(f"✅ [{equipment_id}] Variable '{var_key}' 업데이트 (Skip된 경우): {hard_cap.strftime('%Y-%m-%d %H:%M:%S')}")
        skip_msg = f"⏭️ 연결 실패 (Eq{equipment_id}): {str(e)} - 태스크 Skip"
        raise AirflowSkipException(skip_msg) from e

    val = Variable.get(var_key, default_var="")
    if val:
        try:
            # ISO 형식 시도 (2025-10-31T05:59:59+07:00 또는 2025-10-31T05:59:59)
            if 'T' in val or '+' in val or val.count('-') >= 3:
                parsed = datetime.fromisoformat(val)
            else:
                # 공백 형식 시도 (2025-10-31 05:59:59)
                parsed = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=INDO_TZ)
            else:
                parsed = parsed.astimezone(INDO_TZ)
            last_marker = parsed
        except Exception as e:
            logging.warning(f"⚠️ Variable 파싱 실패 ({var_key}): {val}, 기본값 사용: {e}")
            last_marker = _get_default_marker()
    else:
        last_marker = _get_default_marker()
    
    # 마지막 추출 시간의 다음 시간부터 1시간 동안만 처리
    cur_start = (last_marker + timedelta(seconds=1)).astimezone(INDO_TZ)
    cur_start = cur_start.replace(minute=0, second=0, microsecond=0)
    
    # 정확히 1시간만 처리 (다음 실행에서 그 다음 1시간 처리)
    cur_end = _eoh(cur_start)

    # hard_cap은 함수 시작 부분에서 이미 계산됨

    logging.info(f"🔍 디버깅 정보 (Eq{equipment_id}): Variable={val}, last_marker={last_marker}, cur_start={cur_start}, cur_end={cur_end}, hard_cap={hard_cap}")

    # 처리할 시간이 hard_cap을 초과하면 스킵
    if cur_start > hard_cap:
        logging.info(f"ℹ️ 현재 -1시간 제한으로 처리 구간이 유효하지 않아 스킵합니다 (Eq{equipment_id}): cur_start={cur_start} > hard_cap={hard_cap}")
        return {"status": "success", "rows": 0, "message": "skipped by -1h cap"}

    # cur_end가 hard_cap을 초과하면 hard_cap까지만 처리
    if cur_end > hard_cap:
        cur_end = hard_cap
        logging.info(f"⚠️ 처리 구간이 hard_cap을 초과하여 조정: cur_end={cur_end}")

    sql = build_extract_sql(cur_start, cur_end, equipment, machine_id)
    logging.info(f"🚀 Incremental 실행 쿼리(1시간 단위, Eq{equipment_id}): {cur_start} ~ {cur_end}\n{sql}")

    total_rows = 0
    
    try:
        # 메모리 효율적인 스트리밍 처리 (배치 크기 축소: 5000 -> 2000)
        batch_size = 2000
        batch_total = 0
        
        for batch_rows in mysql.execute_query_streaming(sql, "ph_ctm_hmi_data_incremental_extract", batch_size=batch_size):
            if batch_rows:
                insert_data = prepare_rows(batch_rows, datetime.utcnow())
                columns = [
                    "factory", "equipment", "machine_id", "seq_no", "pid", "rx_date", "p_value",
                    "rxdate_year", "rxdate_month", "rxdate_day", "etl_extract_time", "etl_ingest_time"
                ]
                conflict_columns = ["factory", "equipment", "machine_id", "seq_no", "pid", "rx_date"]
                # Insert 청크도 더 작게 (500 -> 300)
                pg.insert_data(SCHEMA_NAME, TABLE_NAME, insert_data, columns, conflict_columns, chunk_size=300)
                batch_total += len(batch_rows)
                total_rows += len(batch_rows)
                # 메모리 정리 힌트 (Python GC는 자동이지만 명시적으로 표시)
                del insert_data
        
        if batch_total > 0:
            logging.info(f"📦 ph_ctm_hmi_data 추출 row 수 (Eq{equipment_id}): {batch_total:,}")

        # 처리 완료한 경우 Variable 업데이트 (데이터가 없어도 시간은 업데이트하여 다음 시간대로 진행)
        Variable.set(var_key, cur_end.strftime("%Y-%m-%d %H:%M:%S"))
        if total_rows > 0:
            logging.info(f"✅ ph_ctm_hmi_data 1시간 단위 증분 완료 (Eq{equipment_id}), 총 {total_rows:,} rows 처리, 다음 처리 시간: {cur_end + timedelta(seconds=1)}")
        else:
            logging.info(f"✅ ph_ctm_hmi_data 처리 완료 (Eq{equipment_id}), 처리된 row 없음 (Variable 업데이트하여 다음 시간대로 진행: {cur_end + timedelta(seconds=1)})")
        
    except Exception as e:
        error_msg = str(e)
        logging.warning(f"⚠️ MySQL 연결/쿼리 실패 (Eq{equipment_id}): {error_msg} - 이번 시간대 스킵 (Variable 업데이트 안 함)")
        return {"status": "failed", "rows": 0, "error": error_msg}

    return {"status": "success", "rows": total_rows}


# ────────────────────────────────────────────────────────────────
# Backfill Logic
# ────────────────────────────────────────────────────────────────
def process_backfill(equipment_id: str, conn_id: str, var_key: str, equipment: int, machine_id: str, **context):
    """Process backfill for specific equipment"""
    try:
        mysql = MySQLHelper(conn_id=conn_id)
        pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
        # 실제 연결 테스트 (타임아웃 방지)
        if not check_mysql_connection_quick(mysql, conn_id, timeout_seconds=5):
            logging.warning(f"⚠️ 연결 불가 (Eq{equipment_id}, {conn_id}) - 스킵합니다")
            return {"status": "skipped", "reason": "connection_unavailable"}
    except Exception as e:
        logging.warning(f"⚠️ 연결 실패 (Eq{equipment_id}): {str(e)} - 스킵합니다")
        return {"status": "skipped", "reason": "connection_failed", "error": str(e)}

    # 시작 지점 결정: Variable → INITIAL_START_DATE
    base_start = INITIAL_START_DATE.replace(tzinfo=INDO_TZ).replace(hour=0, minute=0, second=0, microsecond=0)

    cursor_str = Variable.get(var_key, default_var=None)
    if cursor_str:
        try:
            cursor_dt = datetime.fromisoformat(cursor_str)
            if cursor_dt.tzinfo is None:
                cursor_dt = cursor_dt.replace(tzinfo=INDO_TZ)
            else:
                cursor_dt = cursor_dt.astimezone(INDO_TZ)
            cur_start = (cursor_dt + timedelta(seconds=1))
        except Exception:
            cur_start = base_start
    else:
        cur_start = base_start

    # 종료는 현재 시각 기준 HOURS_OFFSET_FOR_INCREMENTAL 시간 전 (시간 단위 처리)
    hard_end = (datetime.now(INDO_TZ) - timedelta(hours=HOURS_OFFSET_FOR_INCREMENTAL)).astimezone(INDO_TZ)
    hard_end = _eoh(hard_end)

    chunk_hours = 1
    total_rows = 0

    while cur_start <= hard_end:
        # 정확히 해당 시각의 "시 말"까지(예: 10:00:00 → 10:59:59), hard_end를 넘지 않게 제한
        cur_end = min(_eoh(cur_start), hard_end)
        sql = build_extract_sql(cur_start, cur_end, equipment, machine_id)
        logging.info(f"🚀 Backfill 쿼리(시간단위, {machine_id}): {cur_start} ~ {cur_end}\n{sql}")

        try:
            # 메모리 효율적인 스트리밍 처리 (배치 크기: 2000)
            batch_size = 2000
            batch_total = 0
            
            for batch_rows in mysql.execute_query_streaming(sql, "ph_ctm_hmi_data_backfill_extract", batch_size=batch_size):
                if batch_rows:
                    insert_data = prepare_rows(batch_rows, datetime.utcnow())
                    columns = [
                        "factory", "equipment", "machine_id", "seq_no", "pid", "rx_date", "p_value",
                        "rxdate_year", "rxdate_month", "rxdate_day", "etl_extract_time", "etl_ingest_time"
                    ]
                    conflict_columns = ["factory", "equipment", "machine_id", "seq_no", "pid", "rx_date"]
                    # Insert 청크도 더 작게 (300)
                    pg.insert_data(SCHEMA_NAME, TABLE_NAME, insert_data, columns, conflict_columns, chunk_size=300)
                    batch_total += len(batch_rows)
                    total_rows += len(batch_rows)
                    # 메모리 정리 힌트
                    del insert_data
            
            if batch_total > 0:
                logging.info(f"📦 ph_ctm_hmi_data 추출 row 수 ({machine_id}): {batch_total:,}")

            # 처리한 구간의 끝시간(항상 hh:59:59)을 커서로 저장
            Variable.set(var_key, cur_end.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            error_msg = str(e)
            logging.warning(f"⚠️ MySQL 연결/쿼리 실패 ({machine_id}, {cur_start} ~ {cur_end}): {error_msg} - 해당 시간대 스킵하고 계속 진행")
            # 연결 문제로 실패한 경우, 마커는 업데이트하지 않고 다음으로 넘어감
            # Variable 업데이트를 하지 않아서 다음 실행 시 같은 시간대를 다시 시도할 수 있음

        cur_start = (cur_end + timedelta(seconds=1)).astimezone(INDO_TZ)

    logging.info(f"✅ ph_ctm_hmi_data backfill 완료 ({machine_id}), 총 {total_rows} rows")
    return {"status": "success", "rows": total_rows}
