"""
OS Banbury HMI Sensor Common Functions
======================================
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
TARGET_POSTGRES_CONN_ID = "pg_jj_telemetry_dw"

EQUIPMENTS = [
    {
        "equipment_id": "1",
        "equipment_value": 3001,
        "conn_id": "maria_jj_os_banb_1",
        "var_key_data": "last_extract_time_os_banb_hmi_sensor_data_eq1",
        "var_key_stat": "last_extract_time_os_banb_hmi_sensor_state_eq1",
    },
    {
        "equipment_id": "3",
        "equipment_value": 3003,
        "conn_id": "maria_jj_os_banb_3",
        "var_key_data": "last_extract_time_os_banb_hmi_sensor_data_eq3",
        "var_key_stat": "last_extract_time_os_banb_hmi_sensor_state_eq3",
    },
]

SCHEMA_NAME = "bronze"
TABLE_NAME_DATA = "os_banb_hmi_sensor_data"
TABLE_NAME_STAT = "os_banb_hmi_sensor_state"

HOURS_OFFSET_FOR_INCREMENTAL = 1
DEFAULT_MARKER_HOURS_BACK = 2
INITIAL_START_DATE = datetime(2025, 10, 27, 0, 0, 0)


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


def _eoh(dt: datetime) -> datetime:
    """end of hour: hh:59:59"""
    return dt.replace(minute=59, second=59, microsecond=999999)


def _get_last_marker(var_key: str) -> datetime:
    """Get last marker from Variable"""
    marker = Variable.get(var_key, default_var=None)
    if not marker:
        base = (datetime.now(INDO_TZ) - timedelta(hours=DEFAULT_MARKER_HOURS_BACK)).astimezone(INDO_TZ)
        return _eoh(base)
    try:
        if 'T' in marker or '+' in marker or marker.count('-') >= 3:
            dt = datetime.fromisoformat(marker)
        else:
            dt = datetime.strptime(marker, "%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=INDO_TZ)
        else:
            dt = dt.astimezone(INDO_TZ)
        return dt
    except Exception:
        base = (datetime.now(INDO_TZ) - timedelta(hours=DEFAULT_MARKER_HOURS_BACK)).astimezone(INDO_TZ)
        return _eoh(base)


def _set_marker(var_key: str, dt: datetime) -> None:
    """Set marker in Variable"""
    Variable.set(var_key, dt.strftime("%Y-%m-%d %H:%M:%S"))


# ────────────────────────────────────────────────────────────────
# Data Extraction
# ────────────────────────────────────────────────────────────────
def build_extract_sql_data(start_dt: datetime, end_dt: datetime, equipment_value: int) -> str:
    """Build SQL query for sensor data extraction"""
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    return f"""
        SELECT 
            COALESCE(s.Factory, 1) AS factory,
            COALESCE(s.Equipment, {equipment_value}) AS equipment,
            d.SeqNo AS seq_no,
            d.PID AS pid,
            d.PValue AS p_value,
            d.SensorBridge AS sensor_bridge,
            d.SensorType AS sensor_type,
            d.SensorGroup AS sensor_group,
            d.Level AS level,
            d.Active AS active,
            d.Min AS min_value,
            d.Max AS max_value,
            d.Unit AS unit,
            d.POS_X AS pos_x,
            d.POS_Y AS pos_y,
            d.User AS user_id,
            d.Result AS result,
            d.ResultDate AS result_date,
            d.RxDate AS rx_date,
            d.CreateDate AS create_date
        FROM rtf_sensor_data d
        LEFT JOIN rtf_sensor s ON s.PID = d.PID
        WHERE COALESCE(d.RxDate, d.ResultDate, d.CreateDate) >= '{start_str}' AND COALESCE(d.RxDate, d.ResultDate, d.CreateDate) <= '{end_str}'
        ORDER BY COALESCE(d.RxDate, d.ResultDate, d.CreateDate)
    """


def build_extract_sql_stat(start_dt: datetime, end_dt: datetime, equipment_value: int) -> str:
    """Build SQL query for sensor stat extraction"""
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    return f"""
        SELECT 
            COALESCE(s.Factory, 1) AS factory,
            COALESCE(s.Equipment, {equipment_value}) AS equipment,
            st.PID AS pid,
            st.PValue AS p_value,
            st.SensorBridge AS sensor_bridge,
            st.SensorType AS sensor_type,
            st.SensorGroup AS sensor_group,
            st.Level AS level,
            st.Active AS active,
            st.Min AS min_value,
            st.Max AS max_value,
            st.Unit AS unit,
            st.POS_X AS pos_x,
            st.POS_Y AS pos_y,
            st.User AS user_id,
            st.Result AS result,
            st.ResultDate AS result_date,
            st.RxDate AS rx_date,
            st.CreateDate AS create_date
        FROM rtf_sensor_state st
        LEFT JOIN rtf_sensor s ON s.PID = st.PID
        WHERE COALESCE(st.RxDate, st.ResultDate, st.CreateDate) >= '{start_str}' AND COALESCE(st.RxDate, st.ResultDate, st.CreateDate) <= '{end_str}'
        ORDER BY COALESCE(st.RxDate, st.ResultDate, st.CreateDate)
    """


# ────────────────────────────────────────────────────────────────
# Data Loading
# ────────────────────────────────────────────────────────────────
def prepare_rows(rows: list, extract_time: datetime) -> list:
    """Prepare rows for PostgreSQL insertion"""
    prepared = []
    for r in rows:
        prepared.append(tuple(list(r) + [extract_time, datetime.utcnow()]))
    return prepared


# ────────────────────────────────────────────────────────────────
# Incremental Logic
# ────────────────────────────────────────────────────────────────
def run_incremental_data(equipment_id: str, conn_id: str, var_key: str, equipment_value: int, **context):
    """Run incremental collection for sensor data"""
    # hard_end 계산 (Skip된 경우 Variable 업데이트용)
    hard_end = (datetime.now(INDO_TZ) - timedelta(hours=HOURS_OFFSET_FOR_INCREMENTAL)).astimezone(INDO_TZ)
    hard_end = _eoh(hard_end)
    
    try:
        mysql = MySQLHelper(conn_id=conn_id)
        pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
        if not check_mysql_connection_quick(mysql, conn_id, timeout_seconds=5):
            logging.warning(f"⚠️ 연결 불가 (Eq{equipment_id}, {conn_id}) - 스킵합니다")
            # Skip된 경우에도 Variable 업데이트 (현재 시간 - 1시간까지 처리하려고 했던 마커)
            _set_marker(var_key, hard_end)
            logging.info(f"✅ [{equipment_id}] Variable '{var_key}' 업데이트 (Skip된 경우): {hard_end.strftime('%Y-%m-%d %H:%M:%S')}")
            skip_msg = f"⏭️ 연결 불가 (Eq{equipment_id}, {conn_id}) - 태스크 Skip"
            raise AirflowSkipException(skip_msg)
    except Exception as e:
        # AirflowSkipException은 그대로 전파
        if isinstance(e, AirflowSkipException):
            raise
        logging.warning(f"⚠️ 연결 실패 (Eq{equipment_id}): {str(e)} - 스킵합니다")
        # Skip된 경우에도 Variable 업데이트 (현재 시간 - 1시간까지 처리하려고 했던 마커)
        _set_marker(var_key, hard_end)
        logging.info(f"✅ [{equipment_id}] Variable '{var_key}' 업데이트 (Skip된 경우): {hard_end.strftime('%Y-%m-%d %H:%M:%S')}")
        skip_msg = f"⏭️ 연결 실패 (Eq{equipment_id}): {str(e)} - 태스크 Skip"
        raise AirflowSkipException(skip_msg) from e

    start_dt = _get_last_marker(var_key) + timedelta(seconds=1)

    if start_dt > hard_end:
        logging.info(f"ℹ️ 현재 제한으로 처리 구간이 유효하지 않아 스킵합니다 (Eq{equipment_id})")
        return {"status": "skipped"}

    cur_start = start_dt
    total_rows = 0

    while cur_start <= hard_end:
        cur_end = min(_eoh(cur_start), hard_end)
        sql = build_extract_sql_data(cur_start, cur_end, equipment_value)
        logging.info(f"🚀 Sensor data incremental 쿼리 (Eq{equipment_id}): {cur_start} ~ {cur_end}\n{sql}")

        try:
            batch_size = 2000
            batch_total = 0
            
            for batch_rows in mysql.execute_query_streaming(sql, "os_banb_hmi_sensor_data_incremental_extract", batch_size=batch_size):
                if batch_rows:
                    insert_data = prepare_rows(batch_rows, datetime.utcnow())
                    columns = [
                        "factory", "equipment", "seq_no", "pid", "p_value", "sensor_bridge", "sensor_type", "sensor_group",
                        "level", "active", "min_value", "max_value", "unit", "pos_x", "pos_y",
                        "user_id", "result", "result_date", "rx_date", "create_date", "etl_extract_time", "etl_ingest_time"
                    ]
                    conflict_columns = ["factory", "equipment", "seq_no"]
                    pg.insert_data(SCHEMA_NAME, TABLE_NAME_DATA, insert_data, columns, conflict_columns, chunk_size=300)
                    batch_total += len(batch_rows)
                    total_rows += len(batch_rows)
                    del insert_data
            
            if batch_total > 0:
                logging.info(f"📦 os_banb_hmi_sensor_data 추출 row 수 (Eq{equipment_id}): {batch_total:,}")

            _set_marker(var_key, cur_end)
        except Exception as e:
            error_msg = str(e)
            logging.warning(f"⚠️ MySQL 연결/쿼리 실패 (Eq{equipment_id}): {error_msg} - 해당 시간대 스킵하고 계속 진행")
            pass

        cur_start = (cur_end + timedelta(seconds=1)).astimezone(INDO_TZ)

    logging.info(f"✅ os_banb_hmi_sensor_data incremental 완료 (Eq{equipment_id}), 총 {total_rows} rows")
    return {"status": "success", "rows": total_rows}


def run_incremental_stat(equipment_id: str, conn_id: str, var_key: str, equipment_value: int, **context):
    """Run incremental collection for sensor stat"""
    # hard_end 계산 (Skip된 경우 Variable 업데이트용)
    hard_end = (datetime.now(INDO_TZ) - timedelta(hours=HOURS_OFFSET_FOR_INCREMENTAL)).astimezone(INDO_TZ)
    hard_end = _eoh(hard_end)
    
    try:
        mysql = MySQLHelper(conn_id=conn_id)
        pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
        if not check_mysql_connection_quick(mysql, conn_id, timeout_seconds=5):
            logging.warning(f"⚠️ 연결 불가 (Eq{equipment_id}, {conn_id}) - 스킵합니다")
            # Skip된 경우에도 Variable 업데이트 (현재 시간 - 1시간까지 처리하려고 했던 마커)
            _set_marker(var_key, hard_end)
            logging.info(f"✅ [{equipment_id}] Variable '{var_key}' 업데이트 (Skip된 경우): {hard_end.strftime('%Y-%m-%d %H:%M:%S')}")
            skip_msg = f"⏭️ 연결 불가 (Eq{equipment_id}, {conn_id}) - 태스크 Skip"
            raise AirflowSkipException(skip_msg)
    except Exception as e:
        # AirflowSkipException은 그대로 전파
        if isinstance(e, AirflowSkipException):
            raise
        logging.warning(f"⚠️ 연결 실패 (Eq{equipment_id}): {str(e)} - 스킵합니다")
        # Skip된 경우에도 Variable 업데이트 (현재 시간 - 1시간까지 처리하려고 했던 마커)
        _set_marker(var_key, hard_end)
        logging.info(f"✅ [{equipment_id}] Variable '{var_key}' 업데이트 (Skip된 경우): {hard_end.strftime('%Y-%m-%d %H:%M:%S')}")
        skip_msg = f"⏭️ 연결 실패 (Eq{equipment_id}): {str(e)} - 태스크 Skip"
        raise AirflowSkipException(skip_msg) from e

    start_dt = _get_last_marker(var_key) + timedelta(seconds=1)

    if start_dt > hard_end:
        logging.info(f"ℹ️ 현재 제한으로 처리 구간이 유효하지 않아 스킵합니다 (Eq{equipment_id})")
        return {"status": "skipped"}

    cur_start = start_dt
    total_rows = 0

    while cur_start <= hard_end:
        cur_end = min(_eoh(cur_start), hard_end)
        sql = build_extract_sql_stat(cur_start, cur_end, equipment_value)
        logging.info(f"🚀 Sensor state incremental 쿼리 (Eq{equipment_id}): {cur_start} ~ {cur_end}\n{sql}")

        try:
            batch_size = 2000
            batch_total = 0
            
            for batch_rows in mysql.execute_query_streaming(sql, "os_banb_hmi_sensor_state_incremental_extract", batch_size=batch_size):
                if batch_rows:
                    insert_data = prepare_rows(batch_rows, datetime.utcnow())
                    columns = [
                        "factory", "equipment", "pid", "p_value", "sensor_bridge", "sensor_type", "sensor_group",
                        "level", "active", "min_value", "max_value", "unit", "pos_x", "pos_y",
                        "user_id", "result", "result_date", "rx_date", "create_date", "etl_extract_time", "etl_ingest_time"
                    ]
                    conflict_columns = ["factory", "equipment", "pid"]
                    pg.insert_data(SCHEMA_NAME, TABLE_NAME_STAT, insert_data, columns, conflict_columns, chunk_size=300)
                    batch_total += len(batch_rows)
                    total_rows += len(batch_rows)
                    del insert_data
            
            if batch_total > 0:
                logging.info(f"📦 os_banb_hmi_sensor_state 추출 row 수 (Eq{equipment_id}): {batch_total:,}")

            _set_marker(var_key, cur_end)
        except Exception as e:
            error_msg = str(e)
            logging.warning(f"⚠️ MySQL 연결/쿼리 실패 (Eq{equipment_id}): {error_msg} - 해당 시간대 스킵하고 계속 진행")
            pass

        cur_start = (cur_end + timedelta(seconds=1)).astimezone(INDO_TZ)

    logging.info(f"✅ os_banb_hmi_sensor_state incremental 완료 (Eq{equipment_id}), 총 {total_rows} rows")
    return {"status": "success", "rows": total_rows}


# ────────────────────────────────────────────────────────────────
# Backfill Logic
# ────────────────────────────────────────────────────────────────
def process_backfill_data(equipment_id: str, conn_id: str, var_key: str, equipment_value: int, **context):
    """Process backfill for sensor data"""
    try:
        mysql = MySQLHelper(conn_id=conn_id)
        pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
        if not check_mysql_connection_quick(mysql, conn_id, timeout_seconds=5):
            logging.warning(f"⚠️ 연결 불가 (Eq{equipment_id}, {conn_id}) - 스킵합니다")
            return {"status": "skipped", "reason": "connection_unavailable"}
    except Exception as e:
        logging.warning(f"⚠️ 연결 실패 (Eq{equipment_id}): {str(e)} - 스킵합니다")
        return {"status": "skipped", "reason": "connection_failed", "error": str(e)}

    base_start = INITIAL_START_DATE.replace(tzinfo=INDO_TZ).replace(minute=0, second=0, microsecond=0)

    cursor_str = Variable.get(var_key, default_var=None)
    if cursor_str:
        try:
            if 'T' in cursor_str or '+' in cursor_str or cursor_str.count('-') >= 3:
                cursor_dt = datetime.fromisoformat(cursor_str)
            else:
                cursor_dt = datetime.strptime(cursor_str, "%Y-%m-%d %H:%M:%S")
            if cursor_dt.tzinfo is None:
                cursor_dt = cursor_dt.replace(tzinfo=INDO_TZ)
            else:
                cursor_dt = cursor_dt.astimezone(INDO_TZ)
            cur_start = (cursor_dt + timedelta(seconds=1))
        except Exception:
            cur_start = base_start
    else:
        cur_start = base_start

    hard_end = (datetime.now(INDO_TZ) - timedelta(hours=HOURS_OFFSET_FOR_INCREMENTAL)).astimezone(INDO_TZ)
    hard_end = _eoh(hard_end)

    total_rows = 0

    while cur_start <= hard_end:
        cur_end = min(_eoh(cur_start), hard_end)
        sql = build_extract_sql_data(cur_start, cur_end, equipment_value)
        logging.info(f"🚀 Sensor backfill 쿼리(시간단위, Eq{equipment_id}): {cur_start} ~ {cur_end}\n{sql}")

        try:
            rows = mysql.execute_query(sql, "os_banb_hmi_sensor_data_backfill_extract", None) or []
            logging.info(f"📦 os_banb_hmi_sensor_data 추출 row 수 (Eq{equipment_id}): {len(rows)}")

            if rows:
                insert_data = prepare_rows(rows, datetime.utcnow())
                columns = [
                    "factory", "equipment", "seq_no", "pid", "p_value", "sensor_bridge", "sensor_type", "sensor_group",
                    "level", "active", "min_value", "max_value", "unit", "pos_x", "pos_y",
                    "user_id", "result", "result_date", "rx_date", "create_date", "etl_extract_time", "etl_ingest_time"
                ]
                conflict_columns = ["factory", "equipment", "seq_no"]
                pg.insert_data(SCHEMA_NAME, TABLE_NAME_DATA, insert_data, columns, conflict_columns)
                total_rows += len(rows)

            Variable.set(var_key, cur_end.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            error_msg = str(e)
            logging.warning(f"⚠️ MySQL 연결/쿼리 실패 (Eq{equipment_id}, {cur_start} ~ {cur_end}): {error_msg} - 해당 시간대 스킵하고 계속 진행")

        cur_start = (cur_end + timedelta(seconds=1)).astimezone(INDO_TZ)

    logging.info(f"✅ os_banb_hmi_sensor_data backfill 완료 (Eq{equipment_id}), 총 {total_rows} rows")
    return {"status": "success", "rows": total_rows}


def process_backfill_stat(equipment_id: str, conn_id: str, var_key: str, equipment_value: int, **context):
    """Process backfill for sensor stat"""
    try:
        mysql = MySQLHelper(conn_id=conn_id)
        pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
        if not check_mysql_connection_quick(mysql, conn_id, timeout_seconds=5):
            logging.warning(f"⚠️ 연결 불가 (Eq{equipment_id}, {conn_id}) - 스킵합니다")
            return {"status": "skipped", "reason": "connection_unavailable"}
    except Exception as e:
        logging.warning(f"⚠️ 연결 실패 (Eq{equipment_id}): {str(e)} - 스킵합니다")
        return {"status": "skipped", "reason": "connection_failed", "error": str(e)}

    base_start = INITIAL_START_DATE.replace(tzinfo=INDO_TZ).replace(minute=0, second=0, microsecond=0)

    cursor_str = Variable.get(var_key, default_var=None)
    if cursor_str:
        try:
            if 'T' in cursor_str or '+' in cursor_str or cursor_str.count('-') >= 3:
                cursor_dt = datetime.fromisoformat(cursor_str)
            else:
                cursor_dt = datetime.strptime(cursor_str, "%Y-%m-%d %H:%M:%S")
            if cursor_dt.tzinfo is None:
                cursor_dt = cursor_dt.replace(tzinfo=INDO_TZ)
            else:
                cursor_dt = cursor_dt.astimezone(INDO_TZ)
            cur_start = (cursor_dt + timedelta(seconds=1))
        except Exception:
            cur_start = base_start
    else:
        cur_start = base_start

    hard_end = (datetime.now(INDO_TZ) - timedelta(hours=HOURS_OFFSET_FOR_INCREMENTAL)).astimezone(INDO_TZ)
    hard_end = _eoh(hard_end)

    total_rows = 0

    while cur_start <= hard_end:
        cur_end = min(_eoh(cur_start), hard_end)
        sql = build_extract_sql_stat(cur_start, cur_end, equipment_value)
        logging.info(f"🚀 Sensor state backfill 쿼리(시간단위, Eq{equipment_id}): {cur_start} ~ {cur_end}\n{sql}")
        try:
            rows = mysql.execute_query(sql, "os_banb_hmi_sensor_state_backfill_extract", None) or []
            logging.info(f"📦 os_banb_hmi_sensor_stat 추출 row 수 (Eq{equipment_id}): {len(rows)}")

            if rows:
                insert_data = prepare_rows(rows, datetime.utcnow())
                columns = [
                    "factory", "equipment", "pid", "p_value", "sensor_bridge", "sensor_type", "sensor_group",
                    "level", "active", "min_value", "max_value", "unit", "pos_x", "pos_y",
                    "user_id", "result", "result_date", "rx_date", "create_date", "etl_extract_time", "etl_ingest_time"
                ]
                conflict_columns = ["factory", "equipment", "pid"]
                pg.insert_data(SCHEMA_NAME, TABLE_NAME_STAT, insert_data, columns, conflict_columns)
                total_rows += len(rows)

            Variable.set(var_key, cur_end.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            error_msg = str(e)
            logging.warning(f"⚠️ MySQL 연결/쿼리 실패 (Eq{equipment_id}, {cur_start} ~ {cur_end}): {error_msg} - 해당 시간대 스킵하고 계속 진행")

        cur_start = (cur_end + timedelta(seconds=1)).astimezone(INDO_TZ)

    logging.info(f"✅ os_banb_hmi_sensor_state backfill 완료 (Eq{equipment_id}), 총 {total_rows} rows")
    return {"status": "success", "rows": total_rows}

