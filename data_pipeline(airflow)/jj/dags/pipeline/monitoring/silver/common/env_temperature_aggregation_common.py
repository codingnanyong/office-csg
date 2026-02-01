"""공통 함수 모듈 - Temperature Aggregation (10분 단위 집계)"""
import logging
from datetime import datetime, timedelta, timezone
from plugins.hooks.postgres_hook import PostgresHelper
from plugins.hooks.mysql_hook import MySQLHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Source Configuration (추출)
SOURCE_TABLE = "temperature"
SOURCE_SCHEMA = "public"
SOURCE_POSTGRES_CONN_ID = "pg_jj_env"  # 추출용 PostgreSQL 연결

# Target Configuration (적재)
TARGET_TABLE = "env_temperature"
TARGET_SCHEMA = "ccs_rtf"

# 적재 대상 MySQL 연결 목록
# maria_jj_os_banb_1은 나중에 사용 가능하도록 준비 (현재는 비활성화)
TARGET_MYSQL_CONNECTIONS = [
    # {"conn_id": "maria_jj_os_banb_1", "enabled": False},  # 나중에 활성화 예정
    {"conn_id": "maria_jj_os_banb_3", "enabled": True},  # 현재 활성화
]

INDO_TZ = timezone(timedelta(hours=7))  # 인도네시아 시간 (UTC+7)
INITIAL_START_DATE = datetime(2025, 1, 1, 0, 0, 0, tzinfo=INDO_TZ)
INCREMENT_KEY = "last_extract_time_env_temperature_aggregated"
MAX_INCREMENTAL_RANGE_HOURS = 1  # 증분 처리 시 한 번에 최대 처리할 시간 범위 (시간 단위)


# ════════════════════════════════════════════════════════════════
# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════

def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string with timezone support"""
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=INDO_TZ)
    return dt


def get_incremental_date_range() -> dict | None:
    """증분 처리용 날짜 범위 계산 (10분 단위)
    
    한 번에 최대 MAX_INCREMENTAL_RANGE_HOURS 시간만 처리 (빈 시간이 길 때 한 번에 너무 많이 처리하는 것을 방지)
    최대 시간: 현재 인도네시아 시간을 10분 단위로 정규화 (현재 시점까지 가능한 최대 범위)
    
    Returns:
        Dictionary with 'start_date' and 'end_date' strings, or None if no data to process
    """
    last_extract_time = Variable.get(INCREMENT_KEY, default_var=None)
    
    if not last_extract_time:
        # Variable이 없으면 최근 1일 전부터 시작
        now_indo = datetime.now(INDO_TZ)
        start_date = (now_indo - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        logging.info(f"Variable이 없어 최근 1일 전({start_date.strftime('%Y-%m-%d %H:%M:%S')})부터 수집")
        # 10분 단위로 정규화
        start_date = _normalize_to_10min(start_date)
    else:
        last_time = parse_datetime(last_extract_time)
        # last_extract_time은 이미 10분 단위로 저장되어 있으므로, 다음 10분 구간부터 시작
        last_time_normalized = _normalize_to_10min(last_time)
        start_date = last_time_normalized + timedelta(minutes=10)
        logging.info(f"마지막 추출 시간: {last_extract_time} → 다음 수집 시작 시간: {start_date}")
    
    # 한 번에 처리할 최대 종료 시간: 시작 시간 + MAX_INCREMENTAL_RANGE_HOURS 시간
    max_batch_end_date = start_date + timedelta(hours=MAX_INCREMENTAL_RANGE_HOURS)
    
    # 실제 최대 종료 시간: 현재 인도네시아 시간을 10분 단위로 정규화
    # (현재 시간이 14:10이면 14:10으로 정규화되어 최대 14:10까지 처리 가능)
    now_indo = datetime.now(INDO_TZ)
    actual_max_end_date = _normalize_to_10min(now_indo)
    
    # 둘 중 작은 값을 사용 (한 번에 처리할 양 제한)
    end_date = min(max_batch_end_date, actual_max_end_date)
    
    if start_date >= end_date:
        logging.info(f"⚠️ 처리할 데이터 없음: {start_date} >= {end_date}")
        return None
    
    logging.info(f"📅 증분 처리 범위: {start_date} ~ {end_date} (최대 {MAX_INCREMENTAL_RANGE_HOURS}시간 제한 적용, 현재 시간: {now_indo.strftime('%Y-%m-%d %H:%M:%S')})")
    
    return {
        "start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": end_date.strftime("%Y-%m-%d %H:%M:%S")
    }


def get_backfill_date_range() -> dict | None:
    """백필 처리용 날짜 범위 계산
    
    종료 시간: 현재 인도네시아 시간 -1시간 (10분 단위로 정규화)
    
    Returns:
        Dictionary with 'backfill_start_date' and 'backfill_end_date' strings, or None if no data to process
    """
    last_extract_time = Variable.get(INCREMENT_KEY, default_var=None)
    
    if not last_extract_time:
        start_date = INITIAL_START_DATE
        logging.info(f"초기 시작 날짜 사용: {start_date}")
        # 10분 단위로 정규화
        start_date = _normalize_to_10min(start_date)
    else:
        last_time = parse_datetime(last_extract_time)
        # last_extract_time은 이미 10분 단위로 저장되어 있으므로, 다음 10분 구간부터 시작
        last_time_normalized = _normalize_to_10min(last_time)
        start_date = last_time_normalized + timedelta(minutes=10)
        logging.info(f"이전 진행 지점 사용: {last_extract_time} → 다음 수집 시작 시간: {start_date}")
    
    # 종료 시간: 현재 인도네시아 시간 -1시간 (10분 단위로 정규화)
    now_indo = datetime.now(INDO_TZ)
    end_date = now_indo - timedelta(hours=1)
    end_date = _normalize_to_10min(end_date)
    
    if start_date >= end_date:
        logging.info(f"⚠️ 처리할 데이터 없음: {start_date} >= {end_date}")
        return None
    
    logging.info(f"📅 백필 처리 범위: {start_date} ~ {end_date}")
    
    return {
        "backfill_start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
        "backfill_end_date": end_date.strftime("%Y-%m-%d %H:%M:%S")
    }


def _normalize_to_10min(dt: datetime) -> datetime:
    """10분 단위로 정규화 (예: 14:23:45 -> 14:20:00)
    
    Args:
        dt: Datetime to normalize
    
    Returns:
        Datetime normalized to 10-minute boundary
    """
    # 분을 10으로 나눈 몫에 10을 곱해서 10분 단위로 만들기
    normalized_minute = (dt.minute // 10) * 10
    return dt.replace(minute=normalized_minute, second=0, microsecond=0)


# ════════════════════════════════════════════════════════════════
# 3️⃣ Data Processing
# ════════════════════════════════════════════════════════════════

def build_extraction_sql(start_date: str, end_date: str) -> str:
    """PostgreSQL에서 데이터 추출 쿼리 생성
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD HH:MM:SS)
        end_date: 종료 날짜 (YYYY-MM-DD HH:MM:SS)
    
    Returns:
        SQL query string (PostgreSQL)
    """
    return f"""
        SELECT
            'OSR_ENV' AS sensor_id,
            (
                to_timestamp(
                    floor(
                        extract(
                            epoch FROM (
                                capture_dt AT TIME ZONE 'Asia/Seoul'
                            )
                        ) / 600
                    ) * 600
                )
                AT TIME ZONE 'Asia/Jakarta'
            ) AS "time",
            ROUND(AVG(t1)::numeric, 2) AS temp,
            ROUND(AVG(t2)::numeric, 2) AS humidity,
            ROUND(AVG(t3)::numeric, 2) AS particle
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE capture_dt AT TIME ZONE 'Asia/Seoul' AT TIME ZONE 'Asia/Jakarta' >= '{start_date}'::timestamp
          AND capture_dt AT TIME ZONE 'Asia/Seoul' AT TIME ZONE 'Asia/Jakarta' < '{end_date}'::timestamp
          AND sensor_id = 'TEMPIOT-A207'
        GROUP BY 1, 2
        ORDER BY 2 DESC
    """


def build_insert_sql_mysql() -> str:
    """MySQL/MariaDB 적재용 INSERT ... ON DUPLICATE KEY UPDATE 쿼리 생성
    
    Returns:
        SQL query string (MySQL/MariaDB)
    """
    return f"""
        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE} (
            sensor_id,
            `time`,
            temp,
            humidity,
            particle
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            temp = VALUES(temp),
            humidity = VALUES(humidity),
            particle = VALUES(particle)
    """


def create_target_table_if_not_exists(mysql: MySQLHelper, conn_id: str) -> None:
    """타겟 테이블이 없으면 생성 (MySQL/MariaDB)
    
    Args:
        mysql: MySQLHelper instance
        conn_id: Connection ID (로깅용)
    """
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
            sensor_id VARCHAR(50) NOT NULL,
            `time` DATETIME NOT NULL,
            temp DECIMAL(10, 2),
            humidity DECIMAL(10, 2),
            particle DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (sensor_id, `time`),
            INDEX idx_bucket_time (`time`)
        )
    """
    
    try:
        with mysql.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
            conn.commit()
            cursor.execute(create_table_sql)
            conn.commit()
        logging.info(f"✅ 테이블 확인/생성 완료: {conn_id} - {TARGET_SCHEMA}.{TARGET_TABLE}")
    except Exception as e:
        logging.error(f"❌ 테이블 생성 실패: {conn_id} - {str(e)}")
        raise


def process_aggregation(start_date: str, end_date: str) -> dict:
    """10분 단위 집계 처리
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD HH:MM:SS)
        end_date: 종료 날짜 (YYYY-MM-DD HH:MM:SS)
    
    Returns:
        Processing result dictionary
    """
    total_rows_processed = 0
    target_results = {}
    
    try:
        # 1. PostgreSQL에서 데이터 추출
        logging.info(f"📥 데이터 추출 시작: {start_date} ~ {end_date}")
        pg_source = PostgresHelper(conn_id=SOURCE_POSTGRES_CONN_ID)
        
        extraction_sql = build_extraction_sql(start_date, end_date)
        logging.info(f"추출 쿼리:\n{extraction_sql}")
        
        extracted_data = pg_source.execute_query(extraction_sql, task_id="extract_data", xcom_key=None)
        
        if not extracted_data:
            logging.warning("⚠️ 추출된 데이터가 없습니다.")
            return {
                "status": "success",
                "start_date": start_date,
                "end_date": end_date,
                "rows_processed": 0,
                "extract_time": datetime.utcnow().isoformat(),
                "targets": {}
            }
        
        logging.info(f"✅ 데이터 추출 완료: {len(extracted_data)} rows")
        
        # 추출된 데이터의 실제 마지막 시간 찾기 (time은 인덱스 1)
        actual_last_time = None
        if extracted_data:
            # time은 datetime 객체이므로 직접 비교 가능
            actual_last_time = max(row[1] for row in extracted_data)
            # datetime 객체를 문자열로 변환
            if isinstance(actual_last_time, datetime):
                actual_last_time = actual_last_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                actual_last_time = str(actual_last_time)
            logging.info(f"📌 실제 적재될 마지막 시간: {actual_last_time}")
        
        # 2. 활성화된 MySQL 연결에 데이터 적재
        insert_sql = build_insert_sql_mysql()
        enabled_targets = [t for t in TARGET_MYSQL_CONNECTIONS if t.get("enabled", False)]
        
        if not enabled_targets:
            logging.warning("⚠️ 활성화된 적재 대상이 없습니다.")
            return {
                "status": "success",
                "start_date": start_date,
                "end_date": end_date,
                "actual_last_time": None,
                "rows_processed": 0,
                "extract_time": datetime.utcnow().isoformat(),
                "targets": {}
            }
        
        for target_config in enabled_targets:
            conn_id = target_config["conn_id"]
            try:
                logging.info(f"💾 데이터 적재 시작: {conn_id}")
                mysql_target = MySQLHelper(conn_id=conn_id)
                
                # 테이블 생성 (없으면)
                create_target_table_if_not_exists(mysql_target, conn_id)
                
                # 데이터를 튜플 리스트로 변환 (etl_extract_time 제외)
                insert_data = []
                for row in extracted_data:
                    # row는 (sensor_id, time, temp, humidity, particle, etl_extract_time)
                    # 테이블은 etl_extract_time이 없고 created_at은 자동 생성되므로 마지막 항목 제거
                    insert_data.append(row[:5])  # 처음 5개 컬럼만 사용
                
                # 배치로 INSERT 실행
                with mysql_target.hook.get_conn() as conn, conn.cursor() as cursor:
                    cursor.executemany(insert_sql, insert_data)
                    rows_affected = cursor.rowcount
                    conn.commit()
                
                # 결과 확인을 위한 COUNT 쿼리
                count_sql = f"""
                    SELECT COUNT(*) 
                    FROM {TARGET_SCHEMA}.{TARGET_TABLE}
                    WHERE `time` >= '{start_date}' 
                      AND `time` < '{end_date}'
                """
                count_result = mysql_target.execute_query(count_sql, task_id=f"count_records_{conn_id}", xcom_key=None)
                row_count = count_result[0][0] if count_result and len(count_result) > 0 else 0
                
                total_rows_processed = max(total_rows_processed, row_count)
                target_results[conn_id] = {
                    "status": "success",
                    "rows_inserted": rows_affected,
                    "rows_counted": row_count
                }
                
                logging.info(f"✅ 데이터 적재 완료: {conn_id} - {row_count} 개의 10분 단위 레코드")
                
            except Exception as e:
                logging.error(f"❌ 데이터 적재 실패: {conn_id} - {str(e)}", exc_info=True)
                target_results[conn_id] = {
                    "status": "failed",
                    "error": str(e)
                }
                # 한 타겟 실패해도 다른 타겟은 계속 처리
        
        return {
            "status": "success",
            "start_date": start_date,
            "end_date": end_date,
            "actual_last_time": actual_last_time,  # 실제 적재된 마지막 시간
            "rows_processed": total_rows_processed,
            "extract_time": datetime.utcnow().isoformat(),
            "targets": target_results
        }
        
    except Exception as e:
        logging.error(f"❌ 집계 처리 실패: {str(e)}", exc_info=True)
        return {
            "status": "failed",
            "error": str(e),
            "start_date": start_date,
            "end_date": end_date,
            "targets": target_results
        }


def update_variable(end_date: str) -> None:
    """Airflow Variable 업데이트"""
    Variable.set(INCREMENT_KEY, end_date)
    logging.info(f"📌 Variable `{INCREMENT_KEY}` 업데이트: {end_date}")
