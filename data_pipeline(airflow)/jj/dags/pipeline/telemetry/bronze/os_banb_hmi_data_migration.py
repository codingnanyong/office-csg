import logging
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────
# 1️⃣ Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=24)
}

# Database Configuration
SOURCE_POSTGRES_CONN_ID = "pg_jj_banb_hmi_dw"
TARGET_POSTGRES_CONN_ID = "pg_jj_telemetry_dw"
SCHEMA_NAME = "bronze"
TABLE_NAME = "os_banb_hmi_data"

# Migration Configuration
VAR_KEY = "os_banb_hmi_data_migration_last_date"
BATCH_SIZE = 50000  # 한 번에 처리할 배치 크기 (소스에서 가져올 배치 크기)

# ────────────────────────────────────────────────────────────────
# 2️⃣ Utility Functions
# ────────────────────────────────────────────────────────────────
def _eod(dt: datetime) -> datetime:
    """End of day: 해당 일의 마지막 시간 23:59:59.999999"""
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)

def get_min_max_rx_date(pg: PostgresHelper) -> tuple:
    """소스 테이블에서 rx_date의 최소값과 최대값을 가져옴"""
    sql = f"""
        SELECT 
            MIN(rx_date) AS min_date,
            MAX(rx_date) AS max_date,
            COUNT(*) AS total_count
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE rx_date IS NOT NULL
    """
    
    try:
        with pg.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
            
            if result and result[0] and result[1]:
                min_date = result[0]
                max_date = result[1]
                total_count = result[2] if result[2] else 0
                
                # timezone이 없으면 UTC로 가정
                if min_date.tzinfo is None:
                    min_date = min_date.replace(tzinfo=timezone.utc)
                if max_date.tzinfo is None:
                    max_date = max_date.replace(tzinfo=timezone.utc)
                
                logging.info(f"📊 데이터 범위: {min_date} ~ {max_date} (총 {total_count:,} rows)")
                return min_date, max_date, total_count
            else:
                raise Exception("소스 테이블에 데이터가 없습니다.")
    except Exception as e:
        logging.error(f"❌ 최소/최대 날짜 조회 실패: {str(e)}")
        raise

def get_migration_start_date() -> datetime:
    """마이그레이션 시작 날짜를 가져옴 (Variable 또는 None)"""
    cursor_str = Variable.get(VAR_KEY, default_var=None)
    if cursor_str:
        try:
            cursor_dt = datetime.fromisoformat(cursor_str)
            if cursor_dt.tzinfo is None:
                cursor_dt = cursor_dt.replace(tzinfo=timezone.utc)
            logging.info(f"📌 마이그레이션 재개 지점: {cursor_dt}")
            return cursor_dt + timedelta(seconds=1)
        except Exception as e:
            logging.warning(f"⚠️ Variable 파싱 실패: {str(e)}. 처음부터 시작합니다.")
            return None
    return None

def set_migration_date(dt: datetime) -> None:
    """마이그레이션 진행 날짜를 Variable에 저장"""
    Variable.set(VAR_KEY, dt.isoformat())
    logging.info(f"📌 마이그레이션 진행 날짜 저장: {dt}")

# ────────────────────────────────────────────────────────────────
# 3️⃣ Data Extraction & Loading
# ────────────────────────────────────────────────────────────────
def build_extract_sql(start_date: datetime, end_date: datetime) -> str:
    """소스 테이블에서 데이터를 추출하는 SQL 쿼리"""
    # TIMESTAMPTZ 타입을 위해 타임존 정보 포함
    # PostgreSQL에서 TIMESTAMPTZ는 타임존을 포함하므로 ISO 형식으로 변환
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    
    return f"""
        SELECT 
            factory,
            equipment,
            seq_no,
            pid,
            rx_date,
            p_value,
            rxdate_year,
            rxdate_month,
            rxdate_day,
            etl_extract_time,
            etl_ingest_time
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE rx_date >= '{start_str}'::timestamptz
          AND rx_date <= '{end_str}'::timestamptz
          AND rx_date IS NOT NULL
        ORDER BY rx_date, factory, equipment, seq_no
    """

def extract_and_load_daily_batch(
    source_pg: PostgresHelper,
    target_pg: PostgresHelper,
    start_date: datetime,
    end_date: datetime
) -> int:
    """1일 단위 데이터를 추출하고 타겟에 로드"""
    logging.info(f"🔄 일 단위 배치 처리 시작: {start_date} ~ {end_date}")
    
    sql = build_extract_sql(start_date, end_date)
    
    columns = [
        "factory", "equipment", "seq_no", "pid", "rx_date", "p_value",
        "rxdate_year", "rxdate_month", "rxdate_day", "etl_extract_time", "etl_ingest_time"
    ]
    conflict_columns = ["factory", "equipment", "seq_no", "rx_date"]
    
    total_rows = 0
    batch_count = 0
    
    try:
        # 소스에서 배치 단위로 데이터 추출
        with source_pg.hook.get_conn() as source_conn:
            with source_conn.cursor() as source_cursor:
                source_cursor.execute(sql)
                
                # 배치 단위로 데이터 가져오기
                logging.info(f"📥 소스에서 데이터 추출 시작...")
                while True:
                    batch_data = source_cursor.fetchmany(BATCH_SIZE)
                    
                    if not batch_data:
                        break
                    
                    batch_count += 1
                    batch_rows = len(batch_data)
                    
                    # 타겟에 배치 삽입
                    try:
                        logging.info(f"📤 배치 {batch_count} 삽입 시작: {batch_rows:,} rows")
                        target_pg.insert_data(
                            SCHEMA_NAME,
                            TABLE_NAME,
                            batch_data,
                            columns,
                            conflict_columns,
                            chunk_size=1000
                        )
                        total_rows += batch_rows
                        
                        # 매 배치마다 로그 출력 (진행 상황 추적)
                        logging.info(f"✅ 배치 {batch_count} 삽입 완료: {batch_rows:,} rows (누적: {total_rows:,})")
                        
                        # 메모리 정리 힌트
                        del batch_data
                        
                    except Exception as e:
                        logging.error(f"❌ 배치 {batch_count} 삽입 실패: {str(e)}")
                        raise
        
        logging.info(f"✅ 일 단위 배치 처리 완료: {start_date} ~ {end_date} (총 {batch_count} 배치, {total_rows:,} rows)")
        return total_rows
        
    except Exception as e:
        logging.error(f"❌ 일 단위 배치 처리 실패: {start_date} ~ {end_date} - {str(e)}")
        raise

# ────────────────────────────────────────────────────────────────
# 4️⃣ Main Migration Logic
# ────────────────────────────────────────────────────────────────
def migrate_data(**context) -> dict:
    """데이터 마이그레이션 메인 함수"""
    source_pg = PostgresHelper(conn_id=SOURCE_POSTGRES_CONN_ID)
    target_pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # 소스 테이블 존재 확인
        if not source_pg.check_table(SCHEMA_NAME, TABLE_NAME):
            raise Exception(f"소스 테이블이 존재하지 않습니다: {SCHEMA_NAME}.{TABLE_NAME}")
        
        # 타겟 테이블 존재 확인
        if not target_pg.check_table(SCHEMA_NAME, TABLE_NAME):
            raise Exception(f"타겟 테이블이 존재하지 않습니다: {SCHEMA_NAME}.{TABLE_NAME}")
        
        # 데이터 범위 확인
        min_date, max_date, total_count = get_min_max_rx_date(source_pg)
        logging.info(f"📊 총 데이터 개수: {total_count:,} rows")
        
        # 마이그레이션 시작 날짜 결정
        migration_start = get_migration_start_date()
        if migration_start is None:
            migration_start = min_date.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            # timezone 통일
            if migration_start.tzinfo != min_date.tzinfo:
                migration_start = migration_start.astimezone(min_date.tzinfo)
        
        # 마이그레이션 종료 날짜
        migration_end = max_date
        
        # 예상 일수 계산
        estimated_days = (migration_end.date() - migration_start.date()).days + 1
        logging.info(f"🚀 마이그레이션 시작: {migration_start} ~ {migration_end} (예상 {estimated_days}일)")
        
        # 일 단위로 처리
        current_start = migration_start
        total_migrated = 0
        day_count = 0
        
        while current_start <= migration_end:
            # 현재 일의 끝 날짜 계산
            current_end = _eod(current_start)
            if current_end > migration_end:
                current_end = migration_end
            
            day_count += 1
            day_start_str = current_start.strftime("%Y-%m-%d")
            day_end_str = current_end.strftime("%Y-%m-%d")
            logging.info(f"📅 일 {day_count} 처리 시작: {day_start_str} ~ {day_end_str}")
            
            try:
                # 일 단위 데이터 추출 및 로드
                day_rows = extract_and_load_daily_batch(
                    source_pg,
                    target_pg,
                    current_start,
                    current_end
                )
                total_migrated += day_rows
                
                # 진행 상황 저장 (성공한 경우에만)
                set_migration_date(current_end)
                
                progress_pct = (total_migrated / total_count * 100) if total_count > 0 else 0
                logging.info(f"✅ 일 {day_count} 완료: {day_rows:,} rows (누적: {total_migrated:,} rows, 진행률: {progress_pct:.2f}%)")
                
            except Exception as e:
                logging.error(f"❌ 일 {day_count} 처리 실패 ({day_start_str} ~ {day_end_str}): {str(e)}")
                # 실패한 일은 다음 실행에서 다시 시도할 수 있도록 Variable 업데이트 안 함
                raise
            
            # 다음 일 시작 (다음 날 자정)
            next_day = (current_start + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            current_start = next_day
            if current_start > migration_end:
                break
        
        # 마이그레이션 완료 시 Variable 삭제
        if total_migrated > 0:
            try:
                Variable.delete(VAR_KEY)
                logging.info(f"✅ 마이그레이션 완료. Variable 삭제: {VAR_KEY}")
            except Exception:
                pass
        
        logging.info(f"🎉 마이그레이션 완료: 총 {total_migrated:,} rows 처리 ({day_count}일)")
        return {
            "status": "success",
            "total_migrated": total_migrated,
            "day_count": day_count,
            "start_date": migration_start.isoformat(),
            "end_date": migration_end.isoformat()
        }
        
    except Exception as e:
        logging.error(f"❌ 마이그레이션 실패: {str(e)}")
        return {
            "status": "failed",
            "error": str(e)
        }

# ────────────────────────────────────────────────────────────────
# 5️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="os_banb_hmi_data_migration",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # 수동 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "OS", "Banbury", "HMI", "data", "migration", "bronze layer"],
) as dag:
    
    migrate_task = PythonOperator(
        task_id="migrate_os_banb_hmi_data",
        python_callable=migrate_data,
        provide_context=True,
    )

