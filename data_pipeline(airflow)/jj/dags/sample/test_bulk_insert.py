"""
Bulk Insert Test DAG
====================
PostgresHelper의 bulk_insert 메서드 테스트용 DAG
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from plugins.hooks.postgres_hook import PostgresHelper
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# 테스트용 Connection ID (실제 환경에 맞게 수정 필요)
TEST_CONN_ID = "postgres_default"
TEST_SCHEMA = "public"
TEST_TABLE = "test_bulk_insert"


def check_bulk_insert_method(**kwargs):
    """bulk_insert 메서드가 존재하는지 확인"""
    logger.info("=" * 60)
    logger.info("1️⃣ PostgresHelper 클래스 확인")
    logger.info("=" * 60)
    
    pg = PostgresHelper(conn_id=TEST_CONN_ID)
    
    # 메서드 존재 확인
    has_method = hasattr(pg, 'bulk_insert')
    logger.info(f"✅ bulk_insert 메서드 존재 여부: {has_method}")
    
    if has_method:
        logger.info(f"✅ 메서드 타입: {type(getattr(pg, 'bulk_insert'))}")
        logger.info(f"✅ 메서드 docstring: {getattr(pg, 'bulk_insert').__doc__}")
    else:
        logger.error("❌ bulk_insert 메서드를 찾을 수 없습니다!")
        raise AttributeError("bulk_insert method not found")
    
    # 모든 메서드 목록 확인
    methods = [method for method in dir(pg) if not method.startswith('_')]
    logger.info(f"📋 PostgresHelper 메서드 목록: {', '.join(methods)}")
    
    return {"status": "success", "has_bulk_insert": has_method}


def create_test_table(**kwargs):
    """테스트용 테이블 생성"""
    logger.info("=" * 60)
    logger.info("2️⃣ 테스트 테이블 생성")
    logger.info("=" * 60)
    
    pg = PostgresHelper(conn_id=TEST_CONN_ID)
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TEST_SCHEMA}.{TEST_TABLE} (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        value NUMERIC(10, 2),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        status VARCHAR(20)
    );
    """
    
    try:
        with pg.hook.get_conn() as conn, conn.cursor() as cursor:
            # 스키마 생성
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
            
            # 테이블 생성
            cursor.execute(create_table_sql)
            conn.commit()
            logger.info(f"✅ 테이블 생성 완료: {TEST_SCHEMA}.{TEST_TABLE}")
            
            # 기존 데이터 삭제
            cursor.execute(f"TRUNCATE TABLE {TEST_SCHEMA}.{TEST_TABLE}")
            conn.commit()
            logger.info(f"✅ 테이블 초기화 완료")
            
    except Exception as e:
        logger.error(f"❌ 테이블 생성 실패: {e}")
        raise
    
    return {"status": "success"}


def test_bulk_insert(**kwargs):
    """bulk_insert 메서드 테스트"""
    logger.info("=" * 60)
    logger.info("3️⃣ bulk_insert 메서드 테스트")
    logger.info("=" * 60)
    
    pg = PostgresHelper(conn_id=TEST_CONN_ID)
    
    # 테스트 데이터 생성 (1000개 행)
    test_data = []
    for i in range(1, 1001):
        test_data.append((
            i,  # id
            f"test_name_{i}",  # name
            round(i * 1.5, 2),  # value
            datetime.now(),  # created_at
            "active" if i % 2 == 0 else "inactive"  # status
        ))
    
    logger.info(f"📦 테스트 데이터 생성: {len(test_data):,}개 행")
    logger.info(f"   샘플 데이터 (첫 3개): {test_data[:3]}")
    
    # 컬럼 리스트
    columns = ["id", "name", "value", "created_at", "status"]
    
    try:
        # bulk_insert 실행
        import time
        start_time = time.time()
        
        pg.bulk_insert(
            schema_name=TEST_SCHEMA,
            table_name=TEST_TABLE,
            data=test_data,
            columns=columns,
            chunk_size=1000  # 1000개씩 청크 처리
        )
        
        elapsed_time = time.time() - start_time
        logger.info(f"⏱️ bulk_insert 실행 시간: {elapsed_time:.2f}초")
        logger.info(f"📊 처리 속도: {len(test_data) / elapsed_time:.0f} rows/sec")
        
        # 데이터 확인
        with pg.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TEST_SCHEMA}.{TEST_TABLE}")
            count = cursor.fetchone()[0]
            logger.info(f"✅ 삽입된 행 수: {count:,}개")
            
            if count != len(test_data):
                raise ValueError(f"데이터 개수 불일치: 예상 {len(test_data)}, 실제 {count}")
            
            # 샘플 데이터 확인
            cursor.execute(f"SELECT * FROM {TEST_SCHEMA}.{TEST_TABLE} ORDER BY id LIMIT 5")
            sample = cursor.fetchall()
            logger.info(f"📋 샘플 데이터 (첫 5개): {sample}")
        
        return {
            "status": "success",
            "rows_inserted": count,
            "elapsed_time": elapsed_time
        }
        
    except Exception as e:
        logger.error(f"❌ bulk_insert 테스트 실패: {e}")
        raise


def compare_with_insert_data(**kwargs):
    """insert_data와 bulk_insert 성능 비교"""
    logger.info("=" * 60)
    logger.info("4️⃣ insert_data vs bulk_insert 성능 비교")
    logger.info("=" * 60)
    
    pg = PostgresHelper(conn_id=TEST_CONN_ID)
    
    # 비교용 테이블 생성
    compare_table = f"{TEST_TABLE}_compare"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TEST_SCHEMA}.{compare_table} (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        value NUMERIC(10, 2),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        status VARCHAR(20)
    );
    """
    
    # 테스트 데이터 (100개 행으로 비교)
    test_data = []
    for i in range(1, 101):
        test_data.append((
            i,
            f"compare_name_{i}",
            round(i * 1.5, 2),
            datetime.now(),
            "active" if i % 2 == 0 else "inactive"
        ))
    
    columns = ["id", "name", "value", "created_at", "status"]
    
    try:
        with pg.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
            cursor.execute(create_table_sql)
            cursor.execute(f"TRUNCATE TABLE {TEST_SCHEMA}.{compare_table}")
            conn.commit()
        
        # 1. insert_data 테스트
        import time
        start_time = time.time()
        pg.insert_data(
            schema_name=TEST_SCHEMA,
            table_name=compare_table,
            data=test_data,
            columns=columns,
            chunk_size=100
        )
        insert_data_time = time.time() - start_time
        logger.info(f"⏱️ insert_data 실행 시간: {insert_data_time:.2f}초")
        
        # 테이블 초기화
        with pg.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {TEST_SCHEMA}.{compare_table}")
            conn.commit()
        
        # 2. bulk_insert 테스트
        start_time = time.time()
        pg.bulk_insert(
            schema_name=TEST_SCHEMA,
            table_name=compare_table,
            data=test_data,
            columns=columns,
            chunk_size=100
        )
        bulk_insert_time = time.time() - start_time
        logger.info(f"⏱️ bulk_insert 실행 시간: {bulk_insert_time:.2f}초")
        
        # 성능 비교
        if bulk_insert_time > 0:
            speedup = insert_data_time / bulk_insert_time
            logger.info(f"🚀 성능 향상: {speedup:.2f}x 빠름")
        
        return {
            "status": "success",
            "insert_data_time": insert_data_time,
            "bulk_insert_time": bulk_insert_time,
            "speedup": speedup if bulk_insert_time > 0 else None
        }
        
    except Exception as e:
        logger.error(f"❌ 성능 비교 실패: {e}")
        raise


def cleanup_test_table(**kwargs):
    """테스트 테이블 정리"""
    logger.info("=" * 60)
    logger.info("5️⃣ 테스트 테이블 정리")
    logger.info("=" * 60)
    
    pg = PostgresHelper(conn_id=TEST_CONN_ID)
    
    try:
        with pg.hook.get_conn() as conn, conn.cursor() as cursor:
            # 테스트 테이블 삭제
            cursor.execute(f"DROP TABLE IF EXISTS {TEST_SCHEMA}.{TEST_TABLE}")
            cursor.execute(f"DROP TABLE IF EXISTS {TEST_SCHEMA}.{TEST_TABLE}_compare")
            conn.commit()
            logger.info(f"✅ 테스트 테이블 삭제 완료")
            
    except Exception as e:
        logger.warning(f"⚠️ 테이블 정리 중 오류 (무시 가능): {e}")
    
    return {"status": "success"}


# DAG 정의
with DAG(
    dag_id="test_bulk_insert",
    default_args=default_args,
    description="PostgresHelper bulk_insert 메서드 테스트",
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["test", "postgres", "bulk_insert"],
    max_active_runs=1,
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    # Task 1: 메서드 존재 확인
    check_method = PythonOperator(
        task_id='check_bulk_insert_method',
        python_callable=check_bulk_insert_method,
    )
    
    # Task 2: 테스트 테이블 생성
    create_table = PythonOperator(
        task_id='create_test_table',
        python_callable=create_test_table,
    )
    
    # Task 3: bulk_insert 테스트
    test_bulk = PythonOperator(
        task_id='test_bulk_insert',
        python_callable=test_bulk_insert,
    )
    
    # Task 4: 성능 비교
    compare_performance = PythonOperator(
        task_id='compare_with_insert_data',
        python_callable=compare_with_insert_data,
    )
    
    # Task 5: 정리 (선택적)
    cleanup = PythonOperator(
        task_id='cleanup_test_table',
        python_callable=cleanup_test_table,
        trigger_rule='all_done',  # 성공/실패 관계없이 실행
    )
    
    # DAG 의존성
    start >> check_method >> create_table >> test_bulk >> compare_performance >> cleanup >> end

