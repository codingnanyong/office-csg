import logging
from typing import Optional
from airflow.providers.mysql.hooks.mysql import MySqlHook

logger = logging.getLogger(__name__)

class MySQLHelper:
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.hook = MySqlHook(mysql_conn_id=self.conn_id)

    def check_table(self, schema_name: str, table_name: str) -> bool:
        check_table_sql = """
            SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        """
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.execute(check_table_sql, (schema_name, table_name))
                exists = cursor.fetchone()[0]
                if not exists:
                    logger.warning(f"\u26a0\ufe0f Table `{schema_name}.{table_name}` does not exist.")
                    return False
                logger.info(f"\u2705 Table `{schema_name}.{table_name}` exists in the database.")
                return True
        except Exception as e:
            logger.error(f"\u274c Table check failed: {str(e)}")
            raise

    def clean_table(self, schema_name: str, table_name: str):
        delete_sql = f"DELETE FROM {schema_name}.{table_name};"
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.execute(delete_sql)
                conn.commit()
                logger.info(f"🗑️ Table `{schema_name}.{table_name}` cleaned!")
        except Exception as e:
            conn.rollback()
            logger.error(f"\u274c Cleaning table `{schema_name}.{table_name}` failed: {str(e)}")
            raise

    def insert_data(self, schema_name: str, table_name: str, data: list):
        if not data:
            logger.warning(f"\u26a0\ufe0f No data to insert into `{schema_name}.{table_name}`")
            return
        columns = len(data[0])
        placeholders = ','.join(['%s'] * columns)
        insert_sql = f"INSERT INTO {schema_name}.{table_name} VALUES ({placeholders})"
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.executemany(insert_sql, data)
                conn.commit()
                logger.info(f"\u2705 Inserted {len(data)} rows into `{schema_name}.{table_name}`")
        except Exception as e:
            conn.rollback()
            logger.error(f"\u274c Insert failed: {str(e)}")
            raise

    def execute_query(self, sql: str, task_id: str, xcom_key: Optional[str], **kwargs):
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.execute(sql)
                records = cursor.fetchall()
                if not records:
                    logger.warning(f"\u26a0\ufe0f No data found for `{task_id}`.")
                    return []  # None 대신 빈 리스트 반환
                logger.info(f"\u2705 `{task_id}` Data: {records[:5]} ... (Total: {len(records)})")
                ti = kwargs.get("ti")
                if ti and xcom_key:
                    ti.xcom_push(key=xcom_key, value=records)
                return records
        except Exception as e:
            logger.error(f"\u274c Query failed for `{task_id}`: {str(e)}")
            raise

    def execute_query_streaming(self, sql: str, task_id: str, batch_size: int = 2000, query_timeout_seconds: int = 600, **kwargs):
        """
        대용량 데이터를 배치 단위로 스트리밍 처리
        메모리 효율적인 쿼리 실행 (기본 배치 크기: 2000으로 줄임)
        
        Args:
            sql: 실행할 SQL 쿼리
            task_id: 태스크 ID (로깅용)
            batch_size: 배치 크기 (기본값: 2000, 메모리 절약)
            query_timeout_seconds: 쿼리 실행 타임아웃 (초 단위, 기본값: 600초 = 10분)
            
        Yields:
            배치 단위로 나뉜 결과 리스트
        """
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                # MySQL 쿼리 실행 시간 제한 설정 (밀리초 단위)
                timeout_ms = query_timeout_seconds * 1000
                try:
                    cursor.execute(f"SET SESSION max_execution_time = {timeout_ms}")
                    logger.info(f"⏱️ 쿼리 타임아웃 설정: {query_timeout_seconds}초 ({timeout_ms}ms) - {task_id}")
                except Exception as e:
                    # MariaDB 등에서 변수 미지원인 경우 쿼리는 계속 진행
                    logger.warning(f"⚠️ 쿼리 타임아웃 설정 실패 (계속 진행): {str(e)} - {task_id}")
                
                cursor.execute(sql)
                total_count = 0
                batch_num = 0
                
                while True:
                    batch = cursor.fetchmany(batch_size)
                    if not batch:
                        break
                    
                    batch_num += 1
                    total_count += len(batch)
                    
                    if batch_num == 1:
                        logger.info(f"✅ `{task_id}` First batch: {batch[:3]} ... (Batch size: {len(batch)})")
                    elif batch_num % 10 == 0:
                        logger.info(f"📊 `{task_id}` Processed {batch_num} batches, {total_count:,} records so far")
                    
                    yield batch
                
                if total_count == 0:
                    logger.warning(f"⚠️ No data found for `{task_id}`.")
                else:
                    logger.info(f"✅ `{task_id}` Total records processed: {total_count:,} ({batch_num} batches)")
                    
        except Exception as e:
            logger.error(f"❌ Streaming query failed for `{task_id}`: {str(e)}")
            raise

    def execute_update(self, sql: str, task_id: str, parameters: tuple = None):
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.execute(sql, parameters or ())
                conn.commit()
                logger.info(f"\u2705 `{task_id}` update executed.")
        except Exception as e:
            conn.rollback()
            logger.error(f"\u274c `{task_id}` update failed: {str(e)}")
            raise
