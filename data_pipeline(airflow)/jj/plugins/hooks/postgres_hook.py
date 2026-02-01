import logging
import io
from typing import Optional
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from psycopg2.extras import execute_values, execute_batch

logger = logging.getLogger(__name__)

class PostgresHelper:
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)

    def check_table(self, schema_name: str, table_name: str) -> bool:
        check_table_sql = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = %(schema_name)s
                AND table_name = %(table_name)s
            );
        """
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.execute(check_table_sql, {"schema_name": schema_name, "table_name": table_name})
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    logger.warning(f"⚠️ Table `{schema_name}.{table_name}` does not exist.")
                    return False

                logger.info(f"✅ Table `{schema_name}.{table_name}` exists in the database.")
                return True

        except Exception as e:
            logger.error(f"❌ Table check failed: {str(e)}")
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
            logger.error(f"❌ Cleaning table `{schema_name}.{table_name}` failed: {str(e)}")
            raise

    def execute_query(self, sql: str, task_id: str, xcom_key: Optional[str], **kwargs):
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.execute(sql)
                records = cursor.fetchall()

                if not records:
                    logger.warning(f"⚠️ Warning: No records found for `{task_id}`!")
                    return None

                logger.info(f"✅ `{task_id}` Data: {records[:5]} ... (Total: {len(records)})")

                ti = kwargs.get("ti")
                if ti and xcom_key: 
                    ti.xcom_push(key=xcom_key, value=records)
                elif ti:
                    logger.info(f"[INFO] Skipping XCom push for `{task_id}` because xcom_key is None.")
                else:
                    logger.warning("⚠️ TaskInstance (`ti`) not found, XCom push skipped.")

                return records

        except Exception as e:
            logger.error(f"❌ Query execution failed for `{task_id}`: {str(e)}")
            raise


    def insert_data(self, schema_name: str, table_name: str, data: list, columns: list = None, conflict_columns: list = None, chunk_size: int = 1000) -> None:
        """
        INSERT 문을 사용한 데이터 삽입 (ON CONFLICT 지원)
        execute_values를 사용하여 여러 행을 한 번에 삽입
        """
        if not data:
            logger.warning(f"⚠️ Warning: No data to insert into `{schema_name}.{table_name}`!")
            return

        if not isinstance(data, list) or not all(isinstance(row, tuple) for row in data):
            logger.error(f"❌ Data format error: Expected list of tuples but got {type(data)} with first element {type(data[0])}")
            return

        if conflict_columns:
            # Get all columns except conflict columns for UPDATE
            if columns:
                update_columns = [col for col in columns if col not in conflict_columns]
                if update_columns:
                    update_clause = f"DO UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in update_columns])}"
                else:
                    update_clause = "DO NOTHING"
            else:
                update_clause = "DO NOTHING"
            conflict_clause = f"ON CONFLICT ({', '.join(conflict_columns)}) {update_clause}"
        else:
            conflict_clause = ""

        insert_sql = f"""
            INSERT INTO {schema_name}.{table_name} VALUES %s {conflict_clause}
        """

        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                # Process data in chunks for large datasets
                total_inserted = 0
                for i in range(0, len(data), chunk_size):
                    chunk = data[i:i + chunk_size]
                    execute_values(cursor, insert_sql, chunk)
                    total_inserted += len(chunk)
                    logger.info(f"📦 Processed chunk {i//chunk_size + 1}: {len(chunk):,} records")
                
                conn.commit()
                logger.info(f"✅ Successfully inserted {total_inserted:,} records into `{schema_name}.{table_name}`.")

        except Exception as e:
            logger.error(f"❌ INSERT into `{schema_name}.{table_name}` failed: {str(e)}")
            raise

    def bulk_insert(self, schema_name: str, table_name: str, data: list, columns: list = None, chunk_size: int = 10000) -> None:
        """
        PostgreSQL COPY 명령을 사용한 진정한 Bulk Insert
        INSERT보다 훨씬 빠르지만, ON CONFLICT는 지원하지 않음
        
        Args:
            schema_name: 스키마 이름
            table_name: 테이블 이름
            data: 삽입할 데이터 (list of tuples)
            columns: 컬럼 리스트 (None이면 모든 컬럼)
            chunk_size: 청크 크기 (기본값: 10000)
        
        Returns:
            None
        """
        if not data:
            logger.warning(f"⚠️ Warning: No data to bulk insert into `{schema_name}.{table_name}`!")
            return

        if not isinstance(data, list) or not all(isinstance(row, tuple) for row in data):
            logger.error(f"❌ Data format error: Expected list of tuples but got {type(data)} with first element {type(data[0])}")
            return

        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                # 컬럼 지정 여부에 따라 COPY 문 구성
                if columns:
                    columns_str = f"({', '.join(columns)})"
                else:
                    columns_str = ""
                
                # COPY FROM STDIN 사용
                copy_sql = f"COPY {schema_name}.{table_name} {columns_str} FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '')"
                
                total_inserted = 0
                # 데이터를 청크 단위로 처리
                for i in range(0, len(data), chunk_size):
                    chunk = data[i:i + chunk_size]
                    
                    # StringIO를 사용하여 CSV 형식으로 변환
                    csv_buffer = io.StringIO()
                    for row in chunk:
                        # 각 값을 탭으로 구분하고, None은 빈 문자열로 처리
                        csv_row = '\t'.join([
                            '' if val is None else str(val).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                            for val in row
                        ])
                        csv_buffer.write(csv_row + '\n')
                    
                    csv_buffer.seek(0)
                    
                    # COPY FROM STDIN 실행
                    cursor.copy_expert(copy_sql, csv_buffer)
                    total_inserted += len(chunk)
                    logger.info(f"📦 Bulk insert chunk {i//chunk_size + 1}: {len(chunk):,} records")
                
                conn.commit()
                logger.info(f"✅ Successfully bulk inserted {total_inserted:,} records into `{schema_name}.{table_name}` using COPY command.")

        except Exception as e:
            logger.error(f"❌ Bulk INSERT into `{schema_name}.{table_name}` failed: {str(e)}")
            raise


    def execute_update(self, sql: str, task_id: str, parameters: tuple = None):
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.execute(sql, parameters)
                conn.commit()
                logger.info(f"✅ `{task_id}` update executed successfully.")

        except Exception as e:
            logger.error(f"❌ `{task_id}` update failed: {str(e)}")
            raise

    def clean_table_with_condition(self, schema_name: str, table_name: str, column_name: str, target_date: str):
        delete_sql = f"DELETE FROM {schema_name}.{table_name} WHERE {column_name} = '{target_date}';"
        
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.execute(delete_sql)
                conn.commit()
                logger.info(f"🧹 Table `{schema_name}.{table_name}` cleaned where `{column_name}` = '{target_date}'")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Conditional clean failed on `{schema_name}.{table_name}`: {str(e)}")
            raise
