import logging
from typing import Optional
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

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


    def insert_data(self, schema_name: str, table_name: str, data: list, columns: list = None, conflict_columns: list = None) -> None:
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
                execute_values(cursor, insert_sql, data)
                conn.commit()
                logger.info(f"✅ Successfully inserted {len(data)} records into `{schema_name}.{table_name}`.")

        except Exception as e:
            logger.error(f"❌ INSERT into `{schema_name}.{table_name}` failed: {str(e)}")
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

    def upsert_data(self, schema_name: str, table_name: str, data: list, conflict_columns: list, update_columns: list):
        """
        PostgreSQL UPSERT operation: INSERT ... ON CONFLICT ... DO UPDATE
        
        Args:
            schema_name: Target schema name
            table_name: Target table name  
            data: List of tuples to upsert
            conflict_columns: Columns for conflict detection (usually primary key)
            update_columns: Columns to update on conflict
        """
        if not data:
            logger.warning(f"⚠️ Warning: No data to upsert into `{schema_name}.{table_name}`!")
            return

        if not isinstance(data, list) or not all(isinstance(row, tuple) for row in data):
            logger.error(f"❌ Data format error: Expected list of tuples but got {type(data)} with first element {type(data[0])}")
            return

        # Build UPSERT SQL for execute_values (single %s placeholder)
        conflict_clause = ", ".join(conflict_columns)
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        upsert_sql = f"""
            INSERT INTO {schema_name}.{table_name} VALUES %s
            ON CONFLICT ({conflict_clause}) 
            DO UPDATE SET {update_set}
        """

        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                execute_values(cursor, upsert_sql, data)
                conn.commit()
                logger.info(f"✅ Successfully upserted {len(data)} records into `{schema_name}.{table_name}`.")

        except Exception as e:
            logger.error(f"❌ UPSERT into `{schema_name}.{table_name}` failed: {str(e)}")
            raise
