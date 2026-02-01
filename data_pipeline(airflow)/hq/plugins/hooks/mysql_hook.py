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
                    return None
                logger.info(f"\u2705 `{task_id}` Data: {records[:5]} ... (Total: {len(records)})")
                ti = kwargs.get("ti")
                if ti and xcom_key:
                    ti.xcom_push(key=xcom_key, value=records)
                return records
        except Exception as e:
            logger.error(f"\u274c Query failed for `{task_id}`: {str(e)}")
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
