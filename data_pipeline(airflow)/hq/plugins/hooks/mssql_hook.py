import logging
from typing import Optional
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

logger = logging.getLogger(__name__)

class MSSQLHelper:
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.hook = MsSqlHook(mssql_conn_id=self.conn_id)

    def check_table(self, schema_name: str, table_name: str) -> bool:
        full_table = f"{schema_name}.{table_name}"
        check_table_sql = f"""
            SELECT CASE WHEN OBJECT_ID('{full_table}', 'U') IS NOT NULL THEN 1 ELSE 0 END
        """
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.execute(check_table_sql)
                exists = cursor.fetchone()[0]
                if not exists:
                    logger.warning(f"⚠️ Table `{full_table}` does not exist.")
                    return False
                logger.info(f"✅ Table `{full_table}` exists in the database.")
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

    def insert_data(self, schema_name: str, table_name: str, data: list):
        if not data:
            logger.warning(f"⚠️ No data to insert into `{schema_name}.{table_name}`")
            return
        columns = len(data[0])
        placeholders = ','.join(['?'] * columns)
        insert_sql = f"INSERT INTO {schema_name}.{table_name} VALUES ({placeholders})"
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.executemany(insert_sql, data)
                conn.commit()
                logger.info(f"✅ Inserted {len(data)} rows into `{schema_name}.{table_name}`")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Insert failed: {str(e)}")
            raise

    def execute_query(self, sql: str, task_id: str, xcom_key: Optional[str], **kwargs):
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.execute(sql)
                records = cursor.fetchall()
                if not records:
                    logger.warning(f"⚠️ No data found for `{task_id}`.")
                    return None
                
                # Fix encoding issues for Korean text
                fixed_records = []
                for record in records:
                    fixed_row = []
                    for item in record:
                        if isinstance(item, str):
                            # Try to fix encoding issues for Korean characters
                            try:
                                # First check if string contains garbled characters
                                if any(ord(c) > 255 for c in item):
                                    # Already properly encoded Unicode
                                    fixed_item = item
                                else:
                                    # Try to fix garbled encoding
                                    fixed_item = item.encode('latin1').decode('utf-8')
                            except (UnicodeDecodeError, UnicodeEncodeError):
                                try:
                                    # Try cp949 encoding (Korean Windows)
                                    fixed_item = item.encode('latin1').decode('cp949')
                                except (UnicodeDecodeError, UnicodeEncodeError):
                                    try:
                                        # Try euc-kr encoding
                                        fixed_item = item.encode('latin1').decode('euc-kr')
                                    except (UnicodeDecodeError, UnicodeEncodeError):
                                        # If all else fails, keep original
                                        fixed_item = item
                                        logger.warning(f"⚠️ Could not fix encoding for: {repr(item)}")
                        else:
                            fixed_item = item
                        fixed_row.append(fixed_item)
                    fixed_records.append(fixed_row)
                
                logger.info(f"✅ `{task_id}` Data: {fixed_records[:5]} ... (Total: {len(fixed_records)})")
                ti = kwargs.get("ti")
                if ti and xcom_key:
                    ti.xcom_push(key=xcom_key, value=fixed_records)
                return fixed_records
        except Exception as e:
            logger.error(f"❌ Query failed for `{task_id}`: {str(e)}")
            raise

    def execute_update(self, sql: str, task_id: str, parameters: tuple = None):
        try:
            with self.hook.get_conn() as conn, conn.cursor() as cursor:
                cursor.execute(sql, parameters or ())
                conn.commit()
                logger.info(f"✅ `{task_id}` update executed.")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ `{task_id}` update failed: {str(e)}")
            raise
