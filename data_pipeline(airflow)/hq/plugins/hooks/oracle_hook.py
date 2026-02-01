import os
import logging
import oracledb
import datetime
from typing import Optional
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

class OracleHelper:
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.connection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _init_client(self):
        lib_path = os.environ.get("ORACLE_CLIENT_LIB", "/opt/oracle/instantclient_23_7")
        try:
            oracledb.init_oracle_client(lib_dir=lib_path)
            logger.info(f"🔌 Oracle Client Initialized from: {lib_path}")
        except oracledb.ProgrammingError as e:
            if "DPY-0005" in str(e):
                logger.debug("Oracle client already initialized, skipping.")
            else:
                raise

    def get_conn(self):
        if self.connection:
            return self.connection

        self._init_client()

        conn_info = BaseHook.get_connection(self.conn_id)
        user = conn_info.login
        password = conn_info.password
        host = conn_info.host
        port = conn_info.port or 1521
        service = conn_info.schema

        dsn = f"{host}:{port}/{service}"
        logger.info(f"📡 Connecting to Oracle: {dsn}")

        self.connection = oracledb.connect(user=user, password=password, dsn=dsn)
        return self.connection

    def close(self):
        if self.connection:
            try:
                self.connection.close()
                logger.info("🔌 Oracle connection closed.")
            except Exception as e:
                logger.warning(f"⚠️ Error closing Oracle connection: {str(e)}")
            finally:
                self.connection = None

    def _convert_value(self, value):
        if value is None:
            return None
        elif isinstance(value, oracledb.LOB):
            content = value.read()
            if isinstance(content, bytes):
                return content.decode('utf-8', errors='replace')
            return str(content)
        elif isinstance(value, (datetime.datetime, datetime.date)):
            return value.isoformat()
        elif isinstance(value, (int, float, str, bool)):
            return value
        else:
            return str(value)

    def check_table(self, schema_name: str, table_name: str) -> bool:
        sql = """
            SELECT COUNT(*) FROM all_tables 
            WHERE owner = :1 AND table_name = :2
        """
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(sql, (schema_name.upper(), table_name.upper()))
                exists = cursor.fetchone()[0] > 0
                if not exists:
                    logger.warning(f"⚠️ Table `{schema_name}.{table_name}` does not exist.")
                    return False
                logger.info(f"✅ Table `{schema_name}.{table_name}` exists.")
                return True
        except Exception as e:
            logger.error(f"❌ Error checking table: {str(e)}")
            raise

    def clean_table(self, schema_name: str, table_name: str):
        delete_sql = f"DELETE FROM {schema_name}.{table_name}"
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(delete_sql)
                conn.commit()
                logger.info(f"🧹 Cleaned table `{schema_name}.{table_name}`")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error cleaning table: {str(e)}")
            raise

    def insert_data(self, schema_name: str, table_name: str, data: list):
        if not data:
            logger.warning(f"⚠️ No data to insert into `{schema_name}.{table_name}`")
            return
        placeholders = ', '.join([f":{i + 1}" for i in range(len(data[0]))])
        insert_sql = f"INSERT INTO {schema_name}.{table_name} VALUES ({placeholders})"
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                cursor.executemany(insert_sql, data)
                conn.commit()
                logger.info(f"✅ Inserted {len(data)} rows into `{schema_name}.{table_name}`")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Insert failed: {str(e)}")
            raise

    def execute_query(self, sql: str, task_id: str, xcom_key: Optional[str] = None, parameters: Optional[tuple] = None, **kwargs):
        """Oracle에서 SELECT 쿼리를 실행하고 결과를 반환하며, 필요시 XCom으로도 push."""
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                # ✅ 바인드 변수를 처리하기 위해 parameters를 넘김
                cursor.execute(sql, parameters or ())

                columns = [desc[0] for desc in cursor.description]
                all_records = []
                batch_size = 1000

                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    for row in rows:
                        converted = {
                            col: self._convert_value(val) for col, val in zip(columns, row)
                        }
                        all_records.append(converted)

                if not all_records:
                    logger.warning(f"⚠️ No data found for `{task_id}`.")
                    return None

                # logger.info(f"📄 `{task_id}` result sample: {all_records[:5]}")
                logger.info(f"📄 `{task_id}` total count: {len(all_records)}")

                ti = kwargs.get("ti")
                if ti and xcom_key:
                    ti.xcom_push(key=xcom_key, value=all_records)

                return all_records

        except Exception as e:
            logger.error(f"❌ Query failed for `{task_id}`: {str(e)}")
            raise

    def execute_update(self, sql: str, task_id: str, parameters: Optional[tuple] = None):
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(sql, parameters or ())
                conn.commit()
                logger.info(f"✅ `{task_id}` update executed.")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ `{task_id}` update failed: {str(e)}")
            raise
