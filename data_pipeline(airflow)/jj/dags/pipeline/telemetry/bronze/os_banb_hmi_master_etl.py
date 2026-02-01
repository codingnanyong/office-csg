import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from plugins.hooks.mysql_hook import MySQLHelper
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────
# 1️⃣ Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)
}

# Database Configuration
SOURCE_MYSQL_CONN_ID = "maria_jj_os_banb_1"
SOURCE_MYSQL_CONN_ID_3 = "maria_jj_os_banb_3"
TARGET_POSTGRES_CONN_ID = "pg_jj_telemetry_dw"
SCHEMA_NAME = "bronze"

# Banbury Equipment Configuration
EQUIPMENTS = [
    {"id": "1", "conn_id": SOURCE_MYSQL_CONN_ID, "name": "Banbury 1호기"},
    {"id": "3", "conn_id": SOURCE_MYSQL_CONN_ID_3, "name": "Banbury 3호기"}
]

# Date Configuration
INDO_TZ = timezone(timedelta(hours=7))

# ────────────────────────────────────────────────────────────────
# 2️⃣ Utility Functions
# ────────────────────────────────────────────────────────────────
def get_factory_column_names() -> list:
    """Get column names for Banbury HMI factory table"""
    return [
        "factory", "eng_name", "kor_name", "idn_name", "sp1_name", "sp2_name",
        "sort_no", "create_date", "etl_extract_time", "etl_ingest_time"
    ]

def get_equipment_column_names() -> list:
    """Get column names for Banbury HMI equipment table"""
    return [
        "equipment", "eng_name", "kor_name", "idn_name", "sp1_name", "sp2_name",
        "factory", "sort_no", "create_date", "etl_extract_time", "etl_ingest_time"
    ]

def get_common_column_names() -> list:
    """Get column names for Banbury HMI common table"""
    return [
        "code_id", "code_key", "code_name", "code_type", "sort_no", "create_date",
        "etl_extract_time", "etl_ingest_time"
    ]

def get_maintenance_column_names() -> list:
    """Get column names for Banbury HMI maintenance table"""
    return [
        "maintenance", "eng_name", "kor_name", "idn_name", "sp1_name", "sp2_name",
        "desc_name", "level", "cycle", "min_value", "max_value", "active",
        "pos_x", "pos_y", "sort_no", "base_date", "create_date", "etl_extract_time", "etl_ingest_time"
    ]

def get_alarm_column_names() -> list:
    """Get column names for Banbury HMI alarm table"""
    return [
        "pid", "alarm", "eng_name", "kor_name", "idn_name", "sp1_name", "sp2_name",
        "level", "ucl", "uwl", "cl", "lwl", "lcl", "active", "period", "sort_no",
        "create_date", "usl", "lsl", "etl_extract_time", "etl_ingest_time"
    ]

def get_screen_column_names() -> list:
    """Get column names for Banbury HMI screen table"""
    return [
        "code_id", "eng_name", "kor_name", "idn_name", "sp1_name", "sp2_name",
        "code_key", "sort_no", "create_date", "etl_extract_time", "etl_ingest_time"
    ]

def get_sensorbridge_column_names() -> list:
    """Get column names for Banbury HMI sensorbridge table"""
    return [
        "sensor_bridge", "serial_no", "sensor_hub", "sort_no", "create_date",
        "etl_extract_time", "etl_ingest_time"
    ]

def get_sensorgroup_column_names() -> list:
    """Get column names for Banbury HMI sensorgroup table"""
    return [
        "sensor_group", "eng_name", "kor_name", "idn_name", "sp1_name", "sp2_name",
        "sort_no", "create_date", "etl_extract_time", "etl_ingest_time"
    ]

def get_sensorhub_column_names() -> list:
    """Get column names for Banbury HMI sensorhub table"""
    return [
        "sensor_hub", "serial_no", "factory", "equipment", "sort_no", "create_date",
        "etl_extract_time", "etl_ingest_time"
    ]

def get_sensortype_column_names() -> list:
    """Get column names for Banbury HMI sensortype table"""
    return [
        "sensor_type", "eng_name", "kor_name", "idn_name", "sp1_name", "sp2_name",
        "sort_no", "create_date", "etl_extract_time", "etl_ingest_time"
    ]

def get_sensor_column_names() -> list:
    """Get column names for Banbury HMI sensor table"""
    return [
        "pid", "eng_name", "kor_name", "idn_name", "sp1_name", "sp2_name",
        "sensor_bridge", "sensor_type", "sensor_group", "factory", "equipment",
        "daq_mac", "ip_addr", "level", "active", "pos_x", "pos_y", "sort_no",
        "unit", "max_value", "min_value", "create_date", "etl_extract_time", "etl_ingest_time"
    ]

def prepare_insert_data(data: list, extract_time: datetime) -> list:
    """Prepare data for insertion with ETL timestamps"""
    insert_data = []
    for row in data:
        # Add ETL timestamps to each row
        row_with_etl = list(row) + [extract_time, datetime.utcnow()]
        insert_data.append(tuple(row_with_etl))
    return insert_data

def build_factory_extract_sql() -> str:
    """Build SQL query for Banbury HMI factory data extraction"""
    return '''
        SELECT 
            Factory, EngName, KorName, IdnName, Sp1Name, Sp2Name, SortNo, CreateDate
        FROM rtf_factory
        ORDER BY Factory
    '''

def build_equipment_extract_sql() -> str:
    """Build SQL query for Banbury HMI equipment data extraction"""
    return '''
        SELECT 
            Equipment, EngName, KorName, IdnName, Sp1Name, Sp2Name,
            Factory, SortNo, CreateDate
        FROM rtf_equipment
        ORDER BY Equipment
    '''

def build_common_extract_sql() -> str:
    """Build SQL query for Banbury HMI common data extraction"""
    return '''
        SELECT 
            CodeID, CodeKey, CodeName, CodeType, SortNo, CreateDate
        FROM rtf_common
        ORDER BY CodeID, CodeKey
    '''

def build_maintenance_extract_sql() -> str:
    """Build SQL query for Banbury HMI maintenance data extraction"""
    return '''
        SELECT 
            Maintenance, EngName, KorName, IdnName, Sp1Name, Sp2Name,
            DescName, Level, Cycle, Min, Max, Active, POS_X, POS_Y,
            SortNo, BaseDate, CreateDate
        FROM rtf_maintenance
        ORDER BY Maintenance
    '''

def build_alarm_extract_sql() -> str:
    """Build SQL query for Banbury HMI alarm data extraction"""
    return '''
        SELECT 
            PID, Alarm, EngName, KorName, IdnName, Sp1Name, Sp2Name,
            Level, UCL, UWL, CL, LWL, LCL, Active, Period, SortNo,
            CreateDate, USL, LSL
        FROM rtf_alarm
        ORDER BY PID, Alarm
    '''

def build_screen_extract_sql() -> str:
    """Build SQL query for Banbury HMI screen data extraction"""
    return '''
        SELECT 
            CodeID, EngName, KorName, IdnName, Sp1Name, Sp2Name,
            CodeKey, SortNo, CreateDate
        FROM rtf_screen
        ORDER BY CodeID, EngName
    '''

def build_sensorbridge_extract_sql() -> str:
    """Build SQL query for Banbury HMI sensorbridge data extraction"""
    return '''
        SELECT 
            SensorBridge, SerialNo, SensorHub, SortNo, CreateDate
        FROM rtf_sensorbridge
        ORDER BY SensorBridge
    '''

def build_sensorgroup_extract_sql() -> str:
    """Build SQL query for Banbury HMI sensorgroup data extraction"""
    return '''
        SELECT 
            SensorGroup, EngName, KorName, IdnName, Sp1Name, Sp2Name,
            SortNo, CreateDate
        FROM rtf_sensorgroup
        ORDER BY SensorGroup
    '''

def build_sensorhub_extract_sql() -> str:
    """Build SQL query for Banbury HMI sensorhub data extraction"""
    return '''
        SELECT 
            SensorHub, SerialNo, Factory, Equipment, SortNo, CreateDate
        FROM rtf_sensorhub
        ORDER BY SensorHub
    '''

def build_sensortype_extract_sql() -> str:
    """Build SQL query for Banbury HMI sensortype data extraction"""
    return '''
        SELECT 
            SensorType, EngName, KorName, IdnName, Sp1Name, Sp2Name,
            SortNo, CreateDate
        FROM rtf_sensortype
        ORDER BY SensorType
    '''

def build_sensor_extract_sql(equipment_id: str) -> str:
    """Build SQL query for Banbury HMI sensor data extraction"""
    equipment_value = 3001 if str(equipment_id) == '1' else 3003
    return f'''
        SELECT 
            PID, EngName, KorName, IdnName, Sp1Name, Sp2Name,
            SensorBridge, SensorType, SensorGroup, COALESCE(Factory, 1) AS Factory, {equipment_value} AS Equipment,
            DaqMac, IPAddr, Level, Active, POS_X, POS_Y, SortNo,
            Unit, Max, Min, CreateDate
        FROM rtf_sensor
        ORDER BY PID
    '''

def load_factory_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load factory data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_factory_column_names()
    conflict_columns = ["factory"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, "os_banb_hmi_factory", insert_data, columns, conflict_columns)
    logging.info(f"✅ Factory 데이터 {len(data)} rows upserted (factory 기준).")

def load_equipment_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load equipment data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_equipment_column_names()
    conflict_columns = ["equipment"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, "os_banb_hmi_equipment", insert_data, columns, conflict_columns)
    logging.info(f"✅ Equipment 데이터 {len(data)} rows upserted (equipment 기준).")

def load_common_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load common data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_common_column_names()
    conflict_columns = ["code_id", "code_key"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, "os_banb_hmi_common", insert_data, columns, conflict_columns)
    logging.info(f"✅ Common 데이터 {len(data)} rows upserted (code_id, code_key 기준).")

def load_maintenance_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load maintenance data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_maintenance_column_names()
    conflict_columns = ["maintenance"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, "os_banb_hmi_maintenance", insert_data, columns, conflict_columns)
    logging.info(f"✅ Maintenance 데이터 {len(data)} rows upserted (maintenance 기준).")

def load_alarm_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load alarm data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_alarm_column_names()
    conflict_columns = ["pid", "alarm"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, "os_banb_hmi_alarm", insert_data, columns, conflict_columns)
    logging.info(f"✅ Alarm 데이터 {len(data)} rows upserted (pid, alarm 기준).")

def load_screen_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load screen data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_screen_column_names()
    conflict_columns = ["code_id", "eng_name"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, "os_banb_hmi_screen", insert_data, columns, conflict_columns)
    logging.info(f"✅ Screen 데이터 {len(data)} rows upserted (code_id, eng_name 기준).")

def load_sensorbridge_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load sensorbridge data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_sensorbridge_column_names()
    conflict_columns = ["sensor_bridge"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, "os_banb_hmi_sensorbridge", insert_data, columns, conflict_columns)
    logging.info(f"✅ SensorBridge 데이터 {len(data)} rows upserted (sensor_bridge 기준).")

def load_sensorgroup_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load sensorgroup data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_sensorgroup_column_names()
    conflict_columns = ["sensor_group"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, "os_banb_hmi_sensorgroup", insert_data, columns, conflict_columns)
    logging.info(f"✅ SensorGroup 데이터 {len(data)} rows upserted (sensor_group 기준).")

def load_sensorhub_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load sensorhub data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_sensorhub_column_names()
    conflict_columns = ["sensor_hub"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, "os_banb_hmi_sensorhub", insert_data, columns, conflict_columns)
    logging.info(f"✅ SensorHub 데이터 {len(data)} rows upserted (sensor_hub 기준).")

def load_sensortype_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load sensortype data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_sensortype_column_names()
    conflict_columns = ["sensor_type"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, "os_banb_hmi_sensortype", insert_data, columns, conflict_columns)
    logging.info(f"✅ SensorType 데이터 {len(data)} rows upserted (sensor_type 기준).")

def load_sensor_data(pg: PostgresHelper, data: list, extract_time: datetime) -> None:
    """Load sensor data into PostgreSQL database with upsert"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_sensor_column_names()
    conflict_columns = ["factory", "equipment", "pid"]  # Primary Key 기준으로 upsert
    
    pg.insert_data(SCHEMA_NAME, "os_banb_hmi_sensor", insert_data, columns, conflict_columns)
    logging.info(f"✅ Sensor 데이터 {len(data)} rows upserted (factory, equipment, pid 기준).")

def extract_factory_data(mysql: MySQLHelper) -> list:
    """Extract factory data from MySQL source"""
    sql = build_factory_extract_sql()
    logging.info(f"Factory 실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, "factory_extract_task", None)
    if data is None:
        data = []
    logging.info(f"Banbury HMI Factory 마스터 데이터 추출 row 수: {len(data)}")
    
    return data

def extract_equipment_data(mysql: MySQLHelper) -> list:
    """Extract equipment data from MySQL source"""
    sql = build_equipment_extract_sql()
    logging.info(f"Equipment 실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, "equipment_extract_task", None)
    if data is None:
        data = []
    logging.info(f"Banbury HMI Equipment 마스터 데이터 추출 row 수: {len(data)}")
    
    return data

def extract_common_data(mysql: MySQLHelper) -> list:
    """Extract common data from MySQL source"""
    sql = build_common_extract_sql()
    logging.info(f"Common 실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, "common_extract_task", None)
    if data is None:
        data = []
    logging.info(f"Banbury HMI Common 마스터 데이터 추출 row 수: {len(data)}")
    
    return data

def extract_maintenance_data(mysql: MySQLHelper) -> list:
    """Extract maintenance data from MySQL source"""
    sql = build_maintenance_extract_sql()
    logging.info(f"Maintenance 실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, "maintenance_extract_task", None)
    if data is None:
        data = []
    logging.info(f"Banbury HMI Maintenance 마스터 데이터 추출 row 수: {len(data)}")
    
    return data

def extract_alarm_data(mysql: MySQLHelper) -> list:
    """Extract alarm data from MySQL source"""
    sql = build_alarm_extract_sql()
    logging.info(f"Alarm 실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, "alarm_extract_task", None)
    if data is None:
        data = []
    logging.info(f"Banbury HMI Alarm 마스터 데이터 추출 row 수: {len(data)}")
    
    return data

def extract_screen_data(mysql: MySQLHelper) -> list:
    """Extract screen data from MySQL source"""
    sql = build_screen_extract_sql()
    logging.info(f"Screen 실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, "screen_extract_task", None)
    if data is None:
        data = []
    logging.info(f"Banbury HMI Screen 마스터 데이터 추출 row 수: {len(data)}")
    
    return data

def extract_sensorbridge_data(mysql: MySQLHelper) -> list:
    """Extract sensorbridge data from MySQL source"""
    sql = build_sensorbridge_extract_sql()
    logging.info(f"SensorBridge 실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, "sensorbridge_extract_task", None)
    if data is None:
        data = []
    logging.info(f"Banbury HMI SensorBridge 마스터 데이터 추출 row 수: {len(data)}")
    
    return data

def extract_sensorgroup_data(mysql: MySQLHelper) -> list:
    """Extract sensorgroup data from MySQL source"""
    sql = build_sensorgroup_extract_sql()
    logging.info(f"SensorGroup 실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, "sensorgroup_extract_task", None)
    if data is None:
        data = []
    logging.info(f"Banbury HMI SensorGroup 마스터 데이터 추출 row 수: {len(data)}")
    
    return data

def extract_sensorhub_data(mysql: MySQLHelper) -> list:
    """Extract sensorhub data from MySQL source"""
    sql = build_sensorhub_extract_sql()
    logging.info(f"SensorHub 실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, "sensorhub_extract_task", None)
    if data is None:
        data = []
    logging.info(f"Banbury HMI SensorHub 마스터 데이터 추출 row 수: {len(data)}")
    
    return data

def extract_sensortype_data(mysql: MySQLHelper) -> list:
    """Extract sensortype data from MySQL source"""
    sql = build_sensortype_extract_sql()
    logging.info(f"SensorType 실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, "sensortype_extract_task", None)
    if data is None:
        data = []
    logging.info(f"Banbury HMI SensorType 마스터 데이터 추출 row 수: {len(data)}")
    
    return data

def extract_sensor_data(mysql: MySQLHelper, equipment_id: str) -> list:
    """Extract sensor data from MySQL source"""
    sql = build_sensor_extract_sql(equipment_id)
    logging.info(f"Sensor 실행 쿼리: {sql}")
    
    data = mysql.execute_query(sql, "sensor_extract_task", None)
    if data is None:
        data = []
    logging.info(f"Banbury HMI Sensor 마스터 데이터 추출 row 수: {len(data)}")
    
    return data

def factory_etl_task(equipment_id: str, conn_id: str, equipment_name: str) -> dict:
    """Banbury HMI Factory 마스터 데이터 ETL 태스크"""
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # Extract factory data
        data = extract_factory_data(mysql)
        
        if data:
            extract_time = datetime.utcnow()
            load_factory_data(pg, data, extract_time)
            
            return {
                "status": "success",
                "row_count": len(data),
                "equipment": equipment_id,
                "message": f"Banbury HMI Factory 마스터 데이터 ({equipment_name}) {len(data)} rows 처리 완료"
            }
        else:
            return {
                "status": "success",
                "row_count": 0,
                "equipment": equipment_id,
                "message": f"Factory 처리할 데이터가 없음 ({equipment_name})"
            }
            
    except Exception as e:
        logging.error(f"❌ Banbury HMI Factory 마스터 ETL 실패 ({equipment_name}): {str(e)}")
        return {
            "status": "failed",
            "equipment": equipment_id,
            "error": str(e)
        }

def equipment_etl_task(equipment_id: str, conn_id: str, equipment_name: str) -> dict:
    """Banbury HMI Equipment 마스터 데이터 ETL 태스크"""
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # Extract equipment data
        data = extract_equipment_data(mysql)
        
        if data:
            extract_time = datetime.utcnow()
            load_equipment_data(pg, data, extract_time)
            
            return {
                "status": "success",
                "row_count": len(data),
                "equipment": equipment_id,
                "message": f"Banbury HMI Equipment 마스터 데이터 ({equipment_name}) {len(data)} rows 처리 완료"
            }
        else:
            return {
                "status": "success",
                "row_count": 0,
                "equipment": equipment_id,
                "message": f"Equipment 처리할 데이터가 없음 ({equipment_name})"
            }
            
    except Exception as e:
        logging.error(f"❌ Banbury HMI Equipment 마스터 ETL 실패 ({equipment_name}): {str(e)}")
        return {
            "status": "failed",
            "equipment": equipment_id,
            "error": str(e)
        }

def common_etl_task(equipment_id: str, conn_id: str, equipment_name: str) -> dict:
    """Banbury HMI Common 마스터 데이터 ETL 태스크"""
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # Extract common data
        data = extract_common_data(mysql)
        
        if data:
            extract_time = datetime.utcnow()
            load_common_data(pg, data, extract_time)
            
            return {
                "status": "success",
                "row_count": len(data),
                "equipment": equipment_id,
                "message": f"Banbury HMI Common 마스터 데이터 ({equipment_name}) {len(data)} rows 처리 완료"
            }
        else:
            return {
                "status": "success",
                "row_count": 0,
                "equipment": equipment_id,
                "message": f"Common 처리할 데이터가 없음 ({equipment_name})"
            }
            
    except Exception as e:
        logging.error(f"❌ Banbury HMI Common 마스터 ETL 실패 ({equipment_name}): {str(e)}")
        return {
            "status": "failed",
            "equipment": equipment_id,
            "error": str(e)
        }

def maintenance_etl_task(equipment_id: str, conn_id: str, equipment_name: str) -> dict:
    """Banbury HMI Maintenance 마스터 데이터 ETL 태스크"""
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # Extract maintenance data
        data = extract_maintenance_data(mysql)
        
        if data:
            extract_time = datetime.utcnow()
            load_maintenance_data(pg, data, extract_time)
            
            return {
                "status": "success",
                "row_count": len(data),
                "equipment": equipment_id,
                "message": f"Banbury HMI Maintenance 마스터 데이터 ({equipment_name}) {len(data)} rows 처리 완료"
            }
        else:
            return {
                "status": "success",
                "row_count": 0,
                "equipment": equipment_id,
                "message": f"Maintenance 처리할 데이터가 없음 ({equipment_name})"
            }
            
    except Exception as e:
        logging.error(f"❌ Banbury HMI Maintenance 마스터 ETL 실패 ({equipment_name}): {str(e)}")
        return {
            "status": "failed",
            "equipment": equipment_id,
            "error": str(e)
        }

def alarm_etl_task(equipment_id: str, conn_id: str, equipment_name: str) -> dict:
    """Banbury HMI Alarm 마스터 데이터 ETL 태스크"""
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # Extract alarm data
        data = extract_alarm_data(mysql)
        
        if data:
            extract_time = datetime.utcnow()
            load_alarm_data(pg, data, extract_time)
            
            return {
                "status": "success",
                "row_count": len(data),
                "equipment": equipment_id,
                "message": f"Banbury HMI Alarm 마스터 데이터 ({equipment_name}) {len(data)} rows 처리 완료"
            }
        else:
            return {
                "status": "success",
                "row_count": 0,
                "equipment": equipment_id,
                "message": f"Alarm 처리할 데이터가 없음 ({equipment_name})"
            }
            
    except Exception as e:
        logging.error(f"❌ Banbury HMI Alarm 마스터 ETL 실패 ({equipment_name}): {str(e)}")
        return {
            "status": "failed",
            "equipment": equipment_id,
            "error": str(e)
        }

def screen_etl_task(equipment_id: str, conn_id: str, equipment_name: str) -> dict:
    """Banbury HMI Screen 마스터 데이터 ETL 태스크"""
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # Extract screen data
        data = extract_screen_data(mysql)
        
        if data:
            extract_time = datetime.utcnow()
            load_screen_data(pg, data, extract_time)
            
            return {
                "status": "success",
                "row_count": len(data),
                "equipment": equipment_id,
                "message": f"Banbury HMI Screen 마스터 데이터 ({equipment_name}) {len(data)} rows 처리 완료"
            }
        else:
            return {
                "status": "success",
                "row_count": 0,
                "equipment": equipment_id,
                "message": f"Screen 처리할 데이터가 없음 ({equipment_name})"
            }
            
    except Exception as e:
        logging.error(f"❌ Banbury HMI Screen 마스터 ETL 실패 ({equipment_name}): {str(e)}")
        return {
            "status": "failed",
            "equipment": equipment_id,
            "error": str(e)
        }

def sensorbridge_etl_task(equipment_id: str, conn_id: str, equipment_name: str) -> dict:
    """Banbury HMI SensorBridge 마스터 데이터 ETL 태스크"""
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # Extract sensorbridge data
        data = extract_sensorbridge_data(mysql)
        
        if data:
            extract_time = datetime.utcnow()
            load_sensorbridge_data(pg, data, extract_time)
            
            return {
                "status": "success",
                "row_count": len(data),
                "equipment": equipment_id,
                "message": f"Banbury HMI SensorBridge 마스터 데이터 ({equipment_name}) {len(data)} rows 처리 완료"
            }
        else:
            return {
                "status": "success",
                "row_count": 0,
                "equipment": equipment_id,
                "message": f"SensorBridge 처리할 데이터가 없음 ({equipment_name})"
            }
            
    except Exception as e:
        logging.error(f"❌ Banbury HMI SensorBridge 마스터 ETL 실패 ({equipment_name}): {str(e)}")
        return {
            "status": "failed",
            "equipment": equipment_id,
            "error": str(e)
        }

def sensorgroup_etl_task(equipment_id: str, conn_id: str, equipment_name: str) -> dict:
    """Banbury HMI SensorGroup 마스터 데이터 ETL 태스크"""
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # Extract sensorgroup data
        data = extract_sensorgroup_data(mysql)
        
        if data:
            extract_time = datetime.utcnow()
            load_sensorgroup_data(pg, data, extract_time)
            
            return {
                "status": "success",
                "row_count": len(data),
                "equipment": equipment_id,
                "message": f"Banbury HMI SensorGroup 마스터 데이터 ({equipment_name}) {len(data)} rows 처리 완료"
            }
        else:
            return {
                "status": "success",
                "row_count": 0,
                "equipment": equipment_id,
                "message": f"SensorGroup 처리할 데이터가 없음 ({equipment_name})"
            }
            
    except Exception as e:
        logging.error(f"❌ Banbury HMI SensorGroup 마스터 ETL 실패 ({equipment_name}): {str(e)}")
        return {
            "status": "failed",
            "equipment": equipment_id,
            "error": str(e)
        }

def sensorhub_etl_task(equipment_id: str, conn_id: str, equipment_name: str) -> dict:
    """Banbury HMI SensorHub 마스터 데이터 ETL 태스크"""
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # Extract sensorhub data
        data = extract_sensorhub_data(mysql)
        
        if data:
            extract_time = datetime.utcnow()
            load_sensorhub_data(pg, data, extract_time)
            
            return {
                "status": "success",
                "row_count": len(data),
                "equipment": equipment_id,
                "message": f"Banbury HMI SensorHub 마스터 데이터 ({equipment_name}) {len(data)} rows 처리 완료"
            }
        else:
            return {
                "status": "success",
                "row_count": 0,
                "equipment": equipment_id,
                "message": f"SensorHub 처리할 데이터가 없음 ({equipment_name})"
            }
            
    except Exception as e:
        logging.error(f"❌ Banbury HMI SensorHub 마스터 ETL 실패 ({equipment_name}): {str(e)}")
        return {
            "status": "failed",
            "equipment": equipment_id,
            "error": str(e)
        }

def sensortype_etl_task(equipment_id: str, conn_id: str, equipment_name: str) -> dict:
    """Banbury HMI SensorType 마스터 데이터 ETL 태스크"""
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # Extract sensortype data
        data = extract_sensortype_data(mysql)
        
        if data:
            extract_time = datetime.utcnow()
            load_sensortype_data(pg, data, extract_time)
            
            return {
                "status": "success",
                "row_count": len(data),
                "equipment": equipment_id,
                "message": f"Banbury HMI SensorType 마스터 데이터 ({equipment_name}) {len(data)} rows 처리 완료"
            }
        else:
            return {
                "status": "success",
                "row_count": 0,
                "equipment": equipment_id,
                "message": f"SensorType 처리할 데이터가 없음 ({equipment_name})"
            }
            
    except Exception as e:
        logging.error(f"❌ Banbury HMI SensorType 마스터 ETL 실패 ({equipment_name}): {str(e)}")
        return {
            "status": "failed",
            "equipment": equipment_id,
            "error": str(e)
        }

def sensor_etl_task(equipment_id: str, conn_id: str, equipment_name: str) -> dict:
    """Banbury HMI Sensor 마스터 데이터 ETL 태스크"""
    mysql = MySQLHelper(conn_id=conn_id)
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    try:
        # Extract sensor data
        data = extract_sensor_data(mysql, equipment_id)
        
        if data:
            extract_time = datetime.utcnow()
            load_sensor_data(pg, data, extract_time)
            
            return {
                "status": "success",
                "row_count": len(data),
                "equipment": equipment_id,
                "message": f"Banbury HMI Sensor 마스터 데이터 ({equipment_name}) {len(data)} rows 처리 완료"
            }
        else:
            return {
                "status": "success",
                "row_count": 0,
                "equipment": equipment_id,
                "message": f"Sensor 처리할 데이터가 없음 ({equipment_name})"
            }
            
    except Exception as e:
        logging.error(f"❌ Banbury HMI Sensor 마스터 ETL 실패 ({equipment_name}): {str(e)}")
        return {
            "status": "failed",
            "equipment": equipment_id,
            "error": str(e)
        }

def create_etl_task_wrapper(etl_function, equipment_id: str, conn_id: str, equipment_name: str):
    """ETL 태스크를 위한 래퍼 함수 생성"""
    def task_wrapper(**kwargs):
        return etl_function(equipment_id, conn_id, equipment_name)
    return task_wrapper

# ────────────────────────────────────────────────────────────────
# 3️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="os_banb_hmi_master_etl",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once", 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "OS","Banbury", "HMI", "master", "bronze layer", "daily"]
) as dag:
    
    # 동적으로 태스크 생성
    tasks = []
    
    # ETL 태스크 함수와 테이블명 매핑
    etl_tasks = [
        ("factory", factory_etl_task),
        ("equipment", equipment_etl_task),
        ("common", common_etl_task),
        ("maintenance", maintenance_etl_task),
        ("alarm", alarm_etl_task),
        ("screen", screen_etl_task),
        ("sensorbridge", sensorbridge_etl_task),
        ("sensorgroup", sensorgroup_etl_task),
        ("sensorhub", sensorhub_etl_task),
        ("sensortype", sensortype_etl_task),
        ("sensor", sensor_etl_task)
    ]
    
    for equipment in EQUIPMENTS:
        for table_name, etl_function in etl_tasks:
            # 특정 설비(1호기)에서 센서 관련 태스크 제외
            if equipment['id'] == '1' and table_name in {"sensorbridge", "sensorgroup", "sensorhub", "sensortype"}:
                continue
            # 각 테이블별 ETL 태스크 생성
            task = PythonOperator(
                task_id=f"{table_name}_etl_{equipment['id']}",
                python_callable=create_etl_task_wrapper(
                    etl_function, 
                    equipment['id'], 
                    equipment['conn_id'], 
                    equipment['name']
                ),
                provide_context=True,
            )
            tasks.append(task)
    
    # 모든 태스크가 독립적으로 병렬 실행
    tasks
