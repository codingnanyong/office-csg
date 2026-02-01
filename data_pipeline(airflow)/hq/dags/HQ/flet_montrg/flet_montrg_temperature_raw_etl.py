from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from plugins.hooks.mssql_hook import MSSQLHelper
from plugins.hooks.postgres_hook import PostgresHelper
import logging

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize helpers
mssql_helper = MSSQLHelper(conn_id='ms_flet_montrg')
postgres_helper = PostgresHelper(conn_id='pg_fdw_v1_iot')

# =============================================================================
# 1. CONNECTION CHECK FUNCTIONS
# =============================================================================

def check_connections(**context):
    """Check database connections before starting ETL process"""
    try:
        # Check MSSQL connection (ms_flet_montrg)
        logging.info("🔍 Checking MSSQL connection (ms_flet_montrg)...")
        mssql_test_query = "SELECT 1 as test"
        mssql_result = mssql_helper.execute_query(
            sql=mssql_test_query,
            task_id='mssql_connection_test',
            xcom_key=None,
            **context
        )
        if mssql_result:
            logging.info("✅ MSSQL connection (ms_flet_montrg) successful")
        else:
            raise Exception("MSSQL connection test failed")
        
        # Check PostgreSQL connection (pg_fdw_v1_iot)
        logging.info("🔍 Checking PostgreSQL connection (pg_fdw_v1_iot)...")
        postgres_test_query = "SELECT 1 as test"
        postgres_result = postgres_helper.execute_query(
            sql=postgres_test_query,
            task_id='postgres_connection_test',
            xcom_key=None,
            **context
        )
        if postgres_result:
            logging.info("✅ PostgreSQL connection (pg_fdw_v1_iot) successful")
        else:
            raise Exception("PostgreSQL connection test failed")
        
        # Check if required tables exist in MSSQL
        logging.info("🔍 Checking MSSQL source tables...")
        if not mssql_helper.check_table('montrg.dbo', 'temperature'):
            raise Exception("Source table 'montrg.dbo.temperature' does not exist")
        
        logging.info("✅ All MSSQL source tables exist")
        
        # Check if target tables exist in PostgreSQL
        logging.info("🔍 Checking PostgreSQL target tables...")
        if not postgres_helper.check_table('flet_montrg', 'temperature_raw'):
            raise Exception("Target table 'flet_montrg.temperature_raw' does not exist")
        
        logging.info("✅ All PostgreSQL target tables exist")
        
        logging.info("🎉 All connection and table checks passed!")
        
    except Exception as e:
        logging.error(f"❌ Connection check failed: {str(e)}")
        raise

# =============================================================================
# 2. DATA EXTRACTION FUNCTIONS
# =============================================================================

def extract_temperature_raw_data(**context):
    """Extract raw temperature data from MSSQL"""
    # Get last processing time from Airflow Variable (raw data specific)
    try:
        last_processing_time_str = Variable.get("flet_montrg_temperature_raw_current_time")
        logging.info(f"Variable value: {last_processing_time_str}, type: {type(last_processing_time_str)}")
        
        # Convert string to datetime object
        if isinstance(last_processing_time_str, str):
            start_time = datetime.fromisoformat(last_processing_time_str)
        else:
            start_time = last_processing_time_str
            
        logging.info(f"Last processing time: {start_time}, type: {type(start_time)}")
    except Exception as e:
        logging.error(f"Failed to get last processing time from Variable: {e}")
        # Fallback: start from 10 minutes ago
        current_time = datetime.now(timezone.utc) + timedelta(hours=9)  # UTC to KST
        start_time = current_time.replace(second=0, microsecond=0) - timedelta(minutes=10)
        logging.warning(f"Using fallback start time: {start_time}")
    
    # End time is current time rounded to minute
    current_time = datetime.now(timezone.utc) + timedelta(hours=9)  # UTC to KST
    end_time = current_time.replace(second=0, microsecond=0)  # 현재 분
    
    logging.info(f"📊 Processing from {start_time} to {end_time} (Duration: {(end_time - start_time).total_seconds() / 60:.1f} minutes)")
    
    query = """
        SELECT 
            ymd,
            hmsf,
            sensor_id,
            device_id,
            capture_dt,
            t1,
            t2,
            t3,
            t4,
            t5,
            t6,
            upload_yn,
            upload_dt,
            GETDATE() as extract_time
        FROM montrg.dbo.temperature
        WHERE capture_dt > %s AND capture_dt <= %s
        ORDER BY capture_dt, sensor_id
    """
    
    # Format query with parameters (MSSQL compatible datetime format)
    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
    formatted_query = query.replace('%s', f"'{start_time_str}'", 1).replace('%s', f"'{end_time_str}'", 1)
    
    # Execute query directly like in backfill
    try:
        with mssql_helper.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(formatted_query)
            result = cursor.fetchall()
            
            if not result:
                logging.warning("No temperature raw data found")
                # 데이터가 없어도 Variable은 업데이트해야 함 (다음 시간대로 진행)
                Variable.set("flet_montrg_temperature_raw_current_time", end_time.isoformat())
                logging.info(f"✅ Raw ETL Variable updated to next processing time (no data): {end_time}")
                return None
                
            logging.info(f"✅ Retrieved {len(result)} records from MSSQL")
            
            # Store in XCom
            context['task_instance'].xcom_push(key='temperature_raw_data', value=result)
            
            # Update Variable with next processing time (매분마다)
            Variable.set("flet_montrg_temperature_raw_current_time", end_time.isoformat())
            logging.info(f"✅ Raw ETL Variable updated to next processing time: {end_time}")
            
            return result
    except Exception as e:
        logging.error(f"❌ Query failed: {str(e)}")
        raise

# =============================================================================
# 3. DATA LOADING FUNCTIONS
# =============================================================================

def load_temperature_raw_data(**context):
    """Load raw temperature data to PostgreSQL using UPSERT"""
    # Get temperature data
    temperature_data = context['task_instance'].xcom_pull(task_ids='extract_temperature_raw_data')
    
    if not temperature_data:
        logging.warning("No temperature raw data to load")
        return
    
    # Convert list of lists to list of tuples for PostgreSQL
    temperature_data_tuples = [tuple(row) for row in temperature_data]
    
    # Use PostgreSQL UPSERT function
    postgres_helper.upsert_data(
        schema_name='flet_montrg',
        table_name='temperature_raw',
        data=temperature_data_tuples,
        conflict_columns=['capture_dt', 'ymd', 'hmsf', 'sensor_id'],
        update_columns=['device_id', 't1', 't2', 't3', 't4', 't5', 't6', 'upload_yn', 'upload_dt', 'extract_time']
    )
    
    logging.info("✅ Raw temperature data loaded successfully")

# =============================================================================
# 4. DATA VALIDATION FUNCTIONS
# =============================================================================

def validate_temperature_raw_data(**context):
    """Validate raw temperature data quality after transformation"""
    # Get processing time range from XCom
    processing_time_range = context['task_instance'].xcom_pull(
        task_ids='extract_temperature_raw_data',
        key='processing_time_range'
    )
    
    if not processing_time_range:
        logging.warning("No processing time range found in XCom")
        return
    
    start_time = processing_time_range['start_time']
    end_time = processing_time_range['end_time']
    
    # Check raw temperature data count for the time range
    temperature_query = f"""
        SELECT COUNT(*) FROM flet_montrg.temperature_raw 
        WHERE capture_dt >= '{start_time.isoformat()}' AND capture_dt < '{end_time.isoformat()}'
    """
    temperature_count = postgres_helper.execute_query(
        sql=temperature_query,
        task_id='validate_temperature_raw_count',
        xcom_key=None,
        **context
    )
    
    # Check for sensor coverage
    sensor_count_query = f"""
        SELECT COUNT(DISTINCT sensor_id) FROM flet_montrg.temperature_raw 
        WHERE capture_dt >= '{start_time.isoformat()}' AND capture_dt < '{end_time.isoformat()}'
    """
    sensor_count = postgres_helper.execute_query(
        sql=sensor_count_query,
        task_id='validate_sensor_count',
        xcom_key=None,
        **context
    )
    
    logging.info(f"Raw temperature data quality check results for {start_time} to {end_time}:")
    logging.info(f"- Total records: {temperature_count[0][0] if temperature_count else 0}")
    logging.info(f"- Sensors covered: {sensor_count[0][0] if sensor_count else 0}")
    
    # Update Variable with next processing time for raw ETL
    if processing_time_range:
        next_processing_time = processing_time_range['next_processing_time']
        Variable.set("flet_montrg_temperature_raw_current_time", next_processing_time.isoformat())
        logging.info(f"✅ Raw ETL Variable updated to next processing time: {next_processing_time}")
        
        # Store next processing time in XCom for ETL to use
        context['task_instance'].xcom_push(
            key='next_processing_time_for_etl',
            value=next_processing_time
        )
        logging.info(f"📝 Next processing time stored for ETL: {next_processing_time}")

# =============================================================================
# 5. TASK DEFINITIONS
# =============================================================================

# DAG definition
dag = DAG(
    'flet_montrg_temperature_raw_etl',
    default_args=default_args,
    description='ETL for Flet monitoring raw temperature data',
    schedule_interval='*/10 * * * *',  # 매 10분마다
    catchup=False,
    tags=['HQ', 'IoT', 'Temperature', 'flet_montrg', 'raw'],
    start_date=datetime(2025, 8, 21, 12, 0, 0, tzinfo=timezone(timedelta(hours=9)))  # KST timezone
)

# Start and End tasks
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

# Connection check task
check_connections_task = PythonOperator(
    task_id='check_connections',
    python_callable=check_connections,
    dag=dag
)

# Data extraction task
extract_temperature_raw_task = PythonOperator(
    task_id='extract_temperature_raw_data',
    python_callable=extract_temperature_raw_data,
    dag=dag
)

# Data loading task
load_temperature_raw_task = PythonOperator(
    task_id='load_temperature_raw_data',
    python_callable=load_temperature_raw_data,
    dag=dag
)

# Data validation task
validate_task = PythonOperator(
    task_id='validate_temperature_raw_data',
    python_callable=validate_temperature_raw_data,
    dag=dag
)

# =============================================================================
# 6. TASK DEPENDENCIES
# =============================================================================

# Start with connection check
start_task >> check_connections_task

# Data extraction
check_connections_task >> extract_temperature_raw_task

# Data loading
extract_temperature_raw_task >> load_temperature_raw_task

# Final validation and end
load_temperature_raw_task >> validate_task
validate_task >> end_task
