from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from plugins.hooks.postgres_hook import PostgresHelper
import logging

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
    'sla': timedelta(minutes=60),
}

# Initialize helpers
postgres_source_helper = PostgresHelper(conn_id='pg_fdw_v1_iot')  # Raw data source
postgres_target_helper = PostgresHelper(conn_id='pg_fdw_v2_hq')   # Aggregated data target

# =============================================================================
# 1. CONNECTION CHECK FUNCTIONS
# =============================================================================

def check_connections(**context):
    """Check database connections before starting ETL process"""
    try:
        # Check PostgreSQL source connection (pg_fdw_v1_iot) - Raw data
        logging.info("🔍 Checking PostgreSQL source connection (pg_fdw_v1_iot)...")
        postgres_source_test_query = "SELECT 1 as test"
        postgres_source_result = postgres_source_helper.execute_query(
            sql=postgres_source_test_query,
            task_id='postgres_source_connection_test',
            xcom_key=None,
            **context
        )
        if postgres_source_result:
            logging.info("✅ PostgreSQL source connection (pg_fdw_v1_iot) successful")
        else:
            raise Exception("PostgreSQL source connection test failed")
        
        # Check PostgreSQL target connection (pg_fdw_v2_hq) - Aggregated data
        logging.info("🔍 Checking PostgreSQL target connection (pg_fdw_v2_hq)...")
        postgres_target_test_query = "SELECT 1 as test"
        postgres_target_result = postgres_target_helper.execute_query(
            sql=postgres_target_test_query,
            task_id='postgres_target_connection_test',
            xcom_key=None,
            **context
        )
        if postgres_target_result:
            logging.info("✅ PostgreSQL target connection (pg_fdw_v2_hq) successful")
        else:
            raise Exception("PostgreSQL target connection test failed")
        
        # Check if required tables exist in PostgreSQL source
        logging.info("🔍 Checking PostgreSQL source tables...")
        if not postgres_source_helper.check_table('flet_montrg', 'temperature_raw'):
            raise Exception("Source table 'flet_montrg.temperature_raw' does not exist")
        
        logging.info("✅ All PostgreSQL source tables exist")
        
        # Check if target tables exist in PostgreSQL target
        logging.info("🔍 Checking PostgreSQL target tables...")
        if not postgres_target_helper.check_table('flet_montrg', 'temperature'):
            raise Exception("Target table 'flet_montrg.temperature' does not exist")
        
        logging.info("✅ All PostgreSQL target tables exist")
        
        logging.info("🎉 All connection and table checks passed!")
        
    except Exception as e:
        logging.error(f"❌ Connection check failed: {str(e)}")
        raise

# =============================================================================
# 2. DATA EXTRACTION FUNCTIONS
# =============================================================================

def extract_temperature_data(**context):
    """Extract and aggregate temperature data from PostgreSQL raw data (like backfill)"""
    # Get current processing time from Airflow Variable
    try:
        current_processing_time_str = Variable.get("flet_montrg_temperature_current_time")
        # Convert string to datetime object
        current_processing_time = datetime.fromisoformat(current_processing_time_str)
    except Exception as e:
        logging.error(f"Failed to get current processing time from Variable: {e}")
        # Fallback to current KST time
        current_processing_time = datetime.now(timezone.utc) + timedelta(hours=9)  # UTC to KST
        current_processing_time = current_processing_time.replace(minute=0, second=0, microsecond=0)  # Round to hour
    
    # Calculate time range for 1 hour (process current hour)
    start_time = current_processing_time
    end_time = current_processing_time + timedelta(hours=1)
    
    logging.info(f"📊 Processing 1 hour from {start_time} to {end_time}")
    
    query = """
        SELECT
            TO_CHAR(t.capture_dt, 'YYYYMMDD')                         AS ymd,
            EXTRACT(HOUR FROM t.capture_dt)::INTEGER                  AS hour,
            t.sensor_id,
            ROUND(MAX(CAST(t.t3 AS NUMERIC)), 2)                      AS pcv_temperature_max,
            ROUND(MIN(CAST(t.t3 AS NUMERIC)), 2)                      AS pcv_temperature_min,
            ROUND(AVG(CAST(t.t3 AS NUMERIC)), 2)                      AS pcv_temperature_avg,
            ROUND(MAX(CAST(t.t1 AS NUMERIC)), 2)                      AS temperature_max,
            ROUND(MIN(CAST(t.t1 AS NUMERIC)), 2)                      AS temperature_min,
            ROUND(AVG(CAST(t.t1 AS NUMERIC)), 2)                      AS temperature_avg,
            ROUND(MAX(CAST(t.t2 AS NUMERIC)), 2)                      AS humidity_max,
            ROUND(MIN(CAST(t.t2 AS NUMERIC)), 2)                      AS humidity_min,
            ROUND(AVG(CAST(t.t2 AS NUMERIC)), 2)                      AS humidity_avg,
            NOW()                                                     AS calc_dt
        FROM flet_montrg.temperature_raw AS t
        WHERE
            t.t1 IS NOT NULL AND t.t1 != ''
            AND t.t2 IS NOT NULL AND t.t2 != ''
            AND t.t3 IS NOT NULL AND t.t3 != ''
            AND t.sensor_id NOT IN ('TEMPIOT-A039', 'TEMPIOT-A040', 'TEMPIOT-TEST')
            AND t.capture_dt >= %s::timestamptz
            AND t.capture_dt <  %s::timestamptz
        GROUP BY
            TO_CHAR(t.capture_dt, 'YYYYMMDD'),
            EXTRACT(HOUR FROM t.capture_dt),
            t.sensor_id
        ORDER BY
            ymd, hour, t.sensor_id
    """
    
    # Format query with parameters (PostgreSQL compatible datetime format)
    start_time_str = start_time.isoformat()
    end_time_str = end_time.isoformat()
    formatted_query = query.replace('%s', f"'{start_time_str}'", 1).replace('%s', f"'{end_time_str}'", 1)
    
    # Execute query and load data directly like in backfill
    try:
        with postgres_source_helper.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(formatted_query)
            result = cursor.fetchall()
            
            if not result:
                logging.warning("No temperature data found")
                # 데이터가 없어도 Variable은 업데이트해야 함 (다음 시간대로 진행)
                Variable.set("flet_montrg_temperature_current_time", end_time.isoformat())
                logging.info(f"✅ Variable updated to next processing time (no data): {end_time}")
                return None
                
            logging.info(f"✅ Retrieved {len(result)} records from PostgreSQL")
            
            # Convert and load data directly (like in backfill)
            temperature_data_tuples = [tuple(row) for row in result]
            
            postgres_target_helper.upsert_data(
                schema_name='flet_montrg',
                table_name='temperature',
                data=temperature_data_tuples,
                conflict_columns=['ymd', 'hour', 'sensor_id'],
                update_columns=['pcv_temperature_max', 'pcv_temperature_min', 'pcv_temperature_avg',
                               'temperature_max', 'temperature_min', 'temperature_avg',
                               'humidity_max', 'humidity_min', 'humidity_avg', 'calc_dt']
            )
            
            logging.info(f"✅ Loaded {len(result)} records to PostgreSQL")
            
            # Update Variable with next processing time (like backfill)
            Variable.set("flet_montrg_temperature_current_time", end_time.isoformat())
            logging.info(f"✅ Variable updated to next processing time: {end_time}")
            
            return result
    except Exception as e:
        logging.error(f"❌ Query failed: {str(e)}")
        raise
# =============================================================================
# 3. DATA VALIDATION FUNCTIONS
# =============================================================================

def validate_temperature_data(**context):
    """Validate temperature data quality after transformation"""
    # Get current processing time from Variable
    try:
        current_processing_time_str = Variable.get("flet_montrg_temperature_current_time")
        current_processing_time = datetime.fromisoformat(current_processing_time_str)
        # Use the previous hour for validation (since Variable was already updated)
        start_time = current_processing_time - timedelta(hours=1)
    except Exception as e:
        logging.error(f"Failed to get current processing time from Variable: {e}")
        return
    
    target_date = start_time.strftime('%Y%m%d')
    target_hour = start_time.hour
    
    # Check temperature data count for the target date and hour
    temperature_query = f"SELECT COUNT(*) FROM flet_montrg.temperature WHERE ymd = '{target_date}' AND hour = {target_hour}"
    temperature_count = postgres_target_helper.execute_query(
        sql=temperature_query,
        task_id='validate_temperature_count',
        xcom_key=None,
        **context
    )
    
    # Check for data completeness for the specific hour
    hourly_count_query = f"""
        SELECT COUNT(*) FROM flet_montrg.temperature 
        WHERE ymd = '{target_date}' AND hour = {target_hour}
    """
    hourly_count = postgres_target_helper.execute_query(
        sql=hourly_count_query,
        task_id='validate_hourly_count',
        xcom_key=None,
        **context
    )
    
    # Check for sensor coverage for the specific hour
    sensor_count_query = f"""
        SELECT COUNT(DISTINCT sensor_id) FROM flet_montrg.temperature 
        WHERE ymd = '{target_date}' AND hour = {target_hour}
    """
    sensor_count = postgres_target_helper.execute_query(
        sql=sensor_count_query,
        task_id='validate_sensor_count',
        xcom_key=None,
        **context
    )
    
    logging.info(f"Temperature data quality check results for {target_date} {target_hour}:00:")
    logging.info(f"- Total records: {temperature_count[0][0] if temperature_count else 0}")
    logging.info(f"- Records for this hour: {hourly_count[0][0] if hourly_count else 0}")
    logging.info(f"- Sensors covered: {sensor_count[0][0] if sensor_count else 0}")
    
    if hourly_count and hourly_count[0][0] == 0:
        logging.warning(f"No data found for {target_date} {target_hour}:00")

# =============================================================================
# 5. TASK DEFINITIONS
# =============================================================================

# DAG definition
dag = DAG(
    'flet_montrg_temperature_etl',
    default_args=default_args,
    description='Hourly ETL for Flet monitoring temperature data',
    schedule_interval='@hourly',  # 매 정각 (0분)
    catchup=False,
    tags=['HQ', 'IoT', 'Temperature', 'flet_montrg'],
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
extract_temperature_task = PythonOperator(
    task_id='extract_temperature_data',
    python_callable=extract_temperature_data,
    dag=dag
)

# Data validation task
validate_task = PythonOperator(
    task_id='validate_temperature_data',
    python_callable=validate_temperature_data,
    dag=dag
)

# =============================================================================
# 6. TASK DEPENDENCIES
# =============================================================================

# Start with connection check
start_task >> check_connections_task

# Data extraction, loading, and variable update (all in extract_temperature_data)
check_connections_task >> extract_temperature_task

# Final validation and end
extract_temperature_task >> validate_task
validate_task >> end_task
