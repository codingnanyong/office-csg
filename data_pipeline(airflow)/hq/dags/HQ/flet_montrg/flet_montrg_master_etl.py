from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
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
postgres_helper = PostgresHelper(conn_id='pg_fdw_v2_hq')

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
        
        # Check PostgreSQL connection (pg_fdw_v2_hq)
        logging.info("🔍 Checking PostgreSQL connection (pg_fdw_v2_hq)...")
        postgres_test_query = "SELECT 1 as test"
        postgres_result = postgres_helper.execute_query(
            sql=postgres_test_query,
            task_id='postgres_connection_test',
            xcom_key=None,
            **context
        )
        if postgres_result:
            logging.info("✅ PostgreSQL connection (pg_fdw_v2_hq) successful")
        else:
            raise Exception("PostgreSQL connection test failed")
        
        # Check if required tables exist in MSSQL
        logging.info("🔍 Checking MSSQL source tables...")
        if not mssql_helper.check_table('montrg.dbo', 'sensor'):
            raise Exception("Source table 'montrg.dbo.sensor' does not exist")
        
        if not mssql_helper.check_table('montrg.dbo', 'location'):
            raise Exception("Source table 'montrg.dbo.location' does not exist")
        
        logging.info("✅ All MSSQL source tables exist")
        
        # Check if target tables exist in PostgreSQL
        logging.info("🔍 Checking PostgreSQL target tables...")
        if not postgres_helper.check_table('flet_montrg', 'sensors'):
            raise Exception("Target table 'flet_montrg.sensors' does not exist")
        
        if not postgres_helper.check_table('flet_montrg', 'locations'):
            raise Exception("Target table 'flet_montrg.locations' does not exist")
        
        logging.info("✅ All PostgreSQL target tables exist")
        
        logging.info("🎉 All connection and table checks passed!")
        
    except Exception as e:
        logging.error(f"❌ Connection check failed: {str(e)}")
        raise

# =============================================================================
# 2. DATA EXTRACTION FUNCTIONS
# =============================================================================

def extract_sensors(**context):
    """Extract sensor data from MSSQL"""
    query = """
        SELECT 
            s.sensor_id, 
            RIGHT(l.sensor_id, 4) as loc_id, 
            s.name, 
            s.upd_dt
        FROM dbo.sensor s 
        JOIN dbo.location l ON s.sensor_id = l.sensor_id
    """
    
    return mssql_helper.execute_query(
        sql=query,
        task_id='extract_sensors',
        xcom_key='sensor_data',
        **context
    )

def extract_locations(**context):
    """Extract location data from MSSQL"""
    query = """
        SELECT 
            RIGHT(l.sensor_id, 4) as loc_id,
            'CSI' as plant,
            CASE WHEN l.location1 = '장림공장' THEN 'JangNim' ELSE 'SinPyeong' END as factory,
            CASE 
                WHEN l.location1 = '장림공장' THEN 'Bottom'
                WHEN CHARINDEX('(', l.location1) > 0 
                    THEN LTRIM(RTRIM(LEFT(l.location1, CHARINDEX('(', l.location1) - 1)))
                ELSE l.location1
            END AS building, 
            CASE 
                WHEN CHARINDEX('(', l.location1) > 0 AND CHARINDEX('층', l.location1) > 0
                THEN SUBSTRING(
                    l.location1,
                    CHARINDEX('(', l.location1) + 1,
                    CHARINDEX('층', l.location1) - CHARINDEX('(', l.location1) - 1
                )
                ELSE 1
            END AS floor,
            l.location2 as area,
            l.location1 +'-'+l.location2 as location_name,
            l.upd_dt as upd_dt,
            1 AS is_active
        FROM dbo.location l 
        JOIN dbo.sensor s ON l.sensor_id = s.sensor_id
    """
    
    return mssql_helper.execute_query(
        sql=query,
        task_id='extract_locations',
        xcom_key='location_data',
        **context
    )

# =============================================================================
# 3. DATA LOADING FUNCTIONS
# =============================================================================

def load_locations(**context):
    """Load location data to PostgreSQL using UPSERT"""
    # Get location data
    location_data = context['task_instance'].xcom_pull(task_ids='extract_locations')
    
    if not location_data:
        logging.warning("No location data to load")
        return
    
    # Convert list of lists to list of tuples for PostgreSQL
    # Also convert is_active (last column) from integer to boolean
    location_data_tuples = []
    for row in location_data:
        converted_row = list(row)
        # Convert is_active (last column) from 1/0 to True/False
        converted_row[-1] = bool(converted_row[-1])
        location_data_tuples.append(tuple(converted_row))
    
    # Use PostgreSQL UPSERT function
    postgres_helper.upsert_data(
        schema_name='flet_montrg',
        table_name='locations',
        data=location_data_tuples,
        conflict_columns=['loc_id'],
        update_columns=['plant', 'factory', 'building', 'floor', 'area', 'location_name', 'upd_dt', 'is_active']
    )

def load_sensors(**context):
    """Load sensor data to PostgreSQL using UPSERT"""
    # Get sensor data
    sensor_data = context['task_instance'].xcom_pull(task_ids='extract_sensors')
    
    if not sensor_data:
        logging.warning("No sensor data to load")
        return
    
    # Convert list of lists to list of tuples for PostgreSQL
    sensor_data_tuples = [tuple(row) for row in sensor_data]
    
    # Use PostgreSQL UPSERT function
    postgres_helper.upsert_data(
        schema_name='flet_montrg',
        table_name='sensors',
        data=sensor_data_tuples,
        conflict_columns=['sensor_id'],
        update_columns=['loc_id', 'name', 'upd_dt']
    )

# =============================================================================
# 5. DATA VALIDATION FUNCTIONS
# =============================================================================

def validate_data_quality(**context):
    """Validate data quality after transformation"""
    # Check sensor count
    sensor_query = "SELECT COUNT(*) FROM flet_montrg.sensors"
    sensor_count = postgres_helper.execute_query(
        sql=sensor_query,
        task_id='validate_sensor_count',
        xcom_key=None,
        **context
    )
    
    # Check location count
    location_query = "SELECT COUNT(*) FROM flet_montrg.locations"
    location_count = postgres_helper.execute_query(
        sql=location_query,
        task_id='validate_location_count',
        xcom_key=None,
        **context
    )
    
    # Check for orphaned sensors
    orphaned_query = """
        SELECT COUNT(*) FROM flet_montrg.sensors s
        LEFT JOIN flet_montrg.locations l ON s.loc_id = l.loc_id
        WHERE l.loc_id IS NULL
    """
    orphaned_sensors = postgres_helper.execute_query(
        sql=orphaned_query,
        task_id='validate_orphaned_sensors',
        xcom_key=None,
        **context
    )
    
    logging.info(f"Data quality check results:")
    logging.info(f"- Sensors: {sensor_count[0][0] if sensor_count else 0}")
    logging.info(f"- Locations: {location_count[0][0] if location_count else 0}")
    logging.info(f"- Orphaned sensors: {orphaned_sensors[0][0] if orphaned_sensors else 0}")
    
    if orphaned_sensors and orphaned_sensors[0][0] > 0:
        logging.warning(f"Found {orphaned_sensors[0][0]} sensors without valid location")

# =============================================================================
# 6. TASK DEFINITIONS
# =============================================================================

# DAG definition
dag = DAG(
    'flet_montrg_master_etl',
    default_args=default_args,
    description='Daily ETL for Flet monitoring master data (sensor, location)',
    schedule_interval='0 15 * * *',  # Daily at KST midnight (00:00)
    catchup=False,
    tags=['HQ', 'IoT','Temperature','flet_montrg']
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

# Data extraction tasks
extract_sensors_task = PythonOperator(
    task_id='extract_sensors',
    python_callable=extract_sensors,
    dag=dag
)

extract_locations_task = PythonOperator(
    task_id='extract_locations',
    python_callable=extract_locations,
    dag=dag
)

# Data loading tasks
load_locations_task = PythonOperator(
    task_id='load_locations',
    python_callable=load_locations,
    dag=dag
)

load_sensors_task = PythonOperator(
    task_id='load_sensors',
    python_callable=load_sensors,
    dag=dag
)

# Data validation task
validate_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)

# =============================================================================
# 7. TASK DEPENDENCIES
# =============================================================================

# Start with connection check
start_task >> check_connections_task

# Parallel data extraction
check_connections_task >> [extract_sensors_task, extract_locations_task]

# Data loading - locations first, then sensors (due to foreign key dependency)
extract_locations_task >> load_locations_task
extract_sensors_task >> load_sensors_task
load_locations_task >> load_sensors_task  # sensors depends on locations

# Final validation and end
load_sensors_task >> validate_task
validate_task >> end_task
