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
jj_extract_postgres_helper = PostgresHelper(conn_id='pg_material_jj')
jj2_extract_postgres_helper = PostgresHelper(conn_id='pg_material_jj2')
load_postgres_helper = PostgresHelper(conn_id='pg_jj_monitoring_dw')

# =============================================================================
# 1. CONNECTION CHECK FUNCTIONS
# =============================================================================

def check_connections(**context):
    """Check database connections before starting ETL process"""
    try:
        # Check PostgreSQL source connection (pg_material_jj)
        logging.info("🔍 Checking PostgreSQL source connection (pg_material_jj)...")
        source_test_query = "SELECT 1 as test"
        jj_result = jj_extract_postgres_helper.execute_query(
            sql=source_test_query,
            task_id='postgres_jj_source_connection_test',
            xcom_key=None,
            **context
        )
        if jj_result:
            logging.info("✅ PostgreSQL source connection (pg_material_jj) successful")
        else:
            raise Exception("PostgreSQL JJ source connection test failed")
        
        # Check PostgreSQL source connection (pg_material_jj2) - with exception handling
        logging.info("🔍 Checking PostgreSQL source connection (pg_material_jj2)...")
        try:
            jj2_result = jj2_extract_postgres_helper.execute_query(
                sql=source_test_query,
                task_id='postgres_jj2_source_connection_test',
                xcom_key=None,
                **context
            )
            if jj2_result:
                logging.info("✅ PostgreSQL source connection (pg_material_jj2) successful")
            else:
                logging.warning("⚠️ PostgreSQL JJ2 source connection test failed - JJ2 may be offline")
        except Exception as e:
            logging.warning(f"⚠️ PostgreSQL JJ2 source connection failed: {str(e)} - JJ2 may be offline")
            # Store JJ2 connection status in XCom for later use
            context['task_instance'].xcom_push(key='jj2_connection_status', value='failed')
        
        # Check PostgreSQL target connection (pg_jj_monitoring_dw)
        logging.info("🔍 Checking PostgreSQL target connection (pg_jj_monitoring_dw)...")
        target_test_query = "SELECT 1 as test"
        target_result = load_postgres_helper.execute_query(
            sql=target_test_query,
            task_id='postgres_target_connection_test',
            xcom_key=None,
            **context
        )
        if target_result:
            logging.info("✅ PostgreSQL target connection (pg_jj_monitoring_dw) successful")
        else:
            raise Exception("PostgreSQL target connection test failed")
        
        # Check if required tables exist in PostgreSQL JJ source
        logging.info("🔍 Checking PostgreSQL JJ source tables...")
        if not jj_extract_postgres_helper.check_table('public', 'device'):
            raise Exception("JJ Source table 'public.device' does not exist")
        
        if not jj_extract_postgres_helper.check_table('public', 'machine'):
            raise Exception("JJ Source table 'public.machine' does not exist")
        
        if not jj_extract_postgres_helper.check_table('public', 'sensor'):
            raise Exception("JJ Source table 'public.sensor' does not exist")
        
        logging.info("✅ All PostgreSQL JJ source tables exist")
        
        # Check if required tables exist in PostgreSQL JJ2 source - with exception handling
        logging.info("🔍 Checking PostgreSQL JJ2 source tables...")
        try:
            if not jj2_extract_postgres_helper.check_table('public', 'device'):
                raise Exception("JJ2 Source table 'public.device' does not exist")
            
            if not jj2_extract_postgres_helper.check_table('public', 'machine'):
                raise Exception("JJ2 Source table 'public.machine' does not exist")
            
            if not jj2_extract_postgres_helper.check_table('public', 'sensor'):
                raise Exception("JJ2 Source table 'public.sensor' does not exist")
            
            logging.info("✅ All PostgreSQL JJ2 source tables exist")
        except Exception as e:
            logging.warning(f"⚠️ PostgreSQL JJ2 source table check failed: {str(e)} - JJ2 may be offline")
            # Store JJ2 table status in XCom for later use
            context['task_instance'].xcom_push(key='jj2_table_status', value='failed')
        
        # Check if target tables exist in PostgreSQL target
        logging.info("🔍 Checking PostgreSQL target tables...")
        if not load_postgres_helper.check_table('bronze', 'device'):
            raise Exception("Target table 'bronze.device' does not exist")
        
        if not load_postgres_helper.check_table('bronze', 'machine'):
            raise Exception("Target table 'bronze.machine' does not exist")
        
        if not load_postgres_helper.check_table('bronze', 'sensor'):
            raise Exception("Target table 'bronze.sensor' does not exist")
        
        logging.info("✅ All PostgreSQL target tables exist")
        
        logging.info("🎉 All connection and table checks passed!")
        
    except Exception as e:
        logging.error(f"❌ Connection check failed: {str(e)}")
        raise

# =============================================================================
# 2. DATA EXTRACTION FUNCTIONS
# =============================================================================

def extract_jj_machines(**context):
    """Extract JJ machine data from PostgreSQL source"""
    query = """
        SELECT 
            'JJ' as company_cd,
            mach_id,
            mach_kind,
            mach_name,
            descn,
            orgn_cd,
            loc_cd,
            line_cd,
            mline_cd,
            op_cd,
            upd_dt
        FROM public.machine
    """
    
    return jj_extract_postgres_helper.execute_query(
        sql=query,
        task_id='extract_jj_machines',
        xcom_key='jj_machine_data',
        **context
    )

def extract_jj2_machines(**context):
    """Extract JJ2 machine data from PostgreSQL source"""
    try:
        query = """
            SELECT 
                'JJ2' as company_cd,
                mach_id,
                mach_kind,
                mach_name,
                descn,
                orgn_cd,
                loc_cd,
                line_cd,
                mline_cd,
                op_cd,
                upd_dt
            FROM public.machine
        """
        
        return jj2_extract_postgres_helper.execute_query(
            sql=query,
            task_id='extract_jj2_machines',
            xcom_key='jj2_machine_data',
            **context
        )
    except Exception as e:
        logging.warning(f"⚠️ JJ2 machine data extraction failed: {str(e)} - JJ2 may be offline")
        return []

def extract_jj_devices(**context):
    """Extract JJ device data from PostgreSQL source"""
    query = """
        SELECT 
            company_cd,
            mach_id,
            device_id,
            name,
            descn,
            upd_dt
        FROM public.device
    """
    
    return jj_extract_postgres_helper.execute_query(
        sql=query,
        task_id='extract_jj_devices',
        xcom_key='jj_device_data',
        **context
    )

def extract_jj2_devices(**context):
    """Extract JJ2 device data from PostgreSQL source"""
    try:
        query = """
            SELECT 
                company_cd,
                mach_id,
                device_id,
                name,
                descn,
                upd_dt
            FROM public.device
        """
        
        return jj2_extract_postgres_helper.execute_query(
            sql=query,
            task_id='extract_jj2_devices',
            xcom_key='jj2_device_data',
            **context
        )
    except Exception as e:
        logging.warning(f"⚠️ JJ2 device data extraction failed: {str(e)} - JJ2 may be offline")
        return []

def extract_jj_sensors(**context):
    """Extract JJ sensor data from PostgreSQL source"""
    query = """
        SELECT 
            company_cd,
            mach_id,
            device_id,
            sensor_id,
            name,
            addr,
            topic,
            descn,
            upd_dt
        FROM public.sensor
    """
    
    return jj_extract_postgres_helper.execute_query(
        sql=query,
        task_id='extract_jj_sensors',
        xcom_key='jj_sensor_data',
        **context
    )

def extract_jj2_sensors(**context):
    """Extract JJ2 sensor data from PostgreSQL source"""
    try:
        query = """
            SELECT 
                company_cd,
                mach_id,
                device_id,
                sensor_id,
                name,
                addr,
                topic,
                descn,
                upd_dt
            FROM public.sensor
        """
        
        return jj2_extract_postgres_helper.execute_query(
            sql=query,
            task_id='extract_jj2_sensors',
            xcom_key='jj2_sensor_data',
            **context
        )
    except Exception as e:
        logging.warning(f"⚠️ JJ2 sensor data extraction failed: {str(e)} - JJ2 may be offline")
        return []

# =============================================================================
# 3. DATA LOADING FUNCTIONS
# =============================================================================

def load_machines(**context):
    """Load machine data from JJ and JJ2 to PostgreSQL target using UPSERT"""
    # Get machine data from both sources
    jj_machine_data = context['task_instance'].xcom_pull(task_ids='extract_jj_machines')
    jj2_machine_data = context['task_instance'].xcom_pull(task_ids='extract_jj2_machines')
    
    extract_time = datetime.utcnow()
    
    # Combine data from both sources and add ETL metadata
    all_machine_data = []
    if jj_machine_data:
        for row in jj_machine_data:
            # Add etl_extract_time to each row
            if isinstance(row, (list, tuple)):
                all_machine_data.append(tuple(list(row) + [extract_time]))
            else:
                all_machine_data.append((*row, extract_time))
        logging.info(f"📦 JJ machines: {len(jj_machine_data)} records")
    if jj2_machine_data:
        for row in jj2_machine_data:
            # Add etl_extract_time to each row
            if isinstance(row, (list, tuple)):
                all_machine_data.append(tuple(list(row) + [extract_time]))
            else:
                all_machine_data.append((*row, extract_time))
        logging.info(f"📦 JJ2 machines: {len(jj2_machine_data)} records")
    
    if not all_machine_data:
        logging.warning("No machine data to load")
        return
    
    # Use PostgreSQL INSERT with conflict handling
    columns = ['company_cd', 'mach_id', 'mach_kind', 'mach_name', 'descn', 'orgn_cd', 'loc_cd', 'line_cd', 'mline_cd', 'op_cd', 'upd_dt', 'etl_extract_time']
    conflict_columns = ['company_cd', 'mach_id']
    load_postgres_helper.insert_data(
        schema_name='bronze',
        table_name='machine',
        data=all_machine_data,
        columns=columns,
        conflict_columns=conflict_columns
    )
    logging.info(f"✅ {len(all_machine_data)} total machine records loaded")

def load_devices(**context):
    """Load device data from JJ and JJ2 to PostgreSQL target using UPSERT"""
    # Get device data from both sources
    jj_device_data = context['task_instance'].xcom_pull(task_ids='extract_jj_devices')
    jj2_device_data = context['task_instance'].xcom_pull(task_ids='extract_jj2_devices')
    
    # Get current timestamp for ETL metadata
    from datetime import datetime
    extract_time = datetime.utcnow()
    
    # Combine data from both sources and add ETL metadata
    all_device_data = []
    if jj_device_data:
        for row in jj_device_data:
            # Add etl_extract_time to each row
            if isinstance(row, (list, tuple)):
                all_device_data.append(tuple(list(row) + [extract_time]))
            else:
                all_device_data.append((*row, extract_time))
        logging.info(f"📦 JJ devices: {len(jj_device_data)} records")
    if jj2_device_data:
        for row in jj2_device_data:
            # Add etl_extract_time to each row
            if isinstance(row, (list, tuple)):
                all_device_data.append(tuple(list(row) + [extract_time]))
            else:
                all_device_data.append((*row, extract_time))
        logging.info(f"📦 JJ2 devices: {len(jj2_device_data)} records")
    
    if not all_device_data:
        logging.warning("No device data to load")
        return
    
    # Use PostgreSQL INSERT with conflict handling
    columns = ['company_cd', 'mach_id', 'device_id', 'name', 'descn', 'upd_dt', 'etl_extract_time']
    conflict_columns = ['company_cd', 'device_id']
    load_postgres_helper.insert_data(
        schema_name='bronze',
        table_name='device',
        data=all_device_data,
        columns=columns,
        conflict_columns=conflict_columns
    )
    logging.info(f"✅ {len(all_device_data)} total device records loaded")

def load_sensors(**context):
    """Load sensor data from JJ and JJ2 to PostgreSQL target using UPSERT"""
    # Get sensor data from both sources
    jj_sensor_data = context['task_instance'].xcom_pull(task_ids='extract_jj_sensors')
    jj2_sensor_data = context['task_instance'].xcom_pull(task_ids='extract_jj2_sensors')
    
    extract_time = datetime.utcnow()
    
    # Combine data from both sources and add ETL metadata
    all_sensor_data = []
    if jj_sensor_data:
        for row in jj_sensor_data:
            # Add etl_extract_time to each row
            if isinstance(row, (list, tuple)):
                all_sensor_data.append(tuple(list(row) + [extract_time]))
            else:
                all_sensor_data.append((*row, extract_time))
        logging.info(f"📦 JJ sensors: {len(jj_sensor_data)} records")
    if jj2_sensor_data:
        for row in jj2_sensor_data:
            # Add etl_extract_time to each row
            if isinstance(row, (list, tuple)):
                all_sensor_data.append(tuple(list(row) + [extract_time]))
            else:
                all_sensor_data.append((*row, extract_time))
        logging.info(f"📦 JJ2 sensors: {len(jj2_sensor_data)} records")
    
    if not all_sensor_data:
        logging.warning("No sensor data to load")
        return
    
    # Use PostgreSQL INSERT with conflict handling
    columns = ['company_cd', 'mach_id', 'device_id', 'sensor_id', 'name', 'addr', 'topic', 'descn', 'upd_dt', 'etl_extract_time']
    conflict_columns = ['company_cd', 'sensor_id']
    load_postgres_helper.insert_data(
        schema_name='bronze',
        table_name='sensor',
        data=all_sensor_data,
        columns=columns,
        conflict_columns=conflict_columns
    )
    logging.info(f"✅ {len(all_sensor_data)} total sensor records loaded")

# =============================================================================
# 5. DATA VALIDATION FUNCTIONS
# =============================================================================

def validate_data_quality(**context):
    """Validate data quality after transformation"""
    # Check machine count
    machine_query = "SELECT COUNT(*) FROM bronze.machine"
    machine_count = load_postgres_helper.execute_query(
        sql=machine_query,
        task_id='validate_machine_count',
        xcom_key=None,
        **context
    )
    
    # Check device count
    device_query = "SELECT COUNT(*) FROM bronze.device"
    device_count = load_postgres_helper.execute_query(
        sql=device_query,
        task_id='validate_device_count',
        xcom_key=None,
        **context
    )
    
    # Check sensor count
    sensor_query = "SELECT COUNT(*) FROM bronze.sensor"
    sensor_count = load_postgres_helper.execute_query(
        sql=sensor_query,
        task_id='validate_sensor_count',
        xcom_key=None,
        **context
    )
    
    # Check for orphaned devices
    orphaned_devices_query = """
        SELECT COUNT(*) FROM bronze.device d
        LEFT JOIN bronze.machine m ON d.mach_id = m.mach_id
        WHERE m.mach_id IS NULL
    """
    orphaned_devices = load_postgres_helper.execute_query(
        sql=orphaned_devices_query,
        task_id='validate_orphaned_devices',
        xcom_key=None,
        **context
    )
    
    # Check for orphaned sensors
    orphaned_sensors_query = """
        SELECT COUNT(*) FROM bronze.sensor s
        LEFT JOIN bronze.device d ON s.device_id = d.device_id
        WHERE d.device_id IS NULL
    """
    orphaned_sensors = load_postgres_helper.execute_query(
        sql=orphaned_sensors_query,
        task_id='validate_orphaned_sensors',
        xcom_key=None,
        **context
    )
    
    logging.info(f"Data quality check results:")
    logging.info(f"- Machines: {machine_count[0][0] if machine_count else 0}")
    logging.info(f"- Devices: {device_count[0][0] if device_count else 0}")
    logging.info(f"- Sensors: {sensor_count[0][0] if sensor_count else 0}")
    logging.info(f"- Orphaned devices: {orphaned_devices[0][0] if orphaned_devices else 0}")
    logging.info(f"- Orphaned sensors: {orphaned_sensors[0][0] if orphaned_sensors else 0}")
    
    if orphaned_devices and orphaned_devices[0][0] > 0:
        logging.warning(f"Found {orphaned_devices[0][0]} devices without valid machine")
    
    if orphaned_sensors and orphaned_sensors[0][0] > 0:
        logging.warning(f"Found {orphaned_sensors[0][0]} sensors without valid device")

# =============================================================================
# 6. TASK DEFINITIONS
# =============================================================================

# DAG definition
dag = DAG(
    'material_warehouse_master_etl',
    default_args=default_args,
    description='Daily ETL for Material Warehouse master data (machine, device, sensor)',
    schedule_interval='@daily',  # Daily at 15:00 UTC (00:00 KST)
    catchup=False,
    tags=['JJ', "Material", "Warehouse", 'Master']
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

# Data extraction tasks - JJ
extract_jj_machines_task = PythonOperator(
    task_id='extract_jj_machines',
    python_callable=extract_jj_machines,
    dag=dag
)

extract_jj_devices_task = PythonOperator(
    task_id='extract_jj_devices',
    python_callable=extract_jj_devices,
    dag=dag
)

extract_jj_sensors_task = PythonOperator(
    task_id='extract_jj_sensors',
    python_callable=extract_jj_sensors,
    dag=dag
)

# Data extraction tasks - JJ2
extract_jj2_machines_task = PythonOperator(
    task_id='extract_jj2_machines',
    python_callable=extract_jj2_machines,
    dag=dag
)

extract_jj2_devices_task = PythonOperator(
    task_id='extract_jj2_devices',
    python_callable=extract_jj2_devices,
    dag=dag
)

extract_jj2_sensors_task = PythonOperator(
    task_id='extract_jj2_sensors',
    python_callable=extract_jj2_sensors,
    dag=dag
)

# Data loading tasks
load_machines_task = PythonOperator(
    task_id='load_machines',
    python_callable=load_machines,
    dag=dag
)

load_devices_task = PythonOperator(
    task_id='load_devices',
    python_callable=load_devices,
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

# Parallel data extraction from both JJ and JJ2
check_connections_task >> [
    extract_jj_machines_task, extract_jj_devices_task, extract_jj_sensors_task,
    extract_jj2_machines_task, extract_jj2_devices_task, extract_jj2_sensors_task
]

# Data loading - hierarchical order due to foreign key dependencies
# Wait for both JJ and JJ2 extractions before loading
[extract_jj_machines_task, extract_jj2_machines_task] >> load_machines_task
[extract_jj_devices_task, extract_jj2_devices_task] >> load_devices_task
[extract_jj_sensors_task, extract_jj2_sensors_task] >> load_sensors_task

# machines -> devices -> sensors (FK dependencies)
load_machines_task >> load_devices_task  # devices depends on machines
load_devices_task >> load_sensors_task   # sensors depends on devices

# Final validation and end
load_sensors_task >> validate_task
validate_task >> end_task
