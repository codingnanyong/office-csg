from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

def backfill_temperature_raw_data(**context):
    """Backfill all raw temperature data from start date to 1 hour ago"""
    # Set start date to 2025-07-07 00:00:00 KST (hardcoded for backfill)
    start_date = datetime(2025, 7, 8, 0, 0, 0)
    
    # Set end date to current time - 2 hours (rounded to hour)
    # Convert UTC to KST (UTC + 9 hours)
    current_time = datetime.now(timezone.utc) + timedelta(hours=9)  # UTC to KST
    end_date = current_time.replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)
    
    # Add timezone info to match the data in the database
    start_date = start_date.replace(tzinfo=timezone(timedelta(hours=9)))  # KST
    end_date = end_date.replace(tzinfo=timezone(timedelta(hours=9)))      # KST
    
    logging.info(f"🚀 Starting raw data backfill from {start_date} to {end_date}")
    
    # Clear existing data before backfill
    try:
        logging.info("🗑️ Clearing existing data from temperature_raw table...")
        with postgres_helper.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute("DELETE FROM flet_montrg.temperature_raw")
            deleted_count = cursor.rowcount
            conn.commit()
            logging.info(f"✅ Cleared {deleted_count} existing records from temperature_raw table")
    except Exception as e:
        logging.error(f"❌ Failed to clear existing data: {str(e)}")
        raise
    
    # Check PostgreSQL connection and target table
    try:
        logging.info("🔍 Checking PostgreSQL connection and target table...")
        if not postgres_helper.check_table('flet_montrg', 'temperature_raw'):
            raise Exception("Target table 'flet_montrg.temperature_raw' does not exist in PostgreSQL")
        logging.info("✅ PostgreSQL connection and target table check passed")
    except Exception as e:
        logging.error(f"❌ PostgreSQL check failed: {str(e)}")
        raise
    
    # Check if table has any data at all
    try:
        with mssql_helper.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM montrg.dbo.temperature")
            total_count = cursor.fetchone()[0]
            logging.info(f"🔍 Total records in temperature table: {total_count}")
            
            if total_count > 0:
                cursor.execute("SELECT MIN(capture_dt), MAX(capture_dt) FROM montrg.dbo.temperature")
                date_range = cursor.fetchone()
                logging.info(f"🔍 Date range in table: {date_range[0]} to {date_range[1]}")
    except Exception as e:
        logging.error(f"❌ Failed to check table data: {str(e)}")
    
    # Process in chunks to avoid memory issues
    chunk_hours = 1  # 1 hour per chunk (same as main ETL)
    current_start = start_date
    
    while current_start < end_date:
        current_end = min(current_start + timedelta(hours=chunk_hours), end_date)
        
        logging.info(f"📊 Processing chunk: {current_start} to {current_end}")
        
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
        
        # Format the query with parameters safely - use proper date formatting
        formatted_query = query.replace('%s', f"'{current_start.strftime('%Y-%m-%d %H:%M:%S')}'", 1).replace('%s', f"'{current_end.strftime('%Y-%m-%d %H:%M:%S')}'", 1)
        
        # Debug: Check if data exists in the table
        debug_query = f"""
            SELECT COUNT(*) as total_count,
                   MIN(capture_dt) as min_date,
                   MAX(capture_dt) as max_date
            FROM montrg.dbo.temperature 
            WHERE capture_dt > '{current_start.strftime('%Y-%m-%d %H:%M:%S')}' AND capture_dt <= '{current_end.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        try:
            with mssql_helper.hook.get_conn() as conn, conn.cursor() as cursor:
                # First, check if data exists
                cursor.execute(debug_query)
                debug_result = cursor.fetchone()
                logging.info(f"🔍 Debug - Total records: {debug_result[0]}, Date range: {debug_result[1]} to {debug_result[2]}")
                
                # Then execute the main query
                cursor.execute(formatted_query)
                records = cursor.fetchall()
                
                # Debug: Check what the main query actually returns
                logging.info(f"🔍 Main query executed, found {len(records) if records else 0} records")
                if records:
                    logging.info(f"🔍 Sample main query results: {records[:3]}")
                else:
                    logging.info(f"🔍 Main query returned no results")
                
                if not records:
                    logging.warning(f"No data found for chunk {current_start} to {current_end}")
                    result = None
                else:
                    logging.info(f"✅ Processed {len(records)} records for chunk {current_start} to {current_end}")
                    result = records
        except Exception as e:
            logging.error(f"❌ Query failed for chunk {current_start} to {current_end}: {str(e)}")
            raise
        
        if result:
            # Convert and load data
            temperature_data_tuples = [tuple(row) for row in result]
            
            try:
                postgres_helper.upsert_data(
                    schema_name='flet_montrg',
                    table_name='temperature_raw',
                    data=temperature_data_tuples,
                    conflict_columns=['capture_dt', 'ymd', 'hmsf', 'sensor_id'],
                    update_columns=['device_id', 't1', 't2', 't3', 't4', 't5', 't6', 'upload_yn', 'upload_dt', 'extract_time']
                )
                
                logging.info(f"✅ Loaded {len(result)} records to PostgreSQL for chunk {current_start} to {current_end}")
            except Exception as e:
                logging.error(f"❌ Failed to load data to PostgreSQL for chunk {current_start} to {current_end}: {str(e)}")
                raise
        else:
            logging.warning(f"No data found for chunk {current_start} to {current_end}")
        
        current_start = current_end
    
    logging.info(f"🎉 Raw data backfill completed! Triggering aggregated data backfill...")

# DAG definition
dag = DAG(
    'flet_montrg_temperature_raw_backfill',
    default_args=default_args,
    description='Initial backfill for Flet monitoring raw temperature data (2025-07-07 to 1 hour ago)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['HQ', 'IoT', 'Temperature', 'flet_montrg', 'backfill', 'raw']
)

# Tasks
start_task = DummyOperator(task_id='start', dag=dag)
backfill_task = PythonOperator(
    task_id='backfill_temperature_raw_data',
    python_callable=backfill_temperature_raw_data,
    dag=dag
)
trigger_aggregated_backfill_task = TriggerDagRunOperator(
    task_id='trigger_aggregated_backfill',
    trigger_dag_id='flet_montrg_temperature_backfill',
    wait_for_completion=False,
    poke_interval=60,
    dag=dag
)
end_task = DummyOperator(task_id='end', dag=dag)

# Dependencies
start_task >> backfill_task >> trigger_aggregated_backfill_task >> end_task
