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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize helpers
postgres_source_helper = PostgresHelper(conn_id='pg_fdw_v1_iot')  # Raw data source
postgres_target_helper = PostgresHelper(conn_id='pg_fdw_v2_hq')   # Aggregated data target

def backfill_temperature_data(**context):
    """Backfill all aggregated temperature data from PostgreSQL raw data"""
    # Get start date from Variable (current processing time)
    try:
        current_time_str = Variable.get("flet_montrg_temperature_current_time")
        start_date = datetime.fromisoformat(current_time_str.replace('Z', '+00:00'))
        logging.info(f"📅 Using start date from Variable: {start_date}")
    except Exception as e:
        # Fallback to hardcoded date if Variable doesn't exist
        start_date = datetime(2025, 7, 8, 0, 0, 0)
        logging.warning(f"⚠️ Variable not found, using fallback date: {start_date}")
    
    # Set end date to current time - 2 hours (rounded to hour)
    # Convert UTC to KST (UTC + 9 hours)
    current_time = datetime.now(timezone.utc) + timedelta(hours=9)  # UTC to KST
    end_date = current_time.replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)
    
    logging.info(f"🕐 Current time (KST): {current_time}")
    logging.info(f"🕐 Processing up to: {end_date}")
    
    # Add timezone info to match the data in the database
    start_date = start_date.replace(tzinfo=timezone(timedelta(hours=9)))  # KST
    end_date = end_date.replace(tzinfo=timezone(timedelta(hours=9)))      # KST
    
    logging.info(f"🚀 Starting aggregated data backfill from {start_date} to {end_date}")
    
    # Clear existing data before backfill
    try:
        logging.info("🗑️ Clearing existing data from temperature table...")
        with postgres_target_helper.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute("DELETE FROM flet_montrg.temperature")
            deleted_count = cursor.rowcount
            conn.commit()
            logging.info(f"✅ Cleared {deleted_count} existing records from temperature table")
    except Exception as e:
        logging.error(f"❌ Failed to clear existing data: {str(e)}")
        raise
    
    # Check PostgreSQL connections and target tables
    try:
        logging.info("🔍 Checking PostgreSQL connections and target tables...")
        if not postgres_source_helper.check_table('flet_montrg', 'temperature_raw'):
            raise Exception("Source table 'flet_montrg.temperature_raw' does not exist in PostgreSQL")
        if not postgres_target_helper.check_table('flet_montrg', 'temperature'):
            raise Exception("Target table 'flet_montrg.temperature' does not exist in PostgreSQL")
        logging.info("✅ PostgreSQL connections and target tables check passed")
    except Exception as e:
        logging.error(f"❌ PostgreSQL check failed: {str(e)}")
        raise
    
    # Check if table has any data at all
    try:
        with postgres_source_helper.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM flet_montrg.temperature_raw")
            total_count = cursor.fetchone()[0]
            logging.info(f"🔍 Total records in temperature_raw table: {total_count}")
            
            if total_count > 0:
                cursor.execute("SELECT MIN(capture_dt), MAX(capture_dt) FROM flet_montrg.temperature_raw")
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
        
        # Format the query with parameters safely - use proper date formatting with timezone
        formatted_query = query.replace('%s', f"'{current_start.isoformat()}'", 1).replace('%s', f"'{current_end.isoformat()}'", 1)
        
        # Debug: Check if data exists in the table
        debug_query = f"""
            SELECT COUNT(*) as total_count,
                   MIN(capture_dt) as min_date,
                   MAX(capture_dt) as max_date
            FROM flet_montrg.temperature_raw 
            WHERE capture_dt >= '{current_start.isoformat()}' AND capture_dt < '{current_end.isoformat()}'
        """
        
        try:
            with postgres_source_helper.hook.get_conn() as conn, conn.cursor() as cursor:
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
                postgres_target_helper.upsert_data(
                    schema_name='flet_montrg',
                    table_name='temperature',
                    data=temperature_data_tuples,
                    conflict_columns=['ymd', 'hour', 'sensor_id'],
                    update_columns=['pcv_temperature_max', 'pcv_temperature_min', 'pcv_temperature_avg',
                                   'temperature_max', 'temperature_min', 'temperature_avg',
                                   'humidity_max', 'humidity_min', 'humidity_avg', 'calc_dt']
                )
                
                logging.info(f"✅ Loaded {len(result)} records to PostgreSQL for chunk {current_start} to {current_end}")
            except Exception as e:
                logging.error(f"❌ Failed to load data to PostgreSQL for chunk {current_start} to {current_end}: {str(e)}")
                raise
        else:
            logging.warning(f"No data found for chunk {current_start} to {current_end}")
        
        current_start = current_end
    
    # Update the main ETL variable to end date (1 hour before current time)
    Variable.set("flet_montrg_temperature_current_time", end_date.isoformat())
    logging.info(f"🎉 Aggregated data backfill completed! Updated processing time to: {end_date}")
    logging.info(f"📝 Main ETL will start processing from: {end_date} (1 hour ago)")

# DAG definition
dag = DAG(
    'flet_montrg_temperature_backfill',
    default_args=default_args,
    description='Initial backfill for Flet monitoring aggregated temperature data (2025-07-07 to 1 hour ago)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['HQ', 'IoT', 'Temperature', 'flet_montrg', 'backfill']
)

# Tasks
start_task = DummyOperator(task_id='start', dag=dag)
backfill_task = PythonOperator(
    task_id='backfill_temperature_data',
    python_callable=backfill_temperature_data,
    dag=dag
)
end_task = DummyOperator(task_id='end', dag=dag)

# Dependencies
start_task >> backfill_task >> end_task
