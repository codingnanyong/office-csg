"""공통 함수 모듈 - OS BANB MCH Machine Silver"""
import logging
from datetime import datetime, timedelta, timezone
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
POSTGRES_CONN_ID = "pg_jj_maintenance_dw"
SOURCE_SCHEMA = "bronze"
SOURCE_TABLE = "mch_machine_raw"
TARGET_SCHEMA = "silver"
TARGET_TABLE = "os_banb_mch_machine"
INDO_TZ = timezone(timedelta(hours=7))

# Filter Conditions
TARGET_MACH_IDS = ['3110COP00009', '3110COP00001', '3110COP00015']


# ════════════════════════════════════════════════════════════════
# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════

def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string with microsecond support"""
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


# ════════════════════════════════════════════════════════════════
# 3️⃣ Data Extraction (Bronze → Transform)
# ════════════════════════════════════════════════════════════════

def build_extract_sql(start_date: str = None, end_date: str = None) -> str:
    """Build SQL query for extracting and transforming data from Bronze
    - Bronze의 upd_date 기준 증분 수집 (수정된 데이터)
    - upd_date IS NULL 항상 수집 (한 번도 수정 안 된 데이터)
    - 특정 설비(MACH_ID) 필터링
    
    Args:
        start_date: 시작 날짜 (incremental용, None이면 전체 조회)
        end_date: 종료 날짜 (incremental용, None이면 전체 조회)
    """
    mach_id_list = "', '".join(TARGET_MACH_IDS)
    
    base_sql = f'''
        SELECT 
            company_cd, mach_id, mach_ip, barcode, smart_yn,
            model_cd, model_no, machine_ie, asset_cd, manuf_nm,
            manuf_date, install_date, purch_supp_cd, purch_date, purch_no,
            purch_currency, purch_amount, purch_amountvnd, serial_n0, warranty_type,
            warranty_date, warranty_usage, insurance_com_nm, insurance_type, insurance_no,
            insurance_sdate, insurance_edate, insurance_amount, insurance_price, cost_cd,
            line_cd, mline_cd, loc_cd, ws_orgn_cd, ws_user_id,
            rc_orgn_cd, rc_user_id, day_uptime, usage_standard, usage_unit,
            elec_standard, elec_real, pm_unit, pm_cycle, pm_lead_time,
            pm_band_time, pm_loop_type, status_cd, abrogation_date, comuni_type,
            remark, unid, anln1, attrib2, attrib3,
            attrib4, attrib5, attrib6, attrib7, mach_status,
            use_yn, reg_user, reg_ip, reg_date, upd_user,
            upd_ip, upd_date, process, standard_name, numb,
            type, spec_nm, werks, shtxt, eqtyp,
            class_cmms, eqart, brgew, gewei, groes,
            herld, mapar, stort, msgrp, abckz,
            gsber, iwerk, ingrp, gewrk, anln2,
            ansdt, answt, waers, kaizen_yn, level_cd,
            class_tpm, upd_user_class, upd_date_class, signal_equip,
            etl_ingest_time  -- Bronze 적재 시간 (참고용)
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE mach_id IN ('{mach_id_list}')
    '''
    
    if start_date and end_date:
        # Incremental: date range filter
        base_sql += f'''
          AND (
              upd_date BETWEEN '{start_date}'::timestamp AND '{end_date}'::timestamp
              OR upd_date IS NULL
          )
        '''
    
    base_sql += '''
        ORDER BY company_cd, mach_id
    '''
    
    return base_sql


def extract_and_transform_data(
    postgres: PostgresHelper, 
    start_date: str = None, 
    end_date: str = None
) -> tuple:
    """Extract data from Bronze and transform"""
    sql = build_extract_sql(start_date, end_date)
    
    if start_date and end_date:
        logging.info("🔍 Bronze 데이터 추출 및 변환 시작 (Incremental)")
        logging.info(f"기간: {start_date} ~ {end_date}")
    else:
        logging.info("🔍 Bronze 전체 데이터 추출 및 변환 시작 (Backfill)")
    
    logging.info(f"대상 설비: {TARGET_MACH_IDS}")
    logging.info(f"실행 쿼리:\n{sql}")
    
    try:
        with postgres.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
            row_count = len(data) if data else 0
            
            logging.info(f"✅ 추출 완료: {row_count:,} rows")
            return data, row_count
            
    except Exception as e:
        logging.error(f"❌ 데이터 추출 실패: {str(e)}")
        raise


# ════════════════════════════════════════════════════════════════
# 4️⃣ Data Loading (Silver)
# ════════════════════════════════════════════════════════════════

def prepare_insert_data(data: list, extract_time: datetime, ingest_time: datetime) -> list:
    """Prepare data for Silver layer insertion"""
    if not data:
        return []
    
    # Bronze의 etl_ingest_time 제거하고 Silver의 etl_extract_time, etl_ingest_time 추가
    return [
        (*row[:-1], extract_time, ingest_time)  # 마지막 컬럼(Bronze etl_ingest_time) 제거
        for row in data
    ]


def get_column_names() -> list:
    """Get column names for Silver table"""
    return [
        'company_cd', 'mach_id', 'mach_ip', 'barcode', 'smart_yn',
        'model_cd', 'model_no', 'machine_ie', 'asset_cd', 'manuf_nm',
        'manuf_date', 'install_date', 'purch_supp_cd', 'purch_date', 'purch_no',
        'purch_currency', 'purch_amount', 'purch_amountvnd', 'serial_n0', 'warranty_type',
        'warranty_date', 'warranty_usage', 'insurance_com_nm', 'insurance_type', 'insurance_no',
        'insurance_sdate', 'insurance_edate', 'insurance_amount', 'insurance_price', 'cost_cd',
        'line_cd', 'mline_cd', 'loc_cd', 'ws_orgn_cd', 'ws_user_id',
        'rc_orgn_cd', 'rc_user_id', 'day_uptime', 'usage_standard', 'usage_unit',
        'elec_standard', 'elec_real', 'pm_unit', 'pm_cycle', 'pm_lead_time',
        'pm_band_time', 'pm_loop_type', 'status_cd', 'abrogation_date', 'comuni_type',
        'remark', 'unid', 'anln1', 'attrib2', 'attrib3',
        'attrib4', 'attrib5', 'attrib6', 'attrib7', 'mach_status',
        'use_yn', 'reg_user', 'reg_ip', 'reg_date', 'upd_user',
        'upd_ip', 'upd_date', 'process', 'standard_name', 'numb',
        'type', 'spec_nm', 'werks', 'shtxt', 'eqtyp',
        'class_cmms', 'eqart', 'brgew', 'gewei', 'groes',
        'herld', 'mapar', 'stort', 'msgrp', 'abckz',
        'gsber', 'iwerk', 'ingrp', 'gewrk', 'anln2',
        'ansdt', 'answt', 'waers', 'kaizen_yn', 'level_cd',
        'class_tpm', 'upd_user_class', 'upd_date_class', 'signal_equip',
        'etl_extract_time', 'etl_ingest_time'
    ]


def load_data_to_silver(
    postgres: PostgresHelper, 
    data: list,
    schema_name: str = TARGET_SCHEMA,
    table_name: str = TARGET_TABLE
) -> int:
    """Load data to Silver layer using PostgresHelper"""
    if not data:
        logging.warning("⚠️ 적재할 데이터가 없습니다.")
        return 0
    
    columns = get_column_names()
    conflict_columns = ['mach_id']
    
    logging.info(f"📦 Silver 레이어 적재 시작: {len(data):,} rows")
    
    try:
        postgres.insert_data(
            schema_name=schema_name,
            table_name=table_name,
            data=data,
            columns=columns,
            conflict_columns=conflict_columns,
            chunk_size=1000
        )
        
        loaded_count = len(data)
        logging.info(f"✅ Silver 적재 완료: {loaded_count:,} rows")
        return loaded_count
        
    except Exception as e:
        logging.error(f"❌ Silver 적재 실패: {str(e)}")
        raise


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(variable_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(variable_key, end_extract_time)
    logging.info(f"📌 Variable `{variable_key}` Update: {end_extract_time}")

