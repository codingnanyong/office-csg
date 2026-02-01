"""공통 함수 모듈 - MCH Machine Raw"""
import logging
from datetime import datetime, timedelta, timezone
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
ORACLE_CONN_ID = "orc_jj_cmms"
POSTGRES_CONN_ID = "pg_jj_maintenance_dw"
SCHEMA_NAME = "bronze"
TABLE_NAME = "mch_machine_raw"
INDO_TZ = timezone(timedelta(hours=7))


# ════════════════════════════════════════════════════════════════
# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════

def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string with microsecond support"""
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


def get_month_end_date(start_date: datetime) -> datetime:
    """Get the last day of the month for a given date"""
    next_month = start_date.replace(day=1) + timedelta(days=32)
    month_end = next_month.replace(day=1) - timedelta(days=1)
    return month_end.replace(hour=23, minute=59, second=59, microsecond=999999)


def calculate_expected_monthly_loops(start_date: datetime, end_date: datetime) -> int:
    """Calculate expected number of monthly loops"""
    current_date = start_date
    month_count = 0
    
    while current_date < end_date:
        month_end = get_month_end_date(current_date)
        if month_end > end_date:
            month_end = end_date
        current_date = month_end + timedelta(days=1)
        month_count += 1
    
    return month_count


# ════════════════════════════════════════════════════════════════
# 3️⃣ Data Extraction
# ════════════════════════════════════════════════════════════════

def build_extract_sql(start_date: str, end_date: str) -> str:
    """Build SQL query for data extraction
    - UPD_DATE 기준 증분 수집 (수정된 데이터)
    - UPD_DATE IS NULL 항상 수집 (한 번도 수정 안 된 데이터)
    """
    return f'''
        SELECT
            COMPANY_CD, MACH_ID, MACH_IP, BARCODE, SMART_YN,
            MODEL_CD, MODEL_NO, MACHINE_IE, ASSET_CD, MANUF_NM,
            MANUF_DATE, INSTALL_DATE, PURCH_SUPP_CD, PURCH_DATE, PURCH_NO,
            PURCH_CURRENCY, PURCH_AMOUNT, PURCH_AMOUNTVND, SERIAL_N0, WARRANTY_TYPE,
            WARRANTY_DATE, WARRANTY_USAGE, INSURANCE_COM_NM, INSURANCE_TYPE, INSURANCE_NO,
            INSURANCE_SDATE, INSURANCE_EDATE, INSURANCE_AMOUNT, INSURANCE_PRICE, COST_CD,
            LINE_CD, MLINE_CD, LOC_CD, WS_ORGN_CD, WS_USER_ID,
            RC_ORGN_CD, RC_USER_ID, DAY_UPTIME, USAGE_STANDARD, USAGE_UNIT,
            ELEC_STANDARD, ELEC_REAL, PM_UNIT, PM_CYCLE, PM_LEAD_TIME,
            PM_BAND_TIME, PM_LOOP_TYPE, STATUS_CD, ABROGATION_DATE, COMUNI_TYPE,
            REMARK, UNID, ANLN1, ATTRIB2, ATTRIB3,
            ATTRIB4, ATTRIB5, ATTRIB6, ATTRIB7, MACH_STATUS,
            USE_YN, REG_USER, REG_IP, REG_DATE, UPD_USER,
            UPD_IP, UPD_DATE, PROCESS, STANDARD_NAME, NUMB,
            TYPE, SPEC_NM, WERKS, SHTXT, EQTYP,
            CLASS_CMMS, EQART, BRGEW, GEWEI, GROES,
            HERLD, MAPAR, STORT, MSGRP, ABCKZ,
            GSBER, IWERK, INGRP, GEWRK, ANLN2,
            ANSDT, ANSWT, WAERS, KAIZEN_YN, LEVEL_CD,
            CLASS_TPM, UPD_USER_CLASS, UPD_DATE_CLASS, SIGNAL_EQUIP
        FROM ICMMS.MCH_MACHINE
        WHERE UPD_DATE BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') 
                          AND TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')
           OR UPD_DATE IS NULL
    '''


def extract_data(oracle: OracleHelper, start_date: str, end_date: str) -> tuple:
    """Extract data from Oracle database"""
    sql = build_extract_sql(start_date, end_date)
    logging.info(f"실행 쿼리: {sql}")
    
    data = oracle.execute_query(sql, task_id="extract_data_task", xcom_key=None)
    
    # Calculate row count from Oracle result
    if data and isinstance(data, list):
        row_count = len(data)
    elif data and hasattr(data, 'rowcount'):
        row_count = data.rowcount
    else:
        row_count = 0
    
    logging.info(f"{start_date} ~ {end_date} 추출 row 수: {row_count}")
    return data, row_count


# ════════════════════════════════════════════════════════════════
# 4️⃣ Data Loading
# ════════════════════════════════════════════════════════════════

def prepare_insert_data(data: list, extract_time: datetime) -> list:
    """Prepare data for PostgreSQL insertion"""
    # Oracle 결과가 딕셔너리 형태인지 확인하고 처리
    if data and isinstance(data[0], dict):
        # 딕셔너리 형태인 경우 (Oracle 결과)
        return [
            (
                row['COMPANY_CD'], row['MACH_ID'], row['MACH_IP'], row['BARCODE'], row['SMART_YN'],
                row['MODEL_CD'], row['MODEL_NO'], row['MACHINE_IE'], row['ASSET_CD'], row['MANUF_NM'],
                row['MANUF_DATE'], row['INSTALL_DATE'], row['PURCH_SUPP_CD'], row['PURCH_DATE'], row['PURCH_NO'],
                row['PURCH_CURRENCY'], row['PURCH_AMOUNT'], row['PURCH_AMOUNTVND'], row['SERIAL_N0'], row['WARRANTY_TYPE'],
                row['WARRANTY_DATE'], row['WARRANTY_USAGE'], row['INSURANCE_COM_NM'], row['INSURANCE_TYPE'], row['INSURANCE_NO'],
                row['INSURANCE_SDATE'], row['INSURANCE_EDATE'], row['INSURANCE_AMOUNT'], row['INSURANCE_PRICE'], row['COST_CD'],
                row['LINE_CD'], row['MLINE_CD'], row['LOC_CD'], row['WS_ORGN_CD'], row['WS_USER_ID'],
                row['RC_ORGN_CD'], row['RC_USER_ID'], row['DAY_UPTIME'], row['USAGE_STANDARD'], row['USAGE_UNIT'],
                row['ELEC_STANDARD'], row['ELEC_REAL'], row['PM_UNIT'], row['PM_CYCLE'], row['PM_LEAD_TIME'],
                row['PM_BAND_TIME'], row['PM_LOOP_TYPE'], row['STATUS_CD'], row['ABROGATION_DATE'], row['COMUNI_TYPE'],
                row['REMARK'], row['UNID'], row['ANLN1'], row['ATTRIB2'], row['ATTRIB3'],
                row['ATTRIB4'], row['ATTRIB5'], row['ATTRIB6'], row['ATTRIB7'], row['MACH_STATUS'],
                row['USE_YN'], row['REG_USER'], row['REG_IP'], row['REG_DATE'], row['UPD_USER'],
                row['UPD_IP'], row['UPD_DATE'], row['PROCESS'], row['STANDARD_NAME'], row['NUMB'],
                row['TYPE'], row['SPEC_NM'], row['WERKS'], row['SHTXT'], row['EQTYP'],
                row['CLASS_CMMS'], row['EQART'], row['BRGEW'], row['GEWEI'], row['GROES'],
                row['HERLD'], row['MAPAR'], row['STORT'], row['MSGRP'], row['ABCKZ'],
                row['GSBER'], row['IWERK'], row['INGRP'], row['GEWRK'], row['ANLN2'],
                row['ANSDT'], row['ANSWT'], row['WAERS'], row['KAIZEN_YN'], row['LEVEL_CD'],
                row['CLASS_TPM'], row['UPD_USER_CLASS'], row['UPD_DATE_CLASS'], row['SIGNAL_EQUIP'],
                extract_time
            )
            for row in data
        ]
    else:
        # 튜플 형태인 경우
        return [
            (
                *row,
                extract_time
            )
            for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
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
        'etl_extract_time'
    ]


def load_data(
    pg: PostgresHelper, 
    data: list, 
    extract_time: datetime,
    schema_name: str = SCHEMA_NAME,
    table_name: str = TABLE_NAME,
    batch_size: int = 1000
) -> int:
    """Load data to PostgreSQL using insert_data method"""
    if not data:
        return 0
    
    columns = get_column_names()
    conflict_columns = ['company_cd', 'mach_id']
    
    pg.insert_data(
        schema_name=schema_name,
        table_name=table_name,
        data=data,
        columns=columns,
        conflict_columns=conflict_columns,
        chunk_size=batch_size
    )
    
    loaded_count = len(data)
    logging.info(f"✅ 총 {loaded_count}건 적재 완료")
    return loaded_count


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(variable_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(variable_key, end_extract_time)
    logging.info(f"📌 Variable `{variable_key}` Update: {end_extract_time}")


# ════════════════════════════════════════════════════════════════
# 6️⃣ Single Date Processing
# ════════════════════════════════════════════════════════════════

def process_single_date(
    target_date: str,
    oracle_conn_id: str = ORACLE_CONN_ID,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    schema_name: str = SCHEMA_NAME,
    table_name: str = TABLE_NAME
) -> dict:
    """단일 날짜 데이터 처리 (추출 + 적재)"""
    oracle = OracleHelper(conn_id=oracle_conn_id)
    pg = PostgresHelper(conn_id=postgres_conn_id)
    
    start_date = datetime.strptime(target_date, '%Y-%m-%d').replace(tzinfo=INDO_TZ)
    end_date = start_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"📅 데이터 수집 시작: {start_str} ~ {end_str}")
    
    # 데이터 추출 및 적재
    data, extracted_count = extract_data(oracle, start_str, end_str)
    
    if extracted_count > 0:
        extract_time = datetime.now(INDO_TZ)
        prepared_data = prepare_insert_data(data, extract_time)
        loaded_count = load_data(pg, prepared_data, extract_time, schema_name, table_name)
        logging.info(f"✅ 데이터 수집 완료: 추출 {extracted_count}건, 적재 {loaded_count}건")
        
        return {
            "status": "success",
            "date": target_date,
            "extracted_count": extracted_count,
            "loaded_count": loaded_count,
            "start_time": start_str,
            "end_time": end_str,
            "extract_time": extract_time.isoformat()
        }
    else:
        logging.info(f"⚠️ 수집할 데이터가 없습니다: {start_str} ~ {end_str}")
        
        return {
            "status": "success",
            "date": target_date,
            "extracted_count": 0,
            "loaded_count": 0,
            "start_time": start_str,
            "end_time": end_str,
            "message": "수집할 데이터가 없음"
        }

