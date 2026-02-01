"""공통 함수 모듈 - WOF Order Raw"""
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
TABLE_NAME = "wof_order_raw"
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
            COMPANY_CD, WO_YYMM, WO_ORGN, WO_NO, WO_TYPE,
            WO_DATE, WO_CLASS, WO_TITLE, WORK_TYPE, MACH_ID,
            REQUEST_DATE, REQUEST_PIC, REQUEST_CONTENT, STOP_YN,
            PROBLEM_DATE, DEFE_DATE, DEFE_CD, DEFE_CONTENT,
            SOLU_DATE, SOLU_CD, SOLU_CONTENT, RIME, DOWN_TIME,
            REPAIR_TIME, DUE_DATE, COST_CD, LINE_CD, MLINE_CD,
            LOC_CD, RP_ORGN_CD, RP_USER_ID, WO_STATUS, REMARK,
            UNID, MOVE_ORGN_CD, MOVE_LOC_CD, MOVE_WO_YYMM,
            MOVE_WO_ORGN, MOVE_WO_NO, OWO_YYMM, OWO_ORGN,
            OWO_NO, OR_YYMM, OR_COMPANY, OR_NO, PM_YEAR,
            PM_COMPANY, PM_NO, PM_DATE, APV_USER, APV_IP,
            APV_DATE, CLS_USER, CLS_IP, CLS_DATE, REG_USER,
            REG_IP, REG_DATE, UPD_USER, UPD_IP, UPD_DATE,
            SOLU_CONTENT_CD, DEFE_CD1, DEFE_CONTENT1, DEFE_CD2,
            DEFE_CONTENT2, DEFE_CD3, DEFE_CONTENT3, DEFE_CD4,
            DEFE_CONTENT4
        FROM ICMMS.WOF_ORDER
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
                row['COMPANY_CD'], row['WO_YYMM'], row['WO_ORGN'], row['WO_NO'], row['WO_TYPE'],
                row['WO_DATE'], row['WO_CLASS'], row['WO_TITLE'], row['WORK_TYPE'], row['MACH_ID'],
                row['REQUEST_DATE'], row['REQUEST_PIC'], row['REQUEST_CONTENT'], row['STOP_YN'],
                row['PROBLEM_DATE'], row['DEFE_DATE'], row['DEFE_CD'], row['DEFE_CONTENT'],
                row['SOLU_DATE'], row['SOLU_CD'], row['SOLU_CONTENT'], row['RIME'], row['DOWN_TIME'],
                row['REPAIR_TIME'], row['DUE_DATE'], row['COST_CD'], row['LINE_CD'], row['MLINE_CD'],
                row['LOC_CD'], row['RP_ORGN_CD'], row['RP_USER_ID'], row['WO_STATUS'], row['REMARK'],
                row['UNID'], row['MOVE_ORGN_CD'], row['MOVE_LOC_CD'], row['MOVE_WO_YYMM'],
                row['MOVE_WO_ORGN'], row['MOVE_WO_NO'], row['OWO_YYMM'], row['OWO_ORGN'],
                row['OWO_NO'], row['OR_YYMM'], row['OR_COMPANY'], row['OR_NO'], row['PM_YEAR'],
                row['PM_COMPANY'], row['PM_NO'], row['PM_DATE'], row['APV_USER'], row['APV_IP'],
                row['APV_DATE'], row['CLS_USER'], row['CLS_IP'], row['CLS_DATE'], row['REG_USER'],
                row['REG_IP'], row['REG_DATE'], row['UPD_USER'], row['UPD_IP'], row['UPD_DATE'],
                row['SOLU_CONTENT_CD'], row['DEFE_CD1'], row['DEFE_CONTENT1'], row['DEFE_CD2'],
                row['DEFE_CONTENT2'], row['DEFE_CD3'], row['DEFE_CONTENT3'], row['DEFE_CD4'],
                row['DEFE_CONTENT4'],
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
        'company_cd', 'wo_yymm', 'wo_orgn', 'wo_no', 'wo_type',
        'wo_date', 'wo_class', 'wo_title', 'work_type', 'mach_id',
        'request_date', 'request_pic', 'request_content', 'stop_yn',
        'problem_date', 'defe_date', 'defe_cd', 'defe_content',
        'solu_date', 'solu_cd', 'solu_content', 'rime', 'down_time',
        'repair_time', 'due_date', 'cost_cd', 'line_cd', 'mline_cd',
        'loc_cd', 'rp_orgn_cd', 'rp_user_id', 'wo_status', 'remark',
        'unid', 'move_orgn_cd', 'move_loc_cd', 'move_wo_yymm',
        'move_wo_orgn', 'move_wo_no', 'owo_yymm', 'owo_orgn',
        'owo_no', 'or_yymm', 'or_company', 'or_no', 'pm_year',
        'pm_company', 'pm_no', 'pm_date', 'apv_user', 'apv_ip',
        'apv_date', 'cls_user', 'cls_ip', 'cls_date', 'reg_user',
        'reg_ip', 'reg_date', 'upd_user', 'upd_ip', 'upd_date',
        'solu_content_cd', 'defe_cd1', 'defe_content1', 'defe_cd2',
        'defe_content2', 'defe_cd3', 'defe_content3', 'defe_cd4',
        'defe_content4', 'etl_extract_time'
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
    conflict_columns = ['company_cd', 'wo_yymm', 'wo_orgn', 'wo_no']
    
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

