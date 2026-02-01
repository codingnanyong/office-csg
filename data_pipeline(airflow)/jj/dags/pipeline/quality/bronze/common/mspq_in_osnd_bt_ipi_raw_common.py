"""공통 함수 모듈 - MSPQ IN OSND BT IPI Raw Bronze"""
import logging
from datetime import datetime, timedelta, timezone
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
ORACLE_CONN_ID = "orc_jj_gmes"
POSTGRES_CONN_ID = "pg_jj_quality_dw"
SCHEMA_NAME = "bronze"
TABLE_NAME = "mspq_in_osnd_bt_ipi_raw"
INDO_TZ = timezone(timedelta(hours=7))
INITIAL_START_DATE = datetime(2024, 3, 1, 0, 0, 0)
DAYS_OFFSET_FOR_INCREMENTAL = 2


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
    # 23:59:59로 설정
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
    """Build SQL query for data extraction"""
    return f'''
        SELECT
            PLANT_CD, OSND_ID, SHIFT_CD, OSND_DATE, OSND_DT, PB_CD, SUB_WC_CD, OP_CD,
            INSPECT_POINT_ID, OSND_TYPE, MOLD_CD, MOLD_SIZE_CD, MCS_CD, COLOR_CD, ORD_PLANT_CD,
            PROD_GROUP_NO, PROD_ORDER_NO, STYLE_CD, ITEM_CLASS, ITEM_CD, SIZE_CD, LR_CD,
            OSND_BT_QTY, REASON_CD, MEMO, INPUT_TYPE, REWORK_QTY, REWORK_DATE, SS_APPLY_DATE,
            SS_APPLY_PCS_QTY, SO_ID, APPLY_TO_MC_QTY, REF_CAPTION, REF_VALUE01, REF_VALUE02,
            REF_VALUE03, CREATOR, CREATE_DT, CREATE_PC, UPDATER, UPDATE_DT, UPDATE_PC,
            REPL_QTY, REPL_DT, REPL_USER, REPL_CFM_DT, REPL_CFM_USER, MOLD_SUB_DESC,
            MACHINE_CD, MOLD_ID, RESOURCE_CD, MC_CD, STATION, ST_LR_CD, RST_YMD,
            EXTRA1_FLD, EXTRA2_FLD, EXTRA3_FLD, EXTRA4_FLD, EXTRA5_FLD
        FROM LMES.MSPQ_IN_OSND_BT_IPI
        WHERE (
            OSND_DT BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') 
                        AND TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')
        )
        OR (
            UPDATE_DT BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') 
                          AND TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')
        )
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
                row['PLANT_CD'], row['OSND_ID'], row['SHIFT_CD'], row['OSND_DATE'], row['OSND_DT'],
                row['PB_CD'], row['SUB_WC_CD'], row['OP_CD'], row['INSPECT_POINT_ID'], row['OSND_TYPE'],
                row['MOLD_CD'], row['MOLD_SIZE_CD'], row['MCS_CD'], row['COLOR_CD'], row['ORD_PLANT_CD'],
                row['PROD_GROUP_NO'], row['PROD_ORDER_NO'], row['STYLE_CD'], row['ITEM_CLASS'], row['ITEM_CD'],
                row['SIZE_CD'], row['LR_CD'], row['OSND_BT_QTY'], row['REASON_CD'], row['MEMO'],
                row['INPUT_TYPE'], row['REWORK_QTY'], row['REWORK_DATE'], row['SS_APPLY_DATE'], row['SS_APPLY_PCS_QTY'],
                row['SO_ID'], row['APPLY_TO_MC_QTY'], row['REF_CAPTION'], row['REF_VALUE01'], row['REF_VALUE02'],
                row['REF_VALUE03'], row['CREATOR'], row['CREATE_DT'], row['CREATE_PC'], row['UPDATER'], row['UPDATE_DT'],
                row['UPDATE_PC'], row['REPL_QTY'], row['REPL_DT'], row['REPL_USER'], row['REPL_CFM_DT'], row['REPL_CFM_USER'],
                row['MOLD_SUB_DESC'], row['MACHINE_CD'], row['MOLD_ID'], row['RESOURCE_CD'], row['MC_CD'], row['STATION'],
                row['ST_LR_CD'], row['RST_YMD'], row['EXTRA1_FLD'], row['EXTRA2_FLD'], row['EXTRA3_FLD'],
                row['EXTRA4_FLD'], row['EXTRA5_FLD'],
                extract_time
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0], row[1], row[2], row[3], row[4],
                row[5], row[6], row[7], row[8], row[9],
                row[10], row[11], row[12], row[13], row[14],
                row[15], row[16], row[17], row[18], row[19],
                row[20], row[21], row[22], row[23], row[24],
                row[25], row[26], row[27], row[28], row[29],
                row[30], row[31], row[32], row[33], row[34],
                row[35], row[36], row[37], row[38], row[39],
                row[40], row[41], row[42], row[43], row[44],
                row[45], row[46], row[47], row[48], row[49],
                row[50], row[51], row[52], row[53], row[54],
                row[55], row[56], row[57], row[58], row[59],
                extract_time
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "plant_cd", "osnd_id", "shift_cd", "osnd_date", "osnd_dt",
        "pb_cd", "sub_wc_cd", "op_cd", "inspect_point_id", "osnd_type",
        "mold_cd", "mold_size_cd", "mcs_cd", "color_cd", "ord_plant_cd",
        "prod_group_no", "prod_order_no", "style_cd", "item_class", "item_cd",
        "size_cd", "lr_cd", "osnd_bt_qty", "reason_cd", "memo",
        "input_type", "rework_qty", "rework_date", "ss_apply_date", "ss_apply_pcs_qty",
        "so_id", "apply_to_mc_qty", "ref_caption", "ref_value01", "ref_value02",
        "ref_value03", "creator", "create_dt", "create_pc", "updater", "update_dt",
        "update_pc", "repl_qty", "repl_dt", "repl_user", "repl_cfm_dt", "repl_cfm_user",
        "mold_sub_desc", "machine_cd", "mold_id", "resource_cd", "mc_cd", "station",
        "st_lr_cd", "rst_ymd", "extra1_fld", "extra2_fld", "extra3_fld",
        "extra4_fld", "extra5_fld",
        "etl_extract_time"
    ]


def load_data(
    pg: PostgresHelper, 
    data: list, 
    extract_time: datetime,
    schema_name: str = SCHEMA_NAME,
    table_name: str = TABLE_NAME
) -> None:
    """Load data into PostgreSQL database"""
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_column_names()
    conflict_columns = ["plant_cd", "osnd_id", "shift_cd", "osnd_date", "resource_cd", "mc_cd", "station", "st_lr_cd"]
    
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

