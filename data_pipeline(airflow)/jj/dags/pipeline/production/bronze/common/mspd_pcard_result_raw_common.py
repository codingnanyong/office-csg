"""공통 함수 모듈 - MSPD PCARD Result Raw"""
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
POSTGRES_CONN_ID = "pg_jj_production_dw"
SCHEMA_NAME = "bronze"
TABLE_NAME = "mspd_pcard_result_raw"
INDO_TZ = timezone(timedelta(hours=7))
INITIAL_START_DATE = datetime(2020, 1, 1, 0, 0, 0)
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


def _normalize_to_0630(dt: datetime) -> datetime:
    """Normalize datetime to 06:30:00"""
    return dt.replace(hour=6, minute=30, second=0, microsecond=0)

def get_month_end_date(start_date: datetime) -> datetime:
    """Get the last day of the month for a given date (06:30 기준)"""
    next_month = start_date.replace(day=1) + timedelta(days=32)
    month_end = next_month.replace(day=1) - timedelta(days=1)
    # 다음날 06:30으로 설정 (해당 월의 마지막 날 06:30 ~ 다음달 1일 06:30)
    return _normalize_to_0630(month_end + timedelta(days=1))


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
            PLANT_CD, PCARD_NAME, ITEM_CD, OP_CD, ROUTING_SEQ, PROD_MOVE_TYPE,
            PROD_GROUP_NO, PROD_ORDER_NO, PROD_ORDER_TYPE, FA_PLANT_CD, FA_DATE,
            FA_WC_GROUP_CD, FA_WC_CD, FA_MLINE_CD, ITEM_CLASS_TYPE, ITEM_CLASS,
            STYLE_CD, SIZE_CD, PCARD_QTY, PLAN_PROD_DATE, PLAN_PROD_WC_CD,
            START_ROUTING_YN, END_ROUTING_YN, BARCODE_KEY, ITPO_WC_PLANT_CD,
            IN_DT, IN_WC_CD, IN_WH_CD, IN_DATE, INPUT_DT, INPUT_WC_CD,
            INPUT_WH_CD, INPUT_DATE, PROD_DT, PROD_WC_CD, PROD_WH_CD, PROD_DATE,
            PROD_OUT_YN, OUT_DT, OUT_WC_CD, OUT_WH_CD, OUT_DATE,
            PCARD_ID, PS_ID, DAY_SEQ, PCARD_SEQ, SBD_UID, MEMO, UPC_QTY,
            CLOSE_YN, CLOSE_DATE, CLOSING_IF_NO, ERP_PLANT_CD, ERP_PLAN_PROD_WC_CD,
            ERP_FA_PLANT_CD, ERP_FA_WC_GROUP_CD, ERP_FA_WC_CD, ERP_FA_MLINE_CD,
            ERP_ITPO_WC_PLANT_CD, ERP_IN_WC_CD, ERP_INPUT_WC_CD, ERP_PROD_WC_CD,
            ERP_OUT_WC_CD, CREATOR, CREATE_DT, CREATE_PC, CREATE_PROGRAM_ID,
            UPDATE_DT, UPDATE_PC, UPDATER, UPDATE_PROGRAM_ID, SUMMARY_YN,
            SUMMARY_DT, SUMMARY_MSG, INPUT_MLINE_CD, ZCP_CLOSE_YN, ZCP_CLOSE_DATE,
            OUTCLOSE_YN, BOMLEVEL, SCAN_PSID, RESULT_TYPE, SM_IN_DT, SM_IN_WH_CD,
            SM_IN_DATE, MOVE_CLOSE_YN, MOVE_CLOSE_DATE, MOVE_CLOSING_IF_NO,
            DELIVERY_NO, CAR_NO, CUST_PO_NO, CUST_PO_NO_SEQ
        FROM LMES.MSPD_PCARD_RESULT
        WHERE UPDATE_DT BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') 
            AND TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')
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
    if not data:
        return []
    
    # Oracle 결과가 딕셔너리 형태인지 확인하고 처리
    if isinstance(data[0], dict):
        # 딕셔너리 형태인 경우 (Oracle 결과)
        return [
            (
                row['PLANT_CD'], row['PCARD_NAME'], row['ITEM_CD'], row['OP_CD'], 
                row['ROUTING_SEQ'], row['PROD_MOVE_TYPE'], row['PROD_GROUP_NO'], 
                row['PROD_ORDER_NO'], row['PROD_ORDER_TYPE'], row.get('FA_PLANT_CD'),
                row.get('FA_DATE'), row.get('FA_WC_GROUP_CD'), row.get('FA_WC_CD'),
                row.get('FA_MLINE_CD'), row.get('ITEM_CLASS_TYPE'), row['ITEM_CLASS'],
                row['STYLE_CD'], row.get('SIZE_CD'), row.get('PCARD_QTY'),
                row.get('PLAN_PROD_DATE'), row.get('PLAN_PROD_WC_CD'),
                row.get('START_ROUTING_YN'), row.get('END_ROUTING_YN'),
                row.get('BARCODE_KEY'), row.get('ITPO_WC_PLANT_CD'),
                row.get('IN_DT'), row.get('IN_WC_CD'), row.get('IN_WH_CD'),
                row.get('IN_DATE'), row.get('INPUT_DT'), row.get('INPUT_WC_CD'),
                row.get('INPUT_WH_CD'), row.get('INPUT_DATE'),
                row.get('PROD_DT'), row.get('PROD_WC_CD'), row.get('PROD_WH_CD'),
                row.get('PROD_DATE'), row['PROD_OUT_YN'],
                row.get('OUT_DT'), row.get('OUT_WC_CD'), row.get('OUT_WH_CD'),
                row.get('OUT_DATE'), row.get('PCARD_ID'), row.get('PS_ID'),
                row['DAY_SEQ'], row['PCARD_SEQ'], row.get('SBD_UID'),
                row.get('MEMO'), row['UPC_QTY'], row['CLOSE_YN'],
                row.get('CLOSE_DATE'), row.get('CLOSING_IF_NO'),
                row.get('ERP_PLANT_CD'), row.get('ERP_PLAN_PROD_WC_CD'),
                row.get('ERP_FA_PLANT_CD'), row.get('ERP_FA_WC_GROUP_CD'),
                row.get('ERP_FA_WC_CD'), row.get('ERP_FA_MLINE_CD'),
                row.get('ERP_ITPO_WC_PLANT_CD'), row.get('ERP_IN_WC_CD'),
                row.get('ERP_INPUT_WC_CD'), row.get('ERP_PROD_WC_CD'),
                row.get('ERP_OUT_WC_CD'), row['CREATOR'], row['CREATE_DT'],
                row['CREATE_PC'], row.get('CREATE_PROGRAM_ID'),
                row.get('UPDATE_DT'), row.get('UPDATE_PC'), row.get('UPDATER'),
                row.get('UPDATE_PROGRAM_ID'), row.get('SUMMARY_YN'),
                row.get('SUMMARY_DT'), row.get('SUMMARY_MSG'),
                row.get('INPUT_MLINE_CD'), row.get('ZCP_CLOSE_YN'),
                row.get('ZCP_CLOSE_DATE'), row.get('OUTCLOSE_YN'),
                row.get('BOMLEVEL'), row.get('SCAN_PSID'), row.get('RESULT_TYPE'),
                row.get('SM_IN_DT'), row.get('SM_IN_WH_CD'), row.get('SM_IN_DATE'),
                row.get('MOVE_CLOSE_YN'), row.get('MOVE_CLOSE_DATE'),
                row.get('MOVE_CLOSING_IF_NO'), row.get('DELIVERY_NO'),
                row.get('CAR_NO'), row.get('CUST_PO_NO'), row.get('CUST_PO_NO_SEQ'),
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우
        return [
            (
                row[0], row[1], row[2], row[3], row[4], row[5],  # PLANT_CD ~ PROD_MOVE_TYPE
                row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15],  # PROD_GROUP_NO ~ ITEM_CLASS
                row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24],  # STYLE_CD ~ ITPO_WC_PLANT_CD
                row[25], row[26], row[27], row[28], row[29], row[30], row[31], row[32], row[33],  # IN_DT ~ INPUT_DATE
                row[34], row[35], row[36], row[37], row[38],  # PROD_DT ~ PROD_OUT_YN
                row[39], row[40], row[41], row[42], row[43], row[44], row[45], row[46], row[47],  # OUT_DT ~ UPC_QTY
                row[48], row[49], row[50], row[51], row[52], row[53], row[54], row[55], row[56], row[57],  # CLOSE_YN ~ ERP_FA_WC_CD
                row[58], row[59], row[60], row[61], row[62], row[63], row[64], row[65], row[66], row[67],  # ERP_FA_MLINE_CD ~ CREATE_PROGRAM_ID
                row[68], row[69], row[70], row[71], row[72], row[73], row[74], row[75], row[76], row[77],  # UPDATE_DT ~ ZCP_CLOSE_DATE
                row[78], row[79], row[80], row[81], row[82], row[83], row[84], row[85], row[86], row[87],  # OUTCLOSE_YN ~ MOVE_CLOSING_IF_NO
                row[88], row[89], row[90], row[91],  # DELIVERY_NO ~ CUST_PO_NO_SEQ
                extract_time  # etl_extract_time만 전달
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "plant_cd", "pcard_name", "item_cd", "op_cd", "routing_seq", "prod_move_type",
        "prod_group_no", "prod_order_no", "prod_order_type", "fa_plant_cd", "fa_date",
        "fa_wc_group_cd", "fa_wc_cd", "fa_mline_cd", "item_class_type", "item_class",
        "style_cd", "size_cd", "pcard_qty", "plan_prod_date", "plan_prod_wc_cd",
        "start_routing_yn", "end_routing_yn", "barcode_key", "itpo_wc_plant_cd",
        "in_dt", "in_wc_cd", "in_wh_cd", "in_date", "input_dt", "input_wc_cd",
        "input_wh_cd", "input_date", "prod_dt", "prod_wc_cd", "prod_wh_cd", "prod_date",
        "prod_out_yn", "out_dt", "out_wc_cd", "out_wh_cd", "out_date",
        "pcard_id", "ps_id", "day_seq", "pcard_seq", "sbd_uid", "memo", "upc_qty",
        "close_yn", "close_date", "closing_if_no", "erp_plant_cd", "erp_plan_prod_wc_cd",
        "erp_fa_plant_cd", "erp_fa_wc_group_cd", "erp_fa_wc_cd", "erp_fa_mline_cd",
        "erp_itpo_wc_plant_cd", "erp_in_wc_cd", "erp_input_wc_cd", "erp_prod_wc_cd",
        "erp_out_wc_cd", "creator", "create_dt", "create_pc", "create_program_id",
        "update_dt", "update_pc", "updater", "update_program_id", "summary_yn",
        "summary_dt", "summary_msg", "input_mline_cd", "zcp_close_yn", "zcp_close_date",
        "outclose_yn", "bomlevel", "scan_psid", "result_type", "sm_in_dt", "sm_in_wh_cd",
        "sm_in_date", "move_close_yn", "move_close_date", "move_closing_if_no",
        "delivery_no", "car_no", "cust_po_no", "cust_po_no_seq",
        "etl_extract_time", "etl_ingest_time"
    ]


def load_data(
    pg: PostgresHelper, 
    data: list, 
    extract_time: datetime,
    schema_name: str = SCHEMA_NAME,
    table_name: str = TABLE_NAME
) -> None:
    """Load data into PostgreSQL database"""
    if not data:
        logging.info("⚠️ 수집할 데이터가 없습니다.")
        return
    
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_column_names()
    
    # Primary Key 컬럼들
    conflict_columns = ["plant_cd", "pcard_name", "item_cd", "op_cd", "routing_seq", "prod_move_type"]
    
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

