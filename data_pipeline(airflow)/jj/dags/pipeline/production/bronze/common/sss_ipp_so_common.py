"""공통 함수 모듈 - SMP SS IPP SO"""
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
TABLE_NAME = "sss_ipp_so_raw"
INDO_TZ = timezone(timedelta(hours=7))
INITIAL_START_DATE = datetime(2023, 1, 1, 0, 0, 0)
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
            VERSION_ID, PLANT_CD, RESOURCE_CD, PCARD_ID, YMD,
            HH, START_DATE, END_DATE, ZONE_CD, IPP_LINE_CD,
            MACHINE_CD, STATION_TYPE, MODEL_CD, STYLE_CD, CS_SIZE,
            PRS_QTY, MOLD_CD, MOLD_SIZE_CD, PRS_PER_PRESS, MOLD_CYCLE,
            SS_PER_PRS, JC_TIME, JC_TYPE, RESOURCE_TYPE, STATUS,
            MCS_1_CD, MCS_2_CD, MCS_3_CD, MCS_4_CD, MCS_5_CD,
            COLOR_1_CD, COLOR_2_CD, COLOR_3_CD, COLOR_4_CD, COLOR_5_CD,
            UV_TYPE_CD, DUAL_PART_YN, OP_CD, WC_CD, MLINE_CD,
            ASY_YMD, ASY_HH, SO_ID, SS_ID, SORT_KEY,
            UPD_USER, UPD_YMD, MOLD_CYCLE_IE, CT_MOLD_IE, PLAN_TYPE,
            PLAN_SHIFT, HH_SEQ, SHORT_NAME, L_PRS_QTY, R_PRS_QTY,
            MCS_1_NAME, MCS_2_NAME, COLOR_1_NAME, COLOR_2_NAME, O_IPP_LINE_CD,
            O_MACHINE_NAME, O_STATION_CD, O_CNT_QTY, O_OSND_PRS_QTY, O_OSND_CNT_QTY,
            O_MOLD_ID, O_COLOR_NAME, O_LINE_CD, O_SS_ID, PLAN_DATE
        FROM LMES.SSS_IPP_SO
        WHERE UPD_YMD BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') 
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
    # Oracle 결과가 딕셔너리 형태인지 확인하고 처리
    if data and isinstance(data[0], dict):
        # 딕셔너리 형태인 경우 (Oracle 결과)
        return [
            (
                row['VERSION_ID'], row['PLANT_CD'], row['RESOURCE_CD'], row['PCARD_ID'], row['YMD'],
                row['HH'], row['START_DATE'], row['END_DATE'], row['ZONE_CD'], row['IPP_LINE_CD'],
                row['MACHINE_CD'], row['STATION_TYPE'], row['MODEL_CD'], row['STYLE_CD'], row['CS_SIZE'],
                row['PRS_QTY'], row['MOLD_CD'], row['MOLD_SIZE_CD'], row['PRS_PER_PRESS'], row['MOLD_CYCLE'],
                row['SS_PER_PRS'], row['JC_TIME'], row['JC_TYPE'], row['RESOURCE_TYPE'], row['STATUS'],
                row['MCS_1_CD'], row['MCS_2_CD'], row['MCS_3_CD'], row['MCS_4_CD'], row['MCS_5_CD'],
                row['COLOR_1_CD'], row['COLOR_2_CD'], row['COLOR_3_CD'], row['COLOR_4_CD'], row['COLOR_5_CD'],
                row['UV_TYPE_CD'], row['DUAL_PART_YN'], row['OP_CD'], row['WC_CD'], row['MLINE_CD'],
                row['ASY_YMD'], row['ASY_HH'], row['SO_ID'], row['SS_ID'], row['SORT_KEY'],
                row['UPD_USER'], row['UPD_YMD'], row['MOLD_CYCLE_IE'], row['CT_MOLD_IE'], row['PLAN_TYPE'],
                row['PLAN_SHIFT'], row['HH_SEQ'], row['SHORT_NAME'], row['L_PRS_QTY'], row['R_PRS_QTY'],
                row['MCS_1_NAME'], row['MCS_2_NAME'], row['COLOR_1_NAME'], row['COLOR_2_NAME'], row['O_IPP_LINE_CD'],
                row['O_MACHINE_NAME'], row['O_STATION_CD'], row['O_CNT_QTY'], row['O_OSND_PRS_QTY'], row['O_OSND_CNT_QTY'],
                row['O_MOLD_ID'], row['O_COLOR_NAME'], row['O_LINE_CD'], row['O_SS_ID'], row['PLAN_DATE'],
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]
    else:
        # 리스트 형태인 경우 (기존 코드)
        return [
            (
                row[0], row[1], row[2], row[3], row[4],  # VERSION_ID, PLANT_CD, RESOURCE_CD, PCARD_ID, YMD
                row[5], row[6], row[7], row[8], row[9],  # HH, START_DATE, END_DATE, ZONE_CD, IPP_LINE_CD
                row[10], row[11], row[12], row[13], row[14],  # MACHINE_CD, STATION_TYPE, MODEL_CD, STYLE_CD, CS_SIZE
                row[15], row[16], row[17], row[18], row[19],  # PRS_QTY, MOLD_CD, MOLD_SIZE_CD, PRS_PER_PRESS, MOLD_CYCLE
                row[20], row[21], row[22], row[23], row[24],  # SS_PER_PRS, JC_TIME, JC_TYPE, RESOURCE_TYPE, STATUS
                row[25], row[26], row[27], row[28], row[29],  # MCS_1_CD, MCS_2_CD, MCS_3_CD, MCS_4_CD, MCS_5_CD
                row[30], row[31], row[32], row[33], row[34],  # COLOR_1_CD, COLOR_2_CD, COLOR_3_CD, COLOR_4_CD, COLOR_5_CD
                row[35], row[36], row[37], row[38], row[39],  # UV_TYPE_CD, DUAL_PART_YN, OP_CD, WC_CD, MLINE_CD
                row[40], row[41], row[42], row[43], row[44],  # ASY_YMD, ASY_HH, SO_ID, SS_ID, SORT_KEY
                row[45], row[46], row[47], row[48], row[49],  # UPD_USER, UPD_YMD, MOLD_CYCLE_IE, CT_MOLD_IE, PLAN_TYPE
                row[50], row[51], row[52], row[53], row[54],  # PLAN_SHIFT, HH_SEQ, SHORT_NAME, L_PRS_QTY, R_PRS_QTY
                row[55], row[56], row[57], row[58], row[59],  # MCS_1_NAME, MCS_2_NAME, COLOR_1_NAME, COLOR_2_NAME, O_IPP_LINE_CD
                row[60], row[61], row[62], row[63], row[64],  # O_MACHINE_NAME, O_STATION_CD, O_CNT_QTY, O_OSND_PRS_QTY, O_OSND_CNT_QTY
                row[65], row[66], row[67], row[68], row[69],  # O_MOLD_ID, O_COLOR_NAME, O_LINE_CD, O_SS_ID, PLAN_DATE
                extract_time  # etl_extract_time만 전달, etl_ingest_time은 PostgreSQL DEFAULT now() 사용
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "version_id", "plant_cd", "resource_cd", "pcard_id", "ymd",
        "hh", "start_date", "end_date", "zone_cd", "ipp_line_cd",
        "machine_cd", "station_type", "model_cd", "style_cd", "cs_size",
        "prs_qty", "mold_cd", "mold_size_cd", "prs_per_press", "mold_cycle",
        "ss_per_prs", "jc_time", "jc_type", "resource_type", "status",
        "mcs_1_cd", "mcs_2_cd", "mcs_3_cd", "mcs_4_cd", "mcs_5_cd",
        "color_1_cd", "color_2_cd", "color_3_cd", "color_4_cd", "color_5_cd",
        "uv_type_cd", "dual_part_yn", "op_cd", "wc_cd", "mline_cd",
        "asy_ymd", "asy_hh", "so_id", "ss_id", "sort_key",
        "upd_user", "upd_ymd", "mold_cycle_ie", "ct_mold_ie", "plan_type",
        "plan_shift", "hh_seq", "short_name", "l_prs_qty", "r_prs_qty",
        "mcs_1_name", "mcs_2_name", "color_1_name", "color_2_name", "o_ipp_line_cd",
        "o_machine_name", "o_station_cd", "o_cnt_qty", "o_osnd_prs_qty", "o_osnd_cnt_qty",
        "o_mold_id", "o_color_name", "o_line_cd", "o_ss_id", "plan_date",
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
    insert_data = prepare_insert_data(data, extract_time)
    columns = get_column_names()
    # Primary Key: (VERSION_ID, RESOURCE_CD, PLANT_CD, PCARD_ID, YMD, HH, HH_SEQ)
    conflict_columns = ["version_id", "resource_cd", "plant_cd", "pcard_id", "ymd", "hh", "hh_seq"]
    
    pg.insert_data(schema_name, table_name, insert_data, columns, conflict_columns)
    logging.info(f"✅ {len(data)} rows inserted (duplicates ignored).")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(increment_key, end_extract_time)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_extract_time}")

