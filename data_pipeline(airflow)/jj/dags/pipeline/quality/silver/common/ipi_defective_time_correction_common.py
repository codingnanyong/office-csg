"""공통 함수 모듈 - IPI Defective Time Correction Silver"""
import logging
import pandas as pd
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
PRODUCTION_POSTGRES_CONN_ID = "pg_jj_production_dw"  # MC 테이블용
QUALITY_POSTGRES_CONN_ID = "pg_jj_quality_dw"        # IPI 테이블, Target 테이블용

# Source Tables
SOURCE_SCHEMA = "bronze"
MC_TABLE = "ipi_mc_output_v2_raw"                    # pg_jj_production_dw
IPI_TABLE = "mspq_in_osnd_bt_ipi_raw"                # pg_jj_quality_dw

# Target Table
TARGET_SCHEMA = "silver"
TARGET_TABLE = "ipi_defective_time_corrected"                  # pg_jj_quality_dw

# Configuration
DELTA_SEC_THRESHOLD = 600  # 600초 이하만 필터링
INITIAL_START_DATE = datetime(2025, 7, 1)
DAYS_OFFSET_FOR_INCREMENTAL = 2  # -2일 전까지 (incremental DAG 시작점)


# ════════════════════════════════════════════════════════════════
# 2️⃣ Data Extraction
# ════════════════════════════════════════════════════════════════

def extract_mc_data(pg_prod: PostgresHelper, start_time: str, end_time: str) -> pd.DataFrame:
    """MC 테이블 데이터 추출 (pg_jj_production_dw)"""
    logging.info(f"1️⃣ MC 테이블 데이터 추출 중: {start_time} ~ {end_time}")
    
    # MC 테이블의 rst_ymd는 VARCHAR(30)이고 '2025-07-16 00:00:00.418010' 형식
    sql = f"""
        SELECT 
            mc_cd, st_num, st_side, act_qty, rst_ymd, upd_so_id
        FROM {SOURCE_SCHEMA}.{MC_TABLE}
        WHERE DATE(CAST(rst_ymd AS TIMESTAMP)) BETWEEN DATE('{start_time}') AND DATE('{end_time}')
    """
    
    logging.info(f"🔍 MC 쿼리 실행: {sql}")
    
    data = pg_prod.execute_query(sql, task_id="extract_mc_data", xcom_key=None)
    
    if data:
        # tuple 리스트를 DataFrame으로 변환
        df = pd.DataFrame(data, columns=['mc_cd', 'st_num', 'st_side', 'act_qty', 'rst_ymd', 'upd_so_id'])
        df.columns = df.columns.str.lower()
    else:
        df = pd.DataFrame(columns=['mc_cd', 'st_num', 'st_side', 'act_qty', 'rst_ymd', 'upd_so_id'])
    
    logging.info(f"✅ MC 테이블 추출 완료: {len(df):,} rows")
    return df


def extract_ipi_data(pg_quality: PostgresHelper, start_time: str, end_time: str) -> pd.DataFrame:
    """IPI 테이블 데이터 추출 (pg_jj_quality_dw)"""
    logging.info(f"2️⃣ IPI 테이블 데이터 추출 중: {start_time} ~ {end_time}")
    
    # IPI 테이블의 osnd_dt는 TIMESTAMP 타입
    sql = f"""
        SELECT *
        FROM {SOURCE_SCHEMA}.{IPI_TABLE}
        WHERE osnd_dt BETWEEN TIMESTAMP '{start_time} 00:00:00' AND TIMESTAMP '{end_time} 23:59:59'
    """
    
    logging.info(f"🔍 IPI 쿼리 실행: {sql}")
    
    # 컬럼명 조회를 위한 쿼리
    col_sql = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{SOURCE_SCHEMA}' 
          AND table_name = '{IPI_TABLE}'
        ORDER BY ordinal_position
    """
    columns = pg_quality.execute_query(col_sql, task_id="get_ipi_columns", xcom_key=None)
    column_names = [col[0].lower() for col in columns] if columns else []
    
    data = pg_quality.execute_query(sql, task_id="extract_ipi_data", xcom_key=None)
    
    if data:
        df = pd.DataFrame(data, columns=column_names)
    else:
        df = pd.DataFrame(columns=column_names)
    
    logging.info(f"✅ IPI 테이블 추출 완료: {len(df):,} rows")
    return df


# ════════════════════════════════════════════════════════════════
# 3️⃣ Data Transformation & Time Matching
# ════════════════════════════════════════════════════════════════

def normalize_strings(mc_df: pd.DataFrame, ipi_df: pd.DataFrame) -> tuple:
    """문자열 정규화"""
    logging.info("3️⃣ 문자열 정규화 중...")
    
    # MC 데이터 정규화
    mc_df['mc_cd'] = mc_df['mc_cd'].str.strip()
    mc_df['st_num'] = mc_df['st_num'].str.zfill(2)
    mc_df['st_side'] = mc_df['st_side'].str.strip().str.upper()
    
    # IPI 데이터 정규화
    ipi_df['machine_cd'] = ipi_df['machine_cd'].str.strip()
    ipi_df['station'] = ipi_df['station'].str.zfill(2)
    ipi_df['st_lr_cd'] = ipi_df['st_lr_cd'].str.strip().str.upper()
    
    logging.info("✅ 문자열 정규화 완료")
    return mc_df, ipi_df


def parse_datetimes(mc_df: pd.DataFrame, ipi_df: pd.DataFrame) -> tuple:
    """시간 파싱"""
    logging.info("4️⃣ 시간 파싱 중...")
    
    # MC 데이터 시간 파싱
    mc_df['rst_ymd'] = pd.to_datetime(mc_df['rst_ymd'], errors='coerce')
    
    # IPI 데이터 시간 파싱
    ipi_df['osnd_dt'] = pd.to_datetime(ipi_df['osnd_dt'], errors='coerce')
    
    logging.info("✅ 시간 파싱 완료")
    return mc_df, ipi_df


def perform_time_matching(mc_df: pd.DataFrame, ipi_df: pd.DataFrame) -> pd.DataFrame:
    """시간 매칭 루프 수행"""
    logging.info("5️⃣ 시간 매칭 루프 수행 중...")
    
    # 원본 시간 복사 및 delta 초기화
    ipi_df['origin_dt'] = ipi_df['osnd_dt']
    ipi_df['delta_sec'] = 0.0
    
    matched_count = 0
    total_count = len(ipi_df)
    
    # 매칭 루프 수행
    for idx, row in ipi_df.iterrows():
        key_mc = row['machine_cd']
        key_st = row['station']
        key_side = row['st_lr_cd']
        tgt_time = row['osnd_dt']
        
        if pd.isnull(tgt_time):
            continue
        
        # MC 데이터에서 매칭 조건: 같은 mc_cd, st_num, st_side이고 rst_ymd가 osnd_dt 이전
        matched = mc_df[
            (mc_df['mc_cd'] == key_mc) &
            (mc_df['st_num'] == key_st) &
            (mc_df['st_side'] == key_side) &
            (mc_df['rst_ymd'] < tgt_time)
        ]
        
        if not matched.empty:
            matched = matched.copy()
            matched['time_diff'] = (tgt_time - matched['rst_ymd']).dt.total_seconds()
            best_match = matched.loc[matched['time_diff'].idxmin()]
            
            ipi_df.at[idx, 'osnd_dt'] = best_match['rst_ymd']
            ipi_df.at[idx, 'delta_sec'] = best_match['time_diff']
            matched_count += 1
        
        # 진행 상황 로깅 (1000건마다)
        if (idx + 1) % 1000 == 0:
            logging.info(f"   진행 중: {idx + 1:,}/{total_count:,} ({matched_count:,}건 매칭)")
    
    logging.info(f"✅ 시간 매칭 완료: {matched_count:,}/{total_count:,}건 매칭")
    return ipi_df


def filter_by_delta_threshold(ipi_df: pd.DataFrame, threshold: float = DELTA_SEC_THRESHOLD) -> tuple:
    """Delta 초과 범위 필터링"""
    logging.info(f"6️⃣ Delta 초과 범위 필터링 중 (threshold: {threshold}초)...")
    
    # 필터링된 결과 분리
    df_normal = ipi_df[ipi_df['delta_sec'] <= threshold].copy()
    df_exceed = ipi_df[ipi_df['delta_sec'] > threshold].copy()
    
    # 통계 출력
    logging.info(f"📊 정상 범위 (≤{threshold}초): {len(df_normal):,}건")
    logging.info(f"📊 초과 범위 (>{threshold}초): {len(df_exceed):,}건")
    
    if len(df_normal) > 0:
        logging.info(f"📊 Delta 통계 (정상 범위):")
        logging.info(f"   - 평균: {df_normal['delta_sec'].mean():.2f}초")
        logging.info(f"   - 중앙값: {df_normal['delta_sec'].median():.2f}초")
        logging.info(f"   - 최소: {df_normal['delta_sec'].min():.2f}초")
        logging.info(f"   - 최대: {df_normal['delta_sec'].max():.2f}초")
    
    return df_normal, df_exceed


# ════════════════════════════════════════════════════════════════
# 4️⃣ Data Loading
# ════════════════════════════════════════════════════════════════

def load_to_silver(
    pg_quality: PostgresHelper, 
    df: pd.DataFrame,
    target_schema: str = TARGET_SCHEMA,
    target_table: str = TARGET_TABLE
) -> None:
    """Silver 테이블에 데이터 적재"""
    logging.info("7️⃣ Silver 테이블 적재 중...")
    
    # 테이블 존재 확인
    table_exists = pg_quality.check_table(target_schema, target_table)
    
    if not table_exists:
        logging.warning(f"⚠️ 테이블이 존재하지 않습니다: {target_schema}.{target_table}")
        logging.warning("⚠️ 테이블을 먼저 생성해주세요: /home/user/apps/airflow/db/quality/silver/ipi_defective_time_corrected.sql")
    
    # etl_ingest_time 제거 (DB의 DEFAULT 값 사용)
    if 'etl_ingest_time' in df.columns:
        df = df.drop(columns=['etl_ingest_time'])
    
    # 100% NULL 컬럼 제거 (테이블에 정의되지 않은 컬럼)
    # 제외할 컬럼 리스트: memo, rework_date, ss_apply_date, ref_caption, ref_value01, ref_value02,
    #                   updater, update_dt, update_pc, repl_dt, repl_user, repl_cfm_dt, repl_cfm_user,
    #                   extra1_fld, extra2_fld, extra3_fld, extra4_fld, extra5_fld
    excluded_columns = [
        'memo', 'rework_date', 'ss_apply_date', 'ref_caption', 'ref_value01', 'ref_value02',
        'updater', 'update_dt', 'update_pc', 'repl_dt', 'repl_user', 'repl_cfm_dt', 'repl_cfm_user',
        'extra1_fld', 'extra2_fld', 'extra3_fld', 'extra4_fld', 'extra5_fld'
    ]
    for col in excluded_columns:
        if col in df.columns:
            df = df.drop(columns=[col])
            logging.debug(f"   제외된 컬럼: {col} (100% NULL)")
    
    # 테이블의 컬럼 순서에 맞춰 DataFrame 컬럼 재정렬
    # 테이블 컬럼 순서 조회
    col_order_sql = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{target_schema}' 
          AND table_name = '{target_table}'
        ORDER BY ordinal_position
    """
    table_columns = pg_quality.execute_query(col_order_sql, task_id="get_table_columns", xcom_key=None)
    table_column_names = [col[0].lower() for col in table_columns] if table_columns else []
    
    # DataFrame에 있는 컬럼만 선택하고 테이블 순서에 맞춰 정렬
    available_columns = [col for col in table_column_names if col in df.columns]
    df = df[available_columns]
    
    # DataFrame을 tuple 리스트로 변환
    data_tuples = [tuple(row) for row in df.values]
    columns = df.columns.tolist()
    
    # 데이터 적재
    logging.info(f"📦 데이터 적재 중: {len(df):,} rows")
    pg_quality.insert_data(
        schema_name=target_schema,
        table_name=target_table,
        data=data_tuples,
        columns=columns,
        conflict_columns=['plant_cd', 'osnd_id', 'shift_cd', 'osnd_date', 'resource_cd', 'mc_cd', 'station', 'st_lr_cd']  # Primary Key
    )
    
    logging.info(f"✅ Silver 테이블 적재 완료: {target_schema}.{target_table}")


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(increment_key: str, end_date: str) -> None:
    """Update Airflow variable with last processed date"""
    Variable.set(increment_key, end_date)
    logging.info(f"📌 Variable `{increment_key}` Update: {end_date}")

