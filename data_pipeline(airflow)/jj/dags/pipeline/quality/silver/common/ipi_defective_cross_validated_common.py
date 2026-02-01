"""
IPI Defective Cross Validated Common Functions
===============================================
공통 함수 및 설정을 모아둔 모듈
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────

# Database Configuration
PRODUCTION_POSTGRES_CONN_ID = "pg_jj_production_dw"  # MMS 테이블용
QUALITY_POSTGRES_CONN_ID = "pg_jj_quality_dw"        # OSND, IPI_DMS, Target 테이블용

# Source Tables
MMS_SOURCE_SCHEMA = "silver"
MMS_SOURCE_TABLE = "ip_mold_mc_inout"  # pg_jj_production_dw

OSND_SOURCE_SCHEMA = "bronze"
OSND_SOURCE_TABLE = "mspq_in_osnd_bt_raw"  # pg_jj_quality_dw

IPI_DMS_SOURCE_SCHEMA = "silver"
IPI_DMS_SOURCE_TABLE = "ipi_defective_time_corrected"  # pg_jj_quality_dw

# Target Table
TARGET_SCHEMA = "silver"
TARGET_TABLE = "ipi_defective_cross_validated"  # pg_jj_quality_dw

# Incremental Configuration
INCREMENT_KEY = "ipi_defective_cross_validated_last_date"  # backfill과 공용
INITIAL_START_DATE = datetime(2025, 7, 1)
DAYS_OFFSET_FOR_INCREMENTAL = 2  # -2일 전까지 (incremental DAG 시작점)


# ────────────────────────────────────────────────────────────────
# Data Extraction
# ────────────────────────────────────────────────────────────────
def extract_mms_data(pg_prod: PostgresHelper, start_time: str, end_time: str) -> pd.DataFrame:
    """MMS 데이터 추출 (silver.ip_mold_mc_inout)"""
    logging.info(f"1️⃣ MMS 테이블 데이터 추출 중: {start_time} ~ {end_time}")
    
    # 날짜 형식 변환 (YYYY-MM-DD → YYYYMMDD)
    start_date_formatted = start_time.replace('-', '')
    end_date_formatted = end_time.replace('-', '')
    
    # mold_input_date 기준으로 필터링
    sql = f"""
        SELECT 
            workshop,
            machine,
            mold_id,
            mold_remove_date,
            mold_remove_time,
            mold_input_date,
            mold_input_time
        FROM {MMS_SOURCE_SCHEMA}.{MMS_SOURCE_TABLE}
        WHERE 
            workshop = 'IP'
            AND (
                (mold_input_date >= '{start_date_formatted}' AND mold_input_date <= '{end_date_formatted}')
                OR mold_remove_date IS NULL
            )
        ORDER BY mold_id, mold_input_date, mold_input_time
    """
    
    data = pg_prod.execute_query(sql, task_id="extract_mms_data", xcom_key=None)
    
    if data:
        # tuple 리스트를 DataFrame으로 변환
        df = pd.DataFrame(data, columns=[
            'workshop', 'machine', 'mold_id',
            'mold_remove_date', 'mold_remove_time',
            'mold_input_date', 'mold_input_time'
        ])
        df.columns = df.columns.str.lower()
    else:
        df = pd.DataFrame(columns=[
            'workshop', 'machine', 'mold_id',
            'mold_remove_date', 'mold_remove_time',
            'mold_input_date', 'mold_input_time'
        ])
    
    logging.info(f"✅ MMS 테이블 추출 완료: {len(df):,} rows")
    return df


def extract_osnd_data(pg_quality: PostgresHelper, start_time: str, end_time: str) -> pd.DataFrame:
    """OSND 데이터 추출 (bronze.mspq_in_osnd_bt_raw)"""
    logging.info(f"2️⃣ OSND 테이블 데이터 추출 중: {start_time} ~ {end_time}")
    
    # OSND 테이블의 osnd_dt는 TIMESTAMP 타입
    sql = f"""
        SELECT *
        FROM {OSND_SOURCE_SCHEMA}.{OSND_SOURCE_TABLE}
        WHERE osnd_dt BETWEEN TIMESTAMP '{start_time} 00:00:00' AND TIMESTAMP '{end_time} 23:59:59'
    """
    
    # 컬럼명 조회를 위한 쿼리
    col_sql = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{OSND_SOURCE_SCHEMA}' 
          AND table_name = '{OSND_SOURCE_TABLE}'
        ORDER BY ordinal_position
    """
    columns = pg_quality.execute_query(col_sql, task_id="get_osnd_columns", xcom_key=None)
    column_names = [col[0].lower() for col in columns] if columns else []
    
    data = pg_quality.execute_query(sql, task_id="extract_osnd_data", xcom_key=None)
    
    if data:
        df = pd.DataFrame(data, columns=column_names)
    else:
        df = pd.DataFrame(columns=column_names)
    
    logging.info(f"✅ OSND 테이블 추출 완료: {len(df):,} rows")
    return df


def extract_ipi_dms_data(pg_quality: PostgresHelper, start_time: str, end_time: str) -> pd.DataFrame:
    """IPI_DMS 데이터 추출 (silver.ipi_defective_time_corrected)"""
    logging.info(f"3️⃣ IPI_DMS 테이블 데이터 추출 중: {start_time} ~ {end_time}")
    
    # IPI_DMS 테이블의 osnd_dt는 TIMESTAMP 타입
    sql = f"""
        SELECT *
        FROM {IPI_DMS_SOURCE_SCHEMA}.{IPI_DMS_SOURCE_TABLE}
        WHERE osnd_dt BETWEEN TIMESTAMP '{start_time} 00:00:00' AND TIMESTAMP '{end_time} 23:59:59'
    """
    
    # 컬럼명 조회를 위한 쿼리
    col_sql = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{IPI_DMS_SOURCE_SCHEMA}' 
          AND table_name = '{IPI_DMS_SOURCE_TABLE}'
        ORDER BY ordinal_position
    """
    columns = pg_quality.execute_query(col_sql, task_id="get_ipi_dms_columns", xcom_key=None)
    column_names = [col[0].lower() for col in columns] if columns else []
    
    data = pg_quality.execute_query(sql, task_id="extract_ipi_dms_data", xcom_key=None)
    
    if data:
        df = pd.DataFrame(data, columns=column_names)
    else:
        df = pd.DataFrame(columns=column_names)
    
    logging.info(f"✅ IPI_DMS 테이블 추출 완료: {len(df):,} rows")
    return df


# ────────────────────────────────────────────────────────────────
# MMS Data Correction Logic
# ────────────────────────────────────────────────────────────────
def correct_mms_overlaps(df: pd.DataFrame) -> pd.DataFrame:
    """
    MMS 데이터 겹침 보정 로직
    - 같은 mold_id 내에서 finish > next_start인 경우 겹침으로 판단
    - 겹침 시 앞 구간의 finish를 next_start로 조정
    - 최대 5회 반복
    """
    logging.info("2️⃣ MMS 데이터 보정 및 Station 매칭 작업 시작")
    
    # 공백 문자열을 None 또는 NaT로 변환
    df['mold_remove_date'] = df['mold_remove_date'].replace(' ', pd.NA)
    df['mold_remove_time'] = df['mold_remove_time'].replace(' ', pd.NA)
    
    # Mold Remove Date와 Mold Remove Time이 비어 있는 경우 현재 날짜와 시간으로 채우기
    current_date = datetime.now().strftime('%Y%m%d')  # 'YYYYMMDD' 형식의 문자열
    current_time = datetime.now().strftime('%H:%M')   # 'HH:MM' 형식의 문자열
    
    df['mold_remove_date'] = df['mold_remove_date'].fillna(current_date).astype(str)
    df['mold_remove_time'] = df['mold_remove_time'].fillna(current_time)
    
    # 날짜 형식 정규화 (문자열로 변환 후 datetime 변환)
    df['mold_remove_date'] = df['mold_remove_date'].astype(str)
    df['mold_input_date'] = df['mold_input_date'].astype(str)
    df['mold_input_time'] = df['mold_input_time'].astype(str)
    df['mold_remove_time'] = df['mold_remove_time'].astype(str)
    
    # 날짜와 시간을 합쳐 datetime 형식으로 변환
    df['start'] = pd.to_datetime(df['mold_input_date'] + ' ' + df['mold_input_time'], errors='coerce')
    df['finish'] = pd.to_datetime(df['mold_remove_date'] + ' ' + df['mold_remove_time'], errors='coerce')
    
    # 새로 정렬한 df 생성
    df_sorted = df.sort_values(by=['mold_id', 'start']).reset_index(drop=True)
    
    # 겹침 보정 반복 처리
    iter_cnt = 0
    max_iters = 5
    
    while True:
        iter_cnt += 1
        overlaps = []  # 매 반복마다 반드시 초기화
        
        # 한 턴 보정
        for i in range(len(df_sorted) - 1):
            same_mold = df_sorted.iloc[i]['mold_id'] == df_sorted.iloc[i + 1]['mold_id']
            
            if not same_mold:
                continue
            
            current_finish = df_sorted.iloc[i]['finish']
            next_start = df_sorted.iloc[i + 1]['start']
            
            # NaN 체크
            if pd.isna(current_finish) or pd.isna(next_start):
                continue
            
            # 겹침 판단
            if current_finish > next_start:
                # 로그용 기록
                overlaps.append({
                    'mold_id': df_sorted.iloc[i]['mold_id'],
                    'Overlapping Entries': f"i finish {current_finish} & i+1 start {next_start}",
                    'Overlapping Time': current_finish - next_start
                })
                
                # 보정: 앞 구간의 Finish를 다음 Start로 절단
                df_sorted.at[i, 'finish'] = next_start
        
        # 이번 턴 결과 출력
        logging.info(f"[Iteration {iter_cnt}] overlaps found: {len(overlaps)}")
        
        # 겹침이 없으면 종료
        if len(overlaps) == 0:
            break
        
        # 안전장치
        if iter_cnt >= max_iters:
            logging.warning("⚠️ 무한 루프, 데이터 확인 필요")
            break
    
    # 마지막 반복의 겹침(없어야 정상) 요약 출력
    if overlaps:
        overlap_df = pd.DataFrame(overlaps)
        logging.warning(f"⚠️ 최종 겹침 데이터: {len(overlaps)}건")
        logging.debug(overlap_df.head(10))
    else:
        logging.info("✅ 보정작업 완료!")
    
    logging.info(f"✅ MMS 데이터 보정 완료: {len(df_sorted):,} rows")
    return df_sorted


# ────────────────────────────────────────────────────────────────
# OSND Station Matching Logic
# ────────────────────────────────────────────────────────────────
def extract_station_from_machine(machine_str: str) -> pd.Series:
    """Machine 문자열에서 Station 정보 추출"""
    try:
        if pd.isna(machine_str) or machine_str is None:
            return pd.Series([None, None])
        parts = str(machine_str).split('-')
        if len(parts) >= 2:
            return pd.Series([parts[-2], parts[-1]])
        else:
            return pd.Series([None, None])
    except Exception:
        return pd.Series([None, None])


def match_osnd_with_station(mms_df: pd.DataFrame, osnd_df: pd.DataFrame) -> pd.DataFrame:
    """
    OSND 데이터와 MMS 데이터를 매칭하여 Station 정보 추가
    - MMS 데이터에서 Station 정보 추출 (machine 컬럼 파싱)
    - OSND의 mold_id와 osnd_dt를 기준으로 MMS의 start/finish 범위와 매칭
    """
    logging.info("3️⃣ OSND와 Station 매칭 작업 시작")
    
    if len(osnd_df) == 0:
        logging.warning("⚠️ OSND 데이터가 없어 매칭을 건너뜁니다.")
        return osnd_df
    
    if len(mms_df) == 0:
        logging.warning("⚠️ MMS 데이터가 없어 매칭을 건너뜁니다.")
        # Station 컬럼을 None으로 채워서 반환
        osnd_df['station'] = None
        osnd_df['station_rl'] = None
        return osnd_df
    
    # MMS 데이터에서 Station 정보 추출
    mms_df[['station', 'station_rl']] = mms_df['machine'].apply(extract_station_from_machine)
    
    # MMS 데이터에서 불필요한 컬럼 제거 (start, finish는 유지)
    mms_df_clean = mms_df.drop(
        columns=['mold_remove_date', 'mold_remove_time', 'mold_input_date', 'mold_input_time'],
        errors='ignore'
    ).copy()
    
    # datetime 형식으로 변환 (이미 되어있을 수 있지만 확인)
    mms_df_clean['start'] = pd.to_datetime(mms_df_clean['start'], errors='coerce')
    mms_df_clean['finish'] = pd.to_datetime(mms_df_clean['finish'], errors='coerce')
    osnd_df['osnd_dt'] = pd.to_datetime(osnd_df['osnd_dt'], errors='coerce')
    
    # Station 매핑
    station_info = []
    matched_count = 0
    
    for idx, row in osnd_df.iterrows():
        mold_id = row['mold_id']
        osnd_time = row['osnd_dt']
        
        # NaN 체크
        if pd.isna(mold_id) or pd.isna(osnd_time):
            station_info.append([None, None])
            continue
        
        # MMS에서 매칭: 같은 mold_id이고 시간 범위 내에 있는 경우
        match = mms_df_clean[
            (mms_df_clean['mold_id'] == mold_id) &
            (mms_df_clean['start'] <= osnd_time) &
            (mms_df_clean['finish'] >= osnd_time)
        ]
        
        if not match.empty:
            # 첫 번째 매칭 결과 사용
            station_info.append(match.iloc[0][['station', 'station_rl']].values)
            matched_count += 1
        else:
            station_info.append([None, None])
    
    station_df = pd.DataFrame(station_info, columns=['station', 'station_rl'])
    
    # 병합 전에 중복 방지: 기존 Station 컬럼 제거
    osnd_df = osnd_df.drop(columns=['station', 'station_rl'], errors='ignore')
    
    # 병합 수행
    osnd_df = pd.concat([osnd_df.reset_index(drop=True), station_df], axis=1)
    osnd_df.columns = osnd_df.columns.str.lower()
    
    if len(osnd_df) > 0:
        match_ratio = matched_count / len(osnd_df) * 100
        logging.info(f"✅ OSND Station 매칭 완료: {len(osnd_df):,} rows 중 {matched_count:,} rows 매칭됨 ({match_ratio:.2f}%)")
    else:
        logging.info(f"✅ OSND Station 매칭 완료: 0 rows")
    return osnd_df


# ────────────────────────────────────────────────────────────────
# Cross Check Defective Logic
# ────────────────────────────────────────────────────────────────
def cross_check_defective(osnd_df: pd.DataFrame, ipi_dms_df: pd.DataFrame) -> pd.DataFrame:
    """
    IPI_DMS와 OSND 데이터를 Cross Check하여 완전 일치 데이터만 필터링
    - osnd_id 기준으로 내부 조인
    - machine_cd, station, st_lr_cd 비교하여 매칭 여부 확인
    - 완전 일치 (all_match & valid_comparison)인 데이터만 필터링
    """
    logging.info("4️⃣ IPI_DMS와 OSND Cross Check 작업 시작")
    
    if len(osnd_df) == 0:
        logging.warning("⚠️ OSND 데이터가 없어 Cross Check를 건너뜁니다.")
        return osnd_df
    
    if len(ipi_dms_df) == 0:
        logging.warning("⚠️ IPI_DMS 데이터가 없어 Cross Check를 건너뜁니다.")
        return pd.DataFrame()
    
    # osnd_id 자료형 통일 (문자열로 변환)
    osnd_df['osnd_id'] = osnd_df['osnd_id'].astype(str)
    ipi_dms_df['osnd_id'] = ipi_dms_df['osnd_id'].astype(str)
    
    # osnd_id 기준 내부 조인
    merged_df = pd.merge(ipi_dms_df, osnd_df, on='osnd_id', suffixes=('_ipi', ''))
    
    logging.info(f"📊 조인 결과: {len(merged_df):,} rows (IPI_DMS: {len(ipi_dms_df):,}, OSND: {len(osnd_df):,})")
    
    if len(merged_df) == 0:
        logging.warning("⚠️ 조인 결과가 없습니다.")
        return pd.DataFrame()
    
    # 각 열 비교 (NaN 제외)
    merged_df['machine_cd_match'] = (
        (merged_df['machine_cd_ipi'] == merged_df['machine_cd']) &
        merged_df['machine_cd_ipi'].notna() &
        merged_df['machine_cd'].notna()
    )
    
    merged_df['station_match'] = (
        (merged_df['station_ipi'] == merged_df['station']) &
        merged_df['station_ipi'].notna() &
        merged_df['station'].notna()
    )
    
    merged_df['st_lr_cd_match'] = (
        (merged_df['st_lr_cd'] == merged_df['station_rl']) &
        merged_df['st_lr_cd'].notna() &
        merged_df['station_rl'].notna()
    )
    
    # 세 항목 모두 매칭
    merged_df['all_match'] = (
        merged_df['machine_cd_match'] &
        merged_df['station_match'] &
        merged_df['st_lr_cd_match']
    )
    
    # NaN이 하나라도 있으면 valid_comparison = False
    merged_df['valid_comparison'] = (
        merged_df[['machine_cd_ipi', 'machine_cd', 'station_ipi', 'station',
                   'st_lr_cd', 'station_rl']].notna().all(axis=1)
    )
    
    # 매칭 요약
    valid_matches = merged_df[merged_df['valid_comparison']]['all_match'].value_counts()
    
    logging.info("✅ IPI DMS와 OSND 매칭을 통한 데이터 검증 결과:")
    if len(valid_matches) > 0:
        matched_count = valid_matches.get(True, 0)
        unmatched_count = valid_matches.get(False, 0)
        logging.info(f"   - 유효 비교 중 매칭: {matched_count:,}건")
        logging.info(f"   - 유효 비교 중 미매칭: {unmatched_count:,}건")
    else:
        logging.info("   - 유효 비교 데이터 없음")
    
    # 유효하지 않은 비교 개수
    invalid_count = (~merged_df['valid_comparison']).sum()
    if invalid_count > 0:
        logging.info(f"   - NaN 포함으로 비교 제외된 행 수: {invalid_count:,}건")
    
    # 완전 일치 + 유효 비교
    matched_rows = merged_df[merged_df['all_match'] & merged_df['valid_comparison']]
    
    logging.info(f"📊 최종 매칭 결과: {len(matched_rows):,}건 (전체 조인 결과: {len(merged_df):,}건)")
    
    if len(matched_rows) == 0:
        logging.warning("⚠️ 완전 일치 데이터가 없습니다.")
        return pd.DataFrame()
    
    # 원본 OSND_df에서 해당 osnd_id만 추출
    matched_osnd_ids = matched_rows['osnd_id'].unique()
    matched_osnd_df = osnd_df[osnd_df['osnd_id'].isin(matched_osnd_ids)].copy()
    matched_osnd_df['osnd_dt'] = matched_osnd_df['osnd_id'].map(ipi_dms_df.set_index('osnd_id')['osnd_dt'])  
    
    logging.info(f"✅ Cross Check 완료: {len(matched_osnd_df):,} rows (원본 OSND 기준)")
    return matched_osnd_df


# ────────────────────────────────────────────────────────────────
# Data Loading
# ────────────────────────────────────────────────────────────────
def load_to_silver(pg_quality: PostgresHelper, df: pd.DataFrame) -> None:
    """Silver 테이블에 데이터 적재"""
    logging.info("5️⃣ Silver 테이블 적재 중...")
    
    # 테이블 존재 확인
    table_exists = pg_quality.check_table(TARGET_SCHEMA, TARGET_TABLE)
    
    if not table_exists:
        logging.warning(f"⚠️ 테이블이 존재하지 않습니다: {TARGET_SCHEMA}.{TARGET_TABLE}")
        logging.warning("⚠️ 테이블을 먼저 생성해주세요: /home/user/apps/airflow/db/quality/silver/ipi_defective_cross_validated.sql")
        return
    
    # etl_ingest_time 제거 (DB의 DEFAULT 값 사용)
    if 'etl_ingest_time' in df.columns:
        df = df.drop(columns=['etl_ingest_time'])
    
    # etl_extract_time 제거 (최종 테이블에는 없음)
    if 'etl_extract_time' in df.columns:
        df = df.drop(columns=['etl_extract_time'])
    
    # 100% NULL 가능성이 높은 컬럼 제거 (테이블에 정의되지 않은 컬럼)
    excluded_columns = [
        'memo', 'rework_date', 'ss_apply_date', 'ref_caption', 'ref_value01', 'ref_value02',
        'updater', 'update_dt', 'update_pc', 'repl_qty', 'repl_dt', 'repl_user', 'repl_cfm_dt', 'repl_cfm_user'
    ]
    for col in excluded_columns:
        if col in df.columns:
            df = df.drop(columns=[col])
            logging.debug(f"   제외된 컬럼: {col} (100% NULL)")
    
    # 테이블의 컬럼 순서에 맞춰 DataFrame 컬럼 재정렬
    col_order_sql = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{TARGET_SCHEMA}' 
          AND table_name = '{TARGET_TABLE}'
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
        schema_name=TARGET_SCHEMA,
        table_name=TARGET_TABLE,
        data=data_tuples,
        columns=columns,
        conflict_columns=['plant_cd', 'osnd_id']  # Primary Key
    )
    
    logging.info(f"✅ Silver 테이블 적재 완료: {TARGET_SCHEMA}.{TARGET_TABLE}")


# ────────────────────────────────────────────────────────────────
# Variable Management
# ────────────────────────────────────────────────────────────────
def update_variable(date_str: str) -> None:
    """Variable 업데이트"""
    Variable.set(INCREMENT_KEY, date_str)
    logging.info(f"📌 Variable `{INCREMENT_KEY}` Update: {date_str}")


# ────────────────────────────────────────────────────────────────
# Incremental Logic
# ────────────────────────────────────────────────────────────────
def incremental_ipi_defective_cross_validated(**context) -> dict:
    """
    Incremental 작업: 전일 데이터 처리
    """
    pg_prod = PostgresHelper(conn_id=PRODUCTION_POSTGRES_CONN_ID)
    pg_quality = PostgresHelper(conn_id=QUALITY_POSTGRES_CONN_ID)
    
    # 공용 Variable에서 마지막 처리일을 읽고, 그 다음날을 처리 대상으로 설정
    last_date_str = Variable.get(INCREMENT_KEY, default_var=None)
    
    # UTC 기준 날짜 계산 (날짜 비교를 위해 시간 제거)
    now_utc = datetime.utcnow()
    today_minus_1 = (now_utc - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    if last_date_str:
        try:
            last_date = datetime.strptime(last_date_str, '%Y-%m-%d')
            last_date = last_date.replace(hour=0, minute=0, second=0, microsecond=0)
        except Exception:
            # 형식 오류 시 안전하게 today-1로 재설정
            last_date = today_minus_1 - timedelta(days=1)
        target_date = last_date + timedelta(days=1)
    else:
        # Variable 미설정 시 today-1을 처리
        target_date = today_minus_1
    
    # today-1을 상한으로 캡 (같거나 작으면 처리)
    if target_date > today_minus_1:
        logging.info(f"✅ 최신 상태입니다. 처리할 날짜가 없습니다 (Variable 기준).")
        logging.info(f"   Variable last_date: {last_date_str}")
        logging.info(f"   target_date: {target_date.strftime('%Y-%m-%d')}")
        logging.info(f"   today_minus_1: {today_minus_1.strftime('%Y-%m-%d')}")
        return {"status": "up_to_date", "last_date": last_date_str or None}
    
    # 대상 일자 00:00:00 ~ 23:59:59
    start_time = target_date.strftime('%Y-%m-%d')
    end_time = target_date.strftime('%Y-%m-%d')
    
    logging.info(f"\n{'='*60}")
    logging.info(f"🚀 IPI Defective Cross Validated Incremental 시작")
    logging.info(f"{'='*60}")
    logging.info(f"📅 처리 날짜: {start_time} (전일 데이터)")
    
    try:
        # 1. MMS 데이터 추출
        mms_df = extract_mms_data(pg_prod, start_time, end_time)
        
        # 2. OSND 데이터 추출
        osnd_df = extract_osnd_data(pg_quality, start_time, end_time)
        
        # 3. IPI_DMS 데이터 추출
        ipi_dms_df = extract_ipi_dms_data(pg_quality, start_time, end_time)
        
        if len(mms_df) == 0 and len(osnd_df) == 0 and len(ipi_dms_df) == 0:
            logging.warning(f"⚠️ MMS/OSND/IPI_DMS 데이터가 없습니다: {start_time}")
            update_variable(start_time)
            return {
                "status": "success_no_data",
                "date": start_time,
                "rows_processed": 0
            }
        
        # 4. MMS 데이터 보정 (겹침 제거)
        corrected_mms_df = correct_mms_overlaps(mms_df) if len(mms_df) > 0 else pd.DataFrame()
        
        # 5. OSND와 Station 매칭
        osnd_with_station_df = match_osnd_with_station(corrected_mms_df, osnd_df) if len(osnd_df) > 0 else pd.DataFrame()
        
        # 6. IPI_DMS와 Cross Check
        final_osnd_df = cross_check_defective(osnd_with_station_df, ipi_dms_df) if len(osnd_with_station_df) > 0 and len(ipi_dms_df) > 0 else pd.DataFrame()
        
        # 7. Silver 테이블 적재
        if len(final_osnd_df) > 0:
            load_to_silver(pg_quality, final_osnd_df)
            logging.info(f"✅ Incremental 완료: {start_time} ({len(final_osnd_df):,} rows)")
        else:
            logging.info(f"⚠️ Incremental 데이터 없음: {start_time}")
        
        # Variable 업데이트
        update_variable(start_time)
        
        logging.info(f"\n{'='*60}")
        logging.info(f"✅ Incremental 완료")
        logging.info(f"{'='*60}")
        logging.info(f"📅 처리 날짜: {start_time}")
        logging.info(f"📊 MMS 원본 데이터: {len(mms_df):,} rows")
        logging.info(f"📊 MMS 보정 후 데이터: {len(corrected_mms_df):,} rows")
        logging.info(f"📊 OSND 원본 데이터: {len(osnd_df):,} rows")
        logging.info(f"📊 OSND Station 매칭 후 데이터: {len(osnd_with_station_df):,} rows")
        logging.info(f"📊 IPI_DMS 데이터: {len(ipi_dms_df):,} rows")
        logging.info(f"📊 최종 Cross Check 매칭 데이터: {len(final_osnd_df):,} rows")
        logging.info(f"{'='*60}")
        
        return {
            "status": "success",
            "date": start_time,
            "mms_original_rows": len(mms_df),
            "mms_corrected_rows": len(corrected_mms_df),
            "osnd_original_rows": len(osnd_df),
            "osnd_station_matched_rows": len(osnd_with_station_df),
            "ipi_dms_rows": len(ipi_dms_df),
            "final_matched_rows": len(final_osnd_df)
        }
        
    except Exception as e:
        logging.error(f"\n{'='*60}")
        logging.error(f"❌ Incremental 실패: {str(e)}")
        logging.error(f"{'='*60}")
        raise


# ────────────────────────────────────────────────────────────────
# Backfill Logic
# ────────────────────────────────────────────────────────────────
def process_daily_batch(
    pg_prod: PostgresHelper,
    pg_quality: PostgresHelper,
    start_date: datetime,
    loop_count: int,
    expected_days: int
) -> dict:
    """Process a single daily batch"""
    logging.info(f"🔄 루프 {loop_count}/{expected_days} 시작")
    
    start_time = start_date.strftime("%Y-%m-%d")
    end_time = start_date.strftime("%Y-%m-%d")
    
    logging.info(f"배치 처리 중: {start_time}")
    
    try:
        # 1. MMS 데이터 추출
        mms_df = extract_mms_data(pg_prod, start_time, end_time)
        
        # 2. OSND 데이터 추출
        osnd_df = extract_osnd_data(pg_quality, start_time, end_time)
        
        # 3. IPI_DMS 데이터 추출
        ipi_dms_df = extract_ipi_dms_data(pg_quality, start_time, end_time)
        
        if len(mms_df) == 0 and len(osnd_df) == 0 and len(ipi_dms_df) == 0:
            logging.warning(f"⚠️ MMS/OSND/IPI_DMS 데이터가 없습니다: {start_time}")
            update_variable(start_time)
            return {
                "loop": loop_count,
                "date": start_time,
                "status": "success_no_data",
                "rows_processed": 0
            }
        
        # 4. MMS 데이터 보정 (겹침 제거)
        corrected_mms_df = correct_mms_overlaps(mms_df) if len(mms_df) > 0 else pd.DataFrame()
        
        # 5. OSND와 Station 매칭
        osnd_with_station_df = match_osnd_with_station(corrected_mms_df, osnd_df) if len(osnd_df) > 0 else pd.DataFrame()
        
        # 6. IPI_DMS와 Cross Check
        final_osnd_df = cross_check_defective(osnd_with_station_df, ipi_dms_df) if len(osnd_with_station_df) > 0 and len(ipi_dms_df) > 0 else pd.DataFrame()
        
        # 7. Silver 테이블 적재
        if len(final_osnd_df) > 0:
            load_to_silver(pg_quality, final_osnd_df)
            logging.info(f"✅ 배치 완료: {start_time} ({len(final_osnd_df):,} rows)")
        else:
            logging.info(f"⚠️ 배치에 데이터 없음: {start_time}")
        
        # Variable 업데이트
        update_variable(start_time)
        
        return {
            "loop": loop_count,
            "date": start_time,
            "status": "success",
            "mms_original_rows": len(mms_df),
            "mms_corrected_rows": len(corrected_mms_df),
            "osnd_original_rows": len(osnd_df),
            "osnd_station_matched_rows": len(osnd_with_station_df),
            "ipi_dms_rows": len(ipi_dms_df),
            "final_matched_rows": len(final_osnd_df)
        }
        
    except Exception as e:
        logging.error(f"❌ 배치 실패: {start_time} - {str(e)}")
        update_variable(start_time)  # 실패해도 날짜는 업데이트
        return {
            "loop": loop_count,
            "date": start_time,
            "status": "failed",
            "error": str(e)
        }


def backfill_daily_batch(**context) -> dict:
    """
    Backfill 작업: 과거 데이터 처리 (일별 배치 루프)
    """
    pg_prod = PostgresHelper(conn_id=PRODUCTION_POSTGRES_CONN_ID)
    pg_quality = PostgresHelper(conn_id=QUALITY_POSTGRES_CONN_ID)
    
    # Variable에서 마지막 처리일 읽기
    last_date_str = Variable.get(INCREMENT_KEY, default_var=None)
    
    if not last_date_str:
        start_date = INITIAL_START_DATE
        logging.info(f"초기 시작 날짜 사용: {start_date}")
    else:
        try:
            start_date = datetime.strptime(last_date_str, '%Y-%m-%d')
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            start_date = start_date + timedelta(days=1)  # 다음날부터 시작
            logging.info(f"이전 진행 지점 사용: {last_date_str} → 다음날: {start_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            logging.warning(f"⚠️ Variable 파싱 오류: {e}, 초기 시작 날짜로 재설정")
            start_date = INITIAL_START_DATE
    
    # Calculate end date (today - 2 days)
    now_utc = datetime.utcnow()
    end_date = (now_utc - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    
    # Calculate expected days
    expected_days = (end_date - start_date).days
    
    # Log backfill information
    logging.info(f"\n{'='*60}")
    logging.info(f"🚀 OSI Defective Cross Validated Backfill 시작")
    logging.info(f"{'='*60}")
    logging.info(f"Backfill 시작: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    logging.info(f"배치 크기: 일별 (하루씩 처리)")
    logging.info(f"예상 루프 횟수: {expected_days}회 (일별)")
    logging.info(f"⚠️ 현재 시간에서 {DAYS_OFFSET_FOR_INCREMENTAL}일 전으로 설정 (incremental DAG 시작점)")
    logging.info(f"{'='*60}")
    
    if expected_days <= 0:
        logging.info(f"✅ 최신 상태입니다. 처리할 날짜가 없습니다.")
        logging.info(f"   start_date: {start_date.strftime('%Y-%m-%d')}")
        logging.info(f"   end_date: {end_date.strftime('%Y-%m-%d')}")
        return {"status": "up_to_date", "last_date": last_date_str or None}
    
    # Process daily batches
    results = []
    total_mms_original_rows = 0
    total_mms_corrected_rows = 0
    total_osnd_original_rows = 0
    total_osnd_station_matched_rows = 0
    total_ipi_dms_rows = 0
    total_final_matched_rows = 0
    loop_count = 0
    current_date = start_date
    
    while current_date <= end_date:
        loop_count += 1
        
        # Process batch
        batch_result = process_daily_batch(
            pg_prod, pg_quality, current_date, loop_count, expected_days
        )
        
        results.append(batch_result)
        
        if batch_result.get("status") == "success":
            total_mms_original_rows += batch_result.get("mms_original_rows", 0)
            total_mms_corrected_rows += batch_result.get("mms_corrected_rows", 0)
            total_osnd_original_rows += batch_result.get("osnd_original_rows", 0)
            total_osnd_station_matched_rows += batch_result.get("osnd_station_matched_rows", 0)
            total_ipi_dms_rows += batch_result.get("ipi_dms_rows", 0)
            total_final_matched_rows += batch_result.get("final_matched_rows", 0)
        
        # Move to next day
        current_date += timedelta(days=1)
    
    # Log completion
    logging.info(f"\n{'='*60}")
    logging.info(f"🎉 Backfill 완료! 총 {loop_count}회 루프")
    logging.info(f"{'='*60}")
    if results:
        logging.info(f"처리 기간: {results[0]['date']} ~ {results[-1]['date']}")
        logging.info(f"📊 총 MMS 원본 데이터: {total_mms_original_rows:,} rows")
        logging.info(f"📊 총 MMS 보정 후 데이터: {total_mms_corrected_rows:,} rows")
        logging.info(f"📊 총 OSND 원본 데이터: {total_osnd_original_rows:,} rows")
        logging.info(f"📊 총 OSND Station 매칭 후 데이터: {total_osnd_station_matched_rows:,} rows")
        logging.info(f"📊 총 IPI_DMS 데이터: {total_ipi_dms_rows:,} rows")
        logging.info(f"📊 총 최종 Cross Check 매칭 데이터: {total_final_matched_rows:,} rows")
        logging.info(f"{'='*60}")
    
    return {
        "status": "backfill_completed",
        "total_loops": loop_count,
        "total_days": len(results),
        "total_mms_original_rows": total_mms_original_rows,
        "total_mms_corrected_rows": total_mms_corrected_rows,
        "total_osnd_original_rows": total_osnd_original_rows,
        "total_osnd_station_matched_rows": total_osnd_station_matched_rows,
        "total_ipi_dms_rows": total_ipi_dms_rows,
        "total_final_matched_rows": total_final_matched_rows,
        "results": results
    }

