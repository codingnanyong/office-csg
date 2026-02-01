"""
IP Good Product Common Functions
=================================
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
PRODUCTION_POSTGRES_CONN_ID = "pg_jj_production_dw"  # MC, RST 테이블용
QUALITY_POSTGRES_CONN_ID = "pg_jj_quality_dw"        # OSND 테이블, Target 테이블용

# Source Tables
SOURCE_SCHEMA = "bronze"
MC_TABLE = "ipi_mc_output_v2_raw"      # pg_jj_production_dw
RST_TABLE = "smp_ss_ipi_rst_raw"        # pg_jj_production_dw
OSND_TABLE = "mspq_in_osnd_bt_raw"     # pg_jj_quality_dw

# Target Table
TARGET_SCHEMA = "silver"
TARGET_TABLE = "ipi_good_product"       # pg_jj_quality_dw

# Incremental Configuration
INCREMENT_KEY = "ipi_good_product_last_date"
INITIAL_START_DATE = datetime(2025, 7, 1)
DAYS_OFFSET_FOR_INCREMENTAL = 2  # -2일 전까지 (incremental DAG 시작점)


# ────────────────────────────────────────────────────────────────
# Data Extraction
# ────────────────────────────────────────────────────────────────
def extract_mc_data(pg_prod: PostgresHelper, start_time: str, end_time: str) -> pd.DataFrame:
    """MC 테이블 데이터 추출 (pg_jj_production_dw)"""
    logging.info(f"1️⃣ MC 테이블 데이터 추출 중: {start_time} ~ {end_time}")
    
    # MC 테이블의 rst_ymd는 VARCHAR(30)이고 '2025-07-16 00:00:00.418010' 형식
    # 원래 조건: rst_ymd BETWEEN :start_time AND :end_time
    # DATE(CAST(rst_ymd AS TIMESTAMP))로 날짜만 추출하여 비교
    sql = f"""
        SELECT 
            mc_cd, st_num, st_side, act_qty, rst_ymd, upd_so_id
        FROM {SOURCE_SCHEMA}.{MC_TABLE}
        WHERE DATE(CAST(rst_ymd AS TIMESTAMP)) BETWEEN DATE('{start_time}') AND DATE('{end_time}')
    """
    
    data = pg_prod.execute_query(sql, task_id="extract_mc_data", xcom_key=None)
    
    if data:
        # tuple 리스트를 DataFrame으로 변환
        df = pd.DataFrame(data, columns=['mc_cd', 'st_num', 'st_side', 'act_qty', 'rst_ymd', 'upd_so_id'])
        df.columns = df.columns.str.lower()
    else:
        df = pd.DataFrame(columns=['mc_cd', 'st_num', 'st_side', 'act_qty', 'rst_ymd', 'upd_so_id'])
    
    logging.info(f"✅ MC 테이블 추출 완료: {len(df):,} rows")
    return df


def extract_rst_data(pg_prod: PostgresHelper, start_time: str, end_time: str) -> pd.DataFrame:
    """실적 테이블 데이터 추출 (pg_jj_production_dw)"""
    logging.info(f"2️⃣ 실적 테이블 데이터 추출 중: {start_time} ~ {end_time}")
    
    # 컬럼명 조회를 위한 쿼리
    col_sql = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{SOURCE_SCHEMA}' 
          AND table_name = '{RST_TABLE}'
        ORDER BY ordinal_position
    """
    columns = pg_prod.execute_query(col_sql, task_id="get_rst_columns", xcom_key=None)
    column_names = [col[0].lower() for col in columns] if columns else []
    
    # RST 테이블의 start_date, end_date는 TIMESTAMP 타입
    # 원래 조건: start_date BETWEEN :start_time AND :end_time OR end_date BETWEEN :start_time AND :end_time
    sql = f"""
        SELECT *
        FROM {SOURCE_SCHEMA}.{RST_TABLE}
        WHERE start_date BETWEEN TIMESTAMP '{start_time} 00:00:00' AND TIMESTAMP '{end_time} 23:59:59'
           OR end_date BETWEEN TIMESTAMP '{start_time} 00:00:00' AND TIMESTAMP '{end_time} 23:59:59'
    """
    
    data = pg_prod.execute_query(sql, task_id="extract_rst_data", xcom_key=None)
    
    if data:
        df = pd.DataFrame(data, columns=column_names)
    else:
        df = pd.DataFrame(columns=column_names)
    
    logging.info(f"✅ 실적 테이블 추출 완료: {len(df):,} rows")
    return df


def extract_osnd_data(pg_quality: PostgresHelper, start_time: str, end_time: str) -> pd.DataFrame:
    """OSND 테이블 데이터 추출 (pg_jj_quality_dw)"""
    logging.info(f"3️⃣ OSND 테이블 데이터 추출 중: {start_time} ~ {end_time}")
    
    # 컬럼명 조회를 위한 쿼리
    col_sql = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{SOURCE_SCHEMA}' 
          AND table_name = '{OSND_TABLE}'
        ORDER BY ordinal_position
    """
    columns = pg_quality.execute_query(col_sql, task_id="get_osnd_columns", xcom_key=None)
    column_names = [col[0].lower() for col in columns] if columns else []
    
    # OSND 테이블의 osnd_dt는 TIMESTAMP 타입
    # 원래 조건: osnd_dt BETWEEN :start_time AND :end_time
    sql = f"""
        SELECT *
        FROM {SOURCE_SCHEMA}.{OSND_TABLE}
        WHERE osnd_dt BETWEEN TIMESTAMP '{start_time} 00:00:00' AND TIMESTAMP '{end_time} 23:59:59'
    """
    
    data = pg_quality.execute_query(sql, task_id="extract_osnd_data", xcom_key=None)
    
    if data:
        df = pd.DataFrame(data, columns=column_names)
    else:
        df = pd.DataFrame(columns=column_names)
    
    logging.info(f"✅ OSND 테이블 추출 완료: {len(df):,} rows")
    return df


# ────────────────────────────────────────────────────────────────
# Data Transformation
# ────────────────────────────────────────────────────────────────
def transform_and_join_data(mc_df: pd.DataFrame, rst_df: pd.DataFrame) -> pd.DataFrame:
    """데이터 변환 및 조인"""
    logging.info("4️⃣ 데이터 변환 및 조인 중...")
    
    # 날짜 형식 변환
    mc_df['rst_ymd'] = pd.to_datetime(mc_df['rst_ymd'], errors='coerce').dt.round('s')
    
    # 자료형 정리: 조인을 위한 키값 정수형 변환
    mc_df['upd_so_id'] = pd.to_numeric(mc_df['upd_so_id'], errors='coerce')
    rst_df['so_id'] = pd.to_numeric(rst_df['so_id'], errors='coerce')
    
    # 필요한 열만 추출하여 조인용 데이터프레임 구성
    good_product_join_cols = rst_df[['so_id', 'mold_id', 'mold_bar_key']].copy()
    
    # 조인 수행 (left join)
    merged_df = mc_df.merge(
        good_product_join_cols, 
        how='left', 
        left_on='upd_so_id', 
        right_on='so_id'
    )
    
    # 열 이름 변경
    merged_df = merged_df.rename(columns={
        'mold_id': 'mold_cd',
        'mold_bar_key': 'mold_id'
    })
    
    logging.info(f"✅ 조인 완료: {len(merged_df):,} rows")
    return merged_df


def remove_defective_products(
    merged_df: pd.DataFrame,
    osnd_df: pd.DataFrame,
    rst_df: pd.DataFrame
) -> tuple:
    """불량 제품 제거"""
    logging.info("5️⃣ 불량 제품 제거 중...")
    
    # 날짜 형식 변환
    osnd_df['osnd_dt'] = pd.to_datetime(osnd_df['osnd_dt'], errors='coerce')
    rst_df['start_date'] = pd.to_datetime(rst_df['start_date'], errors='coerce')
    rst_df['end_date'] = pd.to_datetime(rst_df['end_date'], errors='coerce')
    
    # 불량 SO_ID 수집
    bad_so_ids = []
    
    for _, row in osnd_df.iterrows():
        machine = row['machine_cd']
        mold_id = row['mold_id']
        osnd_time = row['osnd_dt']
        
        # machine_cd, mold_id 일치하는 rst row들
        candidates = rst_df[
            (rst_df['machine_cd'] == machine) &
            (rst_df['mold_bar_key'] == mold_id)
        ]
        
        # 시간 범위 내에 포함되는 행 찾기 (osnd_dt -3h ~ -1h)
        for _, rst_row in candidates.iterrows():
            for h in range(1, 4):  # 1~3 시간 차이 순회
                target_time = osnd_time - timedelta(hours=h)
                if rst_row['start_date'] <= target_time <= rst_row['end_date']:
                    bad_so_ids.append(rst_row['so_id'])
                    break  # 가장 이른 시간만 고려
    
    # 중복 제거
    bad_so_ids = set(bad_so_ids)
    logging.info(f"✅ 불량 SO_ID 발견: {len(bad_so_ids):,}개")
    
    # 제거 전 길이
    original_len = len(merged_df)
    
    # mold_id 누락된 행 제거 및 불량 제품 제거
    merged_df = merged_df.dropna(subset=['mold_id'])
    clean_df = merged_df[~merged_df['upd_so_id'].isin(bad_so_ids)].copy()
    
    removed_count = original_len - len(clean_df)
    removed_ratio = removed_count / original_len * 100 if original_len > 0 else 0
    
    logging.info(f"✅ 불량 제품 제거 완료")
    logging.info(f"📊 원본: {original_len:,} rows")
    logging.info(f"📊 제거: {removed_count:,} rows ({removed_ratio:.2f}%)")
    logging.info(f"📊 최종: {len(clean_df):,} rows")
    
    return clean_df, original_len, removed_count, removed_ratio


# ────────────────────────────────────────────────────────────────
# Data Loading
# ────────────────────────────────────────────────────────────────
def load_to_silver(pg_quality: PostgresHelper, clean_df: pd.DataFrame) -> None:
    """Silver 테이블에 데이터 적재"""
    logging.info("6️⃣ Silver 테이블 적재 중...")
    
    # 테이블 존재 확인
    table_exists = pg_quality.check_table(TARGET_SCHEMA, TARGET_TABLE)
    
    if not table_exists:
        logging.warning(f"⚠️ 테이블이 존재하지 않습니다: {TARGET_SCHEMA}.{TARGET_TABLE}")
        logging.warning("⚠️ 테이블을 먼저 생성해주세요: /home/user/apps/airflow/db/quality/silver/ip_good_product.sql")
    
    # etl_ingest_time 추가
    if 'etl_ingest_time' not in clean_df.columns:
        clean_df['etl_ingest_time'] = datetime.now()
    
    # 결과에는 upd_so_id 제외 (so_id만 유지)
    if 'upd_so_id' in clean_df.columns:
        clean_df = clean_df.drop(columns=['upd_so_id'])

    # DataFrame을 tuple 리스트로 변환
    data_tuples = [tuple(row) for row in clean_df.values]
    columns = clean_df.columns.tolist()
    
    # 데이터 적재
    logging.info(f"📦 데이터 적재 중: {len(clean_df):,} rows")
    pg_quality.insert_data(
        schema_name=TARGET_SCHEMA,
        table_name=TARGET_TABLE,
        data=data_tuples,
        columns=columns,
        conflict_columns=['mc_cd', 'st_num', 'st_side', 'rst_ymd', 'so_id']  # Primary Key
    )
    
    logging.info(f"✅ Silver 테이블 적재 완료: {TARGET_SCHEMA}.{TARGET_TABLE}")


# ────────────────────────────────────────────────────────────────
# Variable Management
# ────────────────────────────────────────────────────────────────
def update_variable(end_date: str) -> None:
    """Update Airflow variable with last processed date"""
    Variable.set(INCREMENT_KEY, end_date)
    logging.info(f"📌 Variable `{INCREMENT_KEY}` Update: {end_date}")


# ────────────────────────────────────────────────────────────────
# Incremental Logic
# ────────────────────────────────────────────────────────────────
def incremental_ip_good_product(**context):
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
    logging.info(f"🚀 IP Good Product Incremental 시작")
    logging.info(f"{'='*60}")
    logging.info(f"📅 처리 날짜: {start_time} (전일 데이터)")
    
    try:
        # 데이터 추출
        mc_df = extract_mc_data(pg_prod, start_time, end_time)
        rst_df = extract_rst_data(pg_prod, start_time, end_time)
        osnd_df = extract_osnd_data(pg_quality, start_time, end_time)
        
        # 데이터 변환 및 조인
        merged_df = transform_and_join_data(mc_df, rst_df)
        
        # 불량 제품 제거
        clean_df, original_len, removed_count, removed_ratio = remove_defective_products(
            merged_df, osnd_df, rst_df
        )
        
        # Silver 테이블 적재
        if len(clean_df) > 0:
            load_to_silver(pg_quality, clean_df)
            logging.info(f"✅ Incremental 완료: {start_time} ({len(clean_df):,} rows)")
        else:
            logging.info(f"⚠️ Incremental 데이터 없음: {start_time}")
        
        # Variable 업데이트
        update_variable(start_time)
        
        logging.info(f"\n{'='*60}")
        logging.info(f"✅ Incremental 완료")
        logging.info(f"{'='*60}")
        logging.info(f"📅 처리 날짜: {start_time}")
        logging.info(f"📊 원본 데이터: {original_len:,} rows")
        logging.info(f"📊 최종 데이터: {len(clean_df):,} rows")
        logging.info(f"📊 제거된 데이터: {removed_count:,} rows ({removed_ratio:.2f}%)")
        logging.info(f"{'='*60}")
        
        return {
            "status": "success",
            "date": start_time,
            "original_rows": original_len,
            "final_rows": len(clean_df),
            "removed_rows": removed_count,
            "removed_ratio": removed_ratio
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
        # 데이터 추출
        mc_df = extract_mc_data(pg_prod, start_time, end_time)
        rst_df = extract_rst_data(pg_prod, start_time, end_time)
        osnd_df = extract_osnd_data(pg_quality, start_time, end_time)
        
        # 데이터 변환 및 조인
        merged_df = transform_and_join_data(mc_df, rst_df)
        
        # 불량 제품 제거
        clean_df, original_len, removed_count, removed_ratio = remove_defective_products(
            merged_df, osnd_df, rst_df
        )
        
        # Silver 테이블 적재
        if len(clean_df) > 0:
            load_to_silver(pg_quality, clean_df)
            logging.info(f"✅ 배치 완료: {start_time} ({len(clean_df)} rows)")
        else:
            logging.info(f"배치에 데이터 없음: {start_time}")
        
        # Variable 업데이트
        update_variable(start_time)
        
        return {
            "loop": loop_count,
            "date": start_time,
            "original_rows": original_len,
            "final_rows": len(clean_df),
            "removed_rows": removed_count,
            "status": "success"
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


def backfill_daily_batch_task(**kwargs) -> dict:
    """Main backfill task for daily batch processing"""
    pg_prod = PostgresHelper(conn_id=PRODUCTION_POSTGRES_CONN_ID)
    pg_quality = PostgresHelper(conn_id=QUALITY_POSTGRES_CONN_ID)
    
    # Get start date from variable or use initial date
    last_date_str = Variable.get(INCREMENT_KEY, default_var=None)
    if not last_date_str:
        start_date = INITIAL_START_DATE
        logging.info(f"초기 시작 날짜 사용: {start_date}")
    else:
        start_date = datetime.strptime(last_date_str, '%Y-%m-%d')
        logging.info(f"이전 진행 지점 사용: {start_date}")
    
    # Calculate end date (today - 2 days)
    end_date = (datetime.now() - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    
    # Calculate expected days
    expected_days = (end_date - start_date).days
    
    # Log backfill information
    logging.info(f"Backfill 시작: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    logging.info(f"배치 크기: 일별 (하루씩 처리)")
    logging.info(f"예상 루프 횟수: {expected_days}회 (일별)")
    logging.info(f"⚠️ 현재 시간에서 {DAYS_OFFSET_FOR_INCREMENTAL}일 전으로 설정 (incremental DAG 시작점)")
    
    # Process daily batches
    results = []
    total_original_rows = 0
    total_final_rows = 0
    total_removed_rows = 0
    loop_count = 0
    current_date = start_date
    
    while current_date < end_date:
        loop_count += 1
        
        # Process batch
        batch_result = process_daily_batch(
            pg_prod, pg_quality, current_date, loop_count, expected_days
        )
        
        results.append(batch_result)
        
        if batch_result.get("status") == "success":
            total_original_rows += batch_result.get("original_rows", 0)
            total_final_rows += batch_result.get("final_rows", 0)
            total_removed_rows += batch_result.get("removed_rows", 0)
        
        # Move to next day
        current_date += timedelta(days=1)
    
    # Log completion
    logging.info(f"🎉 Backfill 완료! 총 {loop_count}회 루프, {total_final_rows}개 rows 수집")
    if results:
        logging.info(f"처리 기간: {results[0]['date']} ~ {results[-1]['date']}")
        logging.info(f"📊 총 원본 데이터: {total_original_rows:,} rows")
        logging.info(f"📊 총 최종 데이터: {total_final_rows:,} rows")
        logging.info(f"📊 총 제거된 데이터: {total_removed_rows:,} rows")
    
    return {
        "status": "backfill_completed",
        "total_loops": loop_count,
        "total_days": len(results),
        "total_original_rows": total_original_rows,
        "total_final_rows": total_final_rows,
        "total_removed_rows": total_removed_rows,
        "results": results
    }