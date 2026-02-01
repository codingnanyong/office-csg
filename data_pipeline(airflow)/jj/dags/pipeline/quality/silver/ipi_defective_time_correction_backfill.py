"""
IPI Time Correction Backfill DAG (Bronze → Silver)
====================================================
과거 데이터를 쌓는 Backfill DAG (-2일 전까지)

Source Tables:
- bronze.ipi_mc_output_v2_raw (pg_jj_production_dw)
- bronze.mspq_in_osnd_bt_ipi_raw (pg_jj_quality_dw)

Target: silver.ipi_defective_time_corrected (pg_jj_quality_dw)
Execution: Manual trigger only
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.quality.silver.common.ipi_defective_time_correction_common import (
    extract_mc_data,
    extract_ipi_data,
    normalize_strings,
    parse_datetimes,
    perform_time_matching,
    filter_by_delta_threshold,
    load_to_silver,
    update_variable,
    PRODUCTION_POSTGRES_CONN_ID,
    QUALITY_POSTGRES_CONN_ID,
    DELTA_SEC_THRESHOLD,
    INITIAL_START_DATE,
    DAYS_OFFSET_FOR_INCREMENTAL
)

# ────────────────────────────────────────────────────────────────
# 1️⃣ Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2),
}

# Backfill Configuration
INCREMENT_KEY = "ipi_defective_time_corrected_last_date"  # incremental과 공용


# ────────────────────────────────────────────────────────────────
# 2️⃣ Main Backfill Logic
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
        ipi_df = extract_ipi_data(pg_quality, start_time, end_time)
        
        if len(mc_df) == 0:
            logging.warning(f"⚠️ MC 데이터가 없습니다: {start_time}")
            update_variable(INCREMENT_KEY, start_time)
            return {
                "loop": loop_count,
                "date": start_time,
                "status": "no_mc_data"
            }
        
        if len(ipi_df) == 0:
            logging.warning(f"⚠️ IPI 데이터가 없습니다: {start_time}")
            update_variable(INCREMENT_KEY, start_time)
            return {
                "loop": loop_count,
                "date": start_time,
                "status": "no_ipi_data"
            }
        
        # 문자열 정규화
        mc_df, ipi_df = normalize_strings(mc_df, ipi_df)
        
        # 시간 파싱
        mc_df, ipi_df = parse_datetimes(mc_df, ipi_df)
        
        # 시간 매칭 루프 수행
        ipi_df = perform_time_matching(mc_df, ipi_df)
        
        # Delta 초과 범위 필터링
        df_normal, df_exceed = filter_by_delta_threshold(ipi_df, DELTA_SEC_THRESHOLD)
        
        # Silver 테이블 적재
        if len(df_normal) > 0:
            load_to_silver(pg_quality, df_normal)
            logging.info(f"✅ 배치 완료: {start_time} ({len(df_normal):,} rows)")
        else:
            logging.info(f"배치에 데이터 없음: {start_time}")
        
        # Variable 업데이트
        update_variable(INCREMENT_KEY, start_time)
        
        return {
            "loop": loop_count,
            "date": start_time,
            "original_rows": len(ipi_df),
            "final_rows": len(df_normal),
            "exceed_rows": len(df_exceed),
            "status": "success"
        }
        
    except Exception as e:
        logging.error(f"❌ 배치 실패: {start_time} - {str(e)}")
        update_variable(INCREMENT_KEY, start_time)  # 실패해도 날짜는 업데이트
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
    logging.info(f"⏱️  Delta Threshold: {DELTA_SEC_THRESHOLD}초")
    
    # Process daily batches
    results = []
    total_original_rows = 0
    total_final_rows = 0
    total_exceed_rows = 0
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
            total_exceed_rows += batch_result.get("exceed_rows", 0)
        
        # Move to next day
        current_date += timedelta(days=1)
    
    # Log completion
    logging.info(f"🎉 Backfill 완료! 총 {loop_count}회 루프, {total_final_rows:,}개 rows 수집")
    if results:
        logging.info(f"처리 기간: {results[0]['date']} ~ {results[-1]['date']}")
        logging.info(f"📊 총 원본 데이터: {total_original_rows:,} rows")
        logging.info(f"📊 총 최종 데이터: {total_final_rows:,} rows")
        logging.info(f"📊 총 초과 범위 데이터: {total_exceed_rows:,} rows")
    
    return {
        "status": "backfill_completed",
        "total_loops": loop_count,
        "total_days": len(results),
        "total_original_rows": total_original_rows,
        "total_final_rows": total_final_rows,
        "total_exceed_rows": total_exceed_rows,
        "results": results
    }


# ────────────────────────────────────────────────────────────────
# 3️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id='ipi_defective_time_correction_backfill',
    default_args=DEFAULT_ARGS,
    description='IPI Defective Time Correction Backfill - 과거 데이터 적재 (-2일 전까지)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['JJ', 'quality', 'IP', 'Silver Layer', 'backfill', 'IPI'],
    max_active_runs=1,
) as dag:
    
    backfill_daily_batch = PythonOperator(
        task_id="backfill_daily_batch_task",
        python_callable=backfill_daily_batch_task,
        provide_context=True,
    )
    
    backfill_daily_batch

