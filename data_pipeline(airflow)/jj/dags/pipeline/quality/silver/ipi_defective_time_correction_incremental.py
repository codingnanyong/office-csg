"""
IPI Time Correction Incremental DAG (Bronze → Silver)
=======================================================
전일 데이터를 쌓는 Incremental DAG

Source Tables:
- bronze.ipi_mc_output_v2_raw (pg_jj_production_dw)
- bronze.mspq_in_osnd_bt_ipi_raw (pg_jj_quality_dw)

Target: silver.ipi_defective_time_corrected (pg_jj_quality_dw)
Execution: Daily schedule (@daily)
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
    DELTA_SEC_THRESHOLD
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

# Incremental Configuration
INCREMENT_KEY = "ipi_defective_time_corrected_last_date"


# ────────────────────────────────────────────────────────────────
# 2️⃣ Main Incremental Logic
# ────────────────────────────────────────────────────────────────
def incremental_ipi_defective_time_correction(**context):
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
    logging.info(f"🚀 IPI Defective Time Correction Incremental 시작")
    logging.info(f"{'='*60}")
    logging.info(f"📅 처리 날짜: {start_time}")
    logging.info(f"⏱️  Delta Threshold: {DELTA_SEC_THRESHOLD}초")
    
    try:
        # 데이터 추출
        mc_df = extract_mc_data(pg_prod, start_time, end_time)
        ipi_df = extract_ipi_data(pg_quality, start_time, end_time)
        
        if len(mc_df) == 0:
            logging.warning("⚠️ MC 데이터가 없습니다.")
            update_variable(INCREMENT_KEY, start_time)
            return {"status": "no_data", "message": "MC 데이터 없음", "date": start_time}
        
        if len(ipi_df) == 0:
            logging.warning("⚠️ IPI 데이터가 없습니다.")
            update_variable(INCREMENT_KEY, start_time)
            return {"status": "no_data", "message": "IPI 데이터 없음", "date": start_time}
        
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
            logging.info(f"✅ Incremental 완료: {start_time} ({len(df_normal):,} rows)")
        else:
            logging.info(f"⚠️ Incremental 데이터 없음: {start_time}")
        
        # Variable 업데이트
        update_variable(INCREMENT_KEY, start_time)
        
        logging.info(f"\n{'='*60}")
        logging.info(f"✅ Incremental 완료")
        logging.info(f"{'='*60}")
        logging.info(f"📅 처리 날짜: {start_time}")
        logging.info(f"📊 원본 IPI 데이터: {len(ipi_df):,} rows")
        logging.info(f"📊 정상 범위 데이터: {len(df_normal):,} rows")
        logging.info(f"📊 초과 범위 데이터: {len(df_exceed):,} rows")
        logging.info(f"{'='*60}")
        
        return {
            "status": "success",
            "date": start_time,
            "original_rows": len(ipi_df),
            "normal_rows": len(df_normal),
            "exceed_rows": len(df_exceed)
        }
        
    except Exception as e:
        logging.error(f"\n{'='*60}")
        logging.error(f"❌ Incremental 실패: {str(e)}")
        logging.error(f"{'='*60}")
        raise


# ────────────────────────────────────────────────────────────────
# 3️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id='ipi_defective_time_correction_incremental',
    default_args=DEFAULT_ARGS,
    description='IPI Defective Time Correction Incremental - 전일 데이터 적재',
    schedule_interval=None,  # 매일 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['JJ', 'quality', 'IP', 'Silver Layer', 'incremental', 'IPI'],
    max_active_runs=1,
) as dag:
    
    incremental_task = PythonOperator(
        task_id='ipi_defective_time_correction_incremental',
        python_callable=incremental_ipi_defective_time_correction,
        provide_context=True,
    )
    
    incremental_task

