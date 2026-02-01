from __future__ import annotations

"""
Banbury 공정 이상 감지 dbt 프로젝트를 실행하는 Airflow DAG

증분 처리 및 백필 처리를 지원합니다.
- Incremental: Airflow Variable에서 마지막 처리 시간을 읽어서 증분 처리
- Backfill: 초기 날짜부터 지정된 날짜까지 일괄 처리
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Tuple
import logging
import sys
from dateutil.parser import parse

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from plugins.hooks.postgres_hook import PostgresHelper

# ============================================================================
# 상수 정의
# ============================================================================

# 경로 설정
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/banbury_anomaly_detection"
if DBT_PROJECT_DIR not in sys.path:
    sys.path.insert(0, DBT_PROJECT_DIR)

# 원본 로직 모듈은 런타임에 import (DAG import 시간 단축)

# Airflow 설정
INCREMENT_KEY = "last_extract_time_banbury_anomaly_detection"
INDO_TZ = timezone(timedelta(hours=7))
INITIAL_START_DATE = datetime(2025, 1, 1, 6, 30, 0, tzinfo=INDO_TZ)
DAYS_OFFSET_FOR_INCREMENTAL = 1  # 증분 처리: 오늘 - 1일까지만
DAYS_OFFSET_FOR_BACKFILL = 2  # 백필 처리: 오늘 - 2일까지만

# 데이터베이스 설정
POSTGRES_CONN_ID = "pg_jj_telemetry_dw"
SCHEMA = "silver"  # dbt 모델 저장 스키마

# CNN 모델 설정
MODEL_PATH = "/opt/airflow/models/cnn_anomaly_classifier.h5"
ANOMALY_THRESHOLD = 0.1
NUM_CHANNELS = 2
SEQUENCE_LENGTH = 500  # CNN 모델이 기대하는 시퀀스 길이

# 컬럼명 정의
CYCLE_ID_COL = "cycle_id"
MOTOR_COL = "motor"
TEMP_COL = "temperature"

# dbt 모델 테이블명
TABLE_BANBURY_CYCLES = "banbury_cycles"

# DAG 기본 설정
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1, tzinfo=INDO_TZ),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# dbt Profile 설정
PROFILE_CONFIG = ProfileConfig(
    profile_name="banbury_anomaly_detection",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={"schema": SCHEMA}
    ),
)

# dbt Execution 설정
EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path="dbt",
)

# ============================================================================
# 날짜 범위 계산 함수
# ============================================================================

def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string and ensure timezone is set.
    
    Args:
        dt_str: Datetime string to parse
    
    Returns:
        Datetime object with INDO_TZ timezone
    """
    dt = parse(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=INDO_TZ)
    return dt


def _normalize_to_0630(dt: datetime) -> datetime:
    """Normalize datetime to 06:30:00.
    
    Args:
        dt: Datetime to normalize
    
    Returns:
        Datetime with hour=6, minute=30, second=0, microsecond=0
    """
    return dt.replace(hour=6, minute=30, second=0, microsecond=0)


def get_incremental_date_range(**context) -> Optional[Dict[str, str]]:
    """증분 처리용 날짜 범위 계산 - Variable 시점에서 1일치 처리.
    
    Variable이 2025-12-15 06:30:00이면 2025-12-15 06:30:00 ~ 2025-12-16 06:30:00 처리
    최대 제한: 현재 시간 - 1일까지만 처리 (너무 최신 데이터는 제외)
    
    Returns:
        Dictionary with 'start_date' and 'end_date' strings, or None if no data to process
    """
    last_extract_time = Variable.get(INCREMENT_KEY, default_var=None)
    
    if not last_extract_time:
        raise ValueError(f"Variable '{INCREMENT_KEY}' not found. Please run backfill DAG first.")
    
    # 마지막 처리 시간 이후부터 하루치 처리
    last_time = parse_datetime(last_extract_time)
    start_date = _normalize_to_0630(last_time)
    end_date = _normalize_to_0630(last_time + timedelta(days=1))
    
    logging.info(f"📌 마지막 처리 시간: {last_extract_time} → 처리 범위: {start_date} ~ {end_date}")
    
    # 최대 제한: 현재 인도네시아 시간까지만 처리 (오늘을 넘지 않음)
    now_indo = datetime.now(INDO_TZ)
    max_end_date = _normalize_to_0630(now_indo)
    
    # end_date가 현재 시간을 넘으면 현재 시간까지만 처리
    if end_date > max_end_date:
        logging.info(f"⚠️ end_date({end_date})가 현재 시간({max_end_date})을 초과하여 조정")
        end_date = max_end_date
        # 조정 후에도 start_date >= end_date가 되면 처리할 데이터 없음
        if start_date >= end_date:
            logging.info(f"⚠️ 처리할 데이터 없음: start_date({start_date}) >= end_date({end_date})")
            return None
    
    if start_date >= end_date:
        logging.info(f"⚠️ 처리할 데이터 없음: {start_date} >= {end_date}")
        return None
    
    logging.info(f"📅 증분 처리 범위: {start_date} ~ {end_date}")
    
    return {
        "start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": end_date.strftime("%Y-%m-%d %H:%M:%S")
    }


def get_backfill_date_range(**context) -> Optional[Dict[str, str]]:
    """백필 처리용 날짜 범위 계산 - 2025-01-01부터 (오늘-2일)까지.
    
    Returns:
        Dictionary with 'backfill_start_date' and 'backfill_end_date' strings, or None if no data to process
    """
    last_extract_time = Variable.get(INCREMENT_KEY, default_var=None)
    
    if not last_extract_time:
        start_date = INITIAL_START_DATE
        logging.info(f"초기 시작 날짜 사용: {start_date}")
    else:
        last_time = parse_datetime(last_extract_time)
        start_date = _normalize_to_0630(last_time + timedelta(days=1))
        logging.info(f"이전 진행 지점 사용: {start_date}")
    
    # 종료 시간: (오늘 - 2일) 06:30
    end_date = _normalize_to_0630(
        datetime.now(INDO_TZ) - timedelta(days=DAYS_OFFSET_FOR_BACKFILL)
    )
    
    if start_date >= end_date:
        logging.info(f"⚠️ 처리할 데이터 없음: {start_date} >= {end_date}")
        return None
    
    logging.info(f"📅 백필 처리 범위: {start_date} ~ {end_date}")
    
    return {
        "backfill_start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
        "backfill_end_date": end_date.strftime("%Y-%m-%d %H:%M:%S")
    }


# ============================================================================
# Variable 업데이트 함수
# ============================================================================

def update_variable_after_run(**context) -> None:
    """처리 완료 후 Variable 업데이트 (Incremental)."""
    date_range = context['ti'].xcom_pull(task_ids='get_date_range')
    if date_range:
        Variable.set(INCREMENT_KEY, date_range["end_date"])
        logging.info(f"✅ Variable '{INCREMENT_KEY}' 업데이트: {date_range['end_date']}")


def update_backfill_variable(**context) -> None:
    """백필 처리 완료 후 Variable 업데이트."""
    date_range = context['ti'].xcom_pull(task_ids='get_backfill_date_range')
    if date_range:
        Variable.set(INCREMENT_KEY, date_range["backfill_end_date"])
        logging.info(f"✅ Variable '{INCREMENT_KEY}' 업데이트: {date_range['backfill_end_date']}")


# ============================================================================
# 데이터 처리 함수
# ============================================================================

# ============================================================================
# 헬퍼 함수
# ============================================================================

def _get_date_range_from_context(context: Dict[str, Any]) -> tuple[str, str] | None:
    """날짜 범위를 context에서 가져오기 (incremental 또는 backfill).
    
    Args:
        context: Airflow context
    
    Returns:
        Tuple of (start_date, end_date) or None if not found
    """
    date_range = (
        context['ti'].xcom_pull(task_ids='get_date_range') 
        or context['ti'].xcom_pull(task_ids='get_backfill_date_range')
    )
    
    if not date_range:
        return None
    
    start_date = date_range.get("start_date") or date_range.get("backfill_start_date")
    end_date = date_range.get("end_date") or date_range.get("backfill_end_date")
    
    if not start_date or not end_date:
        return None
    
    return start_date, end_date

def run_cnn_inference(**context) -> None:
    """CNN 모델을 사용한 이상 감지 추론 실행 및 결과 저장.
    
    주요 단계:
    1. banbury_segments 로드
    2. 사이클 계산 및 저장
    3. PLC 세그먼트 생성
    4. CNN 추론
    5. 결과 저장
    """
    pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
    
    # 날짜 범위 가져오기
    date_range = _get_date_range_from_context(context)
    if not date_range:
        logging.warning("⚠️ 날짜 범위가 없습니다.")
        return
    
    start_date, end_date = date_range
    
    # 1. banbury_segments 로드
    df_segments = _load_segments_from_db(pg, start_date, end_date, context)
    if df_segments is None:
        return
    
    # 2. 사이클 계산 및 저장
    df_result = _calculate_cycles(df_segments)
    if df_result is None or df_result.empty:
        logging.warning(f"⚠️ 사이클이 없어 처리할 데이터가 없습니다. 날짜 범위: {start_date} ~ {end_date}")
        return
    
    _save_cycles_to_db(pg, df_result, start_date, end_date)
    
    # 3. PLC 세그먼트 생성 및 행렬 변환
    df_plc_seg = _build_plc_segments(df_segments, df_result)
    X = _build_cycle_matrix(df_plc_seg)
    
    # 4. CNN 추론
    prob = _run_cnn_prediction(X)
    
    # 5. 결과 준비 및 저장 (원본과 동일: df_result와 prob를 그대로 결합)
    df_final = _prepare_final_result(df_result, prob)
    
    
    _save_anomaly_results(pg, df_final)


def _load_segments_from_db(
    pg: PostgresHelper, 
    start_date: str, 
    end_date: str, 
    context: Dict[str, Any]
) -> Optional[pd.DataFrame]:
    """dbt 모델에서 banbury_segments 로드."""
    import pandas as pd

    logging.info(f"📥 banbury_segments 로드 중: {start_date} ~ {end_date}")
    segments_sql = f"""
        SELECT 
            prod_set_id,
            collection_timestamp,
            motor_current,
            chamber_temperature,
            mixer_run,
            run_mode,
            process_stage,
            drop_door_position
        FROM {SCHEMA}.banbury_segments
        WHERE collection_timestamp >= '{start_date}'::timestamp
          AND collection_timestamp < '{end_date}'::timestamp
        ORDER BY prod_set_id, collection_timestamp
    """
    df_segments = pg.execute_query(segments_sql, task_id="get_segments", xcom_key=None, ti=context.get('ti'))
    
    if not df_segments:
        logging.warning("⚠️ banbury_segments 데이터가 없습니다.")
        return None
    
    # DataFrame으로 변환
    df_segments = pd.DataFrame(df_segments, columns=[
        'prod_set_id', 'collection_timestamp', 'motor_current', 'chamber_temperature',
        'mixer_run', 'run_mode', 'process_stage', 'drop_door_position'
    ])
    df_segments['collection_timestamp'] = pd.to_datetime(df_segments['collection_timestamp'])
    
    n_rows = len(df_segments)
    n_segments = df_segments['prod_set_id'].nunique()
    logging.info(f"✅ banbury_segments 로드 완료: {n_rows:,} rows, {n_segments} segments")
    return df_segments


def _calculate_cycles(df_segments):
    """세그먼트별로 사이클 계산 (Python algorithm 사용).
    
    원본 run.py와 동일한 로직:
    - 세그먼트 순서대로 처리
    - filtered_num을 세그먼트 인덱스로 설정
    
    Returns:
        DataFrame with cycles or None if no cycles could be derived
    """
    import pandas as pd
    from algorithm import cycle_features

    n_segments = df_segments['prod_set_id'].nunique()
    logging.info(f"🔄 사이클 계산 중: {n_segments} segments")
    dfs: list[pd.DataFrame] = []
    
    # 원본 run.py와 동일: 세그먼트 순서대로 처리 (prod_set_id 순서 유지)
    for i, (prod_set_id, segment) in enumerate(df_segments.groupby('prod_set_id', sort=False)):
        segment_df = segment[[
            'collection_timestamp', 'motor_current', 'chamber_temperature', 
            'mixer_run', 'run_mode', 'process_stage', 'drop_door_position'
        ]].copy()
        
        # 진단 정보 수집
        diagnostic_info = _diagnose_segment(segment_df, prod_set_id)
        
        df_cycle = cycle_features.compare_peak(segment_df)
        if df_cycle.empty:
            logging.warning(
                f"⚠️ Segment {prod_set_id} (index {i}) produced no cycles. "
                f"Diagnostics: {diagnostic_info}"
            )
            continue
        df_cycle['prod_set_id'] = prod_set_id
        df_cycle['filtered_num'] = i  # 원본과 동일: 세그먼트 인덱스
        dfs.append(df_cycle)
    
    if not dfs:
        # 모든 세그먼트에 대한 진단 정보 수집
        all_diagnostics = []
        for i, (prod_set_id, segment) in enumerate(df_segments.groupby('prod_set_id', sort=False)):
            segment_df = segment[[
                'collection_timestamp', 'motor_current', 'chamber_temperature', 
                'mixer_run', 'run_mode', 'process_stage', 'drop_door_position'
            ]].copy()
            diag = _diagnose_segment(segment_df, prod_set_id)
            all_diagnostics.append(f"Segment {prod_set_id}: {diag}")
        
        warning_msg = (
            "No cycles could be derived from filtered segments.\n"
            f"Total segments processed: {n_segments}\n"
            "Diagnostics:\n" + "\n".join(all_diagnostics)
        )
        logging.warning(f"⚠️ {warning_msg}")
        return None
    
    # 원본과 동일: 정렬하지 않고 그대로 결합
    df_result = pd.concat(dfs, ignore_index=True).reset_index(drop=True)
    logging.info(f"✅ 사이클 계산 완료: {len(df_result)} cycles")
    return df_result


def _diagnose_segment(segment_df, prod_set_id: int) -> str:
    """세그먼트 진단 정보 수집 (디버깅용)."""
    try:
        import numpy as np
        import pandas as pd
        # 기본 정보
        n_rows = len(segment_df)
        time_col = 'collection_timestamp'
        motor_col = 'motor_current'
        mixer_col = 'mixer_run'
        door_col = 'drop_door_position'
        stage_col = 'process_stage'
        
        # 시간 범위
        times = pd.to_datetime(segment_df[time_col], errors='coerce')
        time_span = (times.max() - times.min()).total_seconds() if len(times.dropna()) > 1 else 0
        
        # 경계 탐지 시뮬레이션
        door = segment_df[door_col].where(segment_df[door_col].isin(["close", "open"]))
        door_ffill = door.ffill()
        open_after_close = (door_ffill == "open") & (door_ffill.shift() == "close")
        cycle_marks_count = open_after_close.sum()
        
        mixer = segment_df[mixer_col].where(segment_df[mixer_col].isin(["RUN", "STOP"]))
        mixer_prev = mixer.ffill().shift()
        run_starts_count = ((mixer == "RUN") & (mixer_prev.isin(["STOP", np.nan]))).sum()
        run_stops_count = ((mixer == "STOP") & (mixer_prev == "RUN")).sum()
        
        # mix 단계 확인
        stage = segment_df[stage_col].where(segment_df[stage_col].isin(["load", "mix"])).ffill()
        has_mix = (stage == "mix").any()
        mix_count = (stage == "mix").sum()
        
        # 모터 전류 확인
        motor_values = pd.to_numeric(segment_df[motor_col], errors='coerce')
        motor_valid = motor_values.notna().sum()
        motor_mean = motor_values.mean() if motor_valid > 0 else None
        
        return (
            f"rows={n_rows}, time_span={time_span:.1f}s, "
            f"cycle_marks={cycle_marks_count}, run_starts={run_starts_count}, run_stops={run_stops_count}, "
            f"has_mix={has_mix}, mix_rows={mix_count}, motor_valid={motor_valid}, motor_mean={motor_mean}"
        )
    except Exception as e:
        return f"diagnostic_error: {str(e)}"


def _build_plc_segments(df_segments, df_result):
    """PLC 세그먼트 생성 (Python algorithm 사용)."""
    from algorithm import segments

    n_cycles = len(df_result)
    logging.info(f"🔄 PLC 세그먼트 생성 중: {n_cycles} cycles")
    df_filtered = df_segments[[
        'collection_timestamp', 'motor_current', 'chamber_temperature', 
        'mixer_run', 'run_mode', 'process_stage', 'drop_door_position'
    ]].copy()
    
    df_plc_seg = segments.build_plc_segments(
        df_filtered,
        df_result,
        current_col="motor_current",
        temp_col="chamber_temperature",
        cycle_col=CYCLE_ID_COL
    )
    n_rows = len(df_plc_seg)
    n_cycles_seg = df_plc_seg[CYCLE_ID_COL].nunique()
    logging.info(f"✅ PLC 세그먼트 생성 완료: {n_rows:,} rows, {n_cycles_seg} cycles")
    return df_plc_seg


def _build_cycle_matrix(df_plc_seg):
    """사이클 행렬 생성 (원본과 동일)."""
    from algorithm import segments

    n_cycles = df_plc_seg[CYCLE_ID_COL].nunique()
    logging.info(f"🔄 사이클 행렬 생성 중: {n_cycles} cycles")
    X = segments.build_cycle_matrix(
        df_plc_seg,
        idx_col=CYCLE_ID_COL,
        current_col="motor_current",
        temp_col="chamber_temperature",
        n_points=SEQUENCE_LENGTH
    )
    
    if len(X) == 0:
        raise ValueError("No valid cycles found for CNN inference.")
    
    n_valid = len(X)
    if n_valid < n_cycles:
        invalid_count = n_cycles - n_valid
        logging.warning(f"⚠️ 유효하지 않은 사이클: {invalid_count}개 제외됨")
    
    logging.info(f"✅ 사이클 행렬 생성 완료: {n_valid} cycles, shape={X.shape}")
    return X


def _run_cnn_prediction(X):
    """CNN 모델 추론 실행."""
    import numpy as np
    from tensorflow import keras
    from algorithm import inference

    n_cycles = len(X)
    logging.info(f"🤖 CNN 모델 추론 실행 중: {n_cycles} cycles")
    
    cnn_model = keras.models.load_model(MODEL_PATH)
    prob = inference.predict_cycles_cnn(cnn_model, X)
    
    # 모델 출력 검증: NaN이나 음수 값 확인
    if np.isnan(prob).any():
        nan_count = np.isnan(prob).sum()
        logging.error(f"❌ 모델 출력에 NaN 값 발견: {nan_count}개")
        prob = np.nan_to_num(prob, nan=0.5)
    
    if (prob < 0).any():
        neg_count = (prob < 0).sum()
        logging.warning(f"⚠️ 모델 출력에 음수 값 발견: {neg_count}개 (0으로 클리핑)")
        prob = np.clip(prob, 0, 1)
    
    # 원본 로직과 동일: 모델 출력을 그대로 사용 (최소값 설정 없음)
    # 0.0 값은 모델의 실제 출력일 수 있으므로 그대로 유지
    
    n_anomalies = (prob < ANOMALY_THRESHOLD).sum()
    prob_min, prob_max, prob_mean = prob.min(), prob.max(), prob.mean()
    zero_final = np.sum(prob == 0.0)
    logging.info(f"✅ CNN 추론 완료: {n_cycles} cycles, 이상={n_anomalies}개 (prob: min={prob_min:.6f}, max={prob_max:.4f}, mean={prob_mean:.4f}, 0.0={zero_final}개)")
    
    return prob


def _calculate_shift(start_time):
    """cycle_start 시간과 요일 기준으로 shift 계산.

    월~목:
        1: 06:30 ~ 14:30
        2: 14:30 ~ 22:30
        3: 22:30 ~ (다음날)06:30
    금:
        1: 06:30 ~ 15:00
        2: 15:00 ~ 22:30
        3: 22:30 ~ (다음날)06:30
    토:
        1: 06:30 ~ 11:30
        2: 11:30 ~ 16:30
        3: 16:30 ~ 21:30

    주의: 06:30 이전은 전날 22:30부터 시작한 shift 3에 속함
    
    Args:
        start_time: 사이클 시작 시간 (pd.Timestamp)
    
    Returns:
        shift 번호 (1, 2, 3) 또는 None
    """
    import pandas as pd

    if pd.isna(start_time):
        return None
    
    # tz-aware → naive
    if pd.api.types.is_datetime64tz_dtype(type(start_time)):
        if start_time.tz is not None:
            start_time = start_time.tz_convert(None)
        else:
            start_time = start_time.tz_localize(None)
    
    weekday = start_time.weekday()  # 0=월, 4=금, 5=토, 6=일
    time_only = start_time.time()
    
    # 시간 변수 미리 추출 (반복 호출 방지)
    t0630 = datetime.strptime("06:30", "%H:%M").time()
    t1430 = datetime.strptime("14:30", "%H:%M").time()
    t2230 = datetime.strptime("22:30", "%H:%M").time()
    t1500 = datetime.strptime("15:00", "%H:%M").time()
    t1130 = datetime.strptime("11:30", "%H:%M").time()
    t1630 = datetime.strptime("16:30", "%H:%M").time()
    t2130 = datetime.strptime("21:30", "%H:%M").time()
    
    # 06:30 이전 → 전날 shift 3
    if time_only < t0630:
        prev_weekday = (weekday - 1) % 7
        if prev_weekday <= 4:  # 전날이 월~금
            return 3
        return None
    
    # 월~목
    if weekday <= 3:
        if time_only < t1430:
            return 1
        elif time_only < t2230:
            return 2
        else:
            return 3
    
    # 금요일
    if weekday == 4:
        if time_only < t1500:
            return 1
        elif time_only < t2230:
            return 2
        else:
            return 3
    
    # 토요일
    if weekday == 5:
        if time_only < t1130:
            return 1
        elif time_only < t1630:
            return 2
        elif time_only < t2130:
            return 3
        else:
            return None
    
    # 일요일
    return None


def _prepare_final_result(df_result, prob):
    """최종 결과 DataFrame 준비.
    
    화면 표시 순서: no, shift, cycle_start, cycle_end, mode, mix_duration_sec, max_temp, is_3_stage, is_anomaly, anomaly_prob
    """
    # 결과 결합
    import pandas as pd
    from algorithm import inference

    df_pred = pd.DataFrame({
        "anomaly_prob": prob,
        "is_anomaly": [inference.convert_prob_to_result(p, threshold=ANOMALY_THRESHOLD) for p in prob]
    })
    df_final = pd.concat([df_result.reset_index(drop=True), df_pred], axis=1)
    
    # filtered_num은 이미 _calculate_cycles에서 설정됨 (원본과 동일)
    
    # result 추가: is_anomaly와 is_3_stage 둘 다 true여야만 True, 하나라도 false면 False
    df_final['result'] = df_final.apply(
        lambda row: bool(row['is_anomaly'] and row['is_3_stage']),
        axis=1
    )
    
    # shift 계산: cycle_start 시간과 요일 기준
    df_final['shift'] = df_final['start'].apply(_calculate_shift)
    
    # no 생성: banbury03_yyyyMMdd_{순차순서} 형식
    # 원본 순서 유지: 정렬하지 않고 원본 순서대로 처리 (원본 run.py와 동일)
    df_final['date_str'] = df_final['start'].dt.strftime('%Y%m%d')
    df_final['seq'] = df_final.groupby('date_str').cumcount() + 1
    df_final['no'] = 'banbury03_' + df_final['date_str'] + '_' + df_final['seq'].astype(str)
    df_final = df_final.drop(columns=['date_str', 'seq'])
    
    # 컬럼명 변경: 화면 표시 순서에 맞춤
    df_final = df_final.rename(columns={
        'start': 'cycle_start',
        'end': 'cycle_end',
        'run_mode_start': 'mode'
    })
    
    # 데이터 정제: NaN만 처리 (원본 로직과 동일, 최소값 설정 없음)
    if df_final['anomaly_prob'].isna().any():
        logging.warning(f"⚠️ anomaly_prob에 NaN 값 발견: {df_final['anomaly_prob'].isna().sum()}개")
        df_final['anomaly_prob'] = df_final['anomaly_prob'].fillna(0.5)
    
    if df_final['is_anomaly'].isna().any():
        df_final['is_anomaly'] = df_final['is_anomaly'].fillna(False)
    
    # 화면 표시 순서로 컬럼 정렬
    display_columns = [
        'no', 'shift', 'cycle_start', 'cycle_end', 'mode',
        'mix_duration_sec', 'max_temp', 'is_3_stage', 'is_anomaly', 'anomaly_prob'
    ]
    # 내부 사용 컬럼도 포함
    internal_columns = ['filtered_num', 'peak_count', 'result']
    all_columns = display_columns + [col for col in internal_columns if col in df_final.columns]
    
    # 존재하는 컬럼만 선택
    available_columns = [col for col in all_columns if col in df_final.columns]
    df_final = df_final[available_columns]
    
    logging.info(f"✅ CNN 추론 완료: {len(df_final)} cycles, 이상: {df_final['is_anomaly'].sum()}개")
    return df_final


def _save_anomaly_results(pg: PostgresHelper, df_final) -> None:
    """이상 감지 결과를 데이터베이스에 저장."""
    n_rows = len(df_final)
    n_anomalies = df_final['is_anomaly'].sum()
    logging.info(f"💾 결과 저장 중: {n_rows} cycles (이상={n_anomalies}개)")
    
    insert_data, columns = _prepare_insert_data(df_final)
    
    pg.insert_data(
        schema_name="gold",
        table_name="banbury_anomaly_result",
        data=insert_data,
        columns=columns,
        conflict_columns=["no"],
        chunk_size=500
    )
    
    logging.info(f"✅ 결과 저장 완료: {len(insert_data):,} rows")


def _save_cycles_to_db(pg: PostgresHelper, df_result, start_date: str, end_date: str) -> None:
    """compare_peak 결과를 banbury_cycles 테이블에 저장.
    
    Python algorithm 결과 구조에 맞게 저장:
    - 컬럼: prod_set_id, cycle_id, start, end, run_mode_start, mix_duration_sec, max_temp, peak_count, is_3_stage
    
    Args:
        pg: PostgresHelper instance
        df_result: compare_peak 결과 DataFrame
        start_date: 시작 날짜
        end_date: 종료 날짜
    """
    from psycopg2.extras import execute_values
    import pandas as pd
    
    # 테이블이 없으면 생성 (Python algorithm 결과 구조에 맞게)
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_BANBURY_CYCLES} (
            prod_set_id INTEGER NOT NULL,
            cycle_id INTEGER NOT NULL,
            start TIMESTAMPTZ NOT NULL,
            "end" TIMESTAMPTZ NOT NULL,
            run_mode_start VARCHAR(10),
            mix_duration_sec NUMERIC(10, 1) NOT NULL,
            max_temp NUMERIC(10, 2),
            peak_count INTEGER NOT NULL,
            is_3_stage BOOLEAN NOT NULL,
            CONSTRAINT pk_banbury_cycles PRIMARY KEY (prod_set_id, cycle_id)
        )
    """
    
    # 기존 데이터 삭제 (해당 날짜 범위)
    delete_sql = f"""
        DELETE FROM {SCHEMA}.{TABLE_BANBURY_CYCLES}
        WHERE start >= '{start_date}'::timestamp
          AND start < '{end_date}'::timestamp
    """
    
    # INSERT 데이터 준비
    # NaT (Not a Time) 값 처리
    insert_data = []
    cycle_id_counter = {}  # prod_set_id별 cycle_id 카운터
    
    for idx, row in df_result.iterrows():
        prod_set_id = int(row['prod_set_id'])
        
        # start, end timestamp 처리: NaT 체크
        start_ts = pd.to_datetime(row['start'], errors='coerce')
        end_ts = pd.to_datetime(row['end'], errors='coerce')
        if pd.isna(start_ts) or pd.isna(end_ts):
            continue  # NaT인 행은 건너뛰기
        
        # 각 prod_set_id별로 cycle_id를 1부터 시작
        if prod_set_id not in cycle_id_counter:
            cycle_id_counter[prod_set_id] = 0
        cycle_id_counter[prod_set_id] += 1
        cycle_id = cycle_id_counter[prod_set_id]
        
        insert_data.append((
            prod_set_id,
            cycle_id,
            start_ts.to_pydatetime(),
            end_ts.to_pydatetime(),
            row.get('run_mode_start') if pd.notna(row.get('run_mode_start')) else None,
            float(row['mix_duration_sec']),
            float(row['max_temp']) if pd.notna(row['max_temp']) else None,
            int(row['peak_count']),
            bool(row['is_3_stage'])
        ))
    
    insert_sql = f"""
        INSERT INTO {SCHEMA}.{TABLE_BANBURY_CYCLES}
        (prod_set_id, cycle_id, start, "end", run_mode_start, mix_duration_sec, max_temp, peak_count, is_3_stage)
        VALUES %s
    """
    
    try:
        with pg.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.execute(delete_sql)
            conn.commit()
            if insert_data:
                execute_values(cursor, insert_sql, insert_data)
                conn.commit()
            logging.info(f"✅ banbury_cycles 저장 완료: {len(insert_data):,} rows")
    except Exception as e:
        logging.error(f"❌ banbury_cycles 저장 실패: {str(e)}")
        raise


def _prepare_insert_data(df_final) -> Tuple[list, list]:
    """Prepare data for database insertion.
    
    화면 표시 순서: no, shift, cycle_start, cycle_end, mode, mix_duration_sec, max_temp, is_3_stage, is_anomaly, anomaly_prob
    
    Args:
        df_final: Final DataFrame with predictions
    
    Returns:
        Tuple of (insert_data, columns)
    """
    insert_data = []
    
    import pandas as pd

    for _, row in df_final.iterrows():
        insert_data.append((
            str(row['no']),  # banbury03_yyyyMMdd_{순차순서}
            int(row['shift']) if pd.notna(row['shift']) else None,  # shift (1, 2, 3)
            pd.to_datetime(row['cycle_start']).to_pydatetime(),  # cycle_start
            pd.to_datetime(row['cycle_end']).to_pydatetime(),  # cycle_end
            row.get('mode'),  # mode (run_mode_start)
            round(float(row['mix_duration_sec']), 1),  # 소수점 첫째 자리까지 반올림
            float(row['max_temp']) if pd.notna(row['max_temp']) else None,
            bool(row['is_3_stage']),
            bool(row['is_anomaly']),
            float(row['anomaly_prob']),
            # 내부 사용 컬럼
            int(row['filtered_num']),
            int(row.get('peak_count', 0)),
            bool(row.get('result', False))
        ))
    
    columns = [
        "no", "shift", "cycle_start", "cycle_end", "mode",
        "mix_duration_sec", "max_temp", "is_3_stage", "is_anomaly", "anomaly_prob",
        "filtered_num", "peak_count", "result"
    ]
    
    return insert_data, columns


# ============================================================================
# DAG 정의
# ============================================================================

# Incremental DAG
with DAG(
    dag_id="dbt_banbury_anomaly_detection_incremental",
    default_args=DEFAULT_ARGS,
    description="Banbury 공정 이상 감지 - 증분 처리",
    schedule_interval="30 7 * * *",  # 매일 07:30 (06:30 데이터 처리)
    catchup=False,
    tags=["dbt", "banbury", "anomaly", "incremental"],
) as incremental_dag:
    
    get_date_range = PythonOperator(
        task_id="get_date_range",
        python_callable=get_incremental_date_range,
    )
    
    def prepare_dbt_vars(**context):
        """dbt 실행에 필요한 변수 준비."""
        date_range = context['ti'].xcom_pull(task_ids='get_date_range')
        if date_range:
            return {
                "start_date": date_range["start_date"],
                "end_date": date_range["end_date"]
            }
        return {}
    
    prepare_vars = PythonOperator(
        task_id="prepare_dbt_vars",
        python_callable=prepare_dbt_vars,
    )
    
    dbt_task = DbtTaskGroup(
        group_id="dbt_banbury_anomaly",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        operator_args={
            "vars": "{{ ti.xcom_pull(task_ids='prepare_dbt_vars') }}",
            # banbury_anomaly_result는 최종 결과 테이블이므로 제외
            # banbury_cycles는 Python에서 원본 로직으로 처리하므로 dbt 실행 제외
            # banbury_plc_segments도 Python에서 생성하므로 dbt 실행 제외
            "exclude": ["banbury_anomaly_result", "banbury_cycles", "banbury_plc_segments", "tag:result"],
        },
    )
    
    cnn_inference = PythonOperator(
        task_id="cnn_inference",
        python_callable=run_cnn_inference,
    )
    
    update_var = PythonOperator(
        task_id="update_variable",
        python_callable=update_variable_after_run,
    )
    
    get_date_range >> prepare_vars >> dbt_task >> cnn_inference >> update_var


# Backfill DAG
with DAG(
    dag_id="dbt_banbury_anomaly_detection_backfill",
    default_args=DEFAULT_ARGS,
    description="Banbury 공정 이상 감지 - 백필 처리 (2025-01-01부터 오늘-2일까지)",
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["dbt", "banbury", "anomaly", "backfill"],
) as backfill_dag:
    
    get_backfill_range = PythonOperator(
        task_id="get_backfill_date_range",
        python_callable=get_backfill_date_range,
    )
    
    def prepare_backfill_vars(**context):
        """dbt 실행에 필요한 변수 준비."""
        date_range = context['ti'].xcom_pull(task_ids='get_backfill_date_range')
        if date_range:
            return {
                "backfill_start_date": date_range["backfill_start_date"],
                "backfill_end_date": date_range["backfill_end_date"]
            }
        return {}
    
    prepare_backfill_vars_task = PythonOperator(
        task_id="prepare_backfill_vars",
        python_callable=prepare_backfill_vars,
    )
    
    dbt_backfill_task = DbtTaskGroup(
        group_id="dbt_banbury_anomaly_backfill",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        operator_args={
            "vars": "{{ ti.xcom_pull(task_ids='prepare_backfill_vars') }}",
            "full_refresh": True,
            # banbury_anomaly_result는 최종 결과 테이블이므로 제외
            # banbury_cycles는 Python에서 원본 로직으로 처리하므로 dbt 실행 제외
            # banbury_plc_segments도 Python에서 생성하므로 dbt 실행 제외
            "exclude": ["banbury_anomaly_result", "banbury_cycles", "banbury_plc_segments", "tag:result"],
        },
    )
    
    cnn_inference_backfill = PythonOperator(
        task_id="cnn_inference_backfill",
        python_callable=run_cnn_inference,
    )
    
    update_backfill_var = PythonOperator(
        task_id="update_backfill_variable",
        python_callable=update_backfill_variable,
    )
    
    get_backfill_range >> prepare_backfill_vars_task >> dbt_backfill_task >> cnn_inference_backfill >> update_backfill_var
