"""
IPI Anomaly Transformer Common Functions
=========================================
공통 함수 및 설정을 모아둔 모듈
"""

import logging
import os
import re
from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd
import numpy as np
import torch
from torch import nn
from torch.utils.data import Dataset

from airflow.models import Variable
from plugins.hooks.postgres_hook import PostgresHelper
from plugins.models.anomaly_transformer import AnomalyTransformer

# ────────────────────────────────────────────────────────────────
# Configuration Constants
# ────────────────────────────────────────────────────────────────

# Database Configuration
POSTGRES_CONN_ID = "pg_jj_telemetry_dw"  # 원본 데이터 추출용
TARGET_POSTGRES_CONN_ID = "pg_jj_quality_dw"  # 결과 적재용

# File Paths (Airflow Variable로 오버라이드 가능)
MODEL_PATH_DEFAULT = "/opt/airflow/models/best_anomaly_transformer.pth"

# Target Database Schema and Table
TARGET_SCHEMA = "silver"
TARGET_TABLE = "ipi_anomaly_transformer_result"

# Airflow Variable Key Names
INCREMENT_KEY = "ipi_anomaly_transformer_last_date"  # 마지막 처리일 (YYYY-MM-DD 형식)
VARIABLE_MODEL_PATH = "ipi_temperature_model_path"

# Machine Configuration
# 처리할 machine_no 리스트 (bronze.ip_hmi_data_raw 테이블의 machine_no 형식: "MCA04", "MCA12", "MCA20", "MCA34", "MCA37")
# 예: MACHINE_NO_LIST = ["MCA34", "MCA20", "MCA37"]  # 여러 개 처리
MACHINE_NO_LIST = ["MCA34"]  # 현재 MCA34만 처리

# Model Configuration
WINDOW_SIZE = 60  # 측정 시간간격 20초 / 최소 공정 유지시간 12분 = 60 window size
INPUT_SIZE = 1
D_MODEL = 128
N_HEADS = 4
DROPOUT = 0.005036702813595816
LAMBDA_KL = 0.12210012277592575

# Processing Configuration
MIN_TEMPERATURE = 160
MAX_TEMPERATURE = 180
MIN_SENSOR_DATA_POINTS = 100
BATCH_SIZE = 64
CHUNKSIZE = 200_000
RETRY_COUNT = 3

# Backfill Configuration
INITIAL_START_DATE = datetime(2025, 1, 1)  # 초기 시작 날짜: 2025-01-01
DAYS_OFFSET_FOR_INCREMENTAL = 2  # 오늘로부터 -2일 전까지 (incremental DAG 시작점)


# ────────────────────────────────────────────────────────────────
# Utility Functions
# ────────────────────────────────────────────────────────────────
def get_model_path():
    """Airflow Variable에서 모델 경로 가져오기"""
    try:
        return Variable.get(VARIABLE_MODEL_PATH, default_var=MODEL_PATH_DEFAULT)
    except Exception:
        return MODEL_PATH_DEFAULT


def natural_key(sensor_name):
    """자연스러운 정렬을 위한 키"""
    return [int(s) if s.isdigit() else s for s in re.split(r'(\d+)', sensor_name)]


def create_sequences(data, window_size):
    """시퀀스 생성"""
    sequences, indices = [], []
    for i in range(len(data) - window_size):
        sequences.append(data[i:i+window_size])
        indices.append((i, i+window_size))
    return np.stack(sequences), indices


def preprocess_sensor_df(sensor_df: pd.DataFrame) -> pd.DataFrame:
    """센서별 시계열 전처리"""
    sensor_df = sensor_df.copy()
    sensor_df['T'] = sensor_df['T'].interpolate(method='linear')
    return sensor_df.sort_values('Date').reset_index(drop=True)


# ────────────────────────────────────────────────────────────────
# Data Extraction
# ────────────────────────────────────────────────────────────────
def extract_plate_temperature_data(start_time: str, end_time: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """PostgreSQL에서 플레이트 온도 데이터 추출 (Airflow에서 이미 수집된 데이터)"""
    logging.info(f"📥 데이터 추출 시작: {start_time} ~ {end_time}")
    
    # PostgreSQL 연결 설정
    pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
    
    # PID 리스트를 PostgreSQL에서 직접 조회
    logging.info("📋 PID 리스트를 PostgreSQL에서 조회 중...")
    pid_list_sql = """
        SELECT 
            pid,
            mc,
            prop
        FROM bronze.ip_pid_master
        WHERE mc IN ('st_1', 'st_2', 'st_3', 'st_4', 'st_5', 'st_6', 'st_7', 'st_8')
            AND prop IN (
                'Plate Temperature UR', 
                'Plate Temperature UL', 
                'Plate Temperature LR', 
                'Plate Temperature LL'
            )
    """
    
    try:
        with pg.hook.get_conn() as conn:
            pid_list = pd.read_sql_query(pid_list_sql, conn)
            logging.info(f"✅ PID 리스트 조회 완료: {len(pid_list)} rows")
    except Exception as e:
        logging.error(f"❌ PID 리스트 조회 실패: {e}")
        raise
    
    if len(pid_list) == 0:
        logging.warning("⚠️ 필터링된 PID가 없습니다.")
        return pd.DataFrame(columns=["SeqNo", "PID", "RxDate", "Pvalue", "mc_prop", "Date", "T"]), pd.DataFrame(columns=["pid", "mc", "prop", "mc_prop"])
    
    pid_list["pid"] = pid_list["pid"].astype(str)
    pid_list_r01 = pid_list.copy()
    pid_list_r01['mc_prop'] = pid_list_r01['mc'] + '_' + pid_list_r01['prop']
    pid_tuple = tuple(pid_list_r01['pid'].dropna().unique().astype(str))
    
    if len(pid_tuple) == 0:
        logging.warning("⚠️ 필터링된 PID가 없습니다.")
        return pd.DataFrame(columns=["SeqNo", "PID", "RxDate", "Pvalue", "mc_prop", "Date", "T"]), pid_list_r01
    
    logging.info(f"📊 대상 PID 개수: {len(pid_tuple)}")
    
    # machine_no 리스트 확인
    if not MACHINE_NO_LIST or len(MACHINE_NO_LIST) == 0:
        logging.warning("⚠️ 처리할 machine_no가 설정되지 않았습니다.")
        return pd.DataFrame(columns=["SeqNo", "PID", "RxDate", "Pvalue", "mc_prop", "Date", "T"]), pid_list_r01
    
    logging.info(f"🏭 처리 대상 machine_no: {', '.join(MACHINE_NO_LIST)}")
    
    # SQL 쿼리 구성 (PostgreSQL 형식)
    # 여러 machine_no 지원: WHERE machine_no IN (...)
    machine_no_list_str = ','.join([f"'{mno}'" for mno in MACHINE_NO_LIST])
    pid_list_str = ','.join([f"'{pid}'" for pid in pid_tuple])
    sql = f"""
        SELECT
            seqno AS "SeqNo", 
            pid AS "PID", 
            rxdate AS "RxDate", 
            pvalue AS "Pvalue"
        FROM bronze.ip_hmi_data_raw
        WHERE machine_no IN ({machine_no_list_str})
        AND rxdate BETWEEN '{start_time}' AND '{end_time}'
        AND pid::text IN ({pid_list_str})
        ORDER BY rxdate ASC
    """
    
    # 데이터 추출
    try:
        logging.info("📥 PostgreSQL에서 데이터 추출 중...")
        with pg.hook.get_conn() as conn:
            # chunksize로 스트리밍 읽기
            frames = []
            chunk_iter = pd.read_sql_query(
                sql,
                conn,
                parse_dates=["RxDate"],
                dtype={"PID": "string"},
                chunksize=CHUNKSIZE,
            )
                
            for i, chunk in enumerate(chunk_iter, 1):
                frames.append(chunk)
                if i % 5 == 0:
                    logging.info(f"📦 {i} chunks streamed (~{i*CHUNKSIZE:,} rows)")
            
            # 모든 청크 합치기
            df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
                columns=["SeqNo", "PID", "RxDate", "Pvalue"]
            )
        
            logging.info(f"✅ 데이터 추출 완료: {len(df):,} rows")
            
    except Exception as e:
        logging.error(f"❌ DB 읽기 실패: {e}")
        raise
    
    # 데이터 변환
    if len(df) > 0:
        df["Pvalue"] = pd.to_numeric(df["Pvalue"], errors="coerce")
        
        # mc_prop 병합
        df_r01 = df.merge(pid_list_r01[['pid', 'mc_prop']], how='left', left_on='PID', right_on='pid')
        df_r01 = df_r01.drop(columns=['SeqNo', 'pid'])
        df_r01 = df_r01.dropna(subset=['mc_prop']).reset_index(drop=True)
        
        # 컬럼 정리
        df_r01 = df_r01.rename(columns={'RxDate': 'Date', 'Pvalue': 'T'})
        df_r01['Date'] = pd.to_datetime(df_r01['Date'], errors='coerce')
        df_r01 = df_r01.sort_values('Date')
    else:
        df_r01 = pd.DataFrame(columns=["PID", "Date", "Pvalue", "mc_prop", "T"])
    
    logging.info(f"✅ 데이터 전처리 완료: {len(df_r01):,} rows")
    return df_r01, pid_list_r01


# ────────────────────────────────────────────────────────────────
# Anomaly Detection Model Classes
# ────────────────────────────────────────────────────────────────
class TimeSeriesDataset(Dataset):
    """시계열 데이터셋"""
    def __init__(self, sequences, device='cpu'):
        self.sequences = torch.tensor(sequences, dtype=torch.float32).to(device)
    
    def __len__(self):
        return len(self.sequences)
    
    def __getitem__(self, idx):
        return self.sequences[idx]


def load_anomaly_transformer_model(device='cpu', allow_no_model=False):
    """Anomaly Transformer 모델 로드
    
    Args:
        device: 사용할 장치 ('cpu' 또는 'cuda')
        allow_no_model: 모델 파일이 없어도 초기화된 모델로 진행할지 여부
    
    Returns:
        모델 객체
    
    Raises:
        FileNotFoundError: 모델 파일이 없고 allow_no_model=False인 경우
    """
    logging.info(f"🤖 모델 로드 시작 (device: {device})")
    
    try:
        class AnomalyTransformerWrapper(nn.Module):
            def __init__(self, input_size, window_size, d_model=512, n_heads=8, 
                        dropout=0.0, lambda_kl=0.1):
                super().__init__()
                self.model = AnomalyTransformer(
                    win_size=window_size,
                    enc_in=input_size,
                    c_out=input_size,
                    d_model=d_model,
                    n_heads=n_heads,
                    e_layers=3,
                    d_ff=d_model,
                    dropout=dropout,
                    activation='gelu',
                    output_attention=True
                )
                self.lambda_kl = lambda_kl
            
            def forward(self, x):
                return self.model(x)
        
        model = AnomalyTransformerWrapper(
            input_size=INPUT_SIZE,
            window_size=WINDOW_SIZE,
            d_model=D_MODEL,
            n_heads=N_HEADS,
            dropout=DROPOUT,
            lambda_kl=LAMBDA_KL
        ).to(device)
        
        # 모델 경로 가져오기
        model_path_to_load = get_model_path()
        
        if os.path.exists(model_path_to_load):
            try:
                state_dict = torch.load(model_path_to_load, map_location=device)
                model.load_state_dict(state_dict)
                logging.info(f"✅ 학습된 모델 로드 완료: {model_path_to_load}")
            except Exception as e:
                logging.warning(f"⚠️ 모델 파일 로드 실패 (초기화된 모델 사용): {e}")
                if not allow_no_model:
                    raise
                logging.warning("⚠️ 초기화된 모델(학습되지 않음)로 진행합니다. 결과가 부정확할 수 있습니다.")
        else:
            if allow_no_model:
                logging.warning(f"⚠️ 모델 파일을 찾을 수 없습니다: {model_path_to_load}")
                logging.warning("⚠️ 초기화된 모델(학습되지 않음)로 진행합니다. 결과가 부정확할 수 있습니다.")
            else:
                error_msg = (
                    f"❌ 모델 파일을 찾을 수 없습니다: {model_path_to_load}\n"
                    f"💡 해결 방법:\n"
                    f"   1. 모델 파일을 해당 경로에 업로드하거나\n"
                    f"   2. Airflow Variable 'ipi_temperature_model_path'에 올바른 경로 설정 또는\n"
                    f"   3. 모델 학습 후 저장하세요."
                )
                logging.error(error_msg)
                raise FileNotFoundError(error_msg)
        
        model.eval()
        return model
        
    except ImportError as e:
        error_msg = (
            f"❌ AnomalyTransformer 모듈을 찾을 수 없습니다: {e}\n"
            f"💡 해결 방법: plugins/models/ 디렉토리에 AnomalyTransformer 모듈을 복사하세요.\n"
            f"   예: cp -r Anomaly-Transformer/model/* plugins/models/"
        )
        logging.error(error_msg)
        raise ImportError(error_msg) from e
    except FileNotFoundError:
        raise
    except Exception as e:
        logging.error(f"❌ 모델 로드 실패: {e}")
        raise


# ────────────────────────────────────────────────────────────────
# Anomaly Detection Processing
# ────────────────────────────────────────────────────────────────
def detect_and_remove_anomalies(df_r01: pd.DataFrame, pid_list_r01: pd.DataFrame) -> pd.DataFrame:
    """이상치 탐지 및 제거"""
    logging.info("🔍 이상치 탐지 시작")
    
    # 원본 데이터 저장
    df_r00 = df_r01.copy()
    
    # 필터링 전 온도 통계 로깅
    if len(df_r01) > 0:
        temp_stats = df_r01['T'].describe()
        logging.info(f"📊 필터링 전 온도 통계:")
        logging.info(f"   최소값: {temp_stats.get('min', 'N/A'):.2f}℃")
        logging.info(f"   최대값: {temp_stats.get('max', 'N/A'):.2f}℃")
        logging.info(f"   평균값: {temp_stats.get('mean', 'N/A'):.2f}℃")
        logging.info(f"   중앙값: {temp_stats.get('50%', 'N/A'):.2f}℃")
        logging.info(f"   필터링 범위: {MIN_TEMPERATURE}℃ ~ {MAX_TEMPERATURE}℃")
    
    # 1차 필터링 (온도 범위)
    df_r01 = df_r01[(df_r01['T'] < MAX_TEMPERATURE) & (df_r01['T'] > MIN_TEMPERATURE)]
    logging.info(f"📊 1차 필터링 완료: {len(df_r01):,} rows")
    
    # 필터링 후 통계 (데이터가 있는 경우)
    if len(df_r01) > 0:
        temp_stats_filtered = df_r01['T'].describe()
        logging.info(f"📊 필터링 후 온도 통계:")
        logging.info(f"   최소값: {temp_stats_filtered.get('min', 'N/A'):.2f}℃")
        logging.info(f"   최대값: {temp_stats_filtered.get('max', 'N/A'):.2f}℃")
        logging.info(f"   평균값: {temp_stats_filtered.get('mean', 'N/A'):.2f}℃")
    elif len(df_r00) > 0:
        # 필터링으로 모든 데이터가 제거된 경우
        out_of_range_count = len(df_r00[(df_r00['T'] <= MIN_TEMPERATURE) | (df_r00['T'] >= MAX_TEMPERATURE)])
        logging.warning(f"⚠️ 모든 데이터가 온도 범위를 벗어났습니다.")
        logging.warning(f"   범위 밖 데이터: {out_of_range_count:,} rows")
        logging.warning(f"   범위 내 데이터: {len(df_r00) - out_of_range_count:,} rows")
    
    # 센서별 데이터프레임 분리 및 전처리
    sensor_dfs = {}
    original_dfs = {}
    sensor_list = df_r01['mc_prop'].unique()
    
    for sensor in sensor_list:
        df_sensor = df_r01[df_r01['mc_prop'] == sensor].copy()
        df_sensor = preprocess_sensor_df(df_sensor)
        df_original = df_r00[df_r00['mc_prop'] == sensor].copy()
        df_original = preprocess_sensor_df(df_original)
        
        # 시간 중복 제거
        df_sensor = df_sensor.groupby('Date', as_index=False)['T'].mean()
        df_original = df_original.groupby('Date', as_index=False)['T'].mean()
        
        # 최소 데이터 포인트 확인
        if len(df_sensor) < MIN_SENSOR_DATA_POINTS:
            continue
        
        sensor_dfs[sensor] = df_sensor
        original_dfs[sensor] = df_original
    
    logging.info(f"✅ 센서별 분리 완료! 총 센서 수: {len(sensor_dfs)}")
    
    if len(sensor_dfs) == 0:
        logging.warning("⚠️ 처리할 센서가 없습니다.")
        return pd.DataFrame(columns=["Date", "T", "mc_prop"])
    
    # GPU 사용 가능 여부 확인
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    logging.info(f"🖥️ 사용 장치: {device}")
    
    # 환경 변수 설정
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
    
    # 모델 로드 (모델 파일이 없어도 진행하려면 allow_no_model=True로 설정)
    # 주의: 학습되지 않은 초기화 모델은 부정확할 수 있습니다
    use_uninitialized_model = Variable.get(
        "ipi_temperature_allow_uninitialized_model", 
        default_var="false"
    ).lower() == "true"
    
    model = load_anomaly_transformer_model(device, allow_no_model=use_uninitialized_model)
    
    # 센서별 이상치 탐지 및 제거
    filtered_sensor_dfs = {}
    sorted_sensors = sorted(sensor_dfs.keys(), key=natural_key)
    total_removed_rows = 0
    
    for sensor_name in sorted_sensors:
        sensor_df = sensor_dfs[sensor_name]
        logging.info(f"📡 센서 처리 중: {sensor_name} ({len(sensor_df):,} points)")
        
        # 시퀀스 생성
        if len(sensor_df) < WINDOW_SIZE:
            logging.warning(f"⚠️ {sensor_name}: 데이터가 너무 적어 스킵합니다 ({len(sensor_df)} < {WINDOW_SIZE})")
            filtered_sensor_dfs[sensor_name] = sensor_df
            continue
        
        sequence_data, sequence_indices = create_sequences(sensor_df[['T']].values, WINDOW_SIZE)
        
        # 이상치 탐지
        recon_error_list = []
        with torch.no_grad():
            for i in range(0, len(sequence_data), 128):
                batch = sequence_data[i:i + 128]
                batch = torch.tensor(batch, dtype=torch.float32).to(device)
                
                recon, _, _, _ = model(batch)
                error = torch.mean((batch - recon) ** 2, dim=(1, 2)).cpu().numpy()
                recon_error_list.append(error)
        
        recon_error = np.concatenate(recon_error_list)
        
        # IQR 기반 이상치 판별
        q1, q3 = np.percentile(recon_error, [25, 75])
        iqr = q3 - q1
        lower_threshold = q1 - 1.5 * iqr
        upper_threshold = q3 + 1.5 * iqr
        anomalies = (recon_error < lower_threshold) | (recon_error > upper_threshold)
        
        # 이상치 구간 제거
        remove_indices = set()
        for is_anom, (start, end) in zip(anomalies, sequence_indices):
            if is_anom:
                remove_indices.update(range(start, end))
        
        df_clean = sensor_df.drop(index=sorted(remove_indices)).reset_index(drop=True)
        filtered_sensor_dfs[sensor_name] = df_clean
        
        removed_count = len(remove_indices)
        total_removed_rows += removed_count
        logging.info(f"🧹 {sensor_name} 이상치 제거 완료! 제거된 row 수: {removed_count:,}")
    
    logging.info(f"✅ 전체 센서 이상치 제거 완료! 총 제거된 row 수: {total_removed_rows:,}")
    
    # 센서 정보 추가 및 병합
    for sensor_name, df in filtered_sensor_dfs.items():
        df['mc_prop'] = sensor_name
    
    cleaned_df = pd.concat(filtered_sensor_dfs.values(), ignore_index=True)
    logging.info(f"✅ 최종 정리된 데이터: {len(cleaned_df):,} rows")
    
    return cleaned_df


# ────────────────────────────────────────────────────────────────
# Data Loading
# ────────────────────────────────────────────────────────────────
def prepare_insert_data(
    cleaned_df: pd.DataFrame, 
    start_time: str, 
    end_time: str,
    extract_time: datetime
) -> list:
    """DB 적재를 위한 데이터 준비
    
    Args:
        cleaned_df: 정제된 데이터프레임 (Date, T, mc_prop 컬럼 포함)
        start_time: 처리 시작 시간 (문자열)
        end_time: 처리 종료 시간 (문자열)
        extract_time: 원본 데이터 추출 시간 (datetime)
    
    Returns:
        DB 적재용 리스트 (튜플 리스트)
    """
    insert_data = []
    
    # datetime 변환
    processing_start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") if isinstance(start_time, str) else start_time
    processing_end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") if isinstance(end_time, str) else end_time
    
    # machine_code 설정 (MACHINE_NO_LIST의 첫 번째 값 사용, 현재는 하나만 처리)
    # 여러 machine_no를 처리하는 경우를 대비하여 cleaned_df에 machine_code 컬럼이 있다면 사용
    if 'machine_code' in cleaned_df.columns:
        machine_code_col = cleaned_df['machine_code']
    else:
        # MACHINE_NO_LIST에서 첫 번째 값 사용 (현재는 하나만 처리)
        machine_code = MACHINE_NO_LIST[0] if MACHINE_NO_LIST and len(MACHINE_NO_LIST) > 0 else None
        if machine_code is None:
            logging.warning("⚠️ MACHINE_NO_LIST가 비어있어 machine_code를 설정할 수 없습니다.")
            machine_code = "UNKNOWN"
    
    for idx, row in cleaned_df.iterrows():
        # mc_prop 분리: "st_1_Plate Temperature UR" -> mc="st_1", prop="Plate Temperature UR"
        mc_prop = row['mc_prop']
        if '_' in mc_prop:
            parts = mc_prop.split('_', 2)  # 최대 2번 분리
            if len(parts) >= 3:
                mc = f"{parts[0]}_{parts[1]}"  # "st_1"
                prop = parts[2]  # "Plate Temperature UR"
            else:
                logging.warning(f"⚠️ mc_prop 형식 오류: {mc_prop}")
                continue
        else:
            logging.warning(f"⚠️ mc_prop 형식 오류: {mc_prop}")
            continue
        
        # machine_code 결정 (컬럼이 있으면 사용, 없으면 기본값)
        if 'machine_code' in cleaned_df.columns:
            current_machine_code = row['machine_code']
        else:
            current_machine_code = machine_code
        
        insert_data.append((
            current_machine_code,                  # machine_code
            mc,                                    # mc
            prop,                                  # prop
            row['Date'],                          # measurement_time
            float(row['T']),                      # temperature
            processing_start_dt,                   # processing_start_time
            processing_end_dt,                     # processing_end_time
            extract_time,                          # etl_extract_time
            # etl_ingest_time는 DB에서 DEFAULT now()로 처리
        ))
    
    return insert_data


def load_cleaned_data(
    cleaned_df: pd.DataFrame,
    start_time: str,
    end_time: str,
    extract_time: datetime
) -> int:
    """정제된 데이터를 PostgreSQL에 적재
    
    Args:
        cleaned_df: 정제된 데이터프레임
        start_time: 처리 시작 시간
        end_time: 처리 종료 시간
        extract_time: 원본 데이터 추출 시간
    
    Returns:
        적재된 행 수
    """
    if len(cleaned_df) == 0:
        logging.warning("⚠️ 적재할 데이터가 없습니다.")
        return 0
    
    pg = PostgresHelper(conn_id=TARGET_POSTGRES_CONN_ID)
    
    # 적재 데이터 준비
    insert_data = prepare_insert_data(cleaned_df, start_time, end_time, extract_time)
    
    if not insert_data:
        logging.warning("⚠️ 적재할 데이터가 없습니다 (변환 후).")
        return 0
    
    columns = [
        "machine_code",
        "mc",
        "prop",
        "measurement_time",
        "temperature",
        "processing_start_time",
        "processing_end_time",
        "etl_extract_time"
    ]
    
    conflict_columns = [
        "machine_code",
        "mc",
        "prop",
        "measurement_time"
    ]
    
    try:
        logging.info(f"📦 데이터베이스 적재 시작: {len(insert_data):,} rows")
        pg.insert_data(
            schema_name=TARGET_SCHEMA,
            table_name=TARGET_TABLE,
            data=insert_data,
            columns=columns,
            conflict_columns=conflict_columns,
            chunk_size=1000
        )
        logging.info(f"✅ 데이터베이스 적재 완료: {len(insert_data):,} rows")
        return len(insert_data)
    except Exception as e:
        logging.error(f"❌ 데이터베이스 적재 실패: {e}")
        raise


# ────────────────────────────────────────────────────────────────
# Variable Management
# ────────────────────────────────────────────────────────────────
def update_variable(date_str: str) -> None:
    """Update Airflow variable with last processed date"""
    Variable.set(INCREMENT_KEY, date_str)
    logging.info(f"📌 Variable `{INCREMENT_KEY}` Update: {date_str}")


# ────────────────────────────────────────────────────────────────
# Incremental Logic
# ────────────────────────────────────────────────────────────────
def get_processing_time_range(**context) -> Tuple[str, str]:
    """Airflow Variable에서 처리 시간 범위 가져오기
    
    Args:
        **context: Airflow context (ds, execution_date 등 포함)
    
    Returns:
        (start_time, end_time) 튜플 (문자열 형식: "YYYY-MM-DD HH:MM:SS")
    
    로직:
    1. ipi_anomaly_transformer_last_date Variable이 있으면 사용
       - 마지막 처리일의 다음날을 처리 대상으로 설정
       - start_time: 다음날 00:00:00
       - end_time: 다음날 23:59:59
    2. 없으면 전일(today-1) 처리
    """
    # Variable에서 마지막 처리일 읽기
    last_date_str = None
    try:
        last_date_str = Variable.get(INCREMENT_KEY, default_var=None)
    except Exception as e:
        logging.warning(f"⚠️ Variable 읽기 실패, 기본값 사용: {e}")
    
    # UTC 기준 날짜 계산
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
        # 최신 상태여도 날짜 범위 반환 (빈 결과로 처리)
        target_date = today_minus_1
    
    # 대상 일자 00:00:00 ~ 23:59:59
    date_str = target_date.strftime('%Y-%m-%d')
    start_time = f"{date_str} 00:00:00"
    end_time = f"{date_str} 23:59:59"
    
    logging.info(f"📋 처리 시간 범위: {start_time} ~ {end_time}")
    return start_time, end_time


def run_anomaly_transformer(**context) -> dict:
    """이상치 탐지 메인 함수 (증분 처리)
    
    Args:
        **context: Airflow context (Variable에서 시간 범위 자동 읽기)
    
    Variable:
        - ipi_anomaly_transformer_last_date: 마지막 처리일 (YYYY-MM-DD 형식)
        - 없으면 전일(today-1) 처리
    """
    extract_time = datetime.utcnow()
    
    # Variable에서 시간 범위 가져오기
    start_time, end_time = get_processing_time_range(**context)
    
    # 처리 날짜 추출 (YYYY-MM-DD 형식)
    processed_date = start_time.split()[0]  # "YYYY-MM-DD HH:MM:SS"에서 날짜만 추출
    
    try:
        # 데이터 추출
        df_r01, pid_list_r01 = extract_plate_temperature_data(start_time, end_time)
        
        if len(df_r01) == 0:
            logging.warning("⚠️ 추출된 데이터가 없습니다.")
            # 데이터가 없어도 Variable 업데이트 (중복 처리 방지)
            update_variable(processed_date)
            return {
                "status": "success",
                "rows_processed": 0,
                "rows_inserted": 0,
                "message": "No data to process",
                "processed_date": processed_date
            }
        
        # 이상치 탐지 및 제거
        cleaned_df = detect_and_remove_anomalies(df_r01, pid_list_r01)
        
        if len(cleaned_df) == 0:
            logging.warning("⚠️ 정제된 데이터가 없습니다.")
            # 데이터가 없어도 Variable 업데이트 (중복 처리 방지)
            update_variable(processed_date)
            return {
                "status": "success",
                "rows_processed": 0,
                "rows_inserted": 0,
                "message": "No cleaned data to insert",
                "processed_date": processed_date
            }
        
        # 데이터베이스 적재
        rows_inserted = load_cleaned_data(
            cleaned_df=cleaned_df,
            start_time=start_time,
            end_time=end_time,
            extract_time=extract_time
        )
        
        # 처리 완료 후 Variable 업데이트
        update_variable(processed_date)
        
        logging.info(f"✅ 이상치 탐지 완료: {processed_date} ({len(cleaned_df):,} rows processed, {rows_inserted:,} rows inserted)")
        
        return {
            "status": "success",
            "rows_processed": len(cleaned_df),
            "rows_inserted": rows_inserted,
            "start_time": start_time,
            "end_time": end_time,
            "processed_date": processed_date,
            "target_schema": TARGET_SCHEMA,
            "target_table": TARGET_TABLE
        }
        
    except Exception as e:
        logging.error(f"❌ 이상치 탐지 실패: {str(e)}", exc_info=True)
        return {
            "status": "failed",
            "error": str(e)
        }


# ────────────────────────────────────────────────────────────────
# Backfill Logic
# ────────────────────────────────────────────────────────────────
def process_daily_batch(start_date: datetime, loop_count: int, expected_days: int) -> dict:
    """일별 배치 처리 함수
    
    Args:
        start_date: 처리할 날짜 (00:00:00)
        loop_count: 현재 루프 횟수
        expected_days: 예상 총 일수
    
    Returns:
        처리 결과 딕셔너리
    """
    extract_time = datetime.utcnow()
    
    # 처리 날짜 범위 설정
    date_str = start_date.strftime('%Y-%m-%d')
    start_time = f"{date_str} 00:00:00"
    end_time = f"{date_str} 23:59:59"
    
    logging.info(f"\n{'='*60}")
    logging.info(f"📅 일별 배치 처리: {date_str} ({loop_count}/{expected_days})")
    logging.info(f"{'='*60}")
    
    try:
        # 데이터 추출
        df_r01, pid_list_r01 = extract_plate_temperature_data(start_time, end_time)
        
        if len(df_r01) == 0:
            logging.warning(f"⚠️ {date_str}: 추출된 데이터가 없습니다.")
            return {
                "status": "success",
                "date": date_str,
                "rows_processed": 0,
                "rows_inserted": 0,
                "message": "No data to process"
            }
        
        # 이상치 탐지 및 제거
        cleaned_df = detect_and_remove_anomalies(df_r01, pid_list_r01)
        
        if len(cleaned_df) == 0:
            logging.warning(f"⚠️ {date_str}: 정제된 데이터가 없습니다.")
            return {
                "status": "success",
                "date": date_str,
                "rows_processed": 0,
                "rows_inserted": 0,
                "message": "No cleaned data to insert"
            }
        
        # 데이터베이스 적재
        rows_inserted = load_cleaned_data(
            cleaned_df=cleaned_df,
            start_time=start_time,
            end_time=end_time,
            extract_time=extract_time
        )
        
        logging.info(f"✅ {date_str} 처리 완료: {len(cleaned_df):,} rows processed, {rows_inserted:,} rows inserted")
        
        return {
            "status": "success",
            "date": date_str,
            "rows_processed": len(cleaned_df),
            "rows_inserted": rows_inserted,
            "start_time": start_time,
            "end_time": end_time
        }
        
    except Exception as e:
        logging.error(f"❌ {date_str} 처리 실패: {str(e)}", exc_info=True)
        return {
            "status": "failed",
            "date": date_str,
            "error": str(e)
        }


def backfill_daily_batch_task(**context) -> dict:
    """Backfill 메인 태스크: 일별 배치 처리 루프
    
    Variable 기반으로 시작점 결정:
    - Variable이 있으면: 마지막 처리일 다음날부터 시작
    - Variable이 없으면: INITIAL_START_DATE부터 시작
    
    종료점: today - DAYS_OFFSET_FOR_INCREMENTAL일 (incremental DAG 시작점)
    """
    # Variable에서 마지막 처리일 읽기
    last_date_str = Variable.get(INCREMENT_KEY, default_var=None)
    
    if not last_date_str:
        start_date = INITIAL_START_DATE
        logging.info(f"초기 시작 날짜 사용: {start_date.strftime('%Y-%m-%d')}")
    else:
        try:
            start_date = datetime.strptime(last_date_str, '%Y-%m-%d')
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            start_date = start_date + timedelta(days=1)  # 다음날부터 시작
            logging.info(f"이전 진행 지점 사용: {last_date_str} → 다음날: {start_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            logging.warning(f"⚠️ Variable 파싱 오류: {e}, 초기 시작 날짜로 재설정")
            start_date = INITIAL_START_DATE
    
    # 종료 날짜 계산 (today - DAYS_OFFSET_FOR_INCREMENTAL일)
    now_utc = datetime.utcnow()
    end_date = (now_utc - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    
    # 예상 일수 계산
    expected_days = (end_date - start_date).days
    
    # Backfill 정보 로그
    logging.info(f"\n{'='*60}")
    logging.info(f"🚀 IPI Anomaly Transformer Backfill 시작")
    logging.info(f"{'='*60}")
    logging.info(f"Backfill 시작: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    logging.info(f"배치 크기: 일별 (하루씩 처리)")
    logging.info(f"예상 루프 횟수: {expected_days}회 (일별)")
    logging.info(f"⚠️ 현재 시간에서 {DAYS_OFFSET_FOR_INCREMENTAL}일 전으로 설정 (incremental DAG 시작점)")
    logging.info(f"🏭 처리 대상 machine_no: {', '.join(MACHINE_NO_LIST)}")
    logging.info(f"{'='*60}")
    
    if expected_days <= 0:
        logging.info(f"✅ Backfill 완료: 처리할 날짜가 없습니다.")
        return {
            "status": "success",
            "message": "No dates to process",
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": end_date.strftime('%Y-%m-%d'),
            "total_processed": 0
        }
    
    # 일별 배치 처리 루프
    results = []
    total_processed = 0
    total_inserted = 0
    loop_count = 0
    current_date = start_date
    
    while current_date < end_date:
        loop_count += 1
        result = process_daily_batch(current_date, loop_count, expected_days)
        results.append(result)
        
        if result["status"] == "success":
            total_processed += result.get("rows_processed", 0)
            total_inserted += result.get("rows_inserted", 0)
            
            # Variable 업데이트 (매일 처리 완료 후)
            update_variable(result["date"])
        
        # 다음날로 이동
        current_date = current_date + timedelta(days=1)
        
        # 진행 상황 로그 (10일마다)
        if loop_count % 10 == 0:
            logging.info(f"\n📊 진행 상황: {loop_count}/{expected_days}일 처리 완료 ({total_processed:,} rows processed, {total_inserted:,} rows inserted)")
    
    # 최종 결과 로그
    failed_count = sum(1 for r in results if r["status"] == "failed")
    
    logging.info(f"\n{'='*60}")
    logging.info(f"🎉 Backfill 완료!")
    logging.info(f"{'='*60}")
    logging.info(f"총 처리 일수: {loop_count}일")
    logging.info(f"성공: {loop_count - failed_count}일, 실패: {failed_count}일")
    logging.info(f"총 처리 row 수: {total_processed:,}")
    logging.info(f"총 적재 row 수: {total_inserted:,}")
    logging.info(f"마지막 처리일: {results[-1]['date'] if results else 'N/A'}")
    logging.info(f"{'='*60}")
    
    return {
        "status": "success",
        "total_days": loop_count,
        "success_days": loop_count - failed_count,
        "failed_days": failed_count,
        "total_processed": total_processed,
        "total_inserted": total_inserted,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "last_processed_date": results[-1]['date'] if results else None,
        "results": results
    }

