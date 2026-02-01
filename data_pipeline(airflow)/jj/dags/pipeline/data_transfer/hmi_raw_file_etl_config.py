"""
HMI Raw Data ETL Configuration
==============================
설정 상수 및 HMI 설정 정의
"""

import os
from datetime import datetime, timedelta, timezone

# ════════════════════════════════════════════════════════════════
# DAG 기본 설정
# ════════════════════════════════════════════════════════════════

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ════════════════════════════════════════════════════════════════
# 파일 처리 설정
# ════════════════════════════════════════════════════════════════

# 파일 패턴 (HMI RAW DATA 파일 형식)
REMOTE_FILE_PATTERNS = ["*.csv"]  # RAW DATA 파일 확장자

# 로컬 저장 기본 경로
LOCAL_BASE_DIR = os.getenv("HMI_RAW_DATA_BASE_PATH", "/media/btx/hmi_raw")

# 파일 검증 옵션
VERIFY_FILE_SIZE = True  # 다운로드 후 파일 크기 검증
VERIFY_FILE_HASH = False  # 파일 해시 검증 (MD5, SHA256 등) - 선택적

# 파일 크기 필터링 (선택적)
MIN_FILE_SIZE = 0  # 최소 파일 크기 (bytes, 0이면 제한 없음)
MAX_FILE_SIZE = None  # 최대 파일 크기 (bytes, None이면 제한 없음)

# 대용량 파일 처리 설정
LARGE_FILE_THRESHOLD_MB = 100  # 대용량 파일 기준 (MB, 이 크기 이상이면 상세 로깅)
DOWNLOAD_TIMEOUT_SECONDS = 3600  # 다운로드 타임아웃 (초, 1시간 기본값)

# 시간대 설정
INDO_TZ = timezone(timedelta(hours=7))  # 인도네시아 시간대 (UTC+7)

# 날짜 필터링 설정
INITIAL_START_DATE = datetime(2024, 1, 1)  # Backfill 시작 날짜
DAYS_OFFSET_FOR_INCREMENTAL = 1  # Incremental: 오늘 - 1일까지만
DAYS_OFFSET_FOR_BACKFILL = 2  # Backfill: 오늘 - 2일까지만
HOURS_OFFSET_FOR_HOURLY = 1  # Hourly: 현재 시간 - 1시간까지만

# ════════════════════════════════════════════════════════════════
# HMI별 설정 리스트
# ════════════════════════════════════════════════════════════════

# 각 HMI별로 SFTP 연결, 원격 경로, 로컬 저장 경로를 지정
# process_code: 공정 코드 (예: "os", "extrusion", "mixing" 등)
# equipment_code: 설비 약어 (예: "banb", "extr", "mix" 등)
HMI_CONFIGS = [
    {
        "hmi_id": "banbury03",
        "process_code": "os",  # 공정 코드
        "equipment_code": "banb",  # 설비 약어
        "sftp_conn_id": "sft_jj_banbury_03", 
        "remote_base_path": r"C:\ccs_prg\EA_RAW\RAW_DATA",  # Windows 경로
        "local_save_path": os.path.join(LOCAL_BASE_DIR, "os/osr/banbury/03"),
    },
    # TODO: 추가 HMI 정보를 여기에 추가
    # {
    #     "hmi_id": "banbury_unit03",
    #     "process_code": "os",
    #     "equipment_code": "banb",
    #     "sftp_conn_id": "sftp_hmi_banbury_03",
    #     "remote_base_path": "/media/btx/process/os/osr/banbury/unit03",
    #     "local_save_path": "/media/btx/hmi_raw/os/osr/banbury/03",
    # },
]

