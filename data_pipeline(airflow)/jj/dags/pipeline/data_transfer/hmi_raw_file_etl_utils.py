"""
HMI Raw Data ETL Utilities
===========================
유틸리티 함수들: 파일 처리, 날짜 추출, 경로 정규화 등
"""

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from pipeline.data_transfer.hmi_raw_file_etl_config import (
    REMOTE_FILE_PATTERNS,
    MIN_FILE_SIZE,
    MAX_FILE_SIZE,
)


def get_variable_key(hmi_config: dict) -> str:
    """HMI별 Variable 키 생성: last_extract_time_{공정코드}_{설비약어}_hmi_file_{hmi_id}"""
    process_code = hmi_config.get('process_code', 'unknown')
    equipment_code = hmi_config.get('equipment_code', 'unknown')
    hmi_id = hmi_config.get('hmi_id', 'unknown')
    return f"last_extract_time_{process_code}_{equipment_code}_hmi_file_{hmi_id}"


def format_date_filter_desc(start_date: Optional[datetime], end_date: Optional[datetime]) -> str:
    """날짜 필터 설명 문자열 생성"""
    if start_date and end_date:
        return f"{start_date.strftime('%Y-%m-%d %H:%M')} ~ {end_date.strftime('%Y-%m-%d %H:%M')} 이전"
    elif end_date:
        return f"{end_date.strftime('%Y-%m-%d %H:%M')} 이전"
    elif start_date:
        return f"{start_date.strftime('%Y-%m-%d %H:%M')} 이후"
    else:
        return "제한 없음"


def normalize_remote_path(path: str) -> str:
    """원격 경로 정규화 (Windows 경로를 SFTP 호환 형식으로 변환)"""
    # Windows 경로의 백슬래시를 슬래시로 변환
    # C:\ccs_prg\EA_RAW\RAW_DATA -> C:/ccs_prg/EA_RAW/RAW_DATA
    normalized = path.replace('\\', '/')
    return normalized


def extract_date_from_filename(filename: str) -> Optional[datetime]:
    """
    파일명에서 날짜 추출
    파일명 형식: CBM_192.168.8.51_2025112607_T1_ST1.csv
    날짜 형식: YYYYMMDDHH (예: 2025112607)
    
    Returns:
        datetime 객체 (인도네시아 시간대 포함) 또는 None (날짜 추출 실패 시)
    """
    from pipeline.data_transfer.hmi_raw_file_etl_config import INDO_TZ
    
    # YYYYMMDDHH 형식의 날짜 패턴 찾기 (8자리 또는 10자리)
    # 예: 20251126 또는 2025112607
    pattern = r'_(\d{8})(\d{2})?_'
    match = re.search(pattern, filename)
    
    if match:
        date_str = match.group(1)  # YYYYMMDD
        hour_str = match.group(2) if match.group(2) else '00'  # HH (없으면 00)
        
        try:
            # YYYYMMDDHH 형식으로 파싱하고 인도네시아 시간대 추가
            date_time_str = date_str + hour_str
            dt = datetime.strptime(date_time_str, '%Y%m%d%H')
            return dt.replace(tzinfo=INDO_TZ)
        except ValueError as e:
            logging.warning(f"날짜 파싱 실패 ({filename}): {e}")
            return None
    
    # 대체 패턴: 파일명 끝에 날짜가 있는 경우
    pattern_alt = r'_(\d{8})(\d{2})?\.csv$'
    match_alt = re.search(pattern_alt, filename)
    
    if match_alt:
        date_str = match_alt.group(1)  # YYYYMMDD
        hour_str = match_alt.group(2) if match_alt.group(2) else '00'  # HH
        
        try:
            date_time_str = date_str + hour_str
            dt = datetime.strptime(date_time_str, '%Y%m%d%H')
            return dt.replace(tzinfo=INDO_TZ)
        except ValueError as e:
            logging.warning(f"날짜 파싱 실패 ({filename}): {e}")
            return None
    
    return None


def get_file_size(sftp, remote_path: str, current_dir: str = None) -> int:
    """
    원격 파일 크기 조회
    Windows SFTP 서버 호환성을 위해 상대 경로도 시도
    """
    try:
        # 절대 경로로 시도
        return sftp.stat(remote_path).st_size
    except Exception as e1:
        # 절대 경로 실패 시 상대 경로로 시도
        if current_dir:
            try:
                # 파일명만 추출
                filename = os.path.basename(remote_path)
                relative_path = filename
                return sftp.stat(relative_path).st_size
            except Exception as e2:
                logging.debug(f"파일 크기 조회 실패 (절대: {remote_path}, 상대: {relative_path}): {e1}, {e2}")
                return 0
        else:
            logging.debug(f"파일 크기 조회 실패 ({remote_path}): {e1}")
            return 0


def should_process_file(
    filename: str, 
    file_size: int,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> bool:
    """
    파일 처리 여부 결정 (지정된 날짜 범위의 파일만)
    파일명에서 날짜를 추출하여 날짜 범위 내의 파일인지 확인
    
    Args:
        filename: 파일명
        file_size: 파일 크기 (bytes)
        start_date: 시작 날짜 (None이면 제한 없음)
        end_date: 종료 날짜 (None이면 제한 없음, 이 날짜 이전까지)
    """
    # 파일 크기 필터링 (크기가 0인 경우는 Windows SFTP 조회 실패일 수 있으므로 건너뜀)
    if file_size > 0:
        if MIN_FILE_SIZE > 0 and file_size < MIN_FILE_SIZE:
            logging.debug(f"파일 크기가 최소값보다 작음: {filename} ({file_size} bytes)")
            return False
        
        if MAX_FILE_SIZE and file_size > MAX_FILE_SIZE:
            logging.debug(f"파일 크기가 최대값보다 큼: {filename} ({file_size} bytes)")
            return False
    
    # 파일명에서 날짜 추출
    file_date = extract_date_from_filename(filename)
    
    if file_date is None:
        logging.debug(f"날짜 추출 실패: {filename} (건너뜀)")
        return False
    
    # 날짜 범위 필터링
    if start_date is not None and file_date < start_date:
        logging.debug(
            f"파일이 시작 날짜 이전: {filename} "
            f"(파일 날짜: {file_date.strftime('%Y-%m-%d %H:%M')}, "
            f"시작 날짜: {start_date.strftime('%Y-%m-%d %H:%M')})"
        )
        return False
    
    if end_date is not None and file_date >= end_date:
        logging.debug(
            f"파일이 종료 날짜 이후: {filename} "
            f"(파일 날짜: {file_date.strftime('%Y-%m-%d %H:%M')}, "
            f"종료 날짜: {end_date.strftime('%Y-%m-%d %H:%M')})"
        )
        return False
    
    logging.info(
        f"✅ 처리 대상: {filename} "
        f"(파일 날짜: {file_date.strftime('%Y-%m-%d %H:%M')}, {file_size:,} bytes)"
    )
    
    return True


def filter_remote_files(sftp, all_items: List[str], remote_base_path: str, 
                       start_date: Optional[datetime], end_date: Optional[datetime]) -> List[dict]:
    """원격 파일 목록에서 처리 대상 파일만 필터링"""
    import fnmatch
    remote_files = []
    current_dir = sftp.getcwd()
    
    for item in all_items:
        path_separator = "/" if not remote_base_path.endswith("/") else ""
        remote_path = normalize_remote_path(f"{remote_base_path}{path_separator}{item}")
        
        # 디렉토리 건너뜀
        try:
            if sftp.stat(remote_path).st_mode & 0o040000:
                logging.debug(f"디렉토리 건너뜀: {item}")
                continue
        except:
            pass
        
        # 파일 패턴 매칭
        if not any(fnmatch.fnmatch(item, pattern) for pattern in REMOTE_FILE_PATTERNS):
            logging.debug(f"  ⏭ 패턴 불일치: {item}")
            continue
        
        # 파일 크기 조회
        file_size = get_file_size(sftp, remote_path, current_dir)
        if file_size == 0:
            logging.debug(f"  ⚠️ 파일 크기 조회 실패 (다운로드 시 확인): {item}")
        
        # 파일 처리 여부 확인
        if should_process_file(item, file_size, start_date, end_date) or file_size == 0:
            remote_files.append({
                "name": item,
                "path": remote_path,
                "size": file_size
            })
        else:
            logging.debug(f"  ⏭ 건너뜀: {item}")
    
    return remote_files


def ensure_local_directory(directory: str) -> Path:
    """
    로컬 디렉토리 생성 (없으면 생성)
    Docker 컨테이너에서 호스트 경로에 접근하는 경우를 고려
    """
    path = Path(directory)
    
    # 이미 존재하는 경우 확인
    if path.exists():
        if not path.is_dir():
            raise ValueError(f"경로가 디렉토리가 아닙니다: {directory}")
        if not os.access(path, os.W_OK):
            raise PermissionError(
                f"디렉토리에 쓰기 권한이 없습니다: {directory}\n"
                f"해결 방법: Docker 볼륨 마운트가 올바르게 설정되었는지 확인하세요."
            )
        return path
    
    # 부모 디렉토리 확인 및 생성
    try:
        path.mkdir(parents=True, exist_ok=True)
    except PermissionError as e:
        raise PermissionError(
            f"디렉토리 생성 권한이 없습니다: {directory}\n"
            f"원인: {e}\n"
            f"해결 방법:\n"
            f"  1. Docker 볼륨 마운트 확인: docker-compose.yml에 다음을 추가하세요:\n"
            f"     volumes:\n"
            f"       - /호스트경로/hmi_raw:/media/btx/hmi_raw\n"
            f"  2. 호스트에서 디렉토리 생성 및 권한 설정:\n"
            f"     sudo mkdir -p /호스트경로/hmi_raw\n"
            f"     sudo chown -R {os.getuid()}:{os.getgid()} /호스트경로/hmi_raw"
        ) from e
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"디렉토리를 생성할 수 없습니다: {directory}\n"
            f"원인: {e}\n"
            f"해결 방법:\n"
            f"  1. Docker 볼륨 마운트가 올바르게 설정되었는지 확인\n"
            f"  2. 호스트 경로가 존재하는지 확인\n"
            f"  3. 환경 변수 HMI_RAW_DATA_BASE_PATH가 올바른지 확인"
        ) from e
    
    return path


def check_disk_space(path: str, required_bytes: int) -> bool:
    """
    디스크 공간 확인
    
    Args:
        path: 확인할 경로
        required_bytes: 필요한 바이트 수
    
    Returns:
        공간이 충분하면 True, 부족하면 False
    """
    try:
        import shutil
        
        # 경로가 존재하지 않으면 부모 디렉토리 확인
        check_path = Path(path)
        if not check_path.exists():
            # 부모 디렉토리 확인
            check_path = check_path.parent
            while not check_path.exists() and check_path != check_path.parent:
                check_path = check_path.parent
            
            if not check_path.exists():
                logging.warning(f"디스크 공간 확인: 경로가 존재하지 않아 확인을 건너뜁니다: {path}")
                return True  # 경로가 없으면 확인 건너뜀
        
        stat = shutil.disk_usage(str(check_path))
        free_space = stat.free
        
        if free_space < required_bytes:
            logging.warning(
                f"⚠️ 디스크 공간 부족: "
                f"필요: {required_bytes / 1024 / 1024 / 1024:.2f} GB, "
                f"사용 가능: {free_space / 1024 / 1024 / 1024:.2f} GB"
            )
            return False
        
        logging.info(
            f"✅ 디스크 공간 확인: "
            f"필요: {required_bytes / 1024 / 1024 / 1024:.2f} GB, "
            f"사용 가능: {free_space / 1024 / 1024 / 1024:.2f} GB"
        )
        return True
    except Exception as e:
        logging.warning(f"디스크 공간 확인 실패: {e} (계속 진행)")
        return True  # 확인 실패 시 계속 진행

