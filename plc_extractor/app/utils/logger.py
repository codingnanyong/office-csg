"""
로깅 설정 및 유틸리티
"""
import logging
import sys
import glob
import os
from datetime import datetime, timedelta
from pathlib import Path
from logging.handlers import RotatingFileHandler

from app.config.settings import settings


def setup_logging():
    """로깅 설정"""
    # 로그 디렉토리 생성
    log_dir = Path("/media/btx/plc_extractor/logs")
    log_dir.mkdir(exist_ok=True)
    
    # 로그 포맷 설정
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.app.log_level.upper()))
    
    # 기존 핸들러 제거
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 콘솔 핸들러 (INFO 레벨만)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(log_format, date_format)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # 일반 로그 파일 핸들러 (요약된 메시지)
    log_file = log_dir / f"plc_extractor_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)  # INFO 레벨만 저장
    file_formatter = logging.Formatter(log_format, date_format)
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # 에러 전용 핸들러 (상세한 에러 정보)
    error_file = log_dir / f"plc_extractor_error_{datetime.now().strftime('%Y%m%d')}.err"
    error_handler = RotatingFileHandler(
        error_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)  # ERROR 레벨만 저장
    error_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
        date_format
    )
    error_handler.setFormatter(error_formatter)
    
    # 에러 전용 로거에만 핸들러 추가
    error_detail_logger = logging.getLogger("error_detail")
    error_detail_logger.setLevel(logging.ERROR)
    error_detail_logger.addHandler(error_handler)
    error_detail_logger.propagate = False  # 부모 로거로 전파하지 않음
    
    # 특정 모듈 로그 레벨 설정
    logging.getLogger("pyModbusTCP").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    
    # 오래된 로그 파일 정리
    cleanup_old_logs(log_dir, days=7)
    
    return root_logger


def cleanup_old_logs(log_dir: Path, days: int = 7):
    """오래된 로그 파일 정리"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        # 일반 로그 파일과 에러 파일 모두 정리
        log_files = glob.glob(str(log_dir / "plc_extractor_*.log*")) + glob.glob(str(log_dir / "plc_extractor_*.err*"))
        
        for log_file in log_files:
            file_time = datetime.fromtimestamp(os.path.getmtime(log_file))
            if file_time < cutoff_date:
                os.remove(log_file)
                print(f"오래된 로그 파일 삭제: {log_file}")
    except Exception as e:
        print(f"로그 파일 정리 중 오류: {e}")


def log_error_summary(logger, error_msg: str, details: str = None):
    """에러 요약 로그 (일반 로그 파일용) - 요약된 메시지만"""
    logger.error(error_msg)


def log_error_detail(logger, error_msg: str, exception: Exception = None, context: dict = None):
    """에러 상세 로그 (에러 전용 파일용) - 상세 정보만"""
    # 에러 전용 로거 생성 (ERROR 레벨만 기록)
    error_logger = logging.getLogger("error_detail")
    error_logger.setLevel(logging.ERROR)
    
    if exception:
        error_logger.error(f"예외 발생: {type(exception).__name__}: {str(exception)}")
        error_logger.error(f"에러 메시지: {error_msg}")
        if context:
            error_logger.error(f"컨텍스트: {context}")
    else:
        error_logger.error(f"에러: {error_msg}")
        if context:
            error_logger.error(f"컨텍스트: {context}")


def get_logger(name: str) -> logging.Logger:
    """로거 인스턴스 반환"""
    return logging.getLogger(name)
