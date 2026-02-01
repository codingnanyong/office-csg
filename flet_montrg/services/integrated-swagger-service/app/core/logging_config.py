"""
Logging configuration for API Dashboard Service
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Any

from .config import settings


def setup_logging() -> None:
    """로깅 설정"""
    
    # 로그 레벨 설정
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    
    # 로그 포맷 설정
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # 루트 로거 설정
    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=date_format,
        handlers=[]
    )
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(log_format, date_format)
    console_handler.setFormatter(console_formatter)
    
    # 파일 핸들러 (로그 디렉토리가 있는 경우)
    log_dir = Path("logs")
    if log_dir.exists() or settings.environment == "production":
        try:
            log_dir.mkdir(exist_ok=True)
            file_handler = RotatingFileHandler(
                log_dir / "api_dashboard.log",
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5
            )
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter(log_format, date_format)
            file_handler.setFormatter(file_formatter)
            
            # 루트 로거에 핸들러 추가
            root_logger = logging.getLogger()
            root_logger.addHandler(file_handler)
            root_logger.addHandler(console_handler)
            
        except Exception as e:
            # 파일 핸들러 생성 실패 시 콘솔 핸들러만 사용
            root_logger = logging.getLogger()
            root_logger.addHandler(console_handler)
            logging.warning(f"Failed to create file handler: {e}")
    else:
        # 콘솔 핸들러만 사용
        root_logger = logging.getLogger()
        root_logger.addHandler(console_handler)
    
    # 외부 라이브러리 로그 레벨 조정
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("fastapi").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("kubernetes").setLevel(logging.WARNING)
    
    if settings.debug:
        logging.getLogger("uvicorn.access").setLevel(logging.DEBUG)
    else:
        logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """로거 인스턴스 반환"""
    return logging.getLogger(name)