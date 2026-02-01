"""
PLC 데이터 추출 시스템 설정 관리
"""
import os
import json
from pathlib import Path
from typing import List, Dict, Any
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class PLCSettings(BaseSettings):
    """PLC 연결 설정"""
    host: str = Field(default="localhost", env="PLC_HOST")
    port: int = Field(default=502, env="PLC_PORT")
    timeout: int = Field(default=5, env="PLC_TIMEOUT")
    
    # 연속 읽기 설정
    read_interval: int = Field(default=5, env="READ_INTERVAL")  # 초 단위
    
    class Config:
        env_prefix = "PLC_"


class DatabaseSettings(BaseSettings):
    """데이터베이스 설정"""
    url: str = Field(default="postgresql://user:password@localhost:5432/telemetry", env="DATABASE_URL")
    pool_size: int = Field(default=10, env="DB_POOL_SIZE")
    max_overflow: int = Field(default=20, env="DB_MAX_OVERFLOW")
    echo: bool = Field(default=False, env="DB_ECHO")
    
    class Config:
        env_prefix = ""


class AppSettings(BaseSettings):
    """애플리케이션 설정"""
    name: str = Field(default="PLC Extractor", env="APP_NAME")
    version: str = Field(default="1.0.0", env="APP_VERSION")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    class Config:
        env_prefix = "APP_"


class Settings:
    """전체 설정 관리"""
    def __init__(self):
        self.plc = PLCSettings()
        self.database = DatabaseSettings()
        self.app = AppSettings()
        self._load_address_config()
    
    def _load_address_config(self):
        """외부 JSON 파일에서 PLC 주소 설정 로드"""
        config_path = os.getenv("PLC_ADDRESS_CONFIG", "config/plc_addresses.json")
        config_file = Path(config_path)
        
        if config_file.exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
                self._address_config = config_data.get("addresses", {})
        else:
            # 기본 설정 (최소한의 예시)
            self._address_config = self._get_default_addresses()
    
    def _get_default_addresses(self) -> Dict[str, Dict[str, Any]]:
        """기본 PLC 주소 설정 (예시용)"""
        return {
            "X": {"start_address": 0, "count": 10, "extract_addresses": [0, 1, 2]},
            "Y": {"start_address": 0, "count": 10, "extract_addresses": [0, 1, 2]},
            "M": {"start_address": 0, "count": 100, "modbus_addresses": [32, 33], "extract_addresses": [0, 1, 2]},
            "D": {"start_address": 0, "count": 100, "extract_addresses": [8, 60, 61]}
        }
    
    def get_plc_addresses(self) -> Dict[str, Dict[str, Any]]:
        """PLC 주소 설정 반환"""
        return self._address_config


# 전역 설정 인스턴스
settings = Settings()
