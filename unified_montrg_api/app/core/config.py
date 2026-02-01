"""Application configuration loaded from environment variables."""

import json
import os
import re
from collections import OrderedDict
from typing import Dict, List, Optional, Union

from pydantic import computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Base application settings."""

    app_name: str = "Unified Monitoring API"
    app_version: str = "1.0.0"
    debug: bool = False

    host: str = "0.0.0.0"
    port: int = 8000
    api_prefix: str = "/api"

    # PostgreSQL Database (unified-montrg only)
    montrg_database_url: Optional[str] = None
    
    # Oracle Database (CMMS)
    cmms_database_url: Optional[str] = None

    cors_origins: List[str] = ["*"]

    log_level: str = "INFO"
    environment: str = "development"

    model_config = SettingsConfigDict(
        # Kubernetes uses ConfigMap and Secret for environment variables, so .env file is not needed
        # Only use .env file for local development (if exists)
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,  # .env file is not required
        case_sensitive=False,
    )

    @computed_field
    def database_urls(self) -> Dict[str, str]:
        """Return configured database URLs keyed by alias.
        
        Dynamically read all environment variables matching the patterns IP*_DATABASE_URL, BANB*_DATABASE_URL, CTM*_DATABASE_URL, CKP_*_DATABASE_URL 
        Example: IP04_DATABASE_URL -> ip04, BANB01_DATABASE_URL -> banb01, CTM01_DATABASE_URL -> ctm01, etc.
        """
        urls: Dict[str, str] = OrderedDict()
        
        # Find IP*_DATABASE_URL pattern in environment variables
        # Supports all IP numbers: IP04, IP12, IP20, IP34, IP37, etc.
        ip_pattern = re.compile(r'^IP(\d+)_DATABASE_URL$', re.IGNORECASE)
        banb_pattern = re.compile(r'^BANB(\d+)_DATABASE_URL$', re.IGNORECASE)
        ctm_pattern = re.compile(r'^CTM(\d+)_DATABASE_URL$', re.IGNORECASE)
        
        for key, value in os.environ.items():
            if not value:
                continue
                
            # IP*_DATABASE_URL pattern
            match = ip_pattern.match(key)
            if match:
                ip_number = match.group(1)
                alias = f"ip{ip_number.lower()}"  # ip04, ip12, ip20, ip34, ip37, etc.
                urls[alias] = value
                continue
            
            # BANB*_DATABASE_URL pattern
            match = banb_pattern.match(key)
            if match:
                banb_number = match.group(1)
                alias = f"banb{banb_number.lower()}"  # banb01, banb02, etc.
                urls[alias] = value
                continue
            
            # CTM*_DATABASE_URL pattern
            match = ctm_pattern.match(key)
            if match:
                ctm_number = match.group(1)
                alias = f"ctm{ctm_number.lower()}"  # ctm01, ctm02, etc.
                urls[alias] = value
                continue
            
            # CKP_*_DATABASE_URL pattern
            if key.upper().startswith('CKP_') and key.upper().endswith('_DATABASE_URL'):
                # CKP_IP_DATABASE_URL -> ckp_ip, CKP_CHILLER_DATABASE_URL -> ckp_chiller
                alias = key.replace('_DATABASE_URL', '').lower()
                urls[alias] = value
        
        # PostgreSQL Database (montrg) added
        if self.montrg_database_url:
            urls["montrg"] = self.montrg_database_url
        
        # Oracle Database (CMMS) added
        if self.cmms_database_url:
            urls["cmms"] = self.cmms_database_url
        
        return urls

    @computed_field
    def default_database_alias(self) -> Optional[str]:
        """Return the first configured database alias."""
        return next(iter(self.database_urls.keys()), None)

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, value: Union[str, List[str]]) -> List[str]:
        if isinstance(value, str):
            try:
                loaded = json.loads(value)
                if isinstance(loaded, list):
                    return [str(origin) for origin in loaded if str(origin).strip()]
            except json.JSONDecodeError:
                pass
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        return value.upper()


settings = Settings()
