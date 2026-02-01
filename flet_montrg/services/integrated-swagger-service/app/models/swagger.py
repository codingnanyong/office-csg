"""
Swagger/OpenAPI integration models
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class OpenAPISpec(BaseModel):
    """OpenAPI 스펙 정보"""
    service_name: str
    title: str
    version: str
    description: Optional[str] = None
    base_url: str
    spec: Dict[str, Any] = {}  # 실제 OpenAPI JSON 스펙
    paths: Dict[str, Any] = {}
    components: Dict[str, Any] = {}
    tags: List[Dict[str, Any]] = []
    servers: List[Dict[str, Any]] = []
    last_updated: datetime = Field(default_factory=datetime.now)
    is_available: bool = False
    error_message: Optional[str] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class IntegratedOpenAPISpec(BaseModel):
    """통합된 OpenAPI 스펙"""
    openapi: str = "3.0.3"
    info: Dict[str, Any] = {
        "title": "통합 API 문서",
        "version": "1.0.0",
        "description": "모든 마이크로서비스의 통합 API 문서"
    }
    servers: List[Dict[str, Any]] = []
    paths: Dict[str, Any] = {}
    components: Dict[str, Any] = {
        "schemas": {},
        "securitySchemes": {}
    }
    tags: List[Dict[str, Any]] = []
    services: List[str] = []  # 포함된 서비스 목록
    last_updated: datetime = Field(default_factory=datetime.now)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SwaggerUIConfig(BaseModel):
    """Swagger UI 설정"""
    title: str = "통합 API 문서"
    description: str = "모든 마이크로서비스의 API를 한 곳에서 확인하고 테스트하세요"
    services: List[OpenAPISpec] = []
    integrated_spec_url: str = "/openapi.json"
    proxy_base_url: str = "/api/proxy"
    deep_linking: bool = True
    display_operation_id: bool = False
    default_models_expand_depth: int = 1
    default_model_expand_depth: int = 1
    display_request_duration: bool = True
    show_extensions: bool = False
    show_common_extensions: bool = False
    use_unsafe_inline: bool = False