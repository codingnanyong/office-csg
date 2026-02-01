"""
PLC 연결 및 데이터 읽기 모듈
연속적으로 주소를 읽고 필요한 값만 추출
"""
import asyncio
import time
from typing import Dict, List, Any, Optional
from pyModbusTCP.client import ModbusClient
import logging
from datetime import datetime

from app.config.settings import settings
from app.core.data_reader import DataReader
from app.core.data_extractor import DataExtractor
from app.core.error_handler import error_handler

logger = logging.getLogger(__name__)


class PLCConnector:
    """PLC 연결 및 데이터 읽기 클래스"""
    
    def __init__(self):
        self.client: Optional[ModbusClient] = None
        self.is_connected = False
        self.data_reader: Optional[DataReader] = None
        self.data_extractor = DataExtractor()
        
    async def connect(self) -> bool:
        """PLC에 연결"""
        try:
            logger.info(f"PLC 연결 시도: {settings.plc.host}:{settings.plc.port}")
            logger.info(f"연결 설정: 타임아웃={settings.plc.timeout}초")
            
            self.client = ModbusClient(
                host=settings.plc.host,
                port=settings.plc.port,
                timeout=settings.plc.timeout
            )
            
            # 연결 시도
            self.is_connected = self.client.open()
            
            if self.is_connected:
                logger.info(f"✅ PLC 연결 성공: {settings.plc.host}:{settings.plc.port}")
                # 데이터 리더 초기화
                self.data_reader = DataReader(self.client)
                return True
            else:
                # 연결 실패 시 구체적인 원인 분석
                analysis_result = error_handler.analyze_connection_error(settings.plc.host, settings.plc.port)
                return error_handler.handle_connection_failure(settings.plc.host, settings.plc.port, analysis_result)
                
        except ConnectionRefusedError as e:
            return error_handler.handle_connection_refused(settings.plc.host, settings.plc.port, e)
        except TimeoutError as e:
            return error_handler.handle_timeout_error(settings.plc.host, settings.plc.port, settings.plc.timeout, e)
        except OSError as e:
            if "Network is unreachable" in str(e):
                return error_handler.handle_network_unreachable(settings.plc.host, e)
            elif "No route to host" in str(e):
                return error_handler.handle_no_route_to_host(settings.plc.host, e)
            else:
                return error_handler.handle_network_error(settings.plc.host, e)
        except Exception as e:
            return error_handler.handle_unexpected_error(settings.plc.host, settings.plc.port, e)
    
    
    async def disconnect(self):
        """PLC 연결 해제"""
        if self.client and self.is_connected:
            self.client.close()
            self.is_connected = False
            self.data_reader = None
            logger.info("PLC 연결 해제")
    
    async def read_continuous_addresses(self) -> Dict[str, List[Any]]:
        """
        연속적으로 모든 주소 타입을 읽고 필요한 값만 추출
        """
        if not self.is_connected or not self.data_reader:
            logger.error("PLC가 연결되지 않았습니다")
            return {}
        
        address_config = settings.get_plc_addresses()
        raw_data = {}
        
        try:
            # 1단계: 모든 주소 타입의 원시 데이터 읽기
            for address_type, config in address_config.items():
                raw_data[address_type] = await self.data_reader.read_address_type(address_type, config)
            
            # 2단계: 필요한 주소만 추출
            result = self.data_extractor.extract_all_addresses(raw_data, address_config)
            
            # 3단계: 전체 성능 통계
            total_extracted = sum(len(values) for values in result.values())
            total_read = sum(config['count'] for config in address_config.values())
            logger.info(f"📊 전체 데이터 처리 완료: {total_extracted}개 추출 ({total_extracted/total_read*100:.1f}% 추출률)")
            
            return result
        
        except Exception as e:
            logger.error(f"데이터 읽기 오류: {e}")
            return {}
