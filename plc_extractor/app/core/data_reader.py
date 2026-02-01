"""
PLC 데이터 읽기 모듈
"""
import time
from typing import Dict, List, Any, Optional
from pyModbusTCP.client import ModbusClient
import logging
from datetime import datetime

from app.config.settings import settings
from app.core.address_converter import address_converter

logger = logging.getLogger(__name__)


class DataReader:
    """PLC 데이터 읽기 클래스"""
    
    def __init__(self, client: ModbusClient):
        self.client = client
    
    async def read_chunked_coils(self, start_address: int, total_count: int) -> List[bool]:
        """코일을 125개씩 나누어서 읽기"""
        max_chunk_size = 125
        all_data = []
        
        for i in range(0, total_count, max_chunk_size):
            chunk_size = min(max_chunk_size, total_count - i)
            current_address = start_address + i
            
            logger.debug(f"코일 청크 읽기: 주소 {current_address}, 개수 {chunk_size}")
            
            chunk_data = self.client.read_coils(current_address, chunk_size)
            if chunk_data is None:
                logger.error(f"코일 청크 읽기 실패: 주소 {current_address}, 개수 {chunk_size}")
                return None
            
            all_data.extend(chunk_data)
        
        logger.info(f"코일 청크 읽기 완료: 총 {len(all_data)}개 데이터")
        return all_data
    
    async def read_chunked_registers(self, start_address: int, total_count: int) -> List[int]:
        """레지스터를 125개씩 나누어서 읽기"""
        max_chunk_size = 125
        all_data = []
        
        for i in range(0, total_count, max_chunk_size):
            chunk_size = min(max_chunk_size, total_count - i)
            current_address = start_address + i
            
            logger.debug(f"레지스터 청크 읽기: 주소 {current_address}, 개수 {chunk_size}")
            
            chunk_data = self.client.read_holding_registers(current_address, chunk_size)
            if chunk_data is None:
                logger.error(f"레지스터 청크 읽기 실패: 주소 {current_address}, 개수 {chunk_size}")
                return None
            
            all_data.extend(chunk_data)
        
        logger.info(f"레지스터 청크 읽기 완료: 총 {len(all_data)}개 데이터")
        return all_data
    
    async def read_m_addresses(self, total_count: int) -> List[bool]:
        """M 주소를 미리 준비된 변환 주소로 개별 읽기"""
        all_data = []
        max_chunk_size = 125
        
        # M 주소의 미리 변환된 주소 목록 가져오기
        address_config = settings.get_plc_addresses()
        modbus_addresses = address_config['M'].get('modbus_addresses', [])
        
        if not modbus_addresses:
            logger.error("M 주소의 변환된 주소 목록이 없습니다")
            return []
        
        for i in range(0, total_count, max_chunk_size):
            chunk_size = min(max_chunk_size, total_count - i)
            chunk_data = []
            
            for j in range(chunk_size):
                if i + j < len(modbus_addresses):
                    modbus_address = modbus_addresses[i + j]
                    
                    # 개별 주소 읽기
                    single_data = self.client.read_coils(modbus_address, 1)
                    if single_data is None:
                        logger.warning(f"M{i+j} (Modbus {modbus_address}) 읽기 실패")
                        chunk_data.append(False)
                    else:
                        chunk_data.append(single_data[0] if single_data else False)
                else:
                    chunk_data.append(False)
            
            all_data.extend(chunk_data)
            logger.debug(f"M 주소 청크 읽기 완료: {i}~{i+chunk_size-1} ({chunk_size}개)")
        
        logger.info(f"M 주소 개별 읽기 완료: 총 {len(all_data)}개 데이터")
        return all_data
    
    async def read_address_type(self, address_type: str, config: Dict[str, Any]) -> List[Any]:
        """특정 주소 타입의 데이터 읽기"""
        # 시작 주소 변환
        if address_type == 'M':
            start_modbus_address = config['start_address']  # 이미 변환된 주소
        else:
            start_modbus_address = address_converter.convert_address(address_type, config['start_address'])
        
        logger.info(f"{address_type} 주소 읽기 시작: PLC {config['start_address']} → Modbus {start_modbus_address}, 개수: {config['count']}")
        
        # 시간 측정 시작
        start_time = time.time()
        
        # 주소 타입별 읽기
        if address_type == 'X':
            # X: 디지털 입력
            response = self.client.read_discrete_inputs(start_modbus_address, config['count'])
        elif address_type == 'Y':
            # Y: 코일
            response = self.client.read_coils(start_modbus_address, config['count'])
        elif address_type == 'M':
            # M: 코일 (개별 주소 변환 후 개별 읽기)
            response = await self.read_m_addresses(config['count'])
        elif address_type == 'D':
            # D: 홀딩 레지스터 (125개씩 나누어서 읽기)
            response = await self.read_chunked_registers(start_modbus_address, config['count'])
        else:
            logger.error(f"알 수 없는 주소 타입: {address_type}")
            return None
        
        # 시간 측정 종료
        read_time = time.time() - start_time
        
        if response is None:
            logger.error(f"{address_type} 주소 읽기 오류 (소요시간: {read_time:.3f}초)")
            return None
        
        # 성능 로깅
        logger.info(f"{address_type} 주소 읽기 완료:")
        logger.info(f"  - 읽기 시간: {read_time:.3f}초 ({config['count']}개 주소)")
        logger.info(f"  - 읽기 속도: {config['count']/read_time:.1f} 주소/초")
        
        return response
