"""
PLC 데이터 추출 모듈
"""
import time
from typing import Dict, List, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DataExtractor:
    """PLC 데이터 추출 클래스"""
    
    def __init__(self):
        pass
    
    def extract_addresses(self, address_type: str, response: List[Any], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """필요한 주소만 추출"""
        extract_start_time = time.time()
        extracted_data = []
        
        for addr in config['extract_addresses']:
            if addr < config['count']:
                if address_type in ['X', 'Y']:
                    value = response[addr] if response and len(response) > addr else False
                else:
                    value = response[addr] if response and len(response) > addr else 0
                
                extracted_data.append({
                    'address': addr,
                    'value': value,
                    'timestamp': datetime.now()
                })
        
        extract_time = time.time() - extract_start_time
        
        # 추출 성능 로깅
        logger.info(f"{address_type} 데이터 추출 완료:")
        logger.info(f"  - 추출 시간: {extract_time:.3f}초 ({len(extracted_data)}개 값)")
        logger.info(f"  - 추출 비율: {len(extracted_data)}/{config['count']} ({len(extracted_data)/config['count']*100:.1f}%)")
        
        return extracted_data
    
    def extract_all_addresses(self, data: Dict[str, List[Any]], address_config: Dict[str, Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """모든 주소 타입의 데이터 추출"""
        result = {}
        
        for address_type, response in data.items():
            if response is not None and address_type in address_config:
                config = address_config[address_type]
                extracted_data = self.extract_addresses(address_type, response, config)
                result[address_type] = extracted_data
            else:
                logger.warning(f"{address_type} 주소 데이터가 없어서 추출을 건너뜁니다")
                result[address_type] = []
        
        return result
