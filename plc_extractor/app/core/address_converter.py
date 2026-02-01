"""
PLC 주소 변환 모듈
"""
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


def convert_plc_address(address_type: str, plc_address: int) -> int:
    """
    PLC 주소를 Modbus 주소로 변환
    
    Args:
        address_type: 주소 타입 (X, Y, M, D)
        plc_address: PLC 주소 번호
        
    Returns:
        변환된 Modbus 주소
    """
    if address_type == 'X':
        # X: OCT → DEC
        oct_str = oct(plc_address)[2:]  # '0o' 제거
        return int(oct_str, 8)
    
    elif address_type == 'Y':
        # Y: OCT → DEC
        oct_str = oct(plc_address)[2:]  # '0o' 제거
        return int(oct_str, 8)
    
    elif address_type == 'M':
        # M: DEC → HEX → 앞에 '2' 붙이기 → DEC
        hex_str = hex(plc_address)[2:].upper()  # '0x' 제거하고 대문자로
        hex_with_2 = '2' + hex_str
        return int(hex_with_2, 16)
    
    elif address_type == 'D':
        # D: 그대로
        return plc_address
    
    else:
        return plc_address


class AddressConverter:
    """주소 변환 관리 클래스"""
    
    def __init__(self):
        self.conversion_cache = {}
    
    def convert_address(self, address_type: str, plc_address: int) -> int:
        """주소 변환 (캐시 사용)"""
        cache_key = f"{address_type}_{plc_address}"
        
        if cache_key not in self.conversion_cache:
            self.conversion_cache[cache_key] = convert_plc_address(address_type, plc_address)
        
        return self.conversion_cache[cache_key]
    
    def convert_address_list(self, address_type: str, plc_addresses: List[int]) -> List[int]:
        """주소 리스트 변환"""
        return [self.convert_address(address_type, addr) for addr in plc_addresses]
    
    def clear_cache(self):
        """캐시 초기화"""
        self.conversion_cache.clear()
        logger.debug("주소 변환 캐시 초기화")


# 전역 주소 변환기 인스턴스
address_converter = AddressConverter()
