"""
PLC 연결 에러 처리 모듈
"""
import socket
from typing import Dict, Any, Optional
import logging

from app.config.settings import settings
from app.utils.logger import log_error_summary, log_error_detail

logger = logging.getLogger(__name__)


class PLCErrorHandler:
    """PLC 연결 에러 처리 클래스"""
    
    def __init__(self):
        pass
    
    def handle_connection_refused(self, host: str, port: int, exception: Exception) -> bool:
        """연결 거부 에러 처리"""
        summary_msg = f"❌ PLC 연결 거부: {host}:{port}"
        detail_msg = f"서버가 연결을 거부했습니다. 가능한 원인: PLC가 꺼져있거나, 포트가 잘못되었거나, 방화벽 차단"
        log_error_summary(logger, summary_msg)
        log_error_detail(logger, detail_msg, exception, {"host": host, "port": port})
        return False
    
    def handle_timeout_error(self, host: str, port: int, timeout: int, exception: Exception) -> bool:
        """타임아웃 에러 처리"""
        summary_msg = f"❌ PLC 연결 타임아웃: {host}:{port}"
        detail_msg = f"{timeout}초 초과. 가능한 원인: 네트워크 지연, PLC 응답 지연, 잘못된 IP 주소"
        log_error_summary(logger, summary_msg)
        log_error_detail(logger, detail_msg, exception, {"host": host, "port": port, "timeout": timeout})
        return False
    
    def handle_network_unreachable(self, host: str, exception: Exception) -> bool:
        """네트워크 도달 불가 에러 처리"""
        summary_msg = f"❌ PLC 네트워크 도달 불가: {host}"
        detail_msg = f"네트워크에 도달할 수 없습니다. 가능한 원인: 잘못된 IP 주소, 네트워크 설정 오류, 라우팅 문제"
        log_error_summary(logger, summary_msg)
        log_error_detail(logger, detail_msg, exception, {"host": host, "error": str(exception)})
        return False
    
    def handle_no_route_to_host(self, host: str, exception: Exception) -> bool:
        """호스트 도달 불가 에러 처리"""
        summary_msg = f"❌ PLC 호스트 도달 불가: {host}"
        detail_msg = f"호스트에 도달할 수 없습니다. 가능한 원인: 잘못된 IP 주소, 호스트가 다운됨, 네트워크 분할"
        log_error_summary(logger, summary_msg)
        log_error_detail(logger, detail_msg, exception, {"host": host, "error": str(exception)})
        return False
    
    def handle_network_error(self, host: str, exception: Exception) -> bool:
        """일반 네트워크 에러 처리"""
        summary_msg = f"❌ PLC 네트워크 오류: {host}"
        log_error_summary(logger, summary_msg)
        log_error_detail(logger, f"네트워크 오류: {exception}", exception, {"host": host, "error": str(exception)})
        return False
    
    def handle_unexpected_error(self, host: str, port: int, exception: Exception) -> bool:
        """예상치 못한 에러 처리"""
        summary_msg = f"❌ PLC 연결 예상치 못한 오류: {host}:{port}"
        detail_msg = f"예상치 못한 오류 발생. 가능한 원인: Modbus 라이브러리 오류, 시스템 리소스 부족"
        log_error_summary(logger, summary_msg)
        log_error_detail(logger, detail_msg, exception, {"host": host, "port": port, "error_type": type(exception).__name__})
        return False
    
    def handle_connection_failure(self, host: str, port: int, analysis_result: str) -> bool:
        """연결 실패 분석 결과 처리"""
        summary_msg = f"❌ PLC 연결 실패: {host}:{port}"
        log_error_summary(logger, summary_msg)
        # 에러 로그에는 상세 정보만
        log_error_detail(logger, f"연결 분석 결과: {analysis_result}", context={"host": host, "port": port, "analysis": analysis_result})
        return False
    
    def analyze_connection_error(self, host: str, port: int) -> str:
        """연결 실패 원인 분석"""
        try:
            # 1. 호스트 도달 가능성 확인
            try:
                socket.gethostbyname(host)
            except socket.gaierror:
                return f"호스트 이름 해석 실패: {host} - DNS 서버에서 IP를 찾을 수 없습니다"
            
            # 2. 포트 연결 가능성 확인
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result == 0:
                    return f"포트 {port}는 열려있지만 Modbus 연결 실패 - PLC가 Modbus 서버를 실행하지 않거나 설정 오류"
                else:
                    return f"포트 {port}가 닫혀있음 - PLC가 꺼져있거나 다른 포트를 사용 중"
                    
            except Exception as e:
                return f"포트 연결 테스트 실패: {e}"
                
        except Exception as e:
            return f"연결 오류 분석 실패: {e}"


# 전역 에러 핸들러 인스턴스
error_handler = PLCErrorHandler()
