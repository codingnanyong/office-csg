"""
PLC 데이터 추출 메인 애플리케이션 (단일 실행)
cron으로 5초마다 실행되는 방식
"""
import asyncio
import sys
import logging
from datetime import datetime

from app.utils.logger import setup_logging, get_logger
from app.core.plc_connector import PLCConnector
from app.config.settings import settings
from app.utils.db_manager import db_manager
from app.utils.csv_manager import csv_manager

# 로깅 설정
setup_logging()
logger = get_logger(__name__)


async def extract_plc_data():
    """PLC 데이터 추출 및 저장 (단일 실행)"""
    logger.info("🚀 PLC 데이터 추출 시작")
    logger.info(f"실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    plc_connector = PLCConnector()
    
    try:
        # PLC 연결
        if not await plc_connector.connect():
            logger.error("❌ PLC 연결 실패로 인한 프로그램 종료")
            return False
        
        logger.info("✅ PLC 연결 성공")
        
        # 데이터 읽기
        logger.info("📊 PLC 데이터 읽기 시작")
        data = await plc_connector.read_continuous_addresses()
        
        if not data:
            logger.error("❌ 데이터를 읽지 못했습니다")
            return False
        
        # 결과 통계
        total_count = sum(len(values) for values in data.values())
        logger.info(f"✅ 데이터 읽기 완료: 총 {total_count}개 데이터")
        
        # 주소 타입별 통계
        for address_type, values in data.items():
            logger.info(f"  - {address_type}: {len(values)}개")
        
        # 데이터베이스 저장
        await db_manager.save_plc_data(data)
        
        # CSV 저장 (선택사항)
        await csv_manager.save_plc_data(data)
        
        logger.info("🎉 PLC 데이터 추출 완료")
        return True
        
    except Exception as e:
        logger.error(f"❌ 데이터 추출 중 오류: {e}")
        return False
    finally:
        # PLC 연결 해제
        await plc_connector.disconnect()
        logger.info("🔌 PLC 연결 해제")




async def main():
    """메인 함수"""
    try:
        success = await extract_plc_data()
        if success:
            logger.info("✅ 실행 완료")
            sys.exit(0)
        else:
            logger.error("❌ 실행 실패")
            sys.exit(1)
    except Exception as e:
        logger.error(f"프로그램 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
