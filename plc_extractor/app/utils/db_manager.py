"""
데이터베이스 저장 로직 관리
"""
import pandas as pd
from datetime import datetime
from typing import Dict, Any
import logging

from app.core.database import db_manager

logger = logging.getLogger(__name__)


class DatabaseManager:
    """데이터베이스 저장 로직 관리 클래스"""
    
    def __init__(self):
        self.db_manager = db_manager
    
    async def save_plc_data(self, data: Dict[str, Any]) -> bool:
        """TimescaleDB에 최적화된 배치 삽입"""
        try:
            # 데이터베이스 연결 테스트
            if not self.db_manager.test_connection():
                logger.error("❌ 데이터베이스 연결 실패")
                return False
            
            # 데이터 변환
            all_data = []
            load_dt = datetime.now()  # DB 저장 시간
            
            for address_type, values in data.items():
                for item in values:
                    plc_key = f"{address_type}{item['address']}"
                    all_data.append({
                        'extract_dt': item['timestamp'],  # PLC에서 읽은 실제 시간 사용
                        'plc_key': plc_key,
                        'value': str(item['value']),
                        'load_dt': load_dt  # DB 저장 시간
                    })
            
            if all_data:
                # DataFrame으로 변환
                df = pd.DataFrame(all_data)
                
                # 배치 삽입 (TimescaleDB 최적화)
                df.to_sql(
                    'plc_data_raw',
                    con=self.db_manager.engine,
                    schema='bronze',
                    if_exists='append',
                    index=False,
                    method='multi',  # 배치 삽입 최적화
                    chunksize=1000   # 청크 단위로 삽입
                )
                
                logger.info(f"📊 TimescaleDB 저장 완료: {len(all_data)}개 데이터")
                return True
            else:
                logger.warning("저장할 데이터가 없습니다")
                return False
                
        except Exception as e:
            logger.error(f"TimescaleDB 저장 중 오류: {e}")
            return False


# 전역 데이터베이스 매니저 인스턴스
db_manager = DatabaseManager()
