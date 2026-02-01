"""
PLC 데이터 추출 및 저장 서비스
"""
import asyncio
import json
from typing import Dict, List, Any
from datetime import datetime
import logging

from app.core.plc_connector import PLCConnector
from app.models.database import db_manager
from app.models.plc_data import PLCDataModel, PLCDataPydantic
from app.config.settings import settings

logger = logging.getLogger(__name__)


class PLCDataExtractionService:
    """PLC 데이터 추출 및 저장 서비스"""
    
    def __init__(self):
        self.plc_connector = PLCConnector()
        self.is_running = False
    
    async def start_extraction(self):
        """데이터 추출 시작"""
        logger.info("PLC 데이터 추출 서비스 시작")
        
        # 데이터베이스 연결 테스트
        if not db_manager.test_connection():
            logger.error("데이터베이스 연결 실패로 서비스 시작 불가")
            return False
        
        # 테이블 생성
        try:
            db_manager.create_tables()
        except Exception as e:
            logger.error(f"테이블 생성 실패: {e}")
            return False
        
        self.is_running = True
        
        # 주기적으로 데이터 추출
        while self.is_running:
            try:
                await self._extract_and_save_data()
                # 데이터 추출 완료 후 PLC 연결 해제
                await self.plc_connector.disconnect()
                await asyncio.sleep(settings.plc.read_interval)
            except Exception as e:
                logger.error(f"데이터 추출 중 오류: {e}")
                # 오류 발생 시에도 연결 해제
                await self.plc_connector.disconnect()
                await asyncio.sleep(5)  # 오류 시 5초 대기 후 재시도
        
        return True
    
    async def stop_extraction(self):
        """데이터 추출 중지"""
        logger.info("PLC 데이터 추출 서비스 중지")
        self.is_running = False
        await self.plc_connector.disconnect()
    
    async def _extract_and_save_data(self):
        """데이터 추출 및 저장"""
        try:
            # PLC 연결 (매번 새로 연결)
            if not await self.plc_connector.connect():
                logger.error("PLC 연결 실패")
                return
            
            # PLC에서 데이터 읽기
            plc_data = await self.plc_connector.read_continuous_addresses()
            
            if not plc_data:
                logger.warning("PLC에서 데이터를 읽지 못했습니다")
                return
            
            # 데이터베이스에 저장
            await self._save_to_database(plc_data)
            
            total_count = sum(len(data) for data in plc_data.values())
            logger.info(f"데이터 추출 및 저장 완료: {total_count}개")
            
        except Exception as e:
            logger.error(f"데이터 추출 및 저장 오류: {e}")
            raise
    
    async def _save_to_database(self, plc_data: Dict[str, List[Dict[str, Any]]]):
        """데이터베이스에 데이터 저장"""
        try:
            # 세션 생성
            session_gen = db_manager.get_session()
            session = next(session_gen)
            
            try:
                for address_type, data_list in plc_data.items():
                    for item in data_list:
                        # PLC 데이터 모델 생성
                        plc_data_model = PLCDataModel(
                            address_type=address_type,
                            address=item['address'],
                            value=json.dumps(item['value']),  # JSON 문자열로 저장
                            timestamp=item['timestamp']
                        )
                        
                        session.add(plc_data_model)
                
                # 커밋
                session.commit()
                logger.debug(f"데이터베이스 저장 완료: {len(plc_data)}개 타입")
                
            except Exception as e:
                session.rollback()
                logger.error(f"데이터베이스 저장 오류: {e}")
                raise
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"데이터베이스 세션 오류: {e}")
            raise
    
    async def get_latest_data(self, address_type: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """최신 데이터 조회"""
        try:
            session_gen = db_manager.get_session()
            session = next(session_gen)
            
            try:
                query = session.query(PLCDataModel)
                
                if address_type:
                    query = query.filter(PLCDataModel.address_type == address_type)
                
                results = query.order_by(PLCDataModel.timestamp.desc()).limit(limit).all()
                
                data = []
                for result in results:
                    data.append({
                        'id': result.id,
                        'address_type': result.address_type,
                        'address': result.address,
                        'value': json.loads(result.value),
                        'timestamp': result.timestamp.isoformat()
                    })
                
                return data
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"데이터 조회 오류: {e}")
            return []
    
    async def get_data_by_address(self, address_type: str, address: int, limit: int = 100) -> List[Dict[str, Any]]:
        """특정 주소의 데이터 조회"""
        try:
            session_gen = db_manager.get_session()
            session = next(session_gen)
            
            try:
                results = session.query(PLCDataModel).filter(
                    PLCDataModel.address_type == address_type,
                    PLCDataModel.address == address
                ).order_by(PLCDataModel.timestamp.desc()).limit(limit).all()
                
                data = []
                for result in results:
                    data.append({
                        'id': result.id,
                        'address_type': result.address_type,
                        'address': result.address,
                        'value': json.loads(result.value),
                        'timestamp': result.timestamp.isoformat()
                    })
                
                return data
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"특정 주소 데이터 조회 오류: {e}")
            return []
