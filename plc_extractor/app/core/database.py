"""
데이터베이스 연결 및 세션 관리
"""
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from typing import Generator
import logging

from app.config.settings import settings
from app.models.plc_data import PLCDataModel

logger = logging.getLogger(__name__)

# 데이터베이스 엔진 생성
engine = create_engine(
    settings.database.url,
    pool_size=settings.database.pool_size,
    max_overflow=settings.database.max_overflow,
    echo=settings.database.echo
)

# 세션 팩토리 생성
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base 클래스
Base = declarative_base()


class DatabaseManager:
    """데이터베이스 관리 클래스"""
    
    def __init__(self):
        self.engine = engine
        self.session_factory = SessionLocal
    
    def create_tables(self):
        """테이블 생성"""
        try:
            PLCDataModel.metadata.create_all(bind=self.engine)
            logger.info("데이터베이스 테이블 생성 완료")
        except Exception as e:
            logger.error(f"테이블 생성 오류: {e}")
            raise
    
    def get_session(self) -> Generator[Session, None, None]:
        """데이터베이스 세션 생성"""
        session = self.session_factory()
        try:
            yield session
        except Exception as e:
            session.rollback()
            logger.error(f"데이터베이스 세션 오류: {e}")
            raise
        finally:
            session.close()
    
    def test_connection(self) -> bool:
        """데이터베이스 연결 테스트"""
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            logger.info("데이터베이스 연결 성공")
            return True
        except Exception as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            return False


# 전역 데이터베이스 매니저 인스턴스
db_manager = DatabaseManager()
