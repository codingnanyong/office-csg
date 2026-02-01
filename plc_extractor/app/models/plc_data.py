"""
PLC 데이터 모델 정의
"""
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

Base = declarative_base()


class PLCDataModel(Base):
    """PLC 데이터 데이터베이스 모델"""
    __tablename__ = "plc_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    address_type = Column(String(10), nullable=False)  # X, Y, M, D
    address = Column(Integer, nullable=False)
    value = Column(Text, nullable=False)  # JSON 형태로 저장
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class PLCDataPydantic(BaseModel):
    """PLC 데이터 Pydantic 모델"""
    address_type: str = Field(..., description="주소 타입 (X, Y, M, D)")
    address: int = Field(..., description="주소 번호")
    value: Any = Field(..., description="값")
    timestamp: datetime = Field(default_factory=datetime.now, description="읽기 시간")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class PLCDataBatch(BaseModel):
    """PLC 데이터 배치 모델"""
    data: Dict[str, list] = Field(..., description="주소 타입별 데이터")
    total_count: int = Field(..., description="총 데이터 개수")
    timestamp: datetime = Field(default_factory=datetime.now, description="배치 생성 시간")
    
    def get_all_data(self) -> List[PLCDataPydantic]:
        """모든 데이터를 PLCDataPydantic 리스트로 변환"""
        all_data = []
        for address_type, data_list in self.data.items():
            for item in data_list:
                all_data.append(PLCDataPydantic(
                    address_type=address_type,
                    address=item['address'],
                    value=item['value'],
                    timestamp=item['timestamp']
                ))
        return all_data
