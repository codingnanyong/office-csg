"""
Sensor Threshold Mapping service layer
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, and_, or_
from typing import List, Optional
from datetime import datetime, timezone

from app.models.database_models import SensorThresholdMap
from app.models.schemas import MappingCreate, MappingUpdate
from app.core.logging import get_logger

logger = get_logger(__name__)


class MappingService:
    """센서-임계치 매핑 서비스"""
    
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_mappings(
        self,
        skip: int = 0,
        limit: int = 100,
        sensor_id: Optional[str] = None,
        threshold_id: Optional[int] = None,
        enabled: Optional[bool] = None
    ) -> List[SensorThresholdMap]:
        """매핑 목록 조회"""
        query = select(SensorThresholdMap)
        
        # 필터 조건 추가
        if sensor_id:
            query = query.filter(SensorThresholdMap.sensor_id == sensor_id)
        if threshold_id:
            query = query.filter(SensorThresholdMap.threshold_id == threshold_id)
        if enabled is not None:
            query = query.filter(SensorThresholdMap.enabled == enabled)
        
        # 유효 기간 필터링 (현재 시간 기준)
        now = datetime.now(timezone.utc)
        query = query.filter(
            or_(
                SensorThresholdMap.effective_from.is_(None),
                SensorThresholdMap.effective_from <= now
            )
        ).filter(
            or_(
                SensorThresholdMap.effective_to.is_(None),
                SensorThresholdMap.effective_to >= now
            )
        )
        
        query = query.order_by(desc(SensorThresholdMap.map_id)).offset(skip).limit(limit)
        
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_mapping_by_id(self, map_id: int) -> Optional[SensorThresholdMap]:
        """ID로 매핑 조회"""
        result = await self.db.execute(
            select(SensorThresholdMap).filter(SensorThresholdMap.map_id == map_id)
        )
        return result.scalar_one_or_none()

    async def get_mappings_by_sensor_id(self, sensor_id: str, enabled: bool = True) -> List[SensorThresholdMap]:
        """센서별 활성 매핑 목록 조회"""
        query = select(SensorThresholdMap).filter(
            SensorThresholdMap.sensor_id == sensor_id,
            SensorThresholdMap.enabled == enabled
        )
        
        # 유효 기간 필터링
        now = datetime.now(timezone.utc)
        query = query.filter(
            or_(
                SensorThresholdMap.effective_from.is_(None),
                SensorThresholdMap.effective_from <= now
            )
        ).filter(
            or_(
                SensorThresholdMap.effective_to.is_(None),
                SensorThresholdMap.effective_to >= now
            )
        )
        
        query = query.order_by(desc(SensorThresholdMap.map_id))
        
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_mappings_by_threshold_id(self, threshold_id: int, enabled: bool = True) -> List[SensorThresholdMap]:
        """임계치별 활성 매핑 목록 조회"""
        query = select(SensorThresholdMap).filter(
            SensorThresholdMap.threshold_id == threshold_id,
            SensorThresholdMap.enabled == enabled
        )
        
        # 유효 기간 필터링
        now = datetime.now(timezone.utc)
        query = query.filter(
            or_(
                SensorThresholdMap.effective_from.is_(None),
                SensorThresholdMap.effective_from <= now
            )
        ).filter(
            or_(
                SensorThresholdMap.effective_to.is_(None),
                SensorThresholdMap.effective_to >= now
            )
        )
        
        query = query.order_by(desc(SensorThresholdMap.map_id))
        
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def create_mapping(self, mapping: MappingCreate) -> SensorThresholdMap:
        """새 매핑 생성"""
        from datetime import datetime, timezone
        mapping_data = mapping.model_dump(exclude_unset=True)
        
        # upd_dt가 없으면 현재 시간 설정 (UTC)
        if "upd_dt" not in mapping_data:
            mapping_data["upd_dt"] = datetime.now(timezone.utc)
        
        db_mapping = SensorThresholdMap(**mapping_data)
        self.db.add(db_mapping)
        await self.db.commit()
        await self.db.refresh(db_mapping)
        
        logger.info(f"Mapping created: map_id={db_mapping.map_id}, sensor_id={db_mapping.sensor_id}, threshold_id={db_mapping.threshold_id}")
        return db_mapping

    async def update_mapping(
        self,
        map_id: int,
        mapping_update: MappingUpdate
    ) -> Optional[SensorThresholdMap]:
        """매핑 수정"""
        db_mapping = await self.get_mapping_by_id(map_id)
        if not db_mapping:
            return None
        
        update_data = mapping_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_mapping, field, value)
        
        db_mapping.upd_dt = datetime.now(timezone.utc)
        
        await self.db.commit()
        await self.db.refresh(db_mapping)
        
        logger.info(f"Mapping updated: map_id={map_id}")
        return db_mapping

    async def delete_mapping(self, map_id: int) -> bool:
        """매핑 삭제"""
        db_mapping = await self.get_mapping_by_id(map_id)
        if not db_mapping:
            return False
        
        await self.db.delete(db_mapping)
        await self.db.commit()
        
        logger.info(f"Mapping deleted: map_id={map_id}")
        return True

    async def enable_mapping(self, map_id: int) -> Optional[SensorThresholdMap]:
        """매핑 활성화"""
        db_mapping = await self.get_mapping_by_id(map_id)
        if not db_mapping:
            return None
        
        db_mapping.enabled = True
        db_mapping.upd_dt = datetime.now(timezone.utc)
        
        await self.db.commit()
        await self.db.refresh(db_mapping)
        
        logger.info(f"Mapping enabled: map_id={map_id}")
        return db_mapping

    async def disable_mapping(self, map_id: int) -> Optional[SensorThresholdMap]:
        """매핑 비활성화"""
        db_mapping = await self.get_mapping_by_id(map_id)
        if not db_mapping:
            return None
        
        db_mapping.enabled = False
        db_mapping.upd_dt = datetime.now(timezone.utc)
        
        await self.db.commit()
        await self.db.refresh(db_mapping)
        
        logger.info(f"Mapping disabled: map_id={map_id}")
        return db_mapping
