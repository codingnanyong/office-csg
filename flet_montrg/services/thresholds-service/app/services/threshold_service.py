"""
Threshold service layer
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List, Optional

from app.models.database_models import Thresholds
from app.models.schemas import ThresholdCreate, ThresholdUpdate


class ThresholdService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_thresholds(self, skip: int = 0, limit: int = 100) -> List[Thresholds]:
        """모든 임계치 조회"""
        result = await self.db.execute(
            select(Thresholds)
            .offset(skip)
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_threshold_by_id(self, threshold_id: int) -> Optional[Thresholds]:
        """ID로 임계치 조회"""
        result = await self.db.execute(
            select(Thresholds)
            .filter(Thresholds.threshold_id == threshold_id)
        )
        return result.scalar_one_or_none()

    async def get_thresholds_by_type(self, threshold_type: str) -> List[Thresholds]:
        """타입별 임계치 조회"""
        result = await self.db.execute(
            select(Thresholds)
            .filter(Thresholds.threshold_type == threshold_type)
        )
        return list(result.scalars().all())

    async def create_threshold(self, threshold: ThresholdCreate) -> Thresholds:
        """새 임계치 생성"""
        db_threshold = Thresholds(**threshold.model_dump())
        self.db.add(db_threshold)
        await self.db.commit()
        await self.db.refresh(db_threshold)
        return db_threshold

    async def update_threshold(
        self, threshold_id: int, threshold_update: ThresholdUpdate
    ) -> Optional[Thresholds]:
        """임계치 수정"""
        result = await self.db.execute(
            select(Thresholds)
            .filter(Thresholds.threshold_id == threshold_id)
        )
        db_threshold = result.scalar_one_or_none()
        
        if not db_threshold:
            return None

        update_data = threshold_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_threshold, field, value)

        await self.db.commit()
        await self.db.refresh(db_threshold)
        return db_threshold

    async def delete_threshold(self, threshold_id: int) -> Optional[Thresholds]:
        """임계치 삭제"""
        result = await self.db.execute(
            select(Thresholds)
            .filter(Thresholds.threshold_id == threshold_id)
        )
        db_threshold = result.scalar_one_or_none()
        
        if not db_threshold:
            return None

        await self.db.delete(db_threshold)
        await self.db.commit()
        return db_threshold