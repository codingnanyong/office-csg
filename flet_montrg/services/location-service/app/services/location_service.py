"""
Location service layer
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List, Optional

from app.models.database_models import Location as LocationModel, Sensor
from app.models.schemas import Location

class LocationService:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_all_locations(self, skip: int = 0, limit: int = 100) -> List[Location]:
        """모든 센서 위치 정보 조회"""
        query = select(Sensor.sensor_id, LocationModel.loc_id, LocationModel.factory, 
                      LocationModel.building, LocationModel.floor, LocationModel.area)\
                .join(LocationModel, LocationModel.loc_id == Sensor.loc_id)\
                .order_by(Sensor.sensor_id).offset(skip).limit(limit)
        result = await self.db.execute(query)
        rows = result.fetchall()
        return [Location(
            loc_id=row.loc_id,
            factory=row.factory,
            building=row.building,
            floor=row.floor,
            area=row.area,
            sensor_id=row.sensor_id
        ) for row in rows]
    
    async def get_location_by_sensor_id(self, sensor_id: str) -> Optional[Location]:
        """센서 ID로 위치 정보 조회"""
        query = select(Sensor.sensor_id, LocationModel.loc_id, LocationModel.factory, 
                      LocationModel.building, LocationModel.floor, LocationModel.area)\
                .join(LocationModel, LocationModel.loc_id == Sensor.loc_id)\
                .where(Sensor.sensor_id == sensor_id)
        result = await self.db.execute(query)
        row = result.fetchone()
        if row:
            return Location(
                loc_id=row.loc_id,
                factory=row.factory,
                building=row.building,
                floor=row.floor,
                area=row.area,
                sensor_id=row.sensor_id
            )
        return None
    
    async def get_locations_by_sensor_ids(self, sensor_ids: List[str]) -> List[Location]:
        """여러 센서 ID로 위치 정보 일괄 조회"""
        if not sensor_ids:
            return []
        
        query = select(Sensor.sensor_id, LocationModel.loc_id, LocationModel.factory, 
                      LocationModel.building, LocationModel.floor, LocationModel.area)\
                .join(LocationModel, LocationModel.loc_id == Sensor.loc_id)\
                .where(Sensor.sensor_id.in_(sensor_ids))\
                .order_by(Sensor.sensor_id)
        result = await self.db.execute(query)
        rows = result.fetchall()
        return [Location(
            loc_id=row.loc_id,
            factory=row.factory,
            building=row.building,
            floor=row.floor,
            area=row.area,
            sensor_id=row.sensor_id
        ) for row in rows]
    
    async def get_location_by_loc_id(self, loc_id: str) -> Optional[Location]:
        """위치 ID로 위치 정보 조회"""
        query = select(Sensor.sensor_id, LocationModel.loc_id, LocationModel.factory, 
                      LocationModel.building, LocationModel.floor, LocationModel.area)\
                .join(LocationModel, LocationModel.loc_id == Sensor.loc_id)\
                .where(LocationModel.loc_id == loc_id)
        result = await self.db.execute(query)
        row = result.fetchone()
        if row:
            return Location(
                loc_id=row.loc_id,
                factory=row.factory,
                building=row.building,
                floor=row.floor,
                area=row.area,
                sensor_id=row.sensor_id
            )
        return None
    
    async def get_locations_by_loc_ids(self, loc_ids: List[str]) -> List[Location]:
        """여러 위치 ID로 위치 정보 일괄 조회"""
        if not loc_ids:
            return []
        
        query = select(Sensor.sensor_id, LocationModel.loc_id, LocationModel.factory, 
                      LocationModel.building, LocationModel.floor, LocationModel.area)\
                .join(LocationModel, LocationModel.loc_id == Sensor.loc_id)\
                .where(LocationModel.loc_id.in_(loc_ids))\
                .order_by(LocationModel.loc_id)
        result = await self.db.execute(query)
        rows = result.fetchall()
        return [Location(
            loc_id=row.loc_id,
            factory=row.factory,
            building=row.building,
            floor=row.floor,
            area=row.area,
            sensor_id=row.sensor_id
        ) for row in rows]
    

