"""
Temperature service layer - 단순한 현재 온도 데이터 조회
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Tuple, Optional
from decimal import Decimal
from datetime import datetime, timezone, timedelta

from app.models.schemas import (
    TemperatureCurrentData,
    MeasurementData,
    LocationInfo,
    MetricsData,
    MetricData,
    Location,
    Threshold
)
from app.clients.location_client import location_client
from app.clients.thresholds_client import thresholds_client
from app.core.logging import get_logger

logger = get_logger(__name__)


class TemperatureService:
    """온도 데이터 조회 서비스"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    def _check_thresholds(self, value: Decimal, sensor_type: str, thresholds: List[Threshold]) -> Tuple[str, bool, Optional[Threshold]]:
        """임계치 검사 및 상태 결정"""
        if not thresholds or value is None:
            return "normal", False, None
        
        # 임계치 레벨별 우선순위 (높은 우선순위부터)
        level_priority = {
            "critical": 4,
            "red": 4,
            "orange": 4,
            "high": 3,
            "yellow": 3,
            "medium": 2,
            "warning": 2,
            "low": 1,
            "green": 1,
            "info": 0
        }
        
        highest_status = "normal"
        highest_priority = -1
        matched_threshold = None
        
        for threshold in thresholds:
            if threshold.min_value is not None and value < threshold.min_value:
                continue
            if threshold.max_value is not None and value > threshold.max_value:
                continue
            
            # 임계치 범위 내에 있는 경우
            priority = level_priority.get(threshold.level.lower(), 0)
            if priority > highest_priority:
                highest_priority = priority
                matched_threshold = threshold
                
                # 레벨을 상태로 매핑
                if threshold.level.lower() in ["critical", "red", "orange"]:
                    highest_status = "critical"
                elif threshold.level.lower() in ["high", "yellow"]:
                    highest_status = "warning"
                elif threshold.level.lower() in ["medium", "warning"]:
                    highest_status = "warning"
                else:
                    highest_status = "normal"
        
        is_alert = highest_status in ["warning", "critical"]
        return highest_status, is_alert, matched_threshold
    
    async def get_current_temperature_data(self) -> TemperatureCurrentData:
        """현재 온도 데이터 조회 (새로운 구조)"""
        return await self._get_temperature_data_with_filters()
    
    async def get_current_temperature_data_by_factory(self, factory: str) -> TemperatureCurrentData:
        """공장별 현재 온도 데이터 조회"""
        return await self._get_temperature_data_with_filters(factory=factory)
    
    async def get_current_temperature_data_by_building(self, building: str) -> TemperatureCurrentData:
        """건물별 현재 온도 데이터 조회"""
        return await self._get_temperature_data_with_filters(building=building)
    
    async def get_current_temperature_data_by_floor(self, floor: int) -> TemperatureCurrentData:
        """층별 현재 온도 데이터 조회"""
        return await self._get_temperature_data_with_filters(floor=floor)
    
    async def get_current_temperature_data_by_loc_id(self, loc_id: str) -> TemperatureCurrentData:
        """위치 ID별 현재 온도 데이터 조회"""
        return await self._get_temperature_data_with_filters(loc_id=loc_id)
    
    async def get_current_temperature_data_by_location(
        self, 
        factory: Optional[str] = None, 
        building: Optional[str] = None, 
        floor: Optional[int] = None,
        loc_id: Optional[str] = None
    ) -> TemperatureCurrentData:
        """위치 조건별 현재 온도 데이터 조회 (다중 필터 지원)"""
        return await self._get_temperature_data_with_filters(
            factory=factory, 
            building=building, 
            floor=floor,
            loc_id=loc_id
        )
    
    async def _get_temperature_data_with_filters(
        self, 
        factory: Optional[str] = None, 
        building: Optional[str] = None, 
        floor: Optional[int] = None,
        loc_id: Optional[str] = None
    ) -> TemperatureCurrentData:
        """공통 온도 데이터 조회 메서드 (필터 지원)"""
        try:
            # 오늘의 센서별 최신 데이터 조회 (KST 기준)
            query = text("""
                WITH bounds AS (
                    SELECT
                        date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') AS start_today,
                        date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') + interval '1 day' AS end_today
                ),
                ranked AS (
                    SELECT
                        tr.*,
                        ROW_NUMBER() OVER (PARTITION BY tr.sensor_id ORDER BY tr.capture_dt DESC) AS rn
                    FROM flet_montrg.temperature_raw AS tr
                    CROSS JOIN bounds b
                    WHERE tr.capture_dt >= b.start_today
                        AND tr.capture_dt < b.end_today
                )
                SELECT capture_dt, ymd, hmsf, sensor_id, device_id, t1, t2, t3, t4, t5, t6, 
                       upload_yn, upload_dt, extract_time, load_time
                FROM ranked
                WHERE rn = 1
                ORDER BY capture_dt DESC, sensor_id
            """)
            
            result = await self.db.execute(query)
            rows = result.fetchall()
            
            if not rows:
                filter_info = f"factory={factory}, building={building}, floor={floor}, loc_id={loc_id}" if any([factory, building, floor, loc_id]) else "all"
                logger.info(f"No temperature data found for filters: {filter_info}")
                return TemperatureCurrentData(
                    ymd=datetime.now().strftime('%Y%m%d'),
                    hh=datetime.now().strftime('%H'),
                    measurements=[]
                )
            
            measurements = []
            
            # 모든 row 중에서 가장 최신의 capture_dt 찾기
            latest_capture_dt = None
            latest_ymd = None
            latest_hh = None
            
            for row in rows:
                if latest_capture_dt is None or row.capture_dt > latest_capture_dt:
                    latest_capture_dt = row.capture_dt
                    latest_ymd = row.ymd
                    latest_hh = row.hmsf[:2] if row.hmsf else "00"
            
            ymd = latest_ymd
            hh = latest_hh
            
            for row in rows:
                # 위치 정보 조회
                location_info = None
                try:
                    location_info = await location_client.get_location_by_sensor_id(row.sensor_id)
                except Exception as e:
                    logger.warning(f"Failed to get location for sensor {row.sensor_id}: {e}")
                    continue
                
                if not location_info:
                    continue
                
                # 필터 조건 확인
                if factory and location_info.factory != factory:
                    continue
                if building and location_info.building != building:
                    continue
                if floor is not None and location_info.floor != floor:
                    continue
                if loc_id and location_info.loc_id != loc_id:
                    continue
                
                # 측정값 처리 및 상태 확인 로직
                measurement = await self._process_measurement_data(row, location_info)
                if measurement:
                    measurements.append(measurement)
            
            return TemperatureCurrentData(
                ymd=ymd,
                hh=hh,
                measurements=measurements
            )
            
        except Exception as e:
            filter_info = f"factory={factory}, building={building}, floor={floor}, loc_id={loc_id}" if any([factory, building, floor, loc_id]) else "all"
            logger.error(f"Error getting temperature data with filters {filter_info}: {e}")
            raise
    
    async def _process_measurement_data(self, row, location_info):
        """측정 데이터 처리 및 상태 확인 (공통 로직)"""
        try:
            # t1, t2, t3 값 파싱
            temperature = None
            humidity = None
            pcv_temperature = None
            
            if row.t1:
                try:
                    temperature = Decimal(row.t1)
                except (ValueError, TypeError):
                    pass
            
            if row.t2:
                try:
                    humidity = Decimal(row.t2)
                except (ValueError, TypeError):
                    pass
            
            if row.t3:
                try:
                    pcv_temperature = Decimal(row.t3)
                except (ValueError, TypeError):
                    pass
            
            # 각 측정값의 상태 확인 (threshold가 없으면 null)
            temperature_status = None
            humidity_status = None
            pcv_temperature_status = None
            
            if temperature is not None:
                try:
                    thresholds = await thresholds_client.get_applicable_thresholds("temperature")
                    if thresholds:  # threshold가 존재하는 경우만 상태 확인
                        temperature_status, _, _ = self._check_thresholds(
                            temperature, "temperature", thresholds
                        )
                except Exception as e:
                    logger.warning(f"Failed to get thresholds for temperature: {e}")
            
            if humidity is not None:
                try:
                    thresholds = await thresholds_client.get_applicable_thresholds("humidity")
                    if thresholds:  # threshold가 존재하는 경우만 상태 확인
                        humidity_status, _, _ = self._check_thresholds(
                            humidity, "humidity", thresholds
                        )
                except Exception as e:
                    logger.warning(f"Failed to get thresholds for humidity: {e}")
            
            if pcv_temperature is not None:
                try:
                    thresholds = await thresholds_client.get_applicable_thresholds("pcv_temperature")
                    if thresholds:  # threshold가 존재하는 경우만 상태 확인
                        pcv_temperature_status, _, _ = self._check_thresholds(
                            pcv_temperature, "pcv_temperature", thresholds
                        )
                except Exception as e:
                    logger.warning(f"Failed to get thresholds for pcv_temperature: {e}")
            
            # 측정 데이터 생성
            from app.models.schemas import MeasurementData, LocationInfo, MetricsData, MetricData
            
            measurement = MeasurementData(
                location=LocationInfo(
                    factory=location_info.factory,
                    building=location_info.building,
                    floor=location_info.floor,
                    loc_id=location_info.loc_id,
                    area=location_info.area
                ),
                metrics=MetricsData(
                    temperature=MetricData(
                        value=temperature,
                        status=temperature_status
                    ) if temperature is not None else None,
                    humidity=MetricData(
                        value=humidity,
                        status=humidity_status
                    ) if humidity is not None else None,
                    pcv_temperature=MetricData(
                        value=pcv_temperature,
                        status=pcv_temperature_status
                    ) if pcv_temperature is not None else None
                )
            )
            
            return measurement
            
        except Exception as e:
            logger.error(f"Error processing measurement data: {e}")
            return None
