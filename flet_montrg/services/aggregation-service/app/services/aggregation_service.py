"""
Aggregation service layer
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import uuid
import re

from app.models.database_models import Temperature, Location
from app.models.schemas import (
    AggregationRequest, 
    TemperatureAggregationResponse,
    LocationData,
    HourlyData
)
from app.core.logging import get_logger

logger = get_logger(__name__)


class AggregationService:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    def _validate_date_format(self, date_str: str, field_name: str) -> None:
        """날짜 형식 검증"""
        if not date_str:
            raise ValueError(f"{field_name}는 필수입니다.")
        
        # yyyy, yyyyMM, yyyyMMdd 형식만 허용
        if not re.match(r'^\d{4}(0[1-9]|1[0-2])?(0[1-9]|[12]\d|3[01])?$', date_str):
            raise ValueError(f"{field_name}는 yyyy, yyyyMM, yyyyMMdd 형식이어야 합니다. 입력값: {date_str}")
        
        # 날짜 유효성 검증
        try:
            if len(date_str) == 4:  # yyyy
                datetime.strptime(date_str, '%Y')
            elif len(date_str) == 6:  # yyyyMM
                datetime.strptime(date_str, '%Y%m')
            elif len(date_str) == 8:  # yyyyMMdd
                datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            raise ValueError(f"유효하지 않은 {field_name}입니다: {date_str}")
    
    def _validate_date_range(self, start_date: str, end_date: str) -> None:
        """날짜 범위 검증"""
        if start_date > end_date:
            raise ValueError(f"시작 날짜({start_date})가 종료 날짜({end_date})보다 늦을 수 없습니다.")
    
    def _validate_hour_format(self, hour_str: str, field_name: str) -> None:
        """시간 형식 검증 (HH)"""
        if not hour_str:
            return
        
        if not re.match(r'^([01]?\d|2[0-3])$', hour_str):
            raise ValueError(f"{field_name}는 00-23 형식이어야 합니다. 입력값: {hour_str}")
    
    def _validate_hour_range(self, start_hour: str, end_hour: str) -> None:
        """시간 범위 검증"""
        if start_hour and end_hour and start_hour > end_hour:
            raise ValueError(f"시작 시간({start_hour})이 종료 시간({end_hour})보다 늦을 수 없습니다.")
    
    def _validate_metrics(self, metrics: List[str]) -> None:
        """메트릭 목록 검증"""
        valid_metrics = [
            "pcv_temperature_max", "pcv_temperature_min", "pcv_temperature_avg",
            "temperature_max", "temperature_min", "temperature_avg",
            "humidity_max", "humidity_min", "humidity_avg"
        ]
        
        if not metrics:
            raise ValueError("메트릭 목록이 비어있습니다.")
        
        for metric in metrics:
            if metric not in valid_metrics:
                raise ValueError(f"유효하지 않은 메트릭입니다: {metric}. 허용된 메트릭: {valid_metrics}")
    
    def _validate_request(self, request: AggregationRequest) -> None:
        """요청 파라미터 전체 검증"""
        self._validate_date_format(request.start_date, "시작 날짜")
        self._validate_date_format(request.end_date, "종료 날짜")
        self._validate_date_range(request.start_date, request.end_date)
        self._validate_hour_format(request.start_hour, "시작 시간")
        self._validate_hour_format(request.end_hour, "종료 시간")
        self._validate_hour_range(request.start_hour, request.end_hour)
        self._validate_metrics(request.metrics)
    
    
    
    def _build_hour_check_query(self, base_params: dict, request: AggregationRequest) -> tuple[str, dict]:
        """Hour 데이터 존재 여부 확인 쿼리 생성"""
        # 날짜 형식에 따른 조건 생성
        if len(request.start_date) == 4:  # yyyy 형식
            date_condition = "SUBSTRING(t.ymd, 1, 4) >= :start_date AND SUBSTRING(t.ymd, 1, 4) <= :end_date"
        elif len(request.start_date) == 6:  # yyyyMM 형식
            date_condition = "SUBSTRING(t.ymd, 1, 6) >= :start_date AND SUBSTRING(t.ymd, 1, 6) <= :end_date"
        else:  # yyyyMMdd 형식
            date_condition = "t.ymd >= :start_date AND t.ymd <= :end_date"
        
        hour_check_query = text(f"""
            SELECT COUNT(*) as count
            FROM flet_montrg.temperature t
            JOIN flet_montrg.locations l ON RIGHT(t.sensor_id, 4) = l.loc_id
            WHERE {date_condition}
                AND t.hour IS NOT NULL
                    AND t.hour >= :start_hour
                    AND t.hour <= :end_hour
            """)
            
        hour_check_params = {
                "start_date": request.start_date,
                "end_date": request.end_date,
                "start_hour": int(request.start_hour),
                "end_hour": int(request.end_hour)
            }
            
        # 위치 조건들 추가
        hour_check_query, hour_check_params = self._add_location_conditions(
            hour_check_query, hour_check_params, request
        )
        
        return hour_check_query, hour_check_params
    
    def _add_location_conditions(self, query: text, params: dict, request: AggregationRequest) -> tuple[text, dict]:
        """위치 조건들을 쿼리에 추가"""
        if request.location_id:
            query = text(str(query) + " AND l.loc_id = :location_id")
            params["location_id"] = request.location_id
            
        if request.factory:
            query = text(str(query) + " AND l.factory = :factory")
            params["factory"] = request.factory
                
        if request.building:
            query = text(str(query) + " AND l.building = :building")
            params["building"] = request.building
                
        if request.floor is not None:
            query = text(str(query) + " AND l.floor = :floor")
            params["floor"] = request.floor
                
        if request.area:
            query = text(str(query) + " AND l.area = :area")
            params["area"] = request.area
            
        return query, params
    
    
    
    def _add_location_conditions_to_list(self, conditions: list, request: AggregationRequest, params: dict) -> list:
        """위치 조건들을 조건 리스트에 추가"""
        if request.location_id:
            conditions.append("l.loc_id = :location_id")
            params["location_id"] = request.location_id
        
        if request.factory:
            conditions.append("l.factory = :factory")
            params["factory"] = request.factory
            
        if request.building:
            conditions.append("l.building = :building")
            params["building"] = request.building
            
        if request.floor is not None:
            conditions.append("l.floor = :floor")
            params["floor"] = request.floor
            
        if request.area:
            conditions.append("l.area = :area")
            params["area"] = request.area
        
        return conditions
    
    
    
    async def _check_hour_data_exists(self, request: AggregationRequest) -> bool:
        """Hour 데이터 존재 여부 확인"""
        if not request.start_hour or not request.end_hour:
            return False
        
        hour_check_query, hour_check_params = self._build_hour_check_query(
            {"start_date": request.start_date, "end_date": request.end_date}, 
            request
        )
        
        hour_result = await self.db.execute(hour_check_query, hour_check_params)
        hour_count = hour_result.fetchone().count
        
        if hour_count > 0:
            logger.info(f"Hour data found ({hour_count} records), applying hour filters")
            return True
        else:
            logger.warning("No hour data found, querying without hour filters")
            return False
    
    def _determine_aggregation_level(self, request: AggregationRequest) -> str:
        """집계 단위 결정"""
        if request.factory and not request.building:
            return "factory"  # 공장 단위 집계
        elif request.factory and request.building and request.floor is not None and not request.location_id:
            return "floor"  # 층 단위 집계 (building + floor 조합)
        elif request.factory and request.building and not request.location_id:
            return "building"  # 건물 단위 집계  
        else:
            return "location"  # 개별 위치 단위 (기본값)
    
    def _determine_time_aggregation_unit(self, start_date: str, end_date: str) -> str:
        """날짜 형식에 따른 시간 집계 단위 결정"""
        from datetime import datetime, timedelta
        
        logger.info(f"Date format: {start_date} to {end_date}, length: {len(start_date)}")
        
        if len(start_date) == 4:  # yyyy
            # yyyy 형식: 무조건 월별 집계
            return "monthly"
        elif len(start_date) == 6:  # yyyyMM
            # yyyyMM 형식: 4개월 차이 이상이면 월별, 그 이하는 일별
            start_dt = datetime.strptime(start_date, '%Y%m')
            end_dt = datetime.strptime(end_date, '%Y%m')
            months_diff = (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month)
            logger.info(f"Months difference: {months_diff}")
            
            if months_diff >= 4:
                return "monthly"  # 4개월 이상: 월별 집계
            else:
                return "daily"    # 4개월 미만: 일별 집계
        else:  # yyyyMMdd
            # yyyyMMdd 형식: 무조건 시간별 집계
            return "hourly"
    
    def _build_aggregation_query(self, start_date: str, end_date: str, aggregation_level: str, request: AggregationRequest, has_hour_data: bool = False, time_unit: str = "hourly") -> tuple[text, dict]:
        """집계 쿼리 생성"""
        # 날짜 형식에 따른 조건 생성
        if len(start_date) == 4:  # yyyy 형식
            date_condition = "SUBSTRING(t.ymd, 1, 4) >= :start_date AND SUBSTRING(t.ymd, 1, 4) <= :end_date"
        elif len(start_date) == 6:  # yyyyMM 형식
            date_condition = "SUBSTRING(t.ymd, 1, 6) >= :start_date AND SUBSTRING(t.ymd, 1, 6) <= :end_date"
        else:  # yyyyMMdd 형식
            date_condition = "t.ymd >= :start_date AND t.ymd <= :end_date"
        
        # 위치 조건들 생성
        location_conditions = []
        params = {
            "start_date": start_date,
            "end_date": end_date
        }
        
        if request.location_id:
            location_conditions.append("l.loc_id = :location_id")
            params["location_id"] = request.location_id
        
        if request.factory:
            location_conditions.append("l.factory = :factory")
            params["factory"] = request.factory
            
        if request.building:
            location_conditions.append("l.building = :building")
            params["building"] = request.building
            
        if request.floor is not None:
            location_conditions.append("l.floor = :floor")
            params["floor"] = request.floor
            
        if request.area:
            location_conditions.append("l.area = :area")
            params["area"] = request.area
        
        # Hour 조건 추가
        if has_hour_data and request.start_hour and request.end_hour:
            location_conditions.append("t.hour >= :start_hour AND t.hour <= :end_hour")
            params["start_hour"] = int(request.start_hour)
            params["end_hour"] = int(request.end_hour)
        
        # WHERE 절 구성
        where_clause = date_condition
        if location_conditions:
            where_clause += " AND " + " AND ".join(location_conditions)
        
        # 집계 단위에 따른 GROUP BY 절 결정
        if aggregation_level == "factory":
            group_by = "l.factory"
            select_fields = "l.factory, NULL as building, NULL as floor, NULL as loc_id, NULL as area"
        elif aggregation_level == "building":
            group_by = "l.factory, l.building"
            select_fields = "l.factory, l.building, NULL as floor, NULL as loc_id, NULL as area"
        elif aggregation_level == "floor":
            group_by = "l.factory, l.building, l.floor"
            select_fields = "l.factory, l.building, l.floor, NULL as loc_id, NULL as area"
        else:  # location
            group_by = "l.factory, l.building, l.floor, l.loc_id, l.area"
            select_fields = "l.factory, l.building, l.floor, l.loc_id, l.area"
        
        # 시간 단위별 GROUP BY 및 ORDER BY 결정
        logger.info(f"Time unit for query: {time_unit}")
        if time_unit == "hourly":
            time_group = "t.ymd, t.hour"
            time_order = "t.ymd ASC, t.hour ASC"
            time_select = "t.ymd, t.hour"
        elif time_unit == "daily":
            time_group = "t.ymd"
            time_order = "t.ymd ASC"
            time_select = "t.ymd, '00' as hour"
        elif time_unit == "monthly":
            time_group = "SUBSTRING(t.ymd, 1, 6)"
            time_order = "SUBSTRING(t.ymd, 1, 6) ASC"
            time_select = "SUBSTRING(t.ymd, 1, 6) as ymd, '00' as hour"
        
        # 집계 쿼리 생성
        aggregation_query = text(f"""
            SELECT 
                {time_select},
                {select_fields},
                MAX(t.pcv_temperature_max) as pcv_temperature_max,
                MIN(t.pcv_temperature_min) as pcv_temperature_min,
                ROUND(AVG(t.pcv_temperature_avg), 2) as pcv_temperature_avg,
                MAX(t.temperature_max) as temperature_max,
                MIN(t.temperature_min) as temperature_min,
                ROUND(AVG(t.temperature_avg), 2) as temperature_avg,
                MAX(t.humidity_max) as humidity_max,
                MIN(t.humidity_min) as humidity_min,
                ROUND(AVG(t.humidity_avg), 2) as humidity_avg
            FROM flet_montrg.temperature t
            JOIN flet_montrg.locations l ON RIGHT(t.sensor_id, 4) = l.loc_id
            WHERE {where_clause}
            GROUP BY {time_group}, {group_by}
            ORDER BY {time_order}
        """)
        
        return aggregation_query, params

    async def get_temperature_aggregation(self, request: AggregationRequest) -> TemperatureAggregationResponse:
        """체감 온도 데이터 집계 조회"""
        try:
            # 요청 파라미터 검증
            self._validate_request(request)
            
            # 집계 단위 결정
            aggregation_level = self._determine_aggregation_level(request)
            time_aggregation_unit = self._determine_time_aggregation_unit(request.start_date, request.end_date)
            logger.info(f"Aggregation level determined: {aggregation_level}, Time unit: {time_aggregation_unit}")
            
            # 기본 파라미터 설정
            base_params = {
                "start_date": request.start_date,
                "end_date": request.end_date
            }
            
            # Hour 데이터 존재 여부 확인 (시간별 집계가 아닌 경우에는 hour 필터 무시)
            has_hour_data = False
            if time_aggregation_unit == "hourly":
                has_hour_data = await self._check_hour_data_exists(request)
            
            # 모든 경우에 시간 단위별 집계 쿼리 사용
            query, params = self._build_aggregation_query(
                request.start_date, request.end_date, aggregation_level, request, has_hour_data, time_aggregation_unit
            )
            
            # 쿼리 실행
            result = await self.db.execute(query, params)
            rows = result.fetchall()
            
            if not rows:
                raise ValueError("No data found for the given criteria")
            
            # 데이터 구조화
            return await self._structure_temperature_data(rows, request.metrics, aggregation_level, time_aggregation_unit)
            
        except Exception as e:
            logger.error(f"Failed to get temperature aggregation: {e}")
            raise
    
    
    
    
        
    def _determine_data_grouping_key(self, row, has_hour_data: bool) -> str:
        """데이터 그룹화 키 결정"""
        if has_hour_data and row.hour is not None:
            return f"{row.ymd}_{row.hour}"
        else:
            return row.ymd
    
    def _create_data_entry(self, row, has_hour_data: bool) -> dict:
        """데이터 엔트리 생성"""
        if has_hour_data and row.hour is not None:
            return {
                "ymd": row.ymd,
                "hour": str(row.hour).zfill(2),  # 정수를 2자리 문자열로 변환
                "metrics": {}
            }
        else:
            return {
                    "ymd": row.ymd,
                "hour": "00",  # 기본값으로 00시 설정
                    "metrics": {}
                }
            
    def _add_metrics_to_entry(self, entry: dict, row, metrics: List[str]) -> dict:
        """메트릭 데이터를 엔트리에 추가"""
        for metric in metrics:
            if hasattr(row, metric):
                value = getattr(row, metric)
                if value is not None:
                    entry["metrics"][metric] = str(value)
        return entry
    
    def _group_data_by_time(self, rows: List, metrics: List[str]) -> tuple[dict, bool]:
        """시간별 데이터 그룹화"""
        data_map = {}
        has_hour_data = any(row.hour is not None for row in rows)
        
        for row in rows:
            key = self._determine_data_grouping_key(row, has_hour_data)
            
            if key not in data_map:
                data_map[key] = self._create_data_entry(row, has_hour_data)
            
            data_map[key] = self._add_metrics_to_entry(data_map[key], row, metrics)
        
        return data_map, has_hour_data
    
    def _create_hourly_data_list(self, data_map: dict) -> List[HourlyData]:
        """HourlyData 리스트 생성"""
        hourly_data_list = []
        for key in sorted(data_map.keys()):  # 오래된 순서부터 정렬
            hourly_data_list.append(HourlyData(**data_map[key]))
        return hourly_data_list
    
    async def _structure_temperature_data(self, rows: List, metrics: List[str], aggregation_level: str = "location", time_unit: str = "hourly") -> TemperatureAggregationResponse:
        """온도 데이터를 요청된 구조로 변환 (다중 로케이션 지원)"""
        # 로케이션 키로 그룹핑: (factory, building, floor, loc_id, area)
        grouped_by_location: dict[tuple, List] = {}
        for row in rows:
            key = (row.factory, row.building, row.floor, row.loc_id, row.area)
            grouped_by_location.setdefault(key, []).append(row)

        locations: List[LocationData] = []
        total_points = 0

        for (factory, building, floor, loc_id, area), group_rows in grouped_by_location.items():
            # 그룹 내 시간별 데이터 구성
            data_map, has_hour_data = self._group_data_by_time(group_rows, metrics)
            hourly_data_list = self._create_hourly_data_list(data_map)
            total_points += len(hourly_data_list)

            locations.append(
                LocationData(
                    factory=factory,
                    building=building,
                    floor=floor,
                    loc_id=loc_id,
                    area=area,
                    date=hourly_data_list,
                )
            )

        logger.info(f"Structured {total_points} data points across {len(locations)} locations, aggregation_level: {aggregation_level}")

        return TemperatureAggregationResponse(locations=locations)
    
    
    
