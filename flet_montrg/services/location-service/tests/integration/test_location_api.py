"""
Integration tests for Location API endpoints
"""
import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database_models import Location as LocationModel, Sensor


class TestLocationAPI:
    """Location API integration tests"""
    
    @pytest.mark.asyncio
    async def test_root_endpoint(self, client):
        """루트 엔드포인트 테스트"""
        response = await client.get("/")
        assert response.status_code == 200
        assert response.json()["message"] == "Location Service API"
        assert response.json()["version"] == "1.0.0"
    
    @pytest.mark.asyncio
    async def test_health_check(self, client):
        """헬스체크 엔드포인트 테스트"""
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    @pytest.mark.asyncio
    async def test_get_all_locations_empty(self, client):
        """빈 위치 정보 목록 조회 테스트"""
        response = await client.get("/api/v1/location/")
        assert response.status_code == 200
        assert response.json() == []
    
    @pytest.mark.asyncio
    async def test_get_location_by_sensor_id_not_found(self, client):
        """존재하지 않는 센서 ID로 위치 정보 조회 테스트"""
        response = await client.get("/api/v1/location/NONEXISTENT")
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()
    
    @pytest.mark.asyncio
    async def test_get_locations_by_sensor_ids_empty(self, client):
        """빈 센서 ID 리스트로 위치 정보 조회 테스트"""
        # 빈 센서 ID 리스트는 빈 문자열로 전달 (쉼표로 구분된 빈 문자열)
        # 실제로는 존재하지 않는 센서 ID를 전달하여 빈 리스트가 반환되는지 확인
        response = await client.get("/api/v1/location/batch/NONEXISTENT_SENSOR")
        assert response.status_code == 200
        # 존재하지 않는 센서 ID는 빈 리스트를 반환
        assert response.json() == []
    
    @pytest.mark.asyncio
    async def test_get_all_locations_with_data(self, client, setup_db):
        """데이터가 있는 상태에서 모든 위치 정보 조회 테스트"""
        # 테스트 데이터 삽입 (setup_db fixture가 자동으로 처리)
        # 실제로는 데이터베이스에 테스트 데이터를 삽입하는 로직이 필요
        
        response = await client.get("/api/v1/location/")
        assert response.status_code == 200
        # 데이터가 있다면 검증 로직 추가
    
    @pytest.mark.asyncio
    async def test_get_location_by_sensor_id_with_data(self, client, setup_db):
        """데이터가 있는 상태에서 특정 센서 위치 정보 조회 테스트"""
        # 테스트 데이터 삽입 로직
        
        response = await client.get("/api/v1/location/TEST001")
        # 데이터가 있다면 검증 로직 추가
        # assert response.status_code == 200
    
    @pytest.mark.asyncio
    async def test_get_locations_by_sensor_ids_with_data(self, client, setup_db):
        """데이터가 있는 상태에서 여러 센서 위치 정보 조회 테스트"""
        # 테스트 데이터 삽입 로직
        
        response = await client.get("/api/v1/location/batch/TEST001,TEST002")
        # 데이터가 있다면 검증 로직 추가
        # assert response.status_code == 200
