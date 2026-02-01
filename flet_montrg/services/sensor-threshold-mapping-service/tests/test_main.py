"""
Tests for Sensor Threshold Mapping Service API
"""
import pytest
from httpx import AsyncClient


class TestMappingService:
    """Sensor Threshold Mapping Service API 테스트"""
    
    async def test_create_mapping(self, client: AsyncClient, sample_mapping_data):
        """매핑 생성 테스트"""
        response = await client.post("/api/v1/mappings/", json=sample_mapping_data)
        assert response.status_code == 201
        data = response.json()
        assert data["sensor_id"] == sample_mapping_data["sensor_id"]
        assert data["threshold_id"] == sample_mapping_data["threshold_id"]
        assert data["duration_seconds"] == sample_mapping_data["duration_seconds"]
        assert data["enabled"] == sample_mapping_data["enabled"]
        assert "map_id" in data
    
    async def test_get_mappings(self, client: AsyncClient, sample_mapping_data):
        """매핑 목록 조회 테스트"""
        # 매핑 생성
        await client.post("/api/v1/mappings/", json=sample_mapping_data)
        
        # 목록 조회
        response = await client.get("/api/v1/mappings/")
        assert response.status_code == 200
        data = response.json()
        assert len(data) >= 1
    
    async def test_get_mapping_by_id(self, client: AsyncClient, sample_mapping_data):
        """매핑 상세 조회 테스트"""
        # 매핑 생성
        create_response = await client.post("/api/v1/mappings/", json=sample_mapping_data)
        map_id = create_response.json()["map_id"]
        
        # 상세 조회
        response = await client.get(f"/api/v1/mappings/{map_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["map_id"] == map_id
        assert data["sensor_id"] == sample_mapping_data["sensor_id"]
    
    async def test_get_mappings_by_sensor_id(self, client: AsyncClient, sample_mapping_data):
        """센서별 매핑 조회 테스트"""
        # 매핑 생성
        await client.post("/api/v1/mappings/", json=sample_mapping_data)
        
        # 센서별 조회
        response = await client.get(f"/api/v1/mappings/sensor/{sample_mapping_data['sensor_id']}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) >= 1
        assert all(item["sensor_id"] == sample_mapping_data["sensor_id"] for item in data)
    
    async def test_update_mapping(self, client: AsyncClient, sample_mapping_data):
        """매핑 수정 테스트"""
        # 매핑 생성
        create_response = await client.post("/api/v1/mappings/", json=sample_mapping_data)
        map_id = create_response.json()["map_id"]
        
        # 매핑 수정
        update_data = {"duration_seconds": 120, "enabled": False}
        response = await client.put(f"/api/v1/mappings/{map_id}", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["duration_seconds"] == 120
        assert data["enabled"] == False
    
    async def test_delete_mapping(self, client: AsyncClient, sample_mapping_data):
        """매핑 삭제 테스트"""
        # 매핑 생성
        create_response = await client.post("/api/v1/mappings/", json=sample_mapping_data)
        map_id = create_response.json()["map_id"]
        
        # 매핑 삭제
        response = await client.delete(f"/api/v1/mappings/{map_id}")
        assert response.status_code == 204
        
        # 삭제 확인
        get_response = await client.get(f"/api/v1/mappings/{map_id}")
        assert get_response.status_code == 404
    
    async def test_enable_disable_mapping(self, client: AsyncClient, sample_mapping_data):
        """매핑 활성화/비활성화 테스트"""
        # 매핑 생성
        create_response = await client.post("/api/v1/mappings/", json=sample_mapping_data)
        map_id = create_response.json()["map_id"]
        
        # 비활성화
        disable_response = await client.post(f"/api/v1/mappings/{map_id}/disable")
        assert disable_response.status_code == 200
        assert disable_response.json()["enabled"] == False
        
        # 활성화
        enable_response = await client.post(f"/api/v1/mappings/{map_id}/enable")
        assert enable_response.status_code == 200
        assert enable_response.json()["enabled"] == True
