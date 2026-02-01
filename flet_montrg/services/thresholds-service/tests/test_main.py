import pytest

class TestThresholdsService:
    """Thresholds Service 단위 테스트"""
    
    @pytest.mark.asyncio
    async def test_root_endpoint(self, client):
        """루트 엔드포인트 테스트"""
        response = await client.get("/")
        assert response.status_code == 200
        assert response.json()["message"] == "Thresholds Service API"
        assert response.json()["version"] == "1.0.0"
    
    @pytest.mark.asyncio
    async def test_health_check(self, client):
        """헬스체크 엔드포인트 테스트"""
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    @pytest.mark.asyncio
    async def test_create_threshold(self, client):
        """임계치 생성 테스트"""
        threshold_data = {
            "threshold_type": "temperature",
            "level": "medium",
            "min_value": 10.0,
            "max_value": 50.0
        }
        response = await client.post("/api/v1/thresholds/", json=threshold_data)
        assert response.status_code == 201
        data = response.json()
        assert data["threshold_id"] == 1
        assert data["threshold_type"] == "temperature"
        assert data["level"] == "medium"
        assert float(data["min_value"]) == 10.0
        assert float(data["max_value"]) == 50.0
    
    @pytest.mark.asyncio
    async def test_create_threshold_invalid_values(self, client):
        """잘못된 값으로 임계치 생성 테스트"""
        threshold_data = {
            "threshold_type": "temperature",
            "level": "medium",
            "min_value": 100.0,  # max_value보다 큼
            "max_value": 50.0
        }
        response = await client.post("/api/v1/thresholds/", json=threshold_data)
        assert response.status_code == 422  # Pydantic validation error
        assert "max_value must be greater than min_value" in response.json()["detail"][0]["msg"]
    
    @pytest.mark.asyncio
    async def test_get_thresholds_empty(self, client):
        """빈 임계치 목록 조회 테스트"""
        response = await client.get("/api/v1/thresholds/")
        assert response.status_code == 200
        assert response.json() == []
    
    @pytest.mark.asyncio
    async def test_get_thresholds_with_data(self, client):
        """데이터가 있는 임계치 목록 조회 테스트"""
        # 테스트 데이터 생성
        threshold_data = {
            "threshold_type": "temperature",
            "level": "medium",
            "min_value": 10.0,
            "max_value": 50.0
        }
        await client.post("/api/v1/thresholds/", json=threshold_data)
        
        response = await client.get("/api/v1/thresholds/")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["threshold_type"] == "temperature"
    
    @pytest.mark.asyncio
    async def test_get_threshold_by_id(self, client):
        """ID로 특정 임계치 조회 테스트"""
        # 테스트 데이터 생성
        threshold_data = {
            "threshold_type": "temperature",
            "level": "medium",
            "min_value": 10.0,
            "max_value": 50.0
        }
        create_response = await client.post("/api/v1/thresholds/", json=threshold_data)
        threshold_id = create_response.json()["threshold_id"]
        
        response = await client.get(f"/api/v1/thresholds/{threshold_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["threshold_id"] == threshold_id
        assert data["threshold_type"] == "temperature"
    
    @pytest.mark.asyncio
    async def test_get_threshold_by_id_not_found(self, client):
        """존재하지 않는 ID로 임계치 조회 테스트"""
        response = await client.get("/api/v1/thresholds/999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Threshold not found"
    
    @pytest.mark.asyncio
    async def test_update_threshold(self, client):
        """임계치 수정 테스트"""
        # 테스트 데이터 생성
        threshold_data = {
            "threshold_type": "temperature",
            "level": "medium",
            "min_value": 10.0,
            "max_value": 50.0
        }
        create_response = await client.post("/api/v1/thresholds/", json=threshold_data)
        threshold_id = create_response.json()["threshold_id"]
        
        # 수정할 데이터
        update_data = {
            "min_value": 15.0,
            "max_value": 60.0,
            "level": "high"
        }
        
        response = await client.put(f"/api/v1/thresholds/{threshold_id}", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["threshold_id"] == threshold_id
        assert float(data["min_value"]) == 15.0
        assert float(data["max_value"]) == 60.0
        assert data["level"] == "high"
    
    @pytest.mark.asyncio
    async def test_update_threshold_not_found(self, client):
        """존재하지 않는 임계치 수정 테스트"""
        update_data = {
            "min_value": 15.0,
            "max_value": 60.0
        }
        
        response = await client.put("/api/v1/thresholds/999", json=update_data)
        assert response.status_code == 404
        assert response.json()["detail"] == "Threshold not found"
    
    @pytest.mark.asyncio
    async def test_delete_threshold(self, client):
        """임계치 삭제 테스트"""
        # 테스트 데이터 생성
        threshold_data = {
            "threshold_type": "temperature",
            "level": "medium",
            "min_value": 10.0,
            "max_value": 50.0
        }
        create_response = await client.post("/api/v1/thresholds/", json=threshold_data)
        threshold_id = create_response.json()["threshold_id"]
        
        response = await client.delete(f"/api/v1/thresholds/{threshold_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == f"Threshold {threshold_id} deleted"
        assert data["deleted"]["threshold_id"] == threshold_id
        
        # 삭제 확인
        get_response = await client.get(f"/api/v1/thresholds/{threshold_id}")
        assert get_response.status_code == 404
    
    @pytest.mark.asyncio
    async def test_delete_threshold_not_found(self, client):
        """존재하지 않는 임계치 삭제 테스트"""
        response = await client.delete("/api/v1/thresholds/999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Threshold not found"
    
    @pytest.mark.asyncio
    async def test_get_thresholds_by_type(self, client):
        """타입별 임계치 조회 테스트"""
        # 테스트 데이터 생성
        threshold_data_1 = {
            "threshold_type": "temperature",
            "level": "medium",
            "min_value": 10.0,
            "max_value": 50.0
        }
        threshold_data_2 = {
            "threshold_type": "temperature",
            "level": "high",
            "min_value": 20.0,
            "max_value": 60.0
        }
        threshold_data_3 = {
            "threshold_type": "humidity",
            "level": "low",
            "min_value": 5.0,
            "max_value": 30.0
        }
        
        await client.post("/api/v1/thresholds/", json=threshold_data_1)
        await client.post("/api/v1/thresholds/", json=threshold_data_2)
        await client.post("/api/v1/thresholds/", json=threshold_data_3)
        
        response = await client.get("/api/v1/thresholds/type/temperature")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert all(t["threshold_type"] == "temperature" for t in data)
    
    @pytest.mark.asyncio
    async def test_get_thresholds_by_type_empty(self, client):
        """타입별 임계치 조회 (빈 결과) 테스트"""
        response = await client.get("/api/v1/thresholds/type/nonexistent_type")
        assert response.status_code == 200
        assert response.json() == []
    
    @pytest.mark.asyncio
    async def test_threshold_type_validation(self, client):
        """임계치 타입 유효성 검사 테스트"""
        # 유효한 threshold_type들 (Enum에 정의된 값만)
        valid_types = ["temperature", "humidity", "pcv_temperature"]
        
        for threshold_type in valid_types:
            threshold_data = {
                "threshold_type": threshold_type,
                "level": "medium",
                "min_value": 10.0,
                "max_value": 50.0
            }
            response = await client.post("/api/v1/thresholds/", json=threshold_data)
            assert response.status_code == 201
            assert response.json()["threshold_type"] == threshold_type