"""
Alert Subscription Service API 테스트
"""
import pytest
from datetime import datetime

pytestmark = pytest.mark.asyncio


class TestSubscriptionService:
    """Alert Subscription Service API 테스트"""
    
    async def test_root_endpoint(self, client):
        """루트 엔드포인트 테스트"""
        response = await client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "version" in data
        assert "Alert Subscription Service" in data["message"]
    
    async def test_health_check(self, client):
        """헬스체크 엔드포인트 테스트"""
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    async def test_ready_check(self, client):
        """레디니스 체크 엔드포인트 테스트"""
        response = await client.get("/ready")
        assert response.status_code == 200
        assert response.json()["status"] == "ready"
    
    async def test_create_subscription(self, client, sample_subscription_data):
        """구독 생성 테스트"""
        response = await client.post("/api/v1/subscriptions/", json=sample_subscription_data)
        assert response.status_code == 201
        data = response.json()
        assert "subscription_id" in data
        assert data["subscriber"] == sample_subscription_data["subscriber"]
        assert data["notify_type"] == sample_subscription_data["notify_type"]
        assert data["notify_id"] == sample_subscription_data["notify_id"]
        assert data["enabled"] == sample_subscription_data["enabled"]
        assert "upd_dt" in data
    
    async def test_create_subscription_minimal(self, client):
        """최소 필수 필드만으로 구독 생성 테스트"""
        minimal_data = {
            "subscriber": "minimal_user",
            "notify_type": "email",
            "notify_id": "minimal@example.com"
        }
        response = await client.post("/api/v1/subscriptions/", json=minimal_data)
        assert response.status_code == 201
        data = response.json()
        assert data["subscriber"] == minimal_data["subscriber"]
        assert data["enabled"] == True  # 기본값
    
    async def test_create_subscription_with_kakao(self, client):
        """카카오 알림 타입으로 구독 생성 테스트"""
        kakao_data = {
            "subscriber": "kakao_user",
            "notify_type": "kakao",
            "notify_id": "kakao_account_name"
        }
        response = await client.post("/api/v1/subscriptions/", json=kakao_data)
        assert response.status_code == 201
        data = response.json()
        assert data["notify_type"] == "kakao"
        assert data["notify_id"] == "kakao_account_name"
    
    async def test_create_subscription_invalid_notify_type(self, client):
        """잘못된 알림 타입으로 생성 테스트"""
        invalid_data = {
            "subscriber": "test_user",
            "notify_type": "invalid_type",  # 유효하지 않은 타입
            "notify_id": "test@example.com"
        }
        response = await client.post("/api/v1/subscriptions/", json=invalid_data)
        # Pydantic validation error
        assert response.status_code == 422
    
    async def test_get_subscriptions_empty(self, client):
        """빈 구독 목록 조회 테스트"""
        response = await client.get("/api/v1/subscriptions/")
        assert response.status_code == 200
        assert response.json() == []
    
    async def test_get_subscriptions_with_data(self, client, sample_subscription_data):
        """데이터가 있는 구독 목록 조회 테스트"""
        # 구독 생성
        await client.post("/api/v1/subscriptions/", json=sample_subscription_data)
        
        # 목록 조회
        response = await client.get("/api/v1/subscriptions/")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["subscriber"] == sample_subscription_data["subscriber"]
    
    async def test_get_subscriptions_with_pagination(self, client, sample_subscription_data):
        """페이지네이션을 사용한 구독 목록 조회 테스트"""
        # 여러 구독 생성
        for i in range(5):
            subscription_data = sample_subscription_data.copy()
            subscription_data["subscriber"] = f"user{i}"
            subscription_data["notify_id"] = f"user{i}@example.com"
            await client.post("/api/v1/subscriptions/", json=subscription_data)
        
        # 첫 페이지 조회 (limit=2)
        response = await client.get("/api/v1/subscriptions/?skip=0&limit=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        
        # 두 번째 페이지 조회
        response = await client.get("/api/v1/subscriptions/?skip=2&limit=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
    
    async def test_get_subscriptions_filter_by_subscriber(self, client, sample_subscription_data):
        """구독자로 필터링 테스트"""
        # 여러 구독자 생성
        for i in range(3):
            subscription_data = sample_subscription_data.copy()
            subscription_data["subscriber"] = f"user{i}"
            subscription_data["notify_id"] = f"user{i}@example.com"
            await client.post("/api/v1/subscriptions/", json=subscription_data)
        
        # 특정 구독자로 필터링
        response = await client.get("/api/v1/subscriptions/?subscriber=user1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["subscriber"] == "user1"
    
    async def test_get_subscriptions_filter_by_factory(self, client, sample_subscription_data):
        """공장으로 필터링 테스트"""
        # 여러 공장의 구독 생성
        factories = ["Factory1", "Factory2", "Factory1"]
        for factory in factories:
            subscription_data = sample_subscription_data.copy()
            subscription_data["factory"] = factory
            subscription_data["subscriber"] = f"user_{factory}"
            subscription_data["notify_id"] = f"user_{factory}@example.com"
            await client.post("/api/v1/subscriptions/", json=subscription_data)
        
        # Factory1로 필터링
        response = await client.get("/api/v1/subscriptions/?factory=Factory1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert all(item["factory"] == "Factory1" for item in data)
    
    async def test_get_subscriptions_filter_by_building(self, client, sample_subscription_data):
        """건물로 필터링 테스트"""
        # 여러 건물의 구독 생성
        buildings = ["Building1", "Building2", "Building1"]
        for building in buildings:
            subscription_data = sample_subscription_data.copy()
            subscription_data["building"] = building
            subscription_data["subscriber"] = f"user_{building}"
            subscription_data["notify_id"] = f"user_{building}@example.com"
            await client.post("/api/v1/subscriptions/", json=subscription_data)
        
        # Building1로 필터링
        response = await client.get("/api/v1/subscriptions/?building=Building1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert all(item["building"] == "Building1" for item in data)
    
    async def test_get_subscriptions_filter_by_floor(self, client, sample_subscription_data):
        """층으로 필터링 테스트"""
        # 여러 층의 구독 생성
        floors = [1, 2, 1]
        for floor in floors:
            subscription_data = sample_subscription_data.copy()
            subscription_data["floor"] = floor
            subscription_data["subscriber"] = f"user_floor{floor}"
            subscription_data["notify_id"] = f"user_floor{floor}@example.com"
            await client.post("/api/v1/subscriptions/", json=subscription_data)
        
        # 1층으로 필터링
        response = await client.get("/api/v1/subscriptions/?floor=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert all(item["floor"] == 1 for item in data)
    
    async def test_get_subscriptions_filter_by_sensor_id(self, client, sample_subscription_data):
        """센서 ID로 필터링 테스트"""
        # 여러 센서의 구독 생성
        sensors = ["SENSOR001", "SENSOR002", "SENSOR001"]
        for sensor_id in sensors:
            subscription_data = sample_subscription_data.copy()
            subscription_data["sensor_id"] = sensor_id
            subscription_data["subscriber"] = f"user_{sensor_id}"
            subscription_data["notify_id"] = f"user_{sensor_id}@example.com"
            await client.post("/api/v1/subscriptions/", json=subscription_data)
        
        # SENSOR001로 필터링
        response = await client.get("/api/v1/subscriptions/?sensor_id=SENSOR001")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert all(item["sensor_id"] == "SENSOR001" for item in data)
    
    async def test_get_subscriptions_filter_by_threshold_type(self, client, sample_subscription_data):
        """임계치 타입으로 필터링 테스트"""
        # 여러 임계치 타입의 구독 생성
        threshold_types = ["temperature", "humidity", "temperature"]
        for threshold_type in threshold_types:
            subscription_data = sample_subscription_data.copy()
            subscription_data["threshold_type"] = threshold_type
            subscription_data["subscriber"] = f"user_{threshold_type}"
            subscription_data["notify_id"] = f"user_{threshold_type}@example.com"
            await client.post("/api/v1/subscriptions/", json=subscription_data)
        
        # temperature 타입으로 필터링
        response = await client.get("/api/v1/subscriptions/?threshold_type=temperature")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert all(item["threshold_type"] == "temperature" for item in data)
    
    async def test_get_subscriptions_filter_by_enabled(self, client, sample_subscription_data):
        """활성화 여부로 필터링 테스트"""
        # 활성화/비활성화 구독 생성
        for i, enabled in enumerate([True, False, True]):
            subscription_data = sample_subscription_data.copy()
            subscription_data["enabled"] = enabled
            subscription_data["subscriber"] = f"user{i}"
            subscription_data["notify_id"] = f"user{i}@example.com"
            await client.post("/api/v1/subscriptions/", json=subscription_data)
        
        # 활성화된 구독만 필터링
        response = await client.get("/api/v1/subscriptions/?enabled=true")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert all(item["enabled"] == True for item in data)
    
    async def test_get_subscription_by_id(self, client, sample_subscription_data):
        """ID로 특정 구독 조회 테스트"""
        # 구독 생성
        create_response = await client.post("/api/v1/subscriptions/", json=sample_subscription_data)
        subscription_id = create_response.json()["subscription_id"]
        
        # ID로 조회
        response = await client.get(f"/api/v1/subscriptions/{subscription_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["subscription_id"] == subscription_id
        assert data["subscriber"] == sample_subscription_data["subscriber"]
    
    async def test_get_subscription_by_id_not_found(self, client):
        """존재하지 않는 ID로 구독 조회 테스트"""
        response = await client.get("/api/v1/subscriptions/99999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Subscription not found"
    
    async def test_get_subscriptions_by_subscriber(self, client, sample_subscription_data):
        """구독자별 구독 목록 조회 테스트"""
        subscriber_name = "test_subscriber"
        
        # 같은 구독자의 여러 구독 생성
        for i in range(3):
            subscription_data = sample_subscription_data.copy()
            subscription_data["subscriber"] = subscriber_name
            subscription_data["notify_id"] = f"test{i}@example.com"
            subscription_data["sensor_id"] = f"SENSOR{i:03d}"
            await client.post("/api/v1/subscriptions/", json=subscription_data)
        
        # 다른 구독자의 구독 생성
        other_data = sample_subscription_data.copy()
        other_data["subscriber"] = "other_subscriber"
        other_data["notify_id"] = "other@example.com"
        await client.post("/api/v1/subscriptions/", json=other_data)
        
        # 구독자별 조회
        response = await client.get(f"/api/v1/subscriptions/subscriber/{subscriber_name}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        assert all(item["subscriber"] == subscriber_name for item in data)
        assert all(item["enabled"] == True for item in data)  # 활성화된 것만
    
    async def test_update_subscription(self, client, sample_subscription_data):
        """구독 수정 테스트"""
        # 구독 생성
        create_response = await client.post("/api/v1/subscriptions/", json=sample_subscription_data)
        subscription_id = create_response.json()["subscription_id"]
        
        # 구독 수정
        update_data = {
            "factory": "UpdatedFactory",
            "building": "UpdatedBuilding",
            "enabled": False
        }
        response = await client.put(f"/api/v1/subscriptions/{subscription_id}", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["factory"] == "UpdatedFactory"
        assert data["building"] == "UpdatedBuilding"
        assert data["enabled"] == False
        # 수정되지 않은 필드는 유지
        assert data["subscriber"] == sample_subscription_data["subscriber"]
    
    async def test_update_subscription_not_found(self, client):
        """존재하지 않는 구독 수정 테스트"""
        update_data = {"factory": "UpdatedFactory"}
        response = await client.put("/api/v1/subscriptions/99999", json=update_data)
        assert response.status_code == 404
        assert response.json()["detail"] == "Subscription not found"
    
    async def test_delete_subscription(self, client, sample_subscription_data):
        """구독 삭제 테스트"""
        # 구독 생성
        create_response = await client.post("/api/v1/subscriptions/", json=sample_subscription_data)
        subscription_id = create_response.json()["subscription_id"]
        
        # 구독 삭제
        response = await client.delete(f"/api/v1/subscriptions/{subscription_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == f"Subscription {subscription_id} deleted successfully"
        
        # 삭제 확인
        get_response = await client.get(f"/api/v1/subscriptions/{subscription_id}")
        assert get_response.status_code == 404
    
    async def test_delete_subscription_not_found(self, client):
        """존재하지 않는 구독 삭제 테스트"""
        response = await client.delete("/api/v1/subscriptions/99999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Subscription not found"
    
    async def test_enable_subscription(self, client, sample_subscription_data):
        """구독 활성화 테스트"""
        # 비활성화된 구독 생성
        subscription_data = sample_subscription_data.copy()
        subscription_data["enabled"] = False
        create_response = await client.post("/api/v1/subscriptions/", json=subscription_data)
        subscription_id = create_response.json()["subscription_id"]
        
        # 구독 활성화
        response = await client.post(f"/api/v1/subscriptions/{subscription_id}/enable")
        assert response.status_code == 200
        data = response.json()
        assert data["enabled"] == True
    
    async def test_disable_subscription(self, client, sample_subscription_data):
        """구독 비활성화 테스트"""
        # 활성화된 구독 생성
        create_response = await client.post("/api/v1/subscriptions/", json=sample_subscription_data)
        subscription_id = create_response.json()["subscription_id"]
        
        # 구독 비활성화
        response = await client.post(f"/api/v1/subscriptions/{subscription_id}/disable")
        assert response.status_code == 200
        data = response.json()
        assert data["enabled"] == False
    
    async def test_enable_subscription_not_found(self, client):
        """존재하지 않는 구독 활성화 테스트"""
        response = await client.post("/api/v1/subscriptions/99999/enable")
        assert response.status_code == 404
        assert response.json()["detail"] == "Subscription not found"
    
    async def test_disable_subscription_not_found(self, client):
        """존재하지 않는 구독 비활성화 테스트"""
        response = await client.post("/api/v1/subscriptions/99999/disable")
        assert response.status_code == 404
        assert response.json()["detail"] == "Subscription not found"
    
    async def test_get_subscriptions_ordering(self, client, sample_subscription_data):
        """구독 목록이 ID 역순으로 정렬되는지 테스트"""
        # 여러 구독 생성
        subscriptions = []
        for i in range(3):
            subscription_data = sample_subscription_data.copy()
            subscription_data["subscriber"] = f"user{i}"
            subscription_data["notify_id"] = f"user{i}@example.com"
            create_response = await client.post("/api/v1/subscriptions/", json=subscription_data)
            subscriptions.append(create_response.json())
        
        # 목록 조회
        response = await client.get("/api/v1/subscriptions/")
        assert response.status_code == 200
        data = response.json()
        
        # ID 역순 정렬 확인 (최신 것이 먼저)
        assert len(data) == 3
        for i in range(len(data) - 1):
            assert data[i]["subscription_id"] >= data[i + 1]["subscription_id"]
    
    async def test_create_subscription_with_all_location_fields(self, client):
        """모든 위치 필드를 포함한 구독 생성 테스트"""
        full_location_data = {
            "plant": "Plant1",
            "factory": "Factory1",
            "building": "Building1",
            "floor": 2,
            "area": "Area1",
            "subscriber": "full_location_user",
            "notify_type": "email",
            "notify_id": "full@example.com"
        }
        response = await client.post("/api/v1/subscriptions/", json=full_location_data)
        assert response.status_code == 201
        data = response.json()
        assert data["plant"] == "Plant1"
        assert data["factory"] == "Factory1"
        assert data["building"] == "Building1"
        assert data["floor"] == 2
        assert data["area"] == "Area1"
