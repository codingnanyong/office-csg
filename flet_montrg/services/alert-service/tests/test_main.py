"""
Alert Service API 테스트
"""
import pytest
from datetime import datetime, timedelta
from decimal import Decimal

pytestmark = pytest.mark.asyncio


class TestAlertService:
    """Alert Service API 테스트"""
    
    async def test_root_endpoint(self, client):
        """루트 엔드포인트 테스트"""
        response = await client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "version" in data
        assert "Alert Service" in data["message"]
    
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
    
    async def test_create_alert(self, client, sample_alert_data):
        """알람 생성 테스트"""
        response = await client.post("/api/v1/alerts/", json=sample_alert_data)
        assert response.status_code == 201
        data = response.json()
        assert "alert_id" in data
        assert data["sensor_id"] == sample_alert_data["sensor_id"]
        assert data["alert_type"] == sample_alert_data["alert_type"]
        assert data["alert_level"] == sample_alert_data["alert_level"]
        assert "alert_time" in data
    
    async def test_create_alert_minimal(self, client):
        """최소 필수 필드만으로 알람 생성 테스트"""
        minimal_data = {
            "sensor_id": "SENSOR002",
            "alert_type": "humidity",
            "alert_level": "medium",
            "threshold_id": 2,
            "threshold_type": "humidity",
            "threshold_level": "medium"
        }
        response = await client.post("/api/v1/alerts/", json=minimal_data)
        assert response.status_code == 201
        data = response.json()
        assert data["sensor_id"] == minimal_data["sensor_id"]
        assert data["alert_type"] == minimal_data["alert_type"]
    
    async def test_create_alert_invalid_type(self, client):
        """잘못된 알람 타입으로 생성 테스트"""
        invalid_data = {
            "sensor_id": "SENSOR003",
            "alert_type": "invalid_type",  # 유효하지 않은 타입
            "alert_level": "high",
            "threshold_id": 1,
            "threshold_type": "temperature",
            "threshold_level": "high"
        }
        response = await client.post("/api/v1/alerts/", json=invalid_data)
        # Pydantic validation error
        assert response.status_code == 422
    
    async def test_get_alerts_empty(self, client):
        """빈 알람 목록 조회 테스트"""
        response = await client.get("/api/v1/alerts/")
        assert response.status_code == 200
        assert response.json() == []
    
    async def test_get_alerts_with_data(self, client, sample_alert_data):
        """데이터가 있는 알람 목록 조회 테스트"""
        # 알람 생성
        await client.post("/api/v1/alerts/", json=sample_alert_data)
        
        # 목록 조회
        response = await client.get("/api/v1/alerts/")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["sensor_id"] == sample_alert_data["sensor_id"]
    
    async def test_get_alerts_with_pagination(self, client, sample_alert_data):
        """페이지네이션을 사용한 알람 목록 조회 테스트"""
        # 여러 알람 생성
        for i in range(5):
            alert_data = sample_alert_data.copy()
            alert_data["sensor_id"] = f"SENSOR{i:03d}"
            await client.post("/api/v1/alerts/", json=alert_data)
        
        # 첫 페이지 조회 (limit=2)
        response = await client.get("/api/v1/alerts/?skip=0&limit=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        
        # 두 번째 페이지 조회
        response = await client.get("/api/v1/alerts/?skip=2&limit=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
    
    async def test_get_alerts_filter_by_sensor_id(self, client, sample_alert_data):
        """센서 ID로 필터링 테스트"""
        # 여러 센서의 알람 생성
        for i in range(3):
            alert_data = sample_alert_data.copy()
            alert_data["sensor_id"] = f"SENSOR{i:03d}"
            await client.post("/api/v1/alerts/", json=alert_data)
        
        # 특정 센서로 필터링
        response = await client.get("/api/v1/alerts/?sensor_id=SENSOR001")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["sensor_id"] == "SENSOR001"
    
    async def test_get_alerts_filter_by_loc_id(self, client, sample_alert_data):
        """위치 ID로 필터링 테스트"""
        # 여러 위치의 알람 생성
        locations = ["A031", "A032", "A033"]
        for loc_id in locations:
            alert_data = sample_alert_data.copy()
            alert_data["loc_id"] = loc_id
            alert_data["sensor_id"] = f"SENSOR_{loc_id}"
            await client.post("/api/v1/alerts/", json=alert_data)
        
        # 특정 위치로 필터링
        response = await client.get("/api/v1/alerts/?loc_id=A031")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["loc_id"] == "A031"
    
    async def test_get_alerts_filter_by_alert_type(self, client):
        """알람 타입으로 필터링 테스트"""
        # 여러 타입의 알람 생성
        types = ["temperature", "humidity", "temperature"]
        for alert_type in types:
            alert_data = {
                "sensor_id": f"SENSOR_{alert_type}",
                "alert_type": alert_type,
                "alert_level": "high",
                "threshold_id": 1,
                "threshold_type": alert_type,
                "threshold_level": "high"
            }
            await client.post("/api/v1/alerts/", json=alert_data)
        
        # temperature 타입으로 필터링
        response = await client.get("/api/v1/alerts/?alert_type=temperature")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert all(item["alert_type"] == "temperature" for item in data)
    
    async def test_get_alerts_filter_by_alert_level(self, client):
        """알람 레벨로 필터링 테스트"""
        # 여러 레벨의 알람 생성
        levels = ["low", "medium", "high", "high"]
        for level in levels:
            alert_data = {
                "sensor_id": f"SENSOR_{level}",
                "alert_type": "temperature",
                "alert_level": level,
                "threshold_id": 1,
                "threshold_type": "temperature",
                "threshold_level": level
            }
            await client.post("/api/v1/alerts/", json=alert_data)
        
        # high 레벨로 필터링
        response = await client.get("/api/v1/alerts/?alert_level=high")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert all(item["alert_level"] == "high" for item in data)
    
    async def test_get_alert_by_id(self, client, sample_alert_data):
        """ID로 특정 알람 조회 테스트"""
        # 알람 생성
        create_response = await client.post("/api/v1/alerts/", json=sample_alert_data)
        alert_id = create_response.json()["alert_id"]
        
        # ID로 조회
        response = await client.get(f"/api/v1/alerts/{alert_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["alert_id"] == alert_id
        assert data["sensor_id"] == sample_alert_data["sensor_id"]
    
    async def test_get_alert_by_id_not_found(self, client):
        """존재하지 않는 ID로 알람 조회 테스트"""
        response = await client.get("/api/v1/alerts/99999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Alert not found"
    
    async def test_get_today_alerts_empty(self, client):
        """오늘 알람 목록 조회 (빈 결과) 테스트"""
        response = await client.get("/api/v1/alerts/today")
        assert response.status_code == 200
        assert response.json() == []
    
    async def test_get_today_alerts(self, client, sample_alert_data):
        """오늘 알람 목록 조회 테스트"""
        # 오늘 알람 생성
        await client.post("/api/v1/alerts/", json=sample_alert_data)
        
        # 오늘 알람 조회
        response = await client.get("/api/v1/alerts/today")
        assert response.status_code == 200
        data = response.json()
        assert len(data) >= 1
        # 모든 알람이 오늘 날짜인지 확인
        for alert in data:
            alert_time = datetime.fromisoformat(alert["alert_time"].replace("Z", "+00:00"))
            assert alert_time.date() == datetime.now().date()
    
    async def test_get_today_alerts_with_filters(self, client, sample_alert_data):
        """필터를 사용한 오늘 알람 목록 조회 테스트"""
        # 여러 알람 생성
        for i in range(3):
            alert_data = sample_alert_data.copy()
            alert_data["sensor_id"] = f"SENSOR{i:03d}"
            await client.post("/api/v1/alerts/", json=alert_data)
        
        # 센서 ID로 필터링하여 오늘 알람 조회
        response = await client.get("/api/v1/alerts/today?sensor_id=SENSOR001")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["sensor_id"] == "SENSOR001"
    
    async def test_get_today_alerts_filter_by_alert_level(self, client):
        """알람 레벨로 필터링한 오늘 알람 조회 테스트"""
        # 여러 레벨의 알람 생성
        levels = ["low", "medium", "high"]
        for level in levels:
            alert_data = {
                "sensor_id": f"SENSOR_{level}",
                "alert_type": "temperature",
                "alert_level": level,
                "threshold_id": 1,
                "threshold_type": "temperature",
                "threshold_level": level
            }
            await client.post("/api/v1/alerts/", json=alert_data)
        
        # high 레벨로 필터링
        response = await client.get("/api/v1/alerts/today?alert_level=high")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["alert_level"] == "high"
    
    async def test_create_alert_with_custom_time(self, client, sample_alert_data):
        """사용자 지정 시간으로 알람 생성 테스트"""
        custom_time = datetime.now().isoformat()
        sample_alert_data["alert_time"] = custom_time
        
        response = await client.post("/api/v1/alerts/", json=sample_alert_data)
        assert response.status_code == 201
        data = response.json()
        assert "alert_time" in data
    
    async def test_get_alerts_ordering(self, client, sample_alert_data):
        """알람 목록이 시간 역순으로 정렬되는지 테스트"""
        # 여러 알람을 시간 간격을 두고 생성
        alerts = []
        for i in range(3):
            alert_data = sample_alert_data.copy()
            alert_data["sensor_id"] = f"SENSOR{i:03d}"
            create_response = await client.post("/api/v1/alerts/", json=alert_data)
            alerts.append(create_response.json())
        
        # 목록 조회
        response = await client.get("/api/v1/alerts/")
        assert response.status_code == 200
        data = response.json()
        
        # 시간 역순 정렬 확인 (최신 것이 먼저)
        assert len(data) == 3
        for i in range(len(data) - 1):
            current_time = datetime.fromisoformat(data[i]["alert_time"].replace("Z", "+00:00"))
            next_time = datetime.fromisoformat(data[i + 1]["alert_time"].replace("Z", "+00:00"))
            assert current_time >= next_time
