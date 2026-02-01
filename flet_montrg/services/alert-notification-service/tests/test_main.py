"""
Alert Notification Service API 테스트
"""
import pytest
from datetime import datetime

pytestmark = pytest.mark.asyncio


class TestNotificationService:
    """Alert Notification Service API 테스트"""
    
    async def test_root_endpoint(self, client):
        """루트 엔드포인트 테스트"""
        response = await client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "version" in data
        assert "Alert Notification Service" in data["message"]
    
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
    
    async def test_create_notification(self, client, sample_notification_data):
        """알림 생성 테스트"""
        response = await client.post("/api/v1/notifications/", json=sample_notification_data)
        assert response.status_code == 201
        data = response.json()
        assert "notification_id" in data
        assert data["alert_id"] == sample_notification_data["alert_id"]
        assert data["subscription_id"] == sample_notification_data["subscription_id"]
        assert data["notify_type"] == sample_notification_data["notify_type"]
        assert data["notify_id"] == sample_notification_data["notify_id"]
        assert data["status"] == "PENDING"
        assert data["try_count"] == 0
        assert "created_time" in data
    
    async def test_create_notification_with_kakao(self, client):
        """카카오 알림 타입으로 알림 생성 테스트"""
        kakao_data = {
            "alert_id": 1,
            "subscription_id": 1,
            "notify_type": "kakao",
            "notify_id": "kakao_account_name"
        }
        response = await client.post("/api/v1/notifications/", json=kakao_data)
        assert response.status_code == 201
        data = response.json()
        assert data["notify_type"] == "kakao"
        assert data["notify_id"] == "kakao_account_name"
    
    async def test_create_notification_invalid_notify_type(self, client):
        """잘못된 알림 타입으로 생성 테스트"""
        invalid_data = {
            "alert_id": 1,
            "subscription_id": 1,
            "notify_type": "invalid_type",
            "notify_id": "test@example.com"
        }
        response = await client.post("/api/v1/notifications/", json=invalid_data)
        # Pydantic validation error
        assert response.status_code == 422
    
    async def test_get_notifications_empty(self, client):
        """빈 알림 목록 조회 테스트"""
        response = await client.get("/api/v1/notifications/")
        assert response.status_code == 200
        assert response.json() == []
    
    async def test_get_notifications_with_data(self, client, sample_notification_data):
        """데이터가 있는 알림 목록 조회 테스트"""
        # 알림 생성
        await client.post("/api/v1/notifications/", json=sample_notification_data)
        
        # 목록 조회
        response = await client.get("/api/v1/notifications/")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["alert_id"] == sample_notification_data["alert_id"]
    
    async def test_get_notifications_with_pagination(self, client, sample_notification_data):
        """페이지네이션을 사용한 알림 목록 조회 테스트"""
        # 여러 알림 생성
        for i in range(5):
            notification_data = sample_notification_data.copy()
            notification_data["alert_id"] = i + 1
            notification_data["notify_id"] = f"user{i}@example.com"
            await client.post("/api/v1/notifications/", json=notification_data)
        
        # 첫 페이지 조회 (limit=2)
        response = await client.get("/api/v1/notifications/?skip=0&limit=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        
        # 두 번째 페이지 조회
        response = await client.get("/api/v1/notifications/?skip=2&limit=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
    
    async def test_get_notifications_filter_by_alert_id(self, client, sample_notification_data):
        """알람 ID로 필터링 테스트"""
        # 여러 알람의 알림 생성
        for i in range(3):
            notification_data = sample_notification_data.copy()
            notification_data["alert_id"] = i + 1
            notification_data["notify_id"] = f"user{i}@example.com"
            await client.post("/api/v1/notifications/", json=notification_data)
        
        # 특정 알람으로 필터링
        response = await client.get("/api/v1/notifications/?alert_id=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["alert_id"] == 1
    
    async def test_get_notifications_filter_by_subscription_id(self, client, sample_notification_data):
        """구독 ID로 필터링 테스트"""
        # 여러 구독의 알림 생성
        for i in range(3):
            notification_data = sample_notification_data.copy()
            notification_data["subscription_id"] = i + 1
            notification_data["notify_id"] = f"user{i}@example.com"
            await client.post("/api/v1/notifications/", json=notification_data)
        
        # 특정 구독으로 필터링
        response = await client.get("/api/v1/notifications/?subscription_id=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["subscription_id"] == 1
    
    async def test_get_notifications_filter_by_status(self, client, sample_notification_data):
        """상태로 필터링 테스트"""
        # 알림 생성
        create_response = await client.post("/api/v1/notifications/", json=sample_notification_data)
        notification_id = create_response.json()["notification_id"]
        
        # 상태를 SENT로 변경
        await client.post(f"/api/v1/notifications/{notification_id}/mark-sent")
        
        # PENDING 상태로 필터링 (없어야 함)
        response = await client.get("/api/v1/notifications/?status=PENDING")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 0
        
        # SENT 상태로 필터링
        response = await client.get("/api/v1/notifications/?status=SENT")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["status"] == "SENT"
    
    async def test_get_notification_by_id(self, client, sample_notification_data):
        """ID로 특정 알림 조회 테스트"""
        # 알림 생성
        create_response = await client.post("/api/v1/notifications/", json=sample_notification_data)
        notification_id = create_response.json()["notification_id"]
        
        # ID로 조회
        response = await client.get(f"/api/v1/notifications/{notification_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["notification_id"] == notification_id
        assert data["alert_id"] == sample_notification_data["alert_id"]
    
    async def test_get_notification_by_id_not_found(self, client):
        """존재하지 않는 ID로 알림 조회 테스트"""
        response = await client.get("/api/v1/notifications/99999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Notification not found"
    
    async def test_get_notifications_by_alert_id(self, client, sample_notification_data):
        """알람별 알림 목록 조회 테스트"""
        alert_id = 1
        
        # 같은 알람의 여러 알림 생성
        for i in range(3):
            notification_data = sample_notification_data.copy()
            notification_data["alert_id"] = alert_id
            notification_data["notify_id"] = f"test{i}@example.com"
            notification_data["subscription_id"] = i + 1
            await client.post("/api/v1/notifications/", json=notification_data)
        
        # 다른 알람의 알림 생성
        other_data = sample_notification_data.copy()
        other_data["alert_id"] = 2
        other_data["notify_id"] = "other@example.com"
        await client.post("/api/v1/notifications/", json=other_data)
        
        # 알람별 조회
        response = await client.get(f"/api/v1/notifications/alert/{alert_id}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        assert all(item["alert_id"] == alert_id for item in data)
    
    async def test_get_notifications_by_subscription_id(self, client, sample_notification_data):
        """구독별 알림 목록 조회 테스트"""
        subscription_id = 1
        
        # 같은 구독의 여러 알림 생성
        for i in range(3):
            notification_data = sample_notification_data.copy()
            notification_data["subscription_id"] = subscription_id
            notification_data["notify_id"] = f"test{i}@example.com"
            notification_data["alert_id"] = i + 1
            await client.post("/api/v1/notifications/", json=notification_data)
        
        # 다른 구독의 알림 생성
        other_data = sample_notification_data.copy()
        other_data["subscription_id"] = 2
        other_data["notify_id"] = "other@example.com"
        await client.post("/api/v1/notifications/", json=other_data)
        
        # 구독별 조회
        response = await client.get(f"/api/v1/notifications/subscription/{subscription_id}")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        assert all(item["subscription_id"] == subscription_id for item in data)
    
    async def test_get_pending_notifications(self, client, sample_notification_data):
        """대기 중인 알림 목록 조회 테스트"""
        # 여러 알림 생성
        for i in range(3):
            notification_data = sample_notification_data.copy()
            notification_data["alert_id"] = i + 1
            notification_data["notify_id"] = f"user{i}@example.com"
            await client.post("/api/v1/notifications/", json=notification_data)
        
        # 하나를 SENT로 변경
        create_response = await client.post("/api/v1/notifications/", json=sample_notification_data)
        notification_id = create_response.json()["notification_id"]
        await client.post(f"/api/v1/notifications/{notification_id}/mark-sent")
        
        # 대기 중인 알림 조회
        response = await client.get("/api/v1/notifications/pending")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3  # 처음 생성한 3개만 PENDING
        assert all(item["status"] == "PENDING" for item in data)
    
    async def test_get_failed_notifications(self, client, sample_notification_data):
        """실패한 알림 목록 조회 테스트"""
        # 알림 생성
        create_response = await client.post("/api/v1/notifications/", json=sample_notification_data)
        notification_id = create_response.json()["notification_id"]
        
        # 실패로 표시
        await client.post(f"/api/v1/notifications/{notification_id}/mark-failed?fail_reason=Test failure")
        
        # 실패한 알림 조회
        response = await client.get("/api/v1/notifications/failed")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["status"] == "FAILED"
        assert data[0]["fail_reason"] == "Test failure"
    
    async def test_update_notification(self, client, sample_notification_data):
        """알림 수정 테스트"""
        # 알림 생성
        create_response = await client.post("/api/v1/notifications/", json=sample_notification_data)
        notification_id = create_response.json()["notification_id"]
        
        # 알림 수정
        update_data = {
            "status": "RETRYING",
            "try_count": 1
        }
        response = await client.put(f"/api/v1/notifications/{notification_id}", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "RETRYING"
        assert data["try_count"] == 1
        # 수정되지 않은 필드는 유지
        assert data["alert_id"] == sample_notification_data["alert_id"]
    
    async def test_update_notification_not_found(self, client):
        """존재하지 않는 알림 수정 테스트"""
        update_data = {"status": "SENT"}
        response = await client.put("/api/v1/notifications/99999", json=update_data)
        assert response.status_code == 404
        assert response.json()["detail"] == "Notification not found"
    
    async def test_mark_notification_as_sent(self, client, sample_notification_data):
        """알림을 발송 완료로 표시 테스트"""
        # 알림 생성
        create_response = await client.post("/api/v1/notifications/", json=sample_notification_data)
        notification_id = create_response.json()["notification_id"]
        
        # 발송 완료로 표시
        response = await client.post(f"/api/v1/notifications/{notification_id}/mark-sent")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "SENT"
        assert data["sent_time"] is not None
    
    async def test_mark_notification_as_failed(self, client, sample_notification_data):
        """알림을 실패로 표시 테스트"""
        # 알림 생성
        create_response = await client.post("/api/v1/notifications/", json=sample_notification_data)
        notification_id = create_response.json()["notification_id"]
        
        # 실패로 표시
        response = await client.post(f"/api/v1/notifications/{notification_id}/mark-failed?fail_reason=Connection timeout")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "FAILED"
        assert data["fail_reason"] == "Connection timeout"
        assert data["try_count"] == 1  # increment_try 기본값이 True
        assert data["last_try_time"] is not None
    
    async def test_mark_notification_as_failed_without_increment(self, client, sample_notification_data):
        """재시도 횟수 증가 없이 실패로 표시 테스트"""
        # 알림 생성
        create_response = await client.post("/api/v1/notifications/", json=sample_notification_data)
        notification_id = create_response.json()["notification_id"]
        
        # 실패로 표시 (재시도 횟수 증가 없이)
        response = await client.post(f"/api/v1/notifications/{notification_id}/mark-failed?fail_reason=Test&increment_try=false")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "FAILED"
        assert data["try_count"] == 0  # 증가하지 않음
    
    async def test_mark_notification_as_retrying(self, client, sample_notification_data):
        """알림을 재시도 중으로 표시 테스트"""
        # 알림 생성
        create_response = await client.post("/api/v1/notifications/", json=sample_notification_data)
        notification_id = create_response.json()["notification_id"]
        initial_try_count = create_response.json()["try_count"]
        
        # 재시도 중으로 표시
        response = await client.post(f"/api/v1/notifications/{notification_id}/mark-retrying")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "RETRYING"
        assert data["try_count"] == initial_try_count + 1
        assert data["last_try_time"] is not None
    
    async def test_delete_notification(self, client, sample_notification_data):
        """알림 삭제 테스트"""
        # 알림 생성
        create_response = await client.post("/api/v1/notifications/", json=sample_notification_data)
        notification_id = create_response.json()["notification_id"]
        
        # 알림 삭제
        response = await client.delete(f"/api/v1/notifications/{notification_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == f"Notification {notification_id} deleted successfully"
        
        # 삭제 확인
        get_response = await client.get(f"/api/v1/notifications/{notification_id}")
        assert get_response.status_code == 404
    
    async def test_delete_notification_not_found(self, client):
        """존재하지 않는 알림 삭제 테스트"""
        response = await client.delete("/api/v1/notifications/99999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Notification not found"
    
    async def test_get_notifications_ordering(self, client, sample_notification_data):
        """알림 목록이 ID 역순으로 정렬되는지 테스트"""
        # 여러 알림 생성
        notifications = []
        for i in range(3):
            notification_data = sample_notification_data.copy()
            notification_data["alert_id"] = i + 1
            notification_data["notify_id"] = f"user{i}@example.com"
            create_response = await client.post("/api/v1/notifications/", json=notification_data)
            notifications.append(create_response.json())
        
        # 목록 조회
        response = await client.get("/api/v1/notifications/")
        assert response.status_code == 200
        data = response.json()
        
        # ID 역순 정렬 확인 (최신 것이 먼저)
        assert len(data) == 3
        for i in range(len(data) - 1):
            assert data[i]["notification_id"] >= data[i + 1]["notification_id"]
