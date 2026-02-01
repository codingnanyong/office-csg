# Alert Notification Service

알림 발송 및 발송 이력 관리 API 서비스입니다.

## 기능

- 알림 발송 이력 관리
- 알림 상태 추적 (PENDING, SENT, FAILED, RETRYING)
- 재시도 로직 지원
- 알람별/구독별 알림 조회
- 대기 중/실패한 알림 조회

## API 엔드포인트

### 알림 관리
- `POST /api/v1/notifications/` - 새 알림 생성
- `GET /api/v1/notifications/` - 알림 목록 조회
- `GET /api/v1/notifications/{notification_id}` - 특정 알림 조회
- `PUT /api/v1/notifications/{notification_id}` - 알림 수정
- `DELETE /api/v1/notifications/{notification_id}` - 알림 삭제

### 알림 상태 관리
- `POST /api/v1/notifications/{notification_id}/mark-sent` - 발송 완료로 표시
- `POST /api/v1/notifications/{notification_id}/mark-failed` - 실패로 표시
- `POST /api/v1/notifications/{notification_id}/mark-retrying` - 재시도 중으로 표시

### 조회 엔드포인트
- `GET /api/v1/notifications/alert/{alert_id}` - 알람별 알림 목록
- `GET /api/v1/notifications/subscription/{subscription_id}` - 구독별 알림 목록
- `GET /api/v1/notifications/pending` - 대기 중인 알림 목록
- `GET /api/v1/notifications/failed` - 실패한 알림 목록

## 환경 변수

`.env` 파일을 참고하거나 `env.example`을 복사하여 설정하세요.

## 실행

```bash
# 로컬 개발
uvicorn app.main:app --reload

# Docker
docker build -t alert-notification-service .
docker run -p 8000:8000 alert-notification-service
```

## 테스트

```bash
pytest
```
