# Alert Subscription Service

알람 구독 관리 API 서비스

## 기능

- 알람 구독 생성 및 조회
- 구독자별 구독 관리
- 위치 계층 구조별 구독 필터링 (plant, factory, building, floor, area)
- 센서별/임계치 타입별 구독 필터링
- 구독 활성화/비활성화

## API 엔드포인트

### 구독 관리
- `POST /api/v1/subscriptions` - 구독 생성
- `GET /api/v1/subscriptions` - 구독 목록 조회 (다양한 필터 지원)
- `GET /api/v1/subscriptions/{subscription_id}` - 구독 상세 조회
- `GET /api/v1/subscriptions/subscriber/{subscriber}` - 구독자별 구독 목록 조회
- `PUT /api/v1/subscriptions/{subscription_id}` - 구독 수정
- `DELETE /api/v1/subscriptions/{subscription_id}` - 구독 삭제
- `POST /api/v1/subscriptions/{subscription_id}/enable` - 구독 활성화
- `POST /api/v1/subscriptions/{subscription_id}/disable` - 구독 비활성화

### 기본 엔드포인트
- `GET /` - 서비스 정보
- `GET /health` - 헬스체크
- `GET /ready` - 레디니스 체크
- `GET /docs` - API 문서 (Swagger UI)

## 데이터 모델

### 구독 필드
- `subscription_id`: 구독 ID (자동 생성)
- `plant`: 공장
- `factory`: 공장명
- `building`: 건물
- `floor`: 층
- `area`: 구역
- `sensor_id`: 센서 ID
- `threshold_type`: 임계치 타입
- `min_level`: 최소 알람 레벨
- `subscriber`: 구독자 이름
- `notify_type`: 알림 타입 (email, kakao, sms, app)
- `notify_id`: 알림 ID (email이면 이메일 주소, app이면 계정 이름)
- `enabled`: 활성화 여부
- `upd_dt`: 수정 시간

## 환경 변수

- `APP_NAME`: 애플리케이션 이름 (기본값: Alert Subscription Service)
- `APP_VERSION`: 애플리케이션 버전 (기본값: 1.0.0)
- `DATABASE_URL`: 데이터베이스 연결 URL
- `LOCATION_SERVICE_URL`: Location 서비스 URL

## 로컬 실행

```bash
# 의존성 설치
pip install -r requirements.txt

# 환경 변수 설정
cp env.example .env
# .env 파일 편집

# 개발 서버 실행
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Docker 실행

```bash
# Docker 이미지 빌드
docker build -t flet-montrg/alert-subscription-service:latest .

# 컨테이너 실행
docker run -p 8000:8000 --env-file .env flet-montrg/alert-subscription-service:latest
```
