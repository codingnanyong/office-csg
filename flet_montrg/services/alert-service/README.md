# Alert Service

알람 생성 및 관리 API 서비스

## 기능

- 알람 생성 및 조회
- 센서별/위치별 알람 조회
- 알람 상태 관리 (해결 처리)
- 임계치 초과 알람 기록

## API 엔드포인트

### 알람 관리
- `POST /api/v1/alerts` - 알람 생성
- `GET /api/v1/alerts` - 알람 목록 조회
- `GET /api/v1/alerts/{alert_id}` - 알람 상세 조회
- `GET /api/v1/alerts/by-sensor/{sensor_id}` - 센서별 알람 조회
- `GET /api/v1/alerts/by-location/{loc_id}` - 위치별 알람 조회
- `PUT /api/v1/alerts/{alert_id}` - 알람 수정
- `PUT /api/v1/alerts/{alert_id}/resolve` - 알람 해결 처리

### 기본 엔드포인트
- `GET /` - 서비스 정보
- `GET /health` - 헬스체크
- `GET /ready` - 레디니스 체크
- `GET /docs` - API 문서 (Swagger UI)

## 환경 변수

- `APP_NAME`: 애플리케이션 이름 (기본값: Alert Service)
- `APP_VERSION`: 애플리케이션 버전 (기본값: 1.0.0)
- `DATABASE_URL`: 데이터베이스 연결 URL
- `THRESHOLDS_SERVICE_URL`: Thresholds 서비스 URL
- `LOCATION_SERVICE_URL`: Location 서비스 URL
- `SENSOR_THRESHOLD_MAPPING_SERVICE_URL`: Sensor Threshold Mapping 서비스 URL

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
docker build -t flet-montrg/alert-service:latest .

# 컨테이너 실행
docker run -p 8000:8000 --env-file .env flet-montrg/alert-service:latest
```
