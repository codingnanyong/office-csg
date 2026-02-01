# Sensor Threshold Mapping Service

센서-임계치 매핑 관리 API 서비스

## 기능

- 센서별 임계치 매핑 생성 및 조회
- 센서별/임계치별 매핑 필터링
- 매핑 활성화/비활성화
- 유효 기간 관리 (effective_from, effective_to)

## API 엔드포인트

### 매핑 관리
- `POST /api/v1/mappings` - 매핑 생성
- `GET /api/v1/mappings` - 매핑 목록 조회 (다양한 필터 지원)
- `GET /api/v1/mappings/{map_id}` - 매핑 상세 조회
- `GET /api/v1/mappings/sensor/{sensor_id}` - 센서별 매핑 목록 조회
- `GET /api/v1/mappings/threshold/{threshold_id}` - 임계치별 매핑 목록 조회
- `PUT /api/v1/mappings/{map_id}` - 매핑 수정
- `DELETE /api/v1/mappings/{map_id}` - 매핑 삭제
- `POST /api/v1/mappings/{map_id}/enable` - 매핑 활성화
- `POST /api/v1/mappings/{map_id}/disable` - 매핑 비활성화

### 기본 엔드포인트
- `GET /` - 서비스 정보
- `GET /health` - 헬스체크
- `GET /ready` - 레디니스 체크
- `GET /docs` - API 문서 (Swagger UI)

## 데이터 모델

### 매핑 필드
- `map_id`: 매핑 ID (자동 생성)
- `sensor_id`: 센서 ID
- `threshold_id`: 임계치 ID
- `duration_seconds`: 지속 시간 (초 단위, 기본값: 60)
- `enabled`: 활성화 여부 (기본값: true)
- `effective_from`: 유효 시작 시간
- `effective_to`: 유효 종료 시간
- `upd_dt`: 수정 시간

## 환경 변수

- `APP_NAME`: 애플리케이션 이름 (기본값: Sensor Threshold Mapping Service)
- `APP_VERSION`: 애플리케이션 버전 (기본값: 1.0.0)
- `DATABASE_URL`: 데이터베이스 연결 URL
- `THRESHOLDS_SERVICE_URL`: Thresholds 서비스 URL
- `LOCATION_SERVICE_URL`: Location 서비스 URL

## 로컬 실행

```bash
# 의존성 설치
pip install -r requirements.txt

# 환경 변수 설정
cp env.example .env
# .env 파일 편집

# 개발 서버 실행
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## 테스트

```bash
# 테스트 실행
./tests/test.sh

# 또는 직접 pytest 실행
pytest tests/ -v
```

## Kubernetes 배포

```bash
cd k8s/sensor-threshold-mapping
./deploy.sh
```

## 포트

- HTTP: 30011
- Metrics: 30240
