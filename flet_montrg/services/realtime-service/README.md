# 🚀 Realtime Service

실시간 센서 데이터 처리 및 모니터링 API 서비스

## 📁 프로젝트 구조

```
realtime-service/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI 애플리케이션 진입점
│   ├── api/
│   │   ├── __init__.py
│   │   └── v1/
│   │       ├── __init__.py
│   │       ├── api.py          # API v1 라우터
│   │       └── endpoints/
│   │           ├── __init__.py
│   │           └── realtime.py # 실시간 데이터 API 엔드포인트
│   ├── clients/
│   │   ├── __init__.py
│   │   ├── location_client.py  # Location 서비스 클라이언트
│   │   └── thresholds_client.py # Thresholds 서비스 클라이언트
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py           # 설정 관리
│   │   ├── database.py         # 데이터베이스 연결
│   │   ├── exceptions.py       # 커스텀 예외
│   │   └── logging.py          # 로깅 설정
│   ├── models/
│   │   ├── __init__.py
│   │   ├── database_models.py  # SQLAlchemy 모델
│   │   └── schemas.py          # Pydantic 스키마
│   └── services/
│       ├── __init__.py
│       └── realtime_service.py # 실시간 데이터 처리 로직
├── tests/
│   ├── __init__.py
│   └── conftest.py             # pytest 설정
├── requirements.txt            # Python 의존성
├── env.example                 # 환경 변수 예시
├── Dockerfile                  # Docker 설정
└── README.md                   # 프로젝트 문서
```

## ⚙️ 설치 및 실행

### 1. 📦 의존성 설치
```bash
pip install -r requirements.txt
```

### 2. 🔧 환경 변수 설정
```bash
cp env.example .env
# .env 파일을 편집하여 필요한 설정을 변경
```

### 3. ▶️ 애플리케이션 실행
```bash
# 개발 모드
python -m app.main

# 또는 uvicorn 직접 사용
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 4. 🐳 Docker 실행
```bash
docker build -t realtime-service .
docker run -p 8000:8000 --env-file .env realtime-service
```

## 🔌 API 엔드포인트

### 실시간 온도 데이터 조회
- `GET /api/v1/realtime/` - 전체 온도 데이터 조회 (임계치 검사 포함)

### 다이나믹 필터링
- `GET /api/v1/realtime/factory/{factory}` - 공장별 온도 데이터 조회
- `GET /api/v1/realtime/building/{building}` - 건물별 온도 데이터 조회
- `GET /api/v1/realtime/floor/{floor}` - 층별 온도 데이터 조회
- `GET /api/v1/realtime/loc_id/{loc_id}` - 위치 ID별 온도 데이터 조회

### 다중 필터 조회
- `GET /api/v1/realtime/location?factory=...&building=...&floor=...&loc_id=...` - 위치 조건별 온도 데이터 조회 (다중 필터 지원)

### 응답 구조
```json
{
  "capture_dt": "2025-09-12T05:59:38.837000Z",
  "ymd": "20250912",
  "hh": "14",
  "measurements": [
    {
      "location": {
        "factory": "SinPyeong",
        "building": "MX-1",
        "floor": 1,
        "loc_id": "A011",
        "area": "자재 보관실"
      },
      "metrics": {
        "temperature": {
          "value": "22.1",
          "status": "normal"
        },
        "humidity": {
          "value": "77.7",
          "status": null
        },
        "pcv_temperature": {
          "value": "23.8",
          "status": "normal"
        }
      }
    }
  ]
}
```

## 🔗 외부 서비스 연동

### Location Service
- 센서 위치 정보 조회
- 위치별 센서 그룹핑

### Thresholds Service  
- 임계치 정보 조회
- 센서 타입별 임계치 매핑
- 알림 레벨 결정

## 📊 데이터 모델

### TemperatureCurrentData
- `capture_dt`: 측정 시간
- `ymd`: 년월일 (YYYYMMDD)
- `hh`: 시간 (HH)
- `measurements`: 측정 데이터 목록

### MeasurementData
- `location`: 위치 정보 (LocationInfo)
- `metrics`: 측정값들 (MetricsData)

### LocationInfo
- `factory`: 공장명
- `building`: 건물명
- `floor`: 층수
- `loc_id`: 위치 ID
- `area`: 구역

### MetricsData
- `temperature`: 온도 데이터 (MetricData)
- `humidity`: 습도 데이터 (MetricData)
- `pcv_temperature`: PCV 온도 데이터 (MetricData)

### MetricData
- `value`: 측정값 (Decimal)
- `status`: 상태 ("normal", "warning", "critical", null)

## 🚨 임계치 기반 상태 시스템

### 상태 레벨
- `normal` - 정상 상태
- `warning` - 경고 상태 (임계치 초과)
- `critical` - 위험 상태 (심각한 임계치 초과)
- `null` - 임계치 정보 없음

### 임계치 검사
- 센서 타입별 임계치 매핑 (temperature, humidity, pcv_temperature)
- 실시간 임계치 범위 검사
- 우선순위 기반 상태 결정 (critical > warning > normal)
- threshold가 없는 경우 null 반환

## 🔧 설정

### 환경 변수
- `DATABASE_URL` - PostgreSQL/TimescaleDB 연결 URL
- `LOCATION_SERVICE_URL` - Location 서비스 URL (기본값: http://location-service:80)
- `THRESHOLDS_SERVICE_URL` - Thresholds 서비스 URL (기본값: http://thresholds-service:80)
- `DEBUG` - 디버그 모드 (기본값: false)
- `LOG_LEVEL` - 로그 레벨 (기본값: INFO)
- `CORS_ORIGINS` - CORS 허용 오리진 (기본값: ["*"])

## 📈 모니터링

- 헬스체크 엔드포인트 (`/health`)
- 구조화된 로깅 (JSON 형태)
- 에러 핸들링 및 로깅

## 🧪 테스트

```bash
# 단위 테스트 실행
pytest

# 커버리지 포함 테스트
pytest --cov=app
```

## 📝 개발 가이드

### 새로운 필터 조건 추가
1. `temperature_service.py`의 `_get_temperature_data_with_filters` 메서드에 필터 파라미터 추가
2. `realtime.py`에 새로운 엔드포인트 추가
3. 필터링 로직에서 조건 확인 부분 추가

### 새로운 측정값 타입 추가
1. `schemas.py`의 `MetricsData` 모델에 새 필드 추가
2. `temperature_service.py`의 `_process_measurement_data` 메서드에서 새 측정값 처리 로직 추가
3. `thresholds_client.py`의 매핑 테이블 업데이트

### 외부 서비스 연동
1. `clients/` 디렉토리에 새 클라이언트 추가
2. `config.py`에 서비스 URL 설정 추가
3. `temperature_service.py`에서 클라이언트 사용

### 임계치 레벨 추가
1. `thresholds-service`의 `Level` enum에 새 레벨 추가
2. `temperature_service.py`의 `_check_thresholds` 메서드에서 레벨 매핑 업데이트
