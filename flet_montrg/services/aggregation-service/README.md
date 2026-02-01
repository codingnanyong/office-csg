# Aggregation Service

체감 온도 데이터 집계 및 통계 API 서비스

## 기능

- 체감 온도 데이터 시간별 집계
- 위치별, 공장별 온도 데이터 조회
- 구조화된 JSON 응답 제공

## API 엔드포인트

### 체감 온도 집계 조회
- `GET /api/v1/aggregation/pcv_temperature/` - 모든 체감 온도(max,avg)의 집계 데이터 조회
- `GET /api/v1/aggregation/pcv_temperature/location/{location_id}` - 위치별 체감 온도 조회
- `GET /api/v1/aggregation/pcv_temperature/factory/{factory}` - 공장별 체감 온도 조회

**파라미터:**
- `start_date`, `end_date`: 날짜 범위 (yyyy, yyyyMM, yyyyMMdd 형태 지원)

**참고:** 
- 시간 범위는 자동으로 00시~23시로 설정됩니다
- 메트릭은 자동으로 pcv_temperature_max, pcv_temperature_avg로 고정됩니다
- 날짜는 dynamic하게 yyyy, yyyyMM, yyyyMMdd까지 가능합니다

### 기본 엔드포인트
- `GET /` - 서비스 정보
- `GET /health` - 헬스체크
- `GET /ready` - 레디니스 체크
- `GET /docs` - API 문서 (Swagger UI)

## 요청/응답 구조

### 요청 예시

**위치별 조회:**
```
GET /api/v1/aggregation/pcv_temperature/location/A031?start_date=20240922&end_date=20240922
```

**공장별 조회:**
```
GET /api/v1/aggregation/pcv_temperature/factory/SinPyeong?start_date=20240922&end_date=20240922
```

**전체 조회:**
```
GET /api/v1/aggregation/pcv_temperature/?start_date=20240922&end_date=20240922
```

### 응답 예시
```json
{
  "location": {
    "factory": "SinPyeong",
    "building": "F-2001",
    "floor": 1,
    "loc_id": "A031",
    "area": "조립2",
    "date": [
      {
        "ymd": "20240922",
        "hour": "12",
        "metrics": {
            "pcv_temperature_max": "27.00",
            "pcv_temperature_avg": "27.00"
        }
      }
    ]
  }
}
```

## 환경 변수

- `APP_NAME`: 애플리케이션 이름 (기본값: Aggregation Service)
- `APP_VERSION`: 애플리케이션 버전 (기본값: 1.0.0)
- `DEBUG`: 디버그 모드 (기본값: true)
- `ENVIRONMENT`: 환경 (development/production)
- `HOST`: 서버 호스트 (기본값: 0.0.0.0)
- `PORT`: 서버 포트 (기본값: 8000)
- `DATABASE_URL`: PostgreSQL 연결 문자열
- `CORS_ORIGINS`: CORS 허용 오리진
- `LOG_LEVEL`: 로그 레벨 (기본값: INFO)

## 실행

### 개발 환경
```bash
# 의존성 설치
pip install -r requirements.txt

# 환경 변수 설정
cp env.example .env

# 서버 실행
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Docker
```bash
# 이미지 빌드
docker build -t flet-montrg/aggregation-service:latest .

# 컨테이너 실행
docker run -p 8000:8000 flet-montrg/aggregation-service:latest
```

### 테스트
```bash
# 테스트 실행
./test.sh

# 코드 포맷팅 및 린팅
./test.sh --format --lint
```

## 데이터베이스 스키마

서비스는 다음 테이블을 사용합니다:
- `flet_montrg.temperature`: 온도 데이터
- `flet_montrg.locations`: 위치 정보
- `flet_montrg.aggregations`: 집계 결과 저장
- `flet_montrg.aggregation_jobs`: 집계 작업 관리