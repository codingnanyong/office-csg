# 🚀 Thresholds Service

체감 온도 임계치 관리 API 서비스

## 📁 프로젝트 구조

```
thresholds-service/
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
│   │           └── thresholds.py  # 임계치 API 엔드포인트
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
│       └── threshold_service.py # 비즈니스 로직
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
docker build -t thresholds-service .
docker run -p 8000:8000 thresholds-service
```

## 📘 API 문서

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## 🧪 테스트

```bash
# 모든 테스트 실행
pytest

# 커버리지와 함께 실행
pytest --cov=app

# 특정 테스트 파일 실행
pytest tests/test_thresholds.py
```

## ✨ 주요 기능

- 임계치 CRUD 작업
- 타입별 임계치 조회
- 데이터 유효성 검사
- 로깅 및 예외 처리
- 헬스체크 엔드포인트

## 🔧 환경 변수

| 변수명 | 설명 | 기본값 |
|--------|------|--------|
| APP_NAME | 애플리케이션 이름 | Thresholds Service |
| APP_VERSION | 애플리케이션 버전 | 1.0.0 |
| DEBUG | 디버그 모드 | false |
| ENVIRONMENT | 환경 (development/production) | development |
| HOST | 서버 호스트 | 0.0.0.0 |
| PORT | 서버 포트 | 8000 |
| DATABASE_URL | 데이터베이스 연결 URL | - |
| CORS_ORIGINS | CORS 허용 오리진 | * |
| LOG_LEVEL | 로그 레벨 | INFO |
