# 📊 flet-montrg 프로젝트

IoT 센서를 통한 체감 온도 데이터 모니터링 및 알림 시스템

## 📁 프로젝트 구조

```bash
flet_montrg/
├── services/                              # 마이크로서비스 소스 코드
│   ├── thresholds-service/              # 임계치 CRUD API
│   ├── location-service/                 # 센서 위치 정보 API
│   ├── realtime-service/                 # 실시간 현황 API
│   ├── aggregation-service/             # 기간 조회 API
│   ├── alert-service/                    # 알람 생성 및 관리
│   ├── alert-subscription-service/      # 알람 구독 관리
│   ├── alert-notification-service/       # 알림 발송 관리
│   ├── sensor-threshold-mapping-service/ # 센서-임계치 매핑 관리
│   └── integrated-swagger-service/       # 통합 API 문서 및 프록시
├── k8s/                                  # K8s 배포 파일
│   ├── thresholds/                       # thresholds-service 배포
│   ├── location/                         # location-service 배포
│   ├── realtime/                         # realtime-service 배포
│   ├── aggregation/                      # aggregation-service 배포
│   ├── alert/                            # alert-service 배포
│   ├── alert-subscription/                # alert-subscription-service 배포
│   ├── alert-notification/                # alert-notification-service 배포
│   ├── sensor-threshold-mapping/          # sensor-threshold-mapping-service 배포
│   └── integrated-swagger/                # integrated-swagger-service 배포
├── config/                               # 공통 설정 파일
└── README.md                             # 프로젝트 문서
```

## 🔌 서비스 포트

### 데이터 서비스

- **30001**: thresholds-service (임계치 CRUD API)
- **30002**: location-service (센서 위치 정보 API)
- **30003**: realtime-service (실시간 현황 API)
- **30004**: aggregation-service (기간 조회 API)

### 알람 서비스

- **30007**: alert-service (알람 생성 및 관리)
- **30008**: alert-subscription-service (알람 구독 관리)
- **30009**: alert-notification-service (알림 발송 관리)

### 매핑 서비스

- **30011**: sensor-threshold-mapping-service (센서-임계치 매핑 관리)

### 통합 서비스

- **30005**: integrated-swagger-service (통합 API 문서 및 프록시)

## 🎯 주요 기능

### 데이터 관리

- **임계치 관리**: 센서별 임계치 설정 및 조회
- **위치 정보**: 센서 위치 계층 구조 관리 (공장 > 건물 > 층 > 구역)
- **실시간 모니터링**: 현재 센서 데이터 조회
- **기간별 집계**: 시간대별 데이터 집계 및 분석

### 알람 시스템

- **알람 생성**: 임계치 초과 시 자동 알람 생성
- **구독 관리**: 위치/센서/임계치 타입별 알람 구독 설정
- **알림 발송**: 구독자별 알림 자동 생성 및 발송 관리
- **계층적 매칭**: factory > building > floor > area 계층 구조 기반 구독 매칭

### 매핑 관리

- **센서-임계치 매핑**: 센서별 적용 임계치 설정
- **유효 기간 관리**: 매핑의 유효 시작/종료 시간 설정
- **활성화 제어**: 매핑 활성화/비활성화 관리

### 통합 API

- **통합 문서**: 모든 서비스의 Swagger UI 통합 제공
- **API 프록시**: 단일 엔드포인트를 통한 모든 서비스 접근
- **서비스 디스커버리**: Kubernetes 기반 자동 서비스 발견

## 🛠️ 기술 스택

- 🐍 **Backend**: Python/FastAPI
- 🐳 **Container**: Docker
- ☸️ **Orchestration**: Kubernetes (Kind)
- 📊 **Monitoring**: Kubernetes Dashboard, Prometheus
- 🗄️ **Database**: PostgreSQL

## 🧭 개발 환경

- **K8s Cluster**: Kind (flet-cluster)
- **Dashboard**: https://<K8S_INGRESS>:8083/
- **namespace**: flet-montrg

## 🚀 배포 방법

### 개별 서비스 배포

각 서비스 디렉토리의 `deploy.sh` 스크립트를 사용하여 배포할 수 있습니다:

```bash
# 데이터 서비스
cd k8s/thresholds && ./deploy.sh
cd k8s/location && ./deploy.sh
cd k8s/realtime && ./deploy.sh
cd k8s/aggregation && ./deploy.sh

# 알람 서비스
cd k8s/alert && ./deploy.sh
cd k8s/alert-subscription && ./deploy.sh
cd k8s/alert-notification && ./deploy.sh

# 매핑 서비스
cd k8s/sensor-threshold-mapping && ./deploy.sh

# 통합 서비스
cd k8s/integrated-swagger && ./deploy.sh
```

### 통합 API 문서

모든 서비스의 API는 통합 Swagger UI를 통해 확인할 수 있습니다:

- **Swagger UI**: http://<K8S_INGRESS>:30005/
- **프록시 API**: http://<K8S_INGRESS>:30005/api/{resource}/

예시:

- `/api/thresholds/` → thresholds-service
- `/api/location/` → location-service
- `/api/alerts/` → alert-service
- `/api/subscriptions/` → alert-subscription-service
- `/api/notifications/` → alert-notification-service
- `/api/mappings/` → sensor-threshold-mapping-service
