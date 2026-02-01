# Edge HMI API

FastAPI + SQLAlchemy. 테이블별 단일 API(각각 컨테이너) + **hmi-api 게이트웨이**가 이 컨테이너들로 **프록시** (컨테이너 간 연관 관계).

## 구조

```bash

api/
├── shared/           # config, DB, models (core.*)
├── line_mst/         # 테이블 전용 FastAPI, Dockerfile → 별도 컨테이너
├── equip_mst/
├── … (이하 동일)
├── kpi_sum/
├── hmi_api/          # 게이트웨이: DB 없음. /line_mst, /equip_mst 등 → 해당 테이블 컨테이너로 프록시
└── README.md
```

- **테이블별**: 각 테이블마다 FastAPI 1개, DB 연결. 각각 Docker 이미지 → 각각 컨테이너 (line_mst, equip_mst, …).
- **hmi-api**: 게이트웨이 전용 이미지/컨테이너. **테이블 API 컨테이너에 의존**하며, `/line_mst`, `/equip_mst` 등 요청을 각 컨테이너로 **프록시**. **Felt Montrg 스타일** 통합 Swagger UI(/, /swagger)·aggregated OpenAPI(/openapi.json)·/info·/docs 제공. **Docker/docker-compose** 전제 (K8s 미사용).

## Docker Compose (프로젝트 루트)

```bash
cd ~/proj/edge-hmi
docker compose up -d --build
```

| 서비스     | 포트  |
|------------|-------|
| db         | 5432  |
| **hmi-api**| **8000** |
| line_mst   | 8001  |
| equip_mst  | 8002  |
| sensor_mst | 8003  |
| kpi_sum    | 8004  |
| worker_mst | 8005  |
| shift_cfg  | 8006  |
| kpi_cfg    | 8007  |
| alarm_cfg  | 8008  |
| maint_cfg  | 8009  |
| measurement| 8010  |
| status_his | 8011  |
| prod_his   | 8012  |
| alarm_his  | 8013  |
| maint_his  | 8014  |
| shift_map  | 8015  |

- **통합 진입점**: `http://localhost:8000` → hmi-api 게이트웨이. `/`, `/swagger`(통합 Swagger UI), `/docs`→`/`, `/openapi.json`, `/info`, `/health`, `/line_mst`, `/equip_mst` 등.
- **테이블 직접 호출**: `http://localhost:8001/line_mst`, `8002/equip_mst`, … (각 컨테이너 직접).

## 로컬 실행 (DB 선 기동)

```bash
cd api
cp .env.example .env   # 필요 시 수정
pip install -r line_mst/requirements.txt   # 또는 hmi_api/requirements.txt
export PYTHONPATH="$PWD"
uvicorn line_mst.main:app --reload --port 8001
# 또는 통합: uvicorn hmi_api.main:app --reload --port 8000
```

## DB 연결

- **Host**: `localhost`(로컬) 또는 `db`(compose)
- **Port**: 5432  
- **Database**: `edge_hmi`  
- **Schema**: `core`

## Private Registry 배포 (API 이미지 v1.0 / latest)

각 API 이미지를 레지스트리에 **v1.0** + **latest** 태그로 빌드·푸시.

**전체 푸시:**

```bash
cd ~/proj/edge-hmi/api
./scripts/push-to-registry.sh [registry-url] [version]
```

**특정 서비스만 푸시:**

```bash
./scripts/push-to-registry.sh [registry-url] [version] 서비스1 [서비스2 ...]
```

- **registry-url** (예: `{REGISTRY_HOST}:5000`): Private Registry 주소
- **version** (기본 `v1.0`): 버전 태그. `latest`도 항상 동일 빌드로 갱신
- **서비스**: `line_mst`, `equip_mst`, `sensor_mst`, `kpi_sum`, `worker_mst`, `shift_cfg`, `kpi_cfg`, `alarm_cfg`, `maint_cfg`, `measurement`, `status_his`, `prod_his`, `alarm_his`, `maint_his`, `shift_map`, `hmi-api` 중 하나 이상

예:

```bash
# 전체
./scripts/push-to-registry.sh <REGISTRY_HOST>:5000 v1.0

# 게이트웨이만
./scripts/push-to-registry.sh <REGISTRY_HOST>:5000 v1.0 hmi-api

# 일부만
./scripts/push-to-registry.sh <REGISTRY_HOST>:5000 v1.0 line_mst sensor_mst hmi-api
```

직전에 `docker login <registry-url>` 필요.
