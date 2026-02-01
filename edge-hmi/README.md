# Edge HMI

Edge HMI 모니터링·유지보수 시스템. DB + API 구성.

## 구조

```bash
edge-hmi/
├── README.md         # 이 파일
├── docker-compose.yml # DB + 테이블별 API + hmi-api 게이트웨이 (컨테이너 간 연관)
├── db/               # TimescaleDB (스키마, KPI 스케줄러)
│   └── README.md
└── api/              # FastAPI + SQLAlchemy
    ├── shared/       # config, DB, models
    ├── line_mst/     # 테이블별 API (각각 컨테이너)
    ├── equip_mst/
    ├── sensor_mst/
    ├── worker_mst/
    ├── shift_cfg/
    ├── kpi_cfg/
    ├── alarm_cfg/
    ├── maint_cfg/
    ├── measurement/
    ├── status_his/
    ├── prod_his/
    ├── alarm_his/
    ├── maint_his/
    ├── shift_map/
    ├── kpi_sum/
    ├── hmi_api/      # 게이트웨이: 테이블 API 컨테이너로 프록시
    └── README.md
```

## 빠른 실행

**DB만**

```bash
cd db
# .env 생성 (POSTGRES_*, POSTGRES_SCHEMA=core, TZ). db/README.md 참고
docker compose up -d
```

**DB + 테이블별 API + hmi-api 게이트웨이** (프로젝트 루트)

```bash
# db/.env 필요
docker compose up -d --build
```

- DB: 5432, hmi-api(통합): 8000, line_mst: 8001 … kpi_sum: 8004, worker_mst: 8005 … shift_map: 8015 (api/README.md 참고)

**API 상세**
→ `api/README.md` 참고.

## Git 저장소

- **원격**: `http://<GITEA_HOST>:3000/<namespace>/edge-hmi.git`
- **기본 브랜치**: `main`

**브랜치 작업 흐름** (기능/수정 시):
```bash
git checkout main && git pull
git checkout -b feature/이슈명   # 예: feature/api-auth, fix/db-init
# 작업 후
git add -A && git commit -m "메시지"
git push -u origin feature/이슈명
# 원격에서 MR/PR 생성 → main 머지
```

---

- DB 상세: **db/README.md**
- API 상세: **api/README.md**
