# Edge HMI Database

TimescaleDB 기반의 산업용 모니터링 및 유지보수 시스템 데이터베이스

## 데이터 보호

### 현재 설정 분석

✅ **안전한 설정:**
- Named Volume 사용 (`edge_hmi_data`): 컨테이너가 삭제되어도 데이터는 유지됨
- `init-db.sql`은 첫 초기화 시에만 실행됨 (데이터베이스가 이미 있으면 실행 안 됨)

⚠️ **데이터 소실 위험:**
- `docker compose down -v` 사용 시 volume이 삭제되어 데이터 소실
- Docker volume 삭제 명령어 (`docker volume rm`) 사용 시 데이터 소실

### 안전한 명령어

```bash
# db/에서 실행. 컨테이너만 중지/삭제 (데이터 유지)
docker compose down

# 컨테이너와 네트워크만 삭제 (데이터 유지)
docker compose down --remove-orphans

# ❌ 위험: Volume까지 삭제 (데이터 소실!)
docker compose down -v
```

## 사용 방법

### 1. 환경 설정

`db/` 디렉터리에 `.env` 파일 생성. **DB명·유저 등은 여기서 정한 값으로 생성됩니다.** 스키마명은 `init-db.sql`에 정의되어 있으며, `.env`의 `POSTGRES_SCHEMA`와 동일하게 맞출 것.

```bash
cd db
cat > .env << 'EOF'
POSTGRES_DB=edge_hmi
POSTGRES_USER=admin
POSTGRES_PASSWORD=1q2w3e4r
TZ=UTC
EOF
```

- `POSTGRES_DB`: 생성할 데이터베이스 이름
- `POSTGRES_USER` / `POSTGRES_PASSWORD`: DB 접속 계정
- `TZ`: 타임존 (예: `UTC`, `Asia/Seoul`)

### 2. 실행

```bash
# db/ 디렉터리에서 실행
cd db
docker compose up -d

# 로그 확인
docker compose logs -f edge-hmi-db
```

### 3. 데이터베이스 연결

`.env`의 `POSTGRES_USER`, `POSTGRES_DB`를 사용합니다.

```bash
# 컨테이너 내부에서 접속 (admin, edge_hmi는 .env 예시)
docker exec -it hmi-db-postgres psql -U admin -d edge_hmi

# 외부에서 접속
psql -h localhost -p 5432 -U admin -d edge_hmi
```

**스키마:**

- DB명 = `.env`의 `POSTGRES_DB`. 스키마명 = **`core`** (표준). `.env` `POSTGRES_SCHEMA=core`와 맞출 것.
- 테이블은 `core` 스키마에 생성되며, `search_path`로 스키마명 생략 가능.
- 명시적: `SELECT * FROM core.line_mst;` / 간편: `SELECT * FROM line_mst;`

## Registry 이미지에서 init-db.sql 확인·추출

이미지만 pull한 다른 서버에서 `init-db.sql` 내용을 보거나 파일로 뽑을 수 있다.

**이미지 내 경로** (실행 순서 01 → 02; pg_cron 설정은 Dockerfile에서)

- `init-db.sql` → `/docker-entrypoint-initdb.d/01-init-db.sql`
- `kpi-scheduler.sql` → `/docker-entrypoint-initdb.d/02-kpi-scheduler.sql`
- `docker-compose.yml` (참고용) → `/opt/edge-hmi-db/docker-compose.yml`

**1. 터미널에서 바로 보기**

```bash
export IMG="{REGISTRY_HOST}:5000/btx/edge-hmi-db:latest"
docker run --rm ${IMG} cat /docker-entrypoint-initdb.d/01-init-db.sql
```

**2. 현재 디렉터리에 파일로 추출**

```bash
docker run --rm {REGISTRY_HOST}:5000/btx/edge-hmi-db:latest cat /docker-entrypoint-initdb.d/01-init-db.sql > init-db.sql
# db 프로젝트에서: … > sql/init-db.sql
```

## KPI 요약 스케줄러 (`kpi_sum`)

`kpi-scheduler.sql`이 `init-db.sql` 직후에 실행되며, `fn_kpi_sum_calc(p_calc_date DATE)` 함수를 생성한다.

### 함수 설명

해당 일자의 `shift_map` 기준 (shift/line/equip)별로 `status_his`, `prod_his`, `alarm_his`, `maint_his`, `kpi_cfg`를 이용해 다음을 계산해 `kpi_sum`에 저장:

- **Availability** = Run시간 / 계획시간
- **Performance** = (생산량 × 표준사이클) / Run시간
- **Quality** = 양품수 / 전체수
- **OEE** = Availability × Performance × Quality
- **MTTR** = 평균 수리시간 (분)
- **MTBF** = Run시간 / 고장횟수 (시간)

### 스케줄링 방법

**기본: pg_cron 사용** (이미지에 포함됨)

이 이미지는 pg_cron이 포함되어 있어, 자동으로 매일 01:00에 전일 KPI를 계산합니다.

**첫 실행 후 재시작 필요:**

```bash
# 첫 빌드/실행 후, pg_cron 설정 적용을 위해 컨테이너 재시작
docker-compose restart
```

**pg_cron 상태 확인:**

```bash
# 등록된 스케줄 목록 (jobid, schedule, command 등)
docker exec hmi-db-postgres psql -U admin -d edge_hmi -c "SELECT jobid, schedule, command FROM cron.job;"
```

**스케줄이 실제로 돌았는지 확인:**

pg_cron 1.6에는 `cron.job_run_details` **가 없습니다**. 실행 이력 테이블은 최신/클라우드용 variant에만 있음.

**KPI job (`fn_kpi_sum_calc`) 확인:** `core.kpi_sum` 에 해당 `calc_date` 행이 생겼는지로 판단:

```sql
-- 전일(어제) KPI가 계산됐는지
SELECT calc_date, COUNT(*) FROM core.kpi_sum WHERE calc_date = CURRENT_DATE - 1 GROUP BY 1;
```

행이 있으면 해당 일자 스케줄 실행된 것. 없으면 미실행이거나 `shift_map` 등 원본 데이터 없음.

**수동 실행:**

```bash
# 특정 일자 계산
docker exec hmi-db-postgres psql -U admin -d edge_hmi -c "SELECT fn_kpi_sum_calc('2025-01-25');"

# 전일 계산
docker exec hmi-db-postgres psql -U admin -d edge_hmi -c "SELECT fn_kpi_sum_calc(CURRENT_DATE - 1);"
```

**대안: 호스트 cron 사용** (pg_cron 없이)

pg_cron이 동작하지 않는 경우, 호스트 cron 사용:

```bash
# 호스트 crontab 편집
crontab -e

# 다음 줄 추가 (매일 01:00에 전일 KPI 계산)
0 1 * * * docker exec hmi-db-postgres psql -U admin -d edge_hmi -c "SELECT fn_kpi_sum_calc(CURRENT_DATE - 1);"
```

## 데이터 백업 및 복원

### 백업

```bash
# 데이터베이스 백업
docker exec hmi-db-postgres pg_dump -U admin edge_hmi > backup_$(date +%Y%m%d_%H%M%S).sql

# 또는 Volume 백업
docker run --rm -v edge-hmi-db_edge_hmi_data:/data -v $(pwd):/backup \
  alpine tar czf /backup/volume_backup_$(date +%Y%m%d_%H%M%S).tar.gz -C /data .
```

### 복원

```bash
# SQL 백업 파일로 복원
docker exec -i hmi-db-postgres psql -U admin -d edge_hmi < backup_20240116_120000.sql

# Volume 백업으로 복원 (주의: 기존 데이터 삭제됨). db/에서 실행
docker compose down
docker volume rm edge-hmi-db_edge_hmi_data
docker run --rm -v edge-hmi-db_edge_hmi_data:/data -v $(pwd):/backup \
  alpine tar xzf /backup/volume_backup_20240116_120000.tar.gz -C /data
docker compose up -d
```

## 주의사항

1. **절대 사용하지 말아야 할 명령어:**

   ```bash
   docker compose down -v  # Volume 삭제 → 데이터 소실!
   docker volume rm edge-hmi-db_edge_hmi_data  # Volume 삭제 → 데이터 소실!
   ```

2. **데이터가 있는 상태에서 init-db.sql 변경:**
   - `init-db.sql`은 첫 실행 시에만 적용됨
   - 스키마 변경이 필요하면 마이그레이션 스크립트를 별도로 작성해야 함

3. **포트 충돌:**
   - 로컬에 PostgreSQL이 이미 실행 중이면 포트 5432 충돌 가능
   - docker-compose.yml에서 포트 변경 가능: `"5433:5432"`

## Volume 확인

```bash
# Volume 목록 확인
docker volume ls | grep edge_hmi

# Volume 상세 정보
docker volume inspect edge-hmi-db_edge_hmi_data

# Volume 사용량 확인 (대략적)
docker system df -v
```
