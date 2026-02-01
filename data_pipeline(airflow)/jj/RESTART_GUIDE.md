# Airflow 설정 변경 후 안전한 재시작 가이드

## 📋 변경 사항

다음 설정을 `airflow.cfg`에서 수정해야 합니다:

```ini
# 1. parallelism 증가
parallelism = 80  # 32 → 80

# 2. DB 연결 풀 증가
sql_alchemy_pool_size = 20  # 5 → 20
sql_alchemy_max_overflow = 20  # 10 → 20
```

## ⚠️ 중요 사항

### 재시작이 필요한 이유

**`parallelism`과 `sql_alchemy_pool_size` 같은 설정은 시작 시에만 로드됩니다.**

1. **`parallelism`**: 
   - Scheduler 시작 시 메모리에 로드
   - 런타임에 동적으로 변경 불가
   - **재시작 필수**

2. **`sql_alchemy_pool_size`**:
   - 연결 풀은 시작 시 생성
   - 런타임에 풀 크기 변경 불가
   - **재시작 필수**

3. **설정 로딩 순서**:
   - 환경 변수 (AIRFLOW__SECTION__KEY) → airflow.cfg 파일
   - 둘 다 시작 시에만 로드됨

### 재시작 없이 적용 가능한 설정

일부 설정은 런타임에 다시 로드될 수 있지만, **대부분의 핵심 설정은 재시작이 필요합니다.**

- ✅ 재시작 없이 적용: 일부 로깅 레벨, 일부 UI 설정 (매우 제한적)
- ❌ 재시작 필요: parallelism, pool_size, executor 설정, 대부분의 핵심 설정

### 대안: 환경 변수 사용

환경 변수를 사용해도 **컨테이너 재시작은 필요**하지만, 파일 수정 없이 설정 변경 가능:

```yaml
# docker-compose.yml에 추가
environment:
  AIRFLOW__CORE__PARALLELISM: "80"
  AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE: "20"
  AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW: "20"
```

**결론: 파일 수정이든 환경 변수든, 재시작은 필수입니다.**

## 🔄 재시작 방법 (3가지 옵션)

### 방법 1: 안전한 단계적 재시작 (권장) ⭐

**현재 실행 중인 태스크가 있을 때 권장**

```bash
cd /home/user/apps/airflow

# 1단계: airflow.cfg 파일 수정
# (파일 수정은 이미 완료했다고 가정)

# 2단계: Worker만 재시작 (실행 중인 태스크는 다른 Worker로 재할당됨)
docker-compose restart airflow-worker

# 3단계: 잠시 대기 (Worker가 정상 동작하는지 확인)
sleep 10

# 4단계: Scheduler 재시작 (실행 중인 태스크는 계속 실행됨)
docker-compose restart airflow-scheduler

# 5단계: Webserver 재시작 (UI만 영향, 실행 중인 태스크는 영향 없음)
docker-compose restart airflow-webserver
```

**장점:**
- 실행 중인 태스크가 실패할 가능성 최소화
- Worker는 Celery이므로 태스크가 다른 Worker로 재할당됨
- 단계적으로 재시작하여 문제 발생 시 즉시 중단 가능

### 방법 2: 태스크 완료 대기 후 재시작 (가장 안전)

**현재 실행 중인 태스크가 중요하거나 오래 걸릴 때 권장**

```bash
cd /home/user/apps/airflow

# 1단계: 실행 중인 태스크 확인 (Airflow UI에서 확인)
# http://localhost:8080 → DAGs → Running 상태 확인

# 2단계: 실행 중인 태스크가 모두 완료될 때까지 대기
# (Airflow UI에서 모니터링)

# 3단계: 모든 태스크 완료 후 재시작
docker-compose restart airflow-scheduler airflow-webserver airflow-worker
```

**장점:**
- 실행 중인 태스크가 실패할 가능성 없음
- 가장 안전한 방법

**단점:**
- 태스크 완료까지 대기 시간 필요

### 방법 3: 즉시 재시작 (빠르지만 위험)

**실행 중인 태스크가 없거나 실패해도 괜찮을 때만 사용**

```bash
cd /home/user/apps/airflow

# 모든 Airflow 서비스 재시작
docker-compose restart airflow-scheduler airflow-webserver airflow-worker
```

**주의:**
- 실행 중인 태스크가 실패할 수 있음
- Worker 재시작 시 Celery가 태스크를 재할당하지만, 일부 태스크는 실패할 수 있음

## 📝 단계별 실행 스크립트

### 안전한 재시작 스크립트

```bash
#!/bin/bash
# safe-restart-airflow.sh

set -e

cd /home/user/apps/airflow

echo "🔄 Airflow 안전한 재시작 시작..."

# 1. 현재 실행 중인 태스크 확인
echo "📊 실행 중인 태스크 확인 중..."
RUNNING_TASKS=$(docker exec airflow-webserver airflow dags list-runs --state running -o json 2>/dev/null | python3 -c "import sys, json; data=json.load(sys.stdin) if sys.stdin.read(1) else []; print(len(data))" 2>/dev/null || echo "0")

if [ "$RUNNING_TASKS" -gt 0 ]; then
    echo "⚠️  실행 중인 태스크가 $RUNNING_TASKS개 있습니다."
    echo "   단계적 재시작을 진행합니다..."
    
    # Worker 재시작
    echo "1️⃣  Worker 재시작 중..."
    docker-compose restart airflow-worker
    sleep 10
    
    # Scheduler 재시작
    echo "2️⃣  Scheduler 재시작 중..."
    docker-compose restart airflow-scheduler
    sleep 5
    
    # Webserver 재시작
    echo "3️⃣  Webserver 재시작 중..."
    docker-compose restart airflow-webserver
    
else
    echo "✅ 실행 중인 태스크가 없습니다. 전체 재시작을 진행합니다..."
    docker-compose restart airflow-scheduler airflow-webserver airflow-worker
fi

echo "✅ 재시작 완료!"
echo "📊 상태 확인: docker-compose ps"
```

## ✅ 재시작 후 확인 사항

```bash
# 1. 컨테이너 상태 확인
docker-compose ps

# 2. 설정 확인 (parallelism)
docker exec airflow-webserver airflow config get-value core parallelism

# 3. DB 연결 풀 확인
docker exec airflow-webserver airflow config get-value database sql_alchemy_pool_size

# 4. Scheduler 상태 확인
docker exec airflow-scheduler airflow jobs check --job-type SchedulerJob

# 5. Worker 상태 확인
docker exec airflow-worker-1 celery --app airflow.executors.celery_executor.app inspect ping
```

## 🎯 권장 절차

1. **airflow.cfg 파일 수정**
   ```bash
   # parallelism = 80
   # sql_alchemy_pool_size = 20
   # sql_alchemy_max_overflow = 20
   ```

2. **현재 실행 중인 태스크 확인**
   - Airflow UI: http://localhost:8080
   - 또는: `docker exec airflow-webserver airflow dags list-runs --state running`

3. **재시작 방법 선택**
   - 실행 중인 태스크 있음 → **방법 1 (단계적 재시작)** 또는 **방법 2 (대기 후 재시작)**
   - 실행 중인 태스크 없음 → **방법 3 (즉시 재시작)**

4. **재시작 후 확인**
   - 설정이 올바르게 적용되었는지 확인
   - 태스크가 정상적으로 실행되는지 확인

## ⚠️ 주의사항

1. **PostgreSQL과 Redis는 재시작하지 않아도 됨**
   - 이들은 설정 변경과 무관
   - 재시작 시 연결이 끊길 수 있음

2. **Worker 재시작 시**
   - Celery가 실행 중인 태스크를 다른 Worker로 재할당 시도
   - 일부 태스크는 실패할 수 있음 (retry 설정이 있으면 자동 재시도)

3. **Scheduler 재시작 시**
   - 실행 중인 태스크는 계속 실행됨
   - 새로운 태스크 스케줄링만 일시 중단됨

4. **Webserver 재시작 시**
   - UI 접근만 일시 중단됨
   - 실행 중인 태스크에는 영향 없음

## 🔍 문제 발생 시

재시작 후 문제가 발생하면:

```bash
# 로그 확인
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-worker

# 이전 설정으로 롤백
# airflow.cfg 파일을 이전 값으로 복원 후 재시작
```

