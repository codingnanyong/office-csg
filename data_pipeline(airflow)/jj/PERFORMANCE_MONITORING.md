# Airflow 성능 모니터링 가이드

## 📊 모니터링 방법

### 1. 자동 모니터링 스크립트

```bash
cd /home/user/apps/airflow
./scripts/monitor_performance.sh
```

이 스크립트는 다음 정보를 제공합니다:
- 컨테이너 리소스 사용량 (CPU, 메모리)
- Worker 상태 및 개수
- Flower Worker 모니터링
- 데이터베이스 연결 상태
- Redis 메모리 사용량
- Airflow 설정
- 최근 DAG 실행 상태
- 호스트 시스템 리소스

### 2. Flower UI (Celery Worker 모니터링)

**접속**: http://localhost:5555

**주요 기능**:
- Worker 상태 및 개수 확인
- 실행 중인 태스크 모니터링
- Worker별 CPU/메모리 사용량
- 태스크 실행 시간 및 통계
- 큐 상태 확인

**API 엔드포인트**:
```bash
# Worker 목록
curl http://localhost:5555/api/workers

# 실행 중인 태스크
curl http://localhost:5555/api/tasks?state=STARTED

# Worker 통계
curl http://localhost:5555/api/workers/stats
```

### 3. Airflow UI

**접속**: http://localhost:8080

**주요 기능**:
- DAG 실행 상태 및 통계
- 태스크 실행 시간 분석
- DAG 실행 히스토리
- 태스크 로그 확인
- 성능 메트릭 (DAG별 실행 시간)

**성능 확인 방법**:
1. **DAGs** 탭 → DAG 선택
2. **Graph View** → 태스크별 실행 시간 확인
3. **Gantt Chart** → 타임라인 분석
4. **Task Duration** → 태스크별 평균 실행 시간

### 4. Docker Stats (실시간 리소스 모니터링)

```bash
# 모든 Airflow 컨테이너 모니터링
docker stats $(docker-compose ps -q)

# 특정 컨테이너만 모니터링
docker stats airflow-scheduler airflow-webserver

# Worker만 모니터링
docker stats $(docker-compose ps -q airflow-worker)
```

### 5. 데이터베이스 연결 모니터링

```bash
# PostgreSQL 연결 상태
docker exec airflow-postgres psql -U airflow -d airflow -c "
SELECT 
    count(*) as total_connections,
    count(*) FILTER (WHERE state = 'active') as active,
    count(*) FILTER (WHERE state = 'idle') as idle
FROM pg_stat_activity 
WHERE datname='airflow';
"

# 연결 상세 정보
docker exec airflow-postgres psql -U airflow -d airflow -c "
SELECT 
    pid,
    usename,
    application_name,
    state,
    query_start,
    state_change
FROM pg_stat_activity 
WHERE datname='airflow'
ORDER BY query_start DESC;
"
```

### 6. Redis 모니터링

```bash
# Redis 메모리 사용량
docker exec redis redis-cli INFO memory

# Redis 통계
docker exec redis redis-cli INFO stats

# 연결된 클라이언트 수
docker exec redis redis-cli INFO clients

# 큐 크기 확인
docker exec redis redis-cli LLEN celery
```

### 7. Airflow CLI 명령어

```bash
# DAG 실행 상태 확인
docker exec airflow-webserver airflow dags list-runs --state running

# 태스크 실행 통계
docker exec airflow-webserver airflow tasks list <dag_id>

# Worker 상태 확인
docker exec airflow-worker-1 celery --app airflow.executors.celery_executor.app inspect active

# Worker ping 테스트
docker exec airflow-worker-1 celery --app airflow.executors.celery_executor.app inspect ping
```

### 8. 로그 분석

```bash
# Scheduler 로그
docker-compose logs -f airflow-scheduler

# Worker 로그
docker-compose logs -f airflow-worker

# 특정 Worker 로그
docker-compose logs -f airflow-airflow-worker-1

# 최근 에러 확인
docker-compose logs --tail=100 airflow-scheduler | grep -i error
```

## 📈 주요 성능 지표

### 1. 처리량 (Throughput)
- **측정**: 동시 실행 중인 태스크 수
- **확인 방법**: Flower UI 또는 `monitor_performance.sh`
- **목표**: Parallelism 설정값에 근접

### 2. 응답 시간 (Latency)
- **측정**: 태스크 실행 시간
- **확인 방법**: Airflow UI → Task Duration
- **목표**: DAG별 평균 실행 시간 모니터링

### 3. 리소스 사용률
- **CPU**: Worker별 CPU 사용률
- **메모리**: 컨테이너별 메모리 사용량
- **확인 방법**: `docker stats` 또는 `monitor_performance.sh`

### 4. 데이터베이스 연결
- **측정**: 활성/유휴 연결 수
- **확인 방법**: PostgreSQL 쿼리 또는 `monitor_performance.sh`
- **목표**: Pool Size + Max Overflow 이내

### 5. Worker 상태
- **측정**: Healthy/Unhealthy Worker 비율
- **확인 방법**: `docker-compose ps` 또는 Flower UI
- **목표**: 모든 Worker가 Healthy 상태

## 🔍 성능 문제 진단

### 문제 1: 태스크가 큐에 대기 중
**증상**: 태스크가 `queued` 상태로 오래 대기
**원인**:
- Parallelism 제한
- Worker 부족
- Worker가 과부하 상태

**해결**:
```bash
# 현재 설정 확인
./scripts/check_config.sh

# Worker 상태 확인
./scripts/monitor_performance.sh
```

### 문제 2: 데이터베이스 연결 부족
**증상**: `sqlalchemy.exc.TimeoutError` 또는 연결 타임아웃
**원인**:
- Pool Size 부족
- 연결이 해제되지 않음

**해결**:
```bash
# 연결 상태 확인
docker exec airflow-postgres psql -U airflow -d airflow -c "
SELECT count(*) FROM pg_stat_activity WHERE datname='airflow';
"

# Pool Size 증가 (필요 시)
# .env 파일에서 AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE 증가
```

### 문제 3: Worker 메모리 부족
**증상**: Worker가 자주 재시작되거나 OOM 에러
**원인**:
- Worker당 메모리 부족
- 태스크가 너무 많은 메모리 사용

**해결**:
```bash
# Worker 메모리 사용량 확인
docker stats $(docker-compose ps -q airflow-worker)

# Worker 수 조정 또는 메모리 제한 설정
```

## 📝 정기 모니터링 체크리스트

### 일일 확인
- [ ] Worker 상태 (Healthy/Unhealthy)
- [ ] 실행 중인 태스크 수
- [ ] 데이터베이스 연결 수
- [ ] 최근 실패한 DAG 확인

### 주간 확인
- [ ] 태스크 실행 시간 트렌드
- [ ] 리소스 사용률 트렌드
- [ ] Worker별 성능 비교
- [ ] 데이터베이스 연결 풀 사용률

### 월간 확인
- [ ] 전체 시스템 성능 리뷰
- [ ] 설정 최적화 검토
- [ ] Worker 스케일링 필요성 검토
- [ ] 데이터베이스 연결 풀 크기 조정

## 🛠️ 고급 모니터링 (선택사항)

### StatsD + Prometheus + Grafana

현재 `statsd-exporter`가 설정되어 있습니다. Prometheus와 Grafana를 추가하면 더 상세한 메트릭을 수집할 수 있습니다.

**설정 방법**:
1. Prometheus 설정 파일 생성
2. Grafana 대시보드 설정
3. Airflow StatsD 메트릭 수집

**참고**: 현재는 기본 모니터링 도구(Flower, Airflow UI)로 충분합니다.

## 📚 참고 자료

- [Airflow Monitoring](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/index.html)
- [Flower Documentation](https://flower.readthedocs.io/)
- [Celery Monitoring](https://docs.celeryq.dev/en/stable/userguide/monitoring.html)

