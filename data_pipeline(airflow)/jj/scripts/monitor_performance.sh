#!/bin/bash
# Airflow 성능 모니터링 스크립트

cd /home/user/apps/airflow

echo "=========================================="
echo "📊 Airflow 성능 모니터링"
echo "=========================================="
echo ""

# 1. 컨테이너 리소스 사용량
echo "🖥️  컨테이너 리소스 사용량:"
echo "----------------------------------------"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" \
  $(docker-compose ps -q airflow-scheduler airflow-webserver) \
  $(docker-compose ps -q airflow-worker | head -5) 2>/dev/null | head -10
echo ""

# 2. Worker 상태 및 개수
echo "👷 Worker 상태:"
echo "----------------------------------------"
WORKER_COUNT=$(docker-compose ps airflow-worker | grep -c "airflow-worker" || echo "0")
HEALTHY_WORKERS=$(docker-compose ps airflow-worker | grep -c "healthy" || echo "0")
UNHEALTHY_WORKERS=$((WORKER_COUNT - HEALTHY_WORKERS))
echo "  - 총 Worker 개수: ${WORKER_COUNT}개"
echo "  - Healthy: ${HEALTHY_WORKERS}개"
echo "  - Unhealthy: ${UNHEALTHY_WORKERS}개"
echo ""

# 3. Flower에서 Worker 상태 확인
if docker ps --format '{{.Names}}' | grep -q "^airflow-flower$"; then
    echo "🌸 Flower Worker 모니터링:"
    echo "----------------------------------------"
    FLOWER_WORKER_COUNT=$(curl -s http://localhost:5555/api/workers 2>/dev/null | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "0")
    echo "  - Flower에서 인식된 Worker: ${FLOWER_WORKER_COUNT}개"
    
    # 활성 태스크 확인
    ACTIVE_TASKS=$(curl -s http://localhost:5555/api/tasks?state=STARTED 2>/dev/null | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "0")
    echo "  - 현재 실행 중인 태스크: ${ACTIVE_TASKS}개"
    echo ""
fi

# 4. 데이터베이스 연결 상태
echo "🗄️  데이터베이스 연결 상태:"
echo "----------------------------------------"
PG_CONNECTIONS=$(docker exec airflow-postgres psql -U airflow -d airflow -t -c "SELECT count(*) FROM pg_stat_activity WHERE datname='airflow';" 2>/dev/null | tr -d ' ' || echo "N/A")
PG_ACTIVE=$(docker exec airflow-postgres psql -U airflow -d airflow -t -c "SELECT count(*) FROM pg_stat_activity WHERE datname='airflow' AND state='active';" 2>/dev/null | tr -d ' ' || echo "N/A")
PG_IDLE=$(docker exec airflow-postgres psql -U airflow -d airflow -t -c "SELECT count(*) FROM pg_stat_activity WHERE datname='airflow' AND state='idle';" 2>/dev/null | tr -d ' ' || echo "N/A")
echo "  - PostgreSQL 총 연결: ${PG_CONNECTIONS}개"
echo "  - 활성 연결: ${PG_ACTIVE}개"
echo "  - 유휴 연결: ${PG_IDLE}개"
echo ""

# 5. Redis 메모리 사용량
echo "📦 Redis 메모리 사용량:"
echo "----------------------------------------"
REDIS_MEMORY=$(docker exec redis redis-cli INFO memory 2>/dev/null | grep "used_memory_human" | cut -d: -f2 | tr -d '\r' || echo "N/A")
REDIS_CONNECTIONS=$(docker exec redis redis-cli INFO stats 2>/dev/null | grep "total_connections_received" | cut -d: -f2 | tr -d '\r' || echo "N/A")
echo "  - 사용 메모리: ${REDIS_MEMORY}"
echo "  - 총 연결 수: ${REDIS_CONNECTIONS}"
echo ""

# 6. Airflow 설정 확인
echo "⚙️  Airflow 설정:"
echo "----------------------------------------"
PARALLELISM=$(docker exec airflow-webserver airflow config get-value core parallelism 2>/dev/null | grep -v "FutureWarning\|RemovedInAirflow3Warning" | tail -1)
POOL_SIZE=$(docker exec airflow-webserver airflow config get-value database sql_alchemy_pool_size 2>/dev/null | grep -v "FutureWarning\|RemovedInAirflow3Warning" | tail -1)
MAX_ACTIVE_TASKS=$(docker exec airflow-webserver airflow config get-value core max_active_tasks_per_dag 2>/dev/null | grep -v "FutureWarning\|RemovedInAirflow3Warning" | tail -1)
echo "  - Parallelism: ${PARALLELISM}"
echo "  - DB Pool Size: ${POOL_SIZE}"
echo "  - Max Active Tasks per DAG: ${MAX_ACTIVE_TASKS}"
echo ""

# 7. 최근 DAG 실행 상태
echo "📈 최근 DAG 실행 상태 (최근 10개):"
echo "----------------------------------------"
docker exec airflow-webserver airflow dags list-runs --state running --limit 10 2>/dev/null | head -12 || echo "  실행 중인 DAG 없음"
echo ""

# 8. 시스템 리소스 (호스트)
echo "💻 호스트 시스템 리소스:"
echo "----------------------------------------"
echo "  - CPU 사용률:"
if command -v bc >/dev/null 2>&1; then
    CPU_IDLE=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/")
    CPU_USAGE=$(echo "100 - $CPU_IDLE" | bc)
    echo "    사용 중: ${CPU_USAGE}%"
else
    top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{printf "    사용 중: %.1f%%\n", 100 - $1}'
fi
echo "  - 메모리 사용량:"
MEM_INFO=$(free -h | grep "Mem:")
MEM_TOTAL=$(echo "$MEM_INFO" | awk '{print $2}')
MEM_USED=$(echo "$MEM_INFO" | awk '{print $3}')
MEM_FREE=$(echo "$MEM_INFO" | awk '{print $4}')
MEM_AVAILABLE=$(echo "$MEM_INFO" | awk '{print $7}')
MEM_BUFF_CACHE=$(echo "$MEM_INFO" | awk '{print $6}')
echo "    총: ${MEM_TOTAL}"
echo "    사용: ${MEM_USED}"
echo "    여유: ${MEM_FREE}"
echo "    buff/cache: ${MEM_BUFF_CACHE}"
echo "    available: ${MEM_AVAILABLE}"
echo "  - 디스크 사용량:"
df -h / | tail -1 | awk '{print "    사용: " $3 " / " $2 " (" $5 ")"}'
echo ""

echo "=========================================="
echo "💡 추가 모니터링 도구:"
echo "  - Airflow UI: http://localhost:8080"
echo "  - Flower UI: http://localhost:5555"
echo "  - 실시간 모니터링: docker stats"
echo "=========================================="

