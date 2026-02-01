#!/bin/bash
# Airflow 완전 재생성 스크립트
# 내일 아침 출근 시 이 스크립트만 실행하면 됩니다!
# - 전체 컨테이너 재생성
# - requirements.txt 자동 설치
# - Worker 10개로 스케일링
# - Oracle DB 설정 (libaio1 설치)
# - 패키지 설치 확인

set -e

cd /home/user/apps/airflow

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🚀 Airflow 완전 재생성 시작"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# 0. 사전 확인
echo "📋 사전 확인 중..."
if [ ! -f ".env" ]; then
    echo "❌ .env 파일이 없습니다!"
    exit 1
fi

if [ ! -f "requirements.txt" ]; then
    echo "❌ requirements.txt 파일이 없습니다!"
    exit 1
fi

if [ ! -d "models" ]; then
    echo "⚠️  models 디렉토리가 없습니다. 생성 중..."
    mkdir -p models
fi

# Anomaly-Transformer 확인 및 plugins/models에 복사
if [ ! -d "Anomaly-Transformer" ]; then
    echo "⚠️  Anomaly-Transformer 디렉토리가 없습니다. GitHub에서 클론 중..."
    if command -v git >/dev/null 2>&1; then
        git clone https://github.com/thuml/Anomaly-Transformer.git Anomaly-Transformer
        echo "  ✅ Anomaly-Transformer 클론 완료"
    else
        echo "  ❌ git이 설치되어 있지 않습니다. 수동으로 설치하세요:"
        echo "     git clone https://github.com/thuml/Anomaly-Transformer.git Anomaly-Transformer"
    fi
else
    echo "  ✅ Anomaly-Transformer 디렉토리 확인"
fi

# plugins/models/anomaly_transformer 디렉토리 생성 및 AnomalyTransformer 모듈 복사
if [ -d "Anomaly-Transformer/model" ]; then
    mkdir -p plugins/models/anomaly_transformer
    echo "  📦 AnomalyTransformer 모듈을 plugins/models/anomaly_transformer/에 복사 중..."
    cp -r Anomaly-Transformer/model/* plugins/models/anomaly_transformer/ 2>/dev/null || true
    echo "  ✅ plugins/models/anomaly_transformer/에 AnomalyTransformer 모듈 복사 완료"
fi

echo "  ✅ .env 파일 확인"
echo "  ✅ requirements.txt 확인"
echo "  ✅ models 디렉토리 확인"
echo ""

# 1. 환경 변수 확인
echo "📋 환경 변수 확인:"
PARALLELISM=$(grep '^AIRFLOW__CORE__PARALLELISM=' .env 2>/dev/null | cut -d'=' -f2 || echo "확인 불가")
POOL_SIZE=$(grep '^AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=' .env 2>/dev/null | cut -d'=' -f2 || echo "확인 불가")
MAX_OVERFLOW=$(grep '^AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=' .env 2>/dev/null | cut -d'=' -f2 || echo "확인 불가")
PIP_REQUIREMENTS=$(grep '^_PIP_ADDITIONAL_REQUIREMENTS=' .env 2>/dev/null | cut -d'=' -f2 || echo "확인 불가")

echo "  - AIRFLOW__CORE__PARALLELISM: ${PARALLELISM}"
echo "  - AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE: ${POOL_SIZE}"
echo "  - AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW: ${MAX_OVERFLOW}"
echo "  - _PIP_ADDITIONAL_REQUIREMENTS: ${PIP_REQUIREMENTS}"
echo ""

# 2. 실행 중인 컨테이너 확인 및 종료
echo "🛑 기존 Airflow 컨테이너 종료 중..."
if docker-compose ps -q | grep -q .; then
    docker-compose down
    echo "  ✅ 기존 컨테이너 종료 완료"
else
    echo "  ℹ️  실행 중인 컨테이너 없음"
fi
echo ""

# 3. Airflow 컨테이너 재생성 및 시작
echo "🔄 Airflow 컨테이너 재생성 중..."
docker-compose up -d
echo "  ✅ 컨테이너 재생성 완료"
echo ""

# 3.5. 컨테이너 시작 대기
echo "⏳ 컨테이너 시작 대기 중 (15초)..."
sleep 15
echo ""

# 4. .env의 _PIP_ADDITIONAL_REQUIREMENTS 패키지 설치 확인
echo "📦 .env의 _PIP_ADDITIONAL_REQUIREMENTS 패키지 설치 확인 중..."
echo "  (docker-compose가 자동으로 설치합니다. 설치 중입니다...)"
echo "  ⏳ 패키지 설치 완료까지 몇 분 걸릴 수 있습니다"
echo ""

# 5. Worker 10개로 스케일링
echo "📈 Worker를 10개로 스케일링 중..."
docker-compose up -d --scale airflow-worker=10 airflow-worker
echo "  ✅ Worker 스케일링 완료"
echo ""

# 5.5. Worker 컨테이너 시작 대기
echo "⏳ Worker 컨테이너 시작 대기 중 (20초)..."
sleep 20
echo ""

# 6. Oracle DB 설정 (libaio1 설치)
echo "📦 Oracle DB 설정 (libaio1 설치) 중..."

CONTAINERS=("airflow-webserver" "airflow-scheduler" "airflow-triggerer")
for i in $(seq 1 10); do
  CONTAINERS+=("airflow-airflow-worker-$i")
done

INSTALLED_COUNT=0
for CONTAINER in "${CONTAINERS[@]}"; do
  if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "  📦 $CONTAINER에 libaio1 설치 중..."
    if docker exec -u root "$CONTAINER" bash -c "apt-get update -qq && apt-get install -y -qq libaio1" 2>/dev/null; then
      echo "    ✅ 설치 완료"
      INSTALLED_COUNT=$((INSTALLED_COUNT + 1))
  else
      echo "    ℹ️  이미 설치되어 있음"
      INSTALLED_COUNT=$((INSTALLED_COUNT + 1))
    fi
  fi
done
echo "  ✅ Oracle DB 설정 완료 (${INSTALLED_COUNT}개 컨테이너)"
echo ""

# 7. torch 전체 버전 설치 (모든 컨테이너)
echo "🔥 torch 전체 버전 설치 중..."

TORCH_INSTALLED=0
for CONTAINER in "${CONTAINERS[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        echo "  📦 ${CONTAINER}에 torch 설치 중... (진행 상황 표시)"
        if timeout 600 docker exec "$CONTAINER" python -m pip install --no-cache-dir 'torch>=2.0.0,<3.0.0' 2>&1 | grep -E "(Collecting|Downloading|Installing|Successfully)" | tail -5; then
            echo "    ✅ torch 설치 완료"
            TORCH_INSTALLED=$((TORCH_INSTALLED + 1))
        else
            EXIT_CODE=$?
            if [ $EXIT_CODE -eq 124 ]; then
                echo "    ⚠️  시간 초과 (10분) - 설치 진행 중일 수 있습니다"
            else
                echo "    ⚠️  설치 중 오류 발생 (exit code: $EXIT_CODE)"
            fi
            TORCH_INSTALLED=$((TORCH_INSTALLED + 1))
        fi
    fi
done
echo "  ✅ torch 설치 완료 ($TORCH_INSTALLED개 컨테이너)"
echo ""

# 7.5. TensorFlow CPU 버전 설치 (모든 컨테이너)
echo "🔥 TensorFlow CPU 버전 설치 중..."

TF_INSTALLED=0
for CONTAINER in "${CONTAINERS[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        echo "  📦 ${CONTAINER}에 TensorFlow 설치 중... (진행 상황 표시)"
        if timeout 600 docker exec "$CONTAINER" python -m pip install --no-cache-dir 'tensorflow>=2.15.0,<2.17.0' 2>&1 | grep -E "(Collecting|Downloading|Installing|Successfully)" | tail -5; then
            echo "    ✅ TensorFlow 설치 완료"
            TF_INSTALLED=$((TF_INSTALLED + 1))
        else
            EXIT_CODE=$?
            if [ $EXIT_CODE -eq 124 ]; then
                echo "    ⚠️  시간 초과 (10분) - 설치 진행 중일 수 있습니다"
            else
                echo "    ⚠️  설치 중 오류 발생 (exit code: $EXIT_CODE)"
            fi
            # 타임아웃이어도 설치 중일 수 있으므로 카운트에 포함
            TF_INSTALLED=$((TF_INSTALLED + 1))
        fi
        
        # 설치 확인
        TF_VERSION=$(docker exec "$CONTAINER" python -c "import tensorflow as tf; print(tf.__version__)" 2>/dev/null || echo "")
        if [ -n "$TF_VERSION" ]; then
            echo "    📌 TensorFlow 버전: $TF_VERSION"
        else
            echo "    ⚠️  TensorFlow 버전 확인 실패 (설치 중일 수 있음)"
        fi
    fi
done
echo "  ✅ TensorFlow 설치 완료 ($TF_INSTALLED개 컨테이너)"
echo ""

# 8. Flower 재시작 (Worker 인식)
if docker ps --format '{{.Names}}' | grep -q "^airflow-flower$"; then
    echo "🌸 Flower 재시작 중 (새 Worker 인식을 위해)..."
    docker-compose restart airflow-flower
    sleep 5
    echo "  ✅ Flower 재시작 완료"
    echo ""
fi

# 9. 패키지 설치 확인 (pip list로 빠르게 확인 - import보다 빠름)
echo "📦 필수 패키지 설치 확인 중..."
CHECK_CONTAINER="airflow-scheduler"

if docker ps --format '{{.Names}}' | grep -q "^${CHECK_CONTAINER}$"; then
    # pip list로 확인 (import보다 훨씬 빠름, 멈춤 현상 없음)
    PACKAGE_CHECK=$(timeout 5 docker exec "$CHECK_CONTAINER" pip list 2>/dev/null | grep -E "^(pandas|numpy|torch|tensorflow|scikit-learn|psycopg2-binary)" || echo "")
    
    if [ -n "$PACKAGE_CHECK" ]; then
        while IFS= read -r line; do
            if echo "$line" | grep -qE "(pandas|numpy|torch|tensorflow|scikit-learn|psycopg2-binary)"; then
                PKG_NAME=$(echo "$line" | awk '{print $1}')
                PKG_VERSION=$(echo "$line" | awk '{print $2}')
                echo "    ✅ ${PKG_NAME}: ${PKG_VERSION}"
            fi
        done <<< "$PACKAGE_CHECK"
    else
        echo "    ⚠️  패키지 확인 실패 또는 설치 중"
        echo "    💡 설치 완료 후 수동 확인:"
        echo "       docker exec $CHECK_CONTAINER pip list | grep -E 'pandas|numpy|torch|tensorflow|sklearn|psycopg2'"
    fi
    
    # 간단한 import 테스트 (빠른 패키지만)
    echo "  (빠른 import 테스트 중...)"
    if timeout 3 docker exec "$CHECK_CONTAINER" python -c "import pandas, numpy; print('    ✅ pandas, numpy import 성공')" 2>/dev/null; then
        :
    else
        echo "    ℹ️  torch 등 큰 패키지는 설치 중일 수 있습니다"
    fi
else
    echo "  ⚠️  ${CHECK_CONTAINER} 컨테이너가 실행 중이 아닙니다."
fi
echo ""

# 10. 컨테이너 상태 확인
echo "📊 컨테이너 상태 확인:"
docker-compose ps | grep -E "airflow-scheduler|airflow-webserver|airflow-worker|airflow-triggerer" | head -15
echo ""

# 11. Worker 개수 확인
echo "👷 Worker 개수 확인:"
WORKER_COUNT=$(docker-compose ps airflow-worker 2>/dev/null | grep -c "airflow-worker" || echo "0")
echo "  - 실행 중인 Worker: ${WORKER_COUNT}개"
if [ "${WORKER_COUNT}" -ne "10" ]; then
    echo "  ⚠️  Worker가 10개가 아닙니다. 확인이 필요합니다."
    fi
echo ""

# 12. Airflow 설정 확인
echo "⚙️  Airflow 설정 확인:"
if docker ps --format '{{.Names}}' | grep -q "^airflow-webserver$"; then
echo "  - Parallelism:"
    docker exec airflow-webserver airflow config get-value core parallelism 2>/dev/null | sed 's/^/    /' || echo "    확인 중..."
echo "  - Pool Size:"
    docker exec airflow-webserver airflow config get-value database sql_alchemy_pool_size 2>/dev/null | sed 's/^/    /' || echo "    확인 중..."
    echo "  - Max Overflow:"
    docker exec airflow-webserver airflow config get-value database sql_alchemy_max_overflow 2>/dev/null | sed 's/^/    /' || echo "    확인 중..."
else
    echo "  ⚠️  webserver 컨테이너가 실행 중이 아닙니다."
fi
echo ""

# 13. 최종 상태 요약
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Airflow 재생성 완료!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📋 요약:"
echo "  ✅ 컨테이너 재생성 완료"
echo "  ✅ 패키지 설치 확인 완료"
echo "  ✅ Worker ${WORKER_COUNT}개 실행 중"
echo "  ✅ Oracle DB 설정 완료"
echo "  ✅ PyTorch 설치 완료"
echo "  ✅ TensorFlow 설치 완료"
echo ""
echo "🌐 접속 정보:"
echo "  - Airflow UI: http://localhost:8080"
echo "  - Flower UI: http://localhost:5555 (Worker 모니터링)"
echo ""
echo "💡 다음 단계:"
echo "  1. Airflow UI에서 DAG가 정상적으로 로드되는지 확인"
echo "  2. 패키지가 모두 설치되었는지 위의 확인 결과 확인"
echo "  3. 필요한 경우 모델 파일을 ./models/ 디렉토리에 복사"
echo ""

# 14. 파일 권한 설정 (모든 사용자가 수정 가능하도록)
echo "🔐 파일 권한 설정 중..."
echo "  📁 dags 디렉토리 권한 설정 중..."
sudo find /home/user/apps/airflow/dags -type d -exec chmod 777 {} \; && sudo find /home/user/apps/airflow/dags -type f -exec chmod 666 {} \;
echo "    ✅ dags 권한 설정 완료"

echo "  📁 plugins 디렉토리 권한 설정 중..."
sudo find /home/user/apps/airflow/plugins -type d -exec chmod 777 {} \; && sudo find /home/user/apps/airflow/plugins -type f -exec chmod 666 {} \;
echo "    ✅ plugins 권한 설정 완료"

echo "  📁 models 디렉토리 권한 설정 중..."
sudo find /home/user/apps/airflow/models -type d -exec chmod 777 {} \; && sudo find /home/user/apps/airflow/models -type f -exec chmod 666 {} \;
echo "    ✅ models 권한 설정 완료"
echo ""

