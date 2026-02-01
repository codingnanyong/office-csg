#!/bin/bash
#
# PyTorch 설치 스크립트 (Docker 컨테이너용)
# 호스트에서 실행하여 Airflow 컨테이너들에 PyTorch CPU 버전 설치
#
# 사용법:
#   bash install_pytorch_docker.sh
#   bash install_pytorch_docker.sh <container_name>  # 특정 컨테이너만 설치
#
# 모든 실행 중인 Airflow 컨테이너에 PyTorch CPU 버전을 설치합니다.
#

cd /home/user/apps/airflow

INSTALL_TYPE="cpu"
TARGET_CONTAINER=${1:-""}

SUCCESS_COUNT=0
FAIL_COUNT=0

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 PyTorch CPU 버전 설치 스크립트 (Docker)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "설치 타입: CPU 버전 (PyTorch 2.0.x - 2.9.x)"
echo ""

# 컨테이너 목록 생성
if [ -n "$TARGET_CONTAINER" ]; then
    CONTAINERS=("$TARGET_CONTAINER")
else
    # 실행 중인 Airflow 작업 컨테이너만 찾기 (DB 제외)
    # airflow-worker, airflow-scheduler, airflow-triggerer, airflow-webserver 등
    # airflow-airflow-worker-1, airflow-scheduler 형식도 포함
    ALL_AIRFLOW=($(docker ps --format '{{.Names}}' | grep "^airflow" || true))
    CONTAINERS=()
    for CONTAINER in "${ALL_AIRFLOW[@]}"; do
        # DB 관련 컨테이너 제외
        if [[ ! "$CONTAINER" =~ (postgres|redis|mysql|mariadb|db) ]]; then
            CONTAINERS+=("$CONTAINER")
        fi
    done
fi

if [ ${#CONTAINERS[@]} -eq 0 ]; then
    echo "❌ 실행 중인 Airflow 컨테이너를 찾을 수 없습니다."
    echo "컨테이너 목록 확인: docker compose ps"
    exit 1
fi

echo "📋 설치 대상 컨테이너:"
for CONTAINER in "${CONTAINERS[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        echo "  ✅ $CONTAINER"
    else
        echo "  ❌ $CONTAINER (실행 중이 아님)"
    fi
done
echo ""

# 각 컨테이너에 설치
echo "🔥 PyTorch CPU 버전 설치 중..."

PT_INSTALLED=0
for CONTAINER in "${CONTAINERS[@]}"; do
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        echo "  ⏭ ${CONTAINER} 건너뜀 (실행 중이 아님)"
        continue
    fi
    
    echo "  📦 ${CONTAINER}에 PyTorch 설치 중... (진행 상황 표시)"
    
    # Python 버전 확인
    PYTHON_VERSION=$(docker exec "$CONTAINER" python3 --version 2>&1 | awk '{print $2}' || echo "알 수 없음")
    echo "    Python 버전: $PYTHON_VERSION"
    
    # pip 업그레이드
    echo "    pip 업그레이드 중..."
    docker exec "$CONTAINER" python3 -m pip install --upgrade pip --quiet 2>/dev/null || true
    
    # PyTorch CPU 버전 설치
    echo "    PyTorch CPU 버전 설치 중..."
    if timeout 600 docker exec "$CONTAINER" pip install --no-cache-dir torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu 2>&1 | grep -E "(Collecting|Downloading|Installing|Successfully)" | tail -5; then
        echo "    ✅ PyTorch 설치 완료"
        PT_INSTALLED=$((PT_INSTALLED + 1))
    else
        EXIT_CODE=$?
        if [ $EXIT_CODE -eq 124 ]; then
            echo "    ⚠️  시간 초과 (10분) - 설치 진행 중일 수 있습니다"
        else
            echo "    ⚠️  설치 중 오류 발생 (exit code: $EXIT_CODE)"
        fi
        # 타임아웃이어도 설치 중일 수 있으므로 카운트에 포함
        PT_INSTALLED=$((PT_INSTALLED + 1))
    fi
    
    # 설치 확인
    PT_VERSION=$(docker exec "$CONTAINER" python3 -c "import torch; print(torch.__version__)" 2>/dev/null || echo "")
    if [ -n "$PT_VERSION" ]; then
        echo "    📌 PyTorch 버전: $PT_VERSION"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo "    ❌ PyTorch 버전 확인 실패"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
    
    echo ""
done

# 결과 요약
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 설치 결과 요약"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "성공: $SUCCESS_COUNT개"
if [ $FAIL_COUNT -gt 0 ]; then
    echo "실패: $FAIL_COUNT개"
fi
echo ""

if [ $SUCCESS_COUNT -gt 0 ]; then
    echo "✅ PyTorch 설치가 완료되었습니다!"
    exit 0
else
    echo "❌ 모든 컨테이너에서 설치 확인 실패"
    exit 1
fi

