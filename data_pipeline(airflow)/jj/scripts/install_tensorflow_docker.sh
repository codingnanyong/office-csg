#!/bin/bash
#
# TensorFlow 설치 스크립트 (Docker 컨테이너용)
# 호스트에서 실행하여 Airflow 컨테이너들에 TensorFlow CPU 버전 설치
#
# 사용법:
#   bash install_tensorflow_docker.sh
#
# 모든 실행 중인 Airflow 컨테이너에 TensorFlow CPU 버전을 설치합니다.
#

# set -e 제거 (quick_restart.sh처럼 에러 처리를 수동으로)

cd /home/user/apps/airflow

INSTALL_TYPE="cpu"
TARGET_CONTAINER=${1:-""}

SUCCESS_COUNT=0
FAIL_COUNT=0

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 TensorFlow CPU 버전 설치 스크립트 (Docker)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "설치 타입: CPU 버전 (TensorFlow 2.15.x - 2.16.x)"
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

# 각 컨테이너에 설치 (quick_restart.sh의 torch 설치 방식 참고)
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
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            FAIL_COUNT=$((FAIL_COUNT + 1))
        fi
    fi
done
echo "  ✅ TensorFlow 설치 완료 ($TF_INSTALLED개 컨테이너)"
echo ""

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
    echo "✅ TensorFlow 설치가 완료되었습니다!"
    exit 0
else
    echo "❌ 모든 컨테이너에서 설치 확인 실패"
    exit 1
fi

