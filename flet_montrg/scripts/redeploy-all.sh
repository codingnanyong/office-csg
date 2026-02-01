#!/bin/bash

# 모든 서비스 재배포 스크립트
# Kind 클러스터 재생성 후 사용

set -e

NAMESPACE="flet-montrg"
K8S_BASE_DIR="/home/de/apps/flet_montrg/k8s"

echo "🚀 모든 서비스 재배포 시작..."

# 1. 네임스페이스 확인
echo "📋 네임스페이스 확인: $NAMESPACE"
kubectl get namespace $NAMESPACE >/dev/null 2>&1 || kubectl create namespace $NAMESPACE

# 2. 서비스 배포 순서 (의존성 고려)
SERVICES=(
    "thresholds"
    "location"
    "realtime"
    "aggregation"
    "alert"
    "integrated-swagger"
)

# 3. 각 서비스 배포
for service in "${SERVICES[@]}"; do
    echo ""
    echo "=========================================="
    echo "📦 $service-service 배포 중..."
    echo "=========================================="
    
    SERVICE_DIR="$K8S_BASE_DIR/$service"
    
    if [ -d "$SERVICE_DIR" ] && [ -f "$SERVICE_DIR/deploy.sh" ]; then
        cd "$SERVICE_DIR"
        chmod +x deploy.sh
        if ./deploy.sh --no-build; then
            echo "✅ $service-service 배포 성공"
        else
            echo "⚠️  $service-service 배포 실패 (계속 진행)"
        fi
    else
        echo "⚠️  $service-service 디렉토리 또는 deploy.sh를 찾을 수 없습니다."
        echo "    경로: $SERVICE_DIR"
    fi
done

# 4. 최종 상태 확인
echo ""
echo "=========================================="
echo "✅ 배포 완료! 상태 확인"
echo "=========================================="
echo ""
echo "📦 배포된 서비스:"
kubectl get deployments -n $NAMESPACE

echo ""
echo "🌐 서비스 포트:"
kubectl get svc -n $NAMESPACE -o wide | grep NodePort

echo ""
echo "📊 Pod 상태:"
kubectl get pods -n $NAMESPACE

echo ""
echo "🔗 서비스 엔드포인트:"
echo "  📊 통합 Swagger UI: http://localhost:30005"
echo "  🚨 Alert Service: http://localhost:30007/docs"
echo "  📈 Thresholds Service: http://localhost:30001/docs"
echo "  📍 Location Service: http://localhost:30002/docs"
echo "  ⚡ Realtime Service: http://localhost:30003/docs"
echo "  📊 Aggregation Service: http://localhost:30004/docs"

echo ""
echo "✅ 모든 서비스 재배포 완료!"
