#!/bin/bash

# Kind 클러스터 재생성 스크립트

set -e

KIND_CLUSTER="flet-cluster"
KIND_CONFIG="/home/de/apps/k8s/kind-config.yaml"

echo "⚠️  경고: 이 작업은 기존 클러스터를 삭제하고 새로 생성합니다!"
echo "⚠️  모든 리소스가 삭제되며, 재배포가 필요합니다."
echo ""
read -p "계속하시겠습니까? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "❌ 취소되었습니다."
    exit 1
fi

echo ""
echo "🗑️  기존 클러스터 삭제 중..."
kind delete cluster --name $KIND_CLUSTER || echo "클러스터가 이미 없거나 삭제 중 오류 발생"

echo ""
echo "🔄 새 클러스터 생성 중..."
kind create cluster --config $KIND_CONFIG

echo ""
echo "🔗 Docker 네트워크 연결 중..."
CONTROL_PLANE_NAME="${KIND_CLUSTER}-control-plane"
STORAGE_NETWORK="storage_network"

# storage_network가 존재하는지 확인
if docker network inspect $STORAGE_NETWORK >/dev/null 2>&1; then
    # 이미 연결되어 있는지 확인
    if docker network inspect $STORAGE_NETWORK | grep -q "$CONTROL_PLANE_NAME"; then
        echo "   ℹ️  이미 $STORAGE_NETWORK에 연결되어 있습니다."
    else
        docker network connect $STORAGE_NETWORK $CONTROL_PLANE_NAME || {
            echo "   ⚠️  네트워크 연결 실패 (계속 진행)"
        }
        echo "   ✅ $CONTROL_PLANE_NAME을(를) $STORAGE_NETWORK에 연결했습니다."
    fi
else
    echo "   ⚠️  $STORAGE_NETWORK 네트워크를 찾을 수 없습니다. 수동으로 연결해야 할 수 있습니다:"
    echo "      docker network connect $STORAGE_NETWORK $CONTROL_PLANE_NAME"
fi

echo ""
echo "✅ 클러스터 재생성 완료!"
echo ""
echo "📋 다음 단계:"
echo "  1. 모든 서비스 재배포:"
echo "     cd /home/de/apps/flet_montrg/scripts"
echo "     ./redeploy-all.sh"
echo ""
echo "  2. 또는 개별 서비스 배포:"
echo "     cd /home/de/apps/flet_montrg/k8s/{service-name}"
echo "     ./deploy.sh"
