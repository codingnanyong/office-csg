#!/bin/bash

# integrated-swagger-service Kubernetes 배포 스크립트

set -e

NAMESPACE="flet-montrg"
SERVICE_NAME="integrated-swagger-service"
IMAGE_NAME="flet-montrg/integrated-swagger-service:latest"
KIND_CLUSTER="flet-cluster"

echo "🚀 Integrated Swagger Service 배포 시작..."

# 네임스페이스 확인
echo "📋 네임스페이스 확인: $NAMESPACE"
kubectl get namespace $NAMESPACE || kubectl create namespace $NAMESPACE

# 기존 리소스 삭제 (선택사항)
if [ "$1" == "--clean" ]; then
    echo "🧹 기존 리소스 정리..."
    kubectl delete -k . --ignore-not-found=true
    sleep 5
fi

# Docker 이미지 빌드 (선택사항 - 이미 빌드되어 있으면 스킵)
if [ "$1" != "--no-build" ] && [ "$2" != "--no-build" ]; then
    echo "🔨 Docker 이미지 빌드..."
    cd ../../services/integrated-swagger-service
    docker build -t $IMAGE_NAME .
    cd ../../k8s/integrated-swagger
fi

# Kind에 이미지 로드
echo "📦 Kind에 이미지 로드..."
kind load docker-image $IMAGE_NAME --name $KIND_CLUSTER

# RBAC 및 ConfigMap 먼저 배포
echo "🔐 RBAC 리소스 배포..."
kubectl apply -f rbac.yaml

echo "⚙️ ConfigMap 배포..."
kubectl apply -f configmap.yaml

# 메인 리소스 배포
echo "📦 메인 리소스 배포..."
kubectl apply -k .

# 배포 상태 확인
echo "🔍 배포 상태 확인..."
kubectl rollout status deployment/$SERVICE_NAME -n $NAMESPACE --timeout=300s || true

# 서비스 상태 확인
echo "🌐 서비스 상태 확인..."
kubectl get service $SERVICE_NAME -n $NAMESPACE

# Pod 상태 확인
echo "📦 Pod 상태 확인..."
kubectl get pods -l app=$SERVICE_NAME -n $NAMESPACE

# HPA 상태 확인
echo "📈 HPA 상태 확인..."
kubectl get hpa $SERVICE_NAME-hpa -n $NAMESPACE

# 로그 확인 (선택사항)
if [ "$1" == "--logs" ] || [ "$2" == "--logs" ]; then
    echo "📝 최근 로그 확인..."
    kubectl logs -l app=$SERVICE_NAME -n $NAMESPACE --tail=50
fi

# 사용하지 않는 Docker 이미지 정리
echo "🧹 사용하지 않는 Docker 이미지 정리..."
docker image prune -f

# 접속 정보 표시
echo ""
echo "✅ Integrated Swagger Service 배포 완료!"
echo "🔗 접속 정보:"
echo "  📊 Swagger UI: http://localhost:30005/swagger"
echo "  🔧 API Endpoint: http://localhost:30005"  
echo "  📖 OpenAPI Spec: http://localhost:30005/openapi.json"
echo "  💾 메트릭: http://localhost:30006/metrics"
echo ""
echo "📋 유용한 명령어:"
echo "  kubectl logs -f -l app=$SERVICE_NAME -n $NAMESPACE"
echo "  kubectl port-forward svc/$SERVICE_NAME 8000:80 -n $NAMESPACE"
echo "  kubectl port-forward svc/$SERVICE_NAME 8080:8080 -n $NAMESPACE"