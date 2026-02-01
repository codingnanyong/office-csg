#!/bin/bash

# sensor-threshold-mapping-service Kubernetes 배포 스크립트

set -e

NAMESPACE="flet-montrg"
SERVICE_NAME="sensor-threshold-mapping-service"
IMAGE_NAME="flet-montrg/sensor-threshold-mapping-service:latest"
KIND_CLUSTER="flet-cluster"
K8S_BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🚀 Sensor Threshold Mapping Service 배포 시작..."

# 네임스페이스 확인
echo "📋 네임스페이스 확인: $NAMESPACE"
kubectl get namespace $NAMESPACE || kubectl create namespace $NAMESPACE

# Docker 이미지 빌드
echo "🔨 Docker 이미지 빌드..."
cd "$K8S_BASE_DIR/../../services/sensor-threshold-mapping-service"
docker build -t $IMAGE_NAME .
cd "$K8S_BASE_DIR"

# Kind에 이미지 로드
echo "📦 Kind에 이미지 로드..."
kind load docker-image $IMAGE_NAME --name $KIND_CLUSTER

# Secret 및 ConfigMap 먼저 배포
echo "🔐 Secret 배포..."
kubectl apply -f secret.yaml

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

# 사용하지 않는 Docker 이미지 정리
echo "🧹 사용하지 않는 Docker 이미지 정리..."
docker image prune -f

# 접속 정보 표시
echo ""
echo "✅ Sensor Threshold Mapping Service 배포 완료!"
echo "🔗 접속 정보:"
echo "  📊 API Endpoint: http://localhost:30011"
echo "  📖 API Docs: http://localhost:30011/docs"
echo "  💾 메트릭: http://localhost:30240/metrics"
echo ""
echo "📋 유용한 명령어:"
echo "  kubectl logs -f -l app=$SERVICE_NAME -n $NAMESPACE"
echo "  kubectl port-forward svc/$SERVICE_NAME 8000:80 -n $NAMESPACE"
