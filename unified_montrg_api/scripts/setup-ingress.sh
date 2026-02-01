#!/usr/bin/env bash

set -euo pipefail

info() {
  echo "[ingress-setup] $*"
}

ensure_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found in PATH." >&2
    exit 1
  fi
}

ensure_command kubectl

info "Installing NGINX Ingress Controller..."

# NGINX Ingress Controller 설치 (NodePort 타입으로)
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.8.2/deploy/static/provider/baremetal/deploy.yaml

info "Waiting for Ingress Controller to be ready..."
kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=300s

# NodePort를 30081으로 고정 설정 (30080은 unified-montrg가 사용 중)
info "Configuring Ingress Controller NodePort to 30081..."
# 원본 Service 정보 가져오기
kubectl get service ingress-nginx-controller -n ingress-nginx -o yaml > /tmp/ingress-svc-original.yaml

# NodePort 수정 (selector와 기타 설정 유지)
# http 포트를 30081으로, https 포트를 30444으로 설정
python3 << 'PYTHON_SCRIPT'
import yaml
import sys

with open('/tmp/ingress-svc-original.yaml', 'r') as f:
    svc = yaml.safe_load(f)

# NodePort 수정 (30080이 이미 사용 중이면 30081 사용)
for port in svc['spec']['ports']:
    if port['name'] == 'http':
        port['nodePort'] = 30081
    elif port['name'] == 'https':
        port['nodePort'] = 30444

with open('/tmp/ingress-svc-original.yaml', 'w') as f:
    yaml.dump(svc, f, default_flow_style=False, sort_keys=False)
PYTHON_SCRIPT

# Service 삭제 후 재생성 (NodePort는 생성 후 변경 불가)
kubectl delete service ingress-nginx-controller -n ingress-nginx
sleep 2

# 수정된 Service 적용
kubectl apply -f /tmp/ingress-svc-original.yaml
rm /tmp/ingress-svc-original.yaml

info "Getting Ingress Controller NodePort..."
INGRESS_PORT=$(kubectl get service -n ingress-nginx ingress-nginx-controller -o jsonpath='{.spec.ports[?(@.name=="http")].nodePort}')

info "✅ Ingress Controller installed successfully!"
info ""
info "Access your application via:"
info "  http://10.10.100.80:${INGRESS_PORT}  (or any node IP)"
info "  http://10.10.100.81:${INGRESS_PORT}  (or any node IP)"
info ""
info "All traffic will be automatically load-balanced across all pods."

