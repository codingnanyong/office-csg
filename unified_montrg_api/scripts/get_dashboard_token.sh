#!/bin/bash
# Kubernetes Dashboard 토큰 생성 스크립트

set -euo pipefail

DASHBOARD_NAMESPACE="${DASHBOARD_NAMESPACE:-kubernetes-dashboard}"
DASHBOARD_SA="${DASHBOARD_SA:-admin-user}"
TOKEN_EXPIRY="${TOKEN_EXPIRY:-24h}"

info() {
    echo "ℹ️  $*"
}

success() {
    echo "✅ $*"
}

error() {
    echo "❌ $*" >&2
}

warning() {
    echo "⚠️  $*"
}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔐 Kubernetes Dashboard 토큰 생성"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# 1. kubectl 명령어 확인
if ! command -v kubectl >/dev/null 2>&1; then
    error "kubectl 명령어를 찾을 수 없습니다."
    exit 1
fi

# 2. Kubernetes 클러스터 연결 확인
if ! kubectl cluster-info >/dev/null 2>&1; then
    error "Kubernetes 클러스터에 연결할 수 없습니다."
    exit 1
fi

# 3. Dashboard 네임스페이스 확인
if ! kubectl get namespace "${DASHBOARD_NAMESPACE}" >/dev/null 2>&1; then
    error "Dashboard 네임스페이스 '${DASHBOARD_NAMESPACE}'를 찾을 수 없습니다."
    echo ""
    info "사용 가능한 네임스페이스:"
    kubectl get namespaces | grep -i dashboard || echo "  (dashboard 관련 네임스페이스 없음)"
    exit 1
fi

# 4. ServiceAccount 확인
if ! kubectl get sa -n "${DASHBOARD_NAMESPACE}" "${DASHBOARD_SA}" >/dev/null 2>&1; then
    warning "ServiceAccount '${DASHBOARD_SA}'를 찾을 수 없습니다."
    echo ""
    info "사용 가능한 ServiceAccount:"
    kubectl get sa -n "${DASHBOARD_NAMESPACE}" | tail -n +2 | awk '{print "  - " $1}'
    echo ""
    read -p "사용할 ServiceAccount 이름을 입력하세요 (Enter로 종료): " CUSTOM_SA
    if [ -z "${CUSTOM_SA}" ]; then
        exit 0
    fi
    DASHBOARD_SA="${CUSTOM_SA}"
fi

info "네임스페이스: ${DASHBOARD_NAMESPACE}"
info "ServiceAccount: ${DASHBOARD_SA}"
info "토큰 유효기간: ${TOKEN_EXPIRY}"
echo ""

# 5. 토큰 생성
echo "🔑 토큰 생성 중..."
TOKEN=$(kubectl create token -n "${DASHBOARD_NAMESPACE}" "${DASHBOARD_SA}" --duration="${TOKEN_EXPIRY}" 2>/dev/null)

if [ -z "${TOKEN}" ]; then
    error "토큰 생성에 실패했습니다."
    echo ""
    info "대안 방법을 시도합니다..."
    
    # 대안: Secret에서 토큰 추출
    SECRET_NAME=$(kubectl get sa -n "${DASHBOARD_NAMESPACE}" "${DASHBOARD_SA}" -o jsonpath='{.secrets[0].name}' 2>/dev/null || echo "")
    
    if [ -n "${SECRET_NAME}" ] && [ "${SECRET_NAME}" != "null" ]; then
        info "Secret에서 토큰 추출 시도: ${SECRET_NAME}"
        TOKEN=$(kubectl get secret -n "${DASHBOARD_NAMESPACE}" "${SECRET_NAME}" -o jsonpath='{.data.token}' 2>/dev/null | base64 -d 2>/dev/null || echo "")
    fi
    
    if [ -z "${TOKEN}" ]; then
        error "토큰을 생성할 수 없습니다."
        echo ""
        info "수동으로 토큰을 생성하려면:"
        echo "  kubectl -n ${DASHBOARD_NAMESPACE} create token ${DASHBOARD_SA}"
        exit 1
    fi
fi

success "토큰 생성 완료!"
echo ""

# 6. Dashboard URL 확인
echo "🌐 Dashboard 접속 정보:"
DASHBOARD_SERVICE=$(kubectl get svc -n "${DASHBOARD_NAMESPACE}" -l k8s-app=kubernetes-dashboard -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -n "${DASHBOARD_SERVICE}" ]; then
    # NodePort 확인
    NODEPORT=$(kubectl get svc -n "${DASHBOARD_NAMESPACE}" "${DASHBOARD_SERVICE}" -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "")
    
    if [ -n "${NODEPORT}" ]; then
        # 첫 번째 노드의 IP 가져오기
        NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || echo "localhost")
        echo "  URL: https://${NODE_IP}:${NODEPORT}"
    fi
    
    # Port-forward 정보
    echo "  Port-forward: kubectl port-forward -n ${DASHBOARD_NAMESPACE} svc/${DASHBOARD_SERVICE} 8443:443"
    echo "  Local URL: https://localhost:8443"
else
    echo "  (Dashboard Service를 찾을 수 없습니다)"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 토큰 (아래 토큰을 복사하여 Dashboard에 입력하세요)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "${TOKEN}"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# 7. 클립보드에 복사 (선택 사항)
if command -v xclip >/dev/null 2>&1; then
    echo "${TOKEN}" | xclip -selection clipboard
    success "토큰이 클립보드에 복사되었습니다 (xclip)"
elif command -v pbcopy >/dev/null 2>&1; then
    echo "${TOKEN}" | pbcopy
    success "토큰이 클립보드에 복사되었습니다 (pbcopy)"
else
    info "클립보드 복사 도구가 없습니다. 위의 토큰을 수동으로 복사하세요."
fi

echo ""
info "토큰 유효기간: ${TOKEN_EXPIRY}"
info "새 토큰 생성: $0"
echo ""

