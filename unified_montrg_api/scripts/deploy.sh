#!/usr/bin/env bash

set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-csg/unified-montrg-api:latest}"
TAR_FILE="${TAR_FILE:-unified-montrg-api.tar}"
NAMESPACE="${NAMESPACE:-unified-montrg}"
K8S_DIR="${K8S_DIR:-k8s}"
DOCKERFILE="${DOCKERFILE:-docker/Dockerfile}"
# 공통 비밀번호 (환경 변수로 오버라이드 가능)
SSH_PASSWORD="${SSH_PASSWORD:-user1234*}"
SUDO_PASSWORD="${SUDO_PASSWORD:-user1234*}"
# 배포 변경 사항 메시지 (선택 사항, Git commit message를 자동으로 사용)
DEPLOY_MESSAGE="${DEPLOY_MESSAGE:-}"

info() {
  echo "[deploy] $*"
}

success() {
  echo "[deploy] ✅ $*"
}

warning() {
  echo "[deploy] ⚠️  $*"
}

error() {
  echo "[deploy] ❌ $*" >&2
}

ensure_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    error "Required command '$1' not found in PATH."
    exit 1
  fi
}

ensure_command docker
ensure_command kubectl
ensure_command sudo

# 테스트 실행 옵션 (기본값: true, SKIP_TESTS=true로 스킵 가능)
SKIP_TESTS="${SKIP_TESTS:-false}"

# sshpass 설치 확인 (없으면 설치 시도)
if ! command -v sshpass >/dev/null 2>&1; then
  info "📦 sshpass가 없습니다. 설치 시도 중..."
  if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update && sudo apt-get install -y sshpass
  elif command -v yum >/dev/null 2>&1; then
    sudo yum install -y sshpass
  else
    error "sshpass를 설치할 수 없습니다. 수동으로 설치하세요: apt-get install sshpass 또는 yum install sshpass"
    exit 1
  fi
  success "sshpass 설치 완료"
fi

# 테스트 실행 (SKIP_TESTS=true로 스킵 가능)
if [ "${SKIP_TESTS}" != "true" ]; then
  info "🧪 Running tests before deployment..."
  
  # pytest 설치 확인
  if ! command -v pytest >/dev/null 2>&1; then
    # 가상 환경 확인 및 활성화
    if [ -d "venv" ]; then
      info "  📦 Activating virtual environment..."
      source venv/bin/activate
    elif [ -d ".venv" ]; then
      info "  📦 Activating virtual environment..."
      source .venv/bin/activate
    else
      warning "Virtual environment not found. Attempting to install pytest globally..."
      if command -v pip3 >/dev/null 2>&1; then
        pip3 install pytest pytest-asyncio >/dev/null 2>&1 || {
          error "Failed to install pytest. Please install it manually or set SKIP_TESTS=true"
          exit 1
        }
      else
        error "pytest not found and cannot install. Please install pytest or set SKIP_TESTS=true"
        exit 1
      fi
    fi
  fi
  
  # pytest 실행
  if pytest tests/ -v --tb=short; then
    success "All tests passed"
  else
    error "Tests failed! Deployment aborted."
    error "To skip tests, run: SKIP_TESTS=true ./scripts/deploy.sh"
    exit 1
  fi
else
  warning "Skipping tests (SKIP_TESTS=true)"
fi

info "🔨 Building Docker image: ${IMAGE_NAME}"
# INSTANTCLIENT_ZIP 환경 변수를 통해 Oracle Instant Client ZIP 파일 이름을 지정할 수 있음
# 예: INSTANTCLIENT_ZIP=instantclient-basic-linux.x64-23.26.0.0.0.zip ./scripts/deploy.sh
docker build -f "${DOCKERFILE}" \
  --build-arg INSTANTCLIENT_ZIP="${INSTANTCLIENT_ZIP:-instantclient-basic.zip}" \
  -t "${IMAGE_NAME}" .
success "Docker image built successfully"

info "📦 Exporting image to ${TAR_FILE}"
docker save "${IMAGE_NAME}" -o "${TAR_FILE}"
success "Image exported to ${TAR_FILE}"

info "📥 Importing image into containerd on all nodes"
# Get all node names
NODES=$(kubectl get nodes -o jsonpath='{.items[*].metadata.name}')

for NODE in ${NODES}; do
  info "🖥️  Processing node: ${NODE}"
  
  # Get node IP
  NODE_IP=$(kubectl get node "${NODE}" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
  
  if [ -z "${NODE_IP}" ]; then
    warning "Could not get IP for node ${NODE}, skipping..."
    continue
  fi
  
  # Check if this is the current node (localhost)
  CURRENT_HOSTNAME=$(hostname)
  CURRENT_IP=$(hostname -I | awk '{print $1}')
  if [ "${NODE}" = "${CURRENT_HOSTNAME}" ] || [ "${NODE_IP}" = "${CURRENT_IP}" ]; then
    info "  📍 Importing on local node (${NODE})"
    # 기존 이미지 삭제 후 새로 import (업데이트를 위해)
    sudo ctr -n k8s.io images remove csg/unified-montrg-api:latest 2>/dev/null || true
    sudo ctr -n k8s.io images import "${TAR_FILE}"
    sudo ctr -n k8s.io images tag docker.io/csg/unified-montrg-api:latest csg/unified-montrg-api:latest 2>/dev/null || true
    success "Image imported on local node (${NODE})"
  else
    info "  🌐 Copying image to ${NODE} (${NODE_IP}) and importing"
    # Try using IP address first, fallback to hostname
    SSH_TARGET="${NODE_IP}"
    # Try to use current username if available
    if [ -n "${USER:-}" ]; then
      SSH_TARGET="${USER}@${NODE_IP}"
    fi
    
    # Copy tar file to remote node using IP (비밀번호 자동 입력)
    sshpass -p "${SSH_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "${TAR_FILE}" "${SSH_TARGET}:/tmp/${TAR_FILE}" || {
      warning "Failed to copy to ${NODE} (${NODE_IP})."
      info "  → Manual step: Copy ${TAR_FILE} to ${NODE_IP} and run: sudo ctr -n k8s.io images import /tmp/${TAR_FILE}"
      continue
    }
    # Import on remote node using IP (sudo 비밀번호 자동 입력)
    # 기존 이미지 삭제 후 새로 import (업데이트를 위해)
    sshpass -p "${SSH_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -t "${SSH_TARGET}" "
      # 기존 이미지 태그 삭제 (이미지 업데이트를 위해)
      echo '${SUDO_PASSWORD}' | sudo -S ctr -n k8s.io images remove csg/unified-montrg-api:latest 2>&1 || true
      # 새 이미지 import
      echo '${SUDO_PASSWORD}' | sudo -S ctr -n k8s.io images import /tmp/${TAR_FILE} 2>&1
      # 이미지 태그 확인 및 설정
      echo '${SUDO_PASSWORD}' | sudo -S ctr -n k8s.io images tag docker.io/csg/unified-montrg-api:latest csg/unified-montrg-api:latest 2>&1 || true
      # 임시 파일 삭제
      rm -f /tmp/${TAR_FILE}
    " && {
      success "Image imported/updated on ${NODE} (${NODE_IP})"
    } || {
      warning "Failed to import/update image on ${NODE} (${NODE_IP})"
      info "  → Manual step: SSH to ${SSH_TARGET} and run:"
      info "     sudo ctr -n k8s.io images remove csg/unified-montrg-api:latest"
      info "     sudo ctr -n k8s.io images import /tmp/unified-montrg-api.tar"
    }
  fi
done
success "Image import completed on all nodes"

info "🚀 Applying Kubernetes manifests"
# 1. Namespace (먼저 생성)
info "  📦 Creating namespace..."
kubectl apply -f "${K8S_DIR}/namespace.yaml"

# 2. Secret (다른 리소스가 참조하므로 먼저 생성)
if [ -f "${K8S_DIR}/secret.yaml" ]; then
  info "  🔐 Applying Secret configuration..."
  kubectl apply -f "${K8S_DIR}/secret.yaml"
fi

# 3. ConfigMap (Secret 다음)
info "  ⚙️  Applying ConfigMap..."
kubectl apply -f "${K8S_DIR}/configmap.yaml"

# 4. Service (Deployment보다 먼저 생성 가능)
info "  🔌 Applying Service..."
kubectl apply -f "${K8S_DIR}/service.yaml"

# 5. Deployment (Secret과 ConfigMap을 참조)
info "  🚀 Applying Deployment..."
kubectl apply -f "${K8S_DIR}/deployment.yaml"

# 6. Ingress Controller DaemonSet (Ingress보다 먼저)
if [ -f "${K8S_DIR}/ingress-controller-daemonset.yaml" ]; then
  info "  🌐 Applying Ingress Controller DaemonSet..."
  kubectl apply -f "${K8S_DIR}/ingress-controller-daemonset.yaml"
fi

# 7. Ingress Controller Service Patch
if [ -f "${K8S_DIR}/ingress-controller-service-patch.yaml" ]; then
  info "  🔧 Applying Ingress Controller Service Patch..."
  kubectl apply -f "${K8S_DIR}/ingress-controller-service-patch.yaml"
fi

# 8. Ingress (마지막에 적용)
if [ -f "${K8S_DIR}/ingress.yaml" ]; then
  info "  🌍 Applying Ingress configuration..."
  kubectl apply -f "${K8S_DIR}/ingress.yaml"
fi
success "All Kubernetes manifests applied successfully"

info "🗑️  Cleaning up temporary files..."
rm "${TAR_FILE}"

info "🔄 Restarting deployment ${NAMESPACE}/unified-montrg"
kubectl rollout restart deployment/unified-montrg -n "${NAMESPACE}"
success "Deployment restart triggered"

# 배포 변경 사항 기록
if [ -z "${DEPLOY_MESSAGE}" ]; then
  # Git commit message를 자동으로 가져오기 (있는 경우)
  if command -v git >/dev/null 2>&1 && git rev-parse --git-dir >/dev/null 2>&1; then
    DEPLOY_MESSAGE=$(git log -1 --pretty=format:"%s" 2>/dev/null || echo "Deployment update")
  else
    DEPLOY_MESSAGE="Deployment update"
  fi
fi

# 배포가 완료될 때까지 대기 (최대 60초)
info "⏳ Waiting for deployment to be ready..."
if kubectl rollout status deployment/unified-montrg -n "${NAMESPACE}" --timeout=60s >/dev/null 2>&1; then
  # 배포 변경 사항을 annotation으로 기록
  info "📝 Recording deployment change: ${DEPLOY_MESSAGE}"
  kubectl annotate deployment/unified-montrg -n "${NAMESPACE}" \
    kubernetes.io/change-cause="${DEPLOY_MESSAGE}" \
    --overwrite >/dev/null 2>&1 || warning "Failed to record deployment change"
  success "Deployment change recorded in history"
else
  warning "Deployment not ready, skipping change record"
fi

info "🧹 Cleaning up dangling Docker images"
docker image prune -f
success "Docker image cleanup completed"

info "📊 Current pod status:"
kubectl get pods -n "${NAMESPACE}"

success "✨ Deployment script completed successfully!"

