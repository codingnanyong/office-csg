#!/usr/bin/env bash

set -euo pipefail

DASHBOARD_VERSION="${DASHBOARD_VERSION:-v2.7.0}"
NAMESPACE="${NAMESPACE:-kubernetes-dashboard}"
DASHBOARD_URL="https://raw.githubusercontent.com/kubernetes/dashboard/${DASHBOARD_VERSION}/aio/deploy/recommended.yaml"

echo "[dashboard] Deploying Kubernetes Dashboard ${DASHBOARD_VERSION}"
kubectl apply -f "${DASHBOARD_URL}"

echo "[dashboard] Ensuring admin ServiceAccount and ClusterRoleBinding"
kubectl apply -f "$(dirname "$0")/../dashboard/dashboard-admin.yaml"

echo "[dashboard] Dashboard deployed. Generate access token with:"
cat <<EOF
kubectl -n ${NAMESPACE} create token admin-user
EOF

echo "[dashboard] Access via:"
cat <<'EOF'
kubectl proxy
# Browser:
# http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/
EOF

