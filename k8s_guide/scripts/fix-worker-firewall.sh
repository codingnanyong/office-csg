#!/usr/bin/env bash

set -uo pipefail

info() {
  echo "[fix-firewall] $*"
}

info "Fixing firewall settings on worker node..."

# 1. FORWARD 정책 확인
info "1. Setting iptables FORWARD policy to ACCEPT..."
sudo iptables -P FORWARD ACCEPT

# 2. kubelet 포트(10250) 허용
info "2. Allowing kubelet port (10250)..."
sudo iptables -I INPUT -p tcp --dport 10250 -j ACCEPT 2>/dev/null || true
sudo iptables -I INPUT -p tcp --sport 10250 -j ACCEPT 2>/dev/null || true

# 3. VXLAN 포트(8472/UDP) 허용
info "3. Allowing VXLAN port (8472/UDP)..."
sudo iptables -I INPUT -p udp --dport 8472 -j ACCEPT 2>/dev/null || true
sudo iptables -I INPUT -p udp --sport 8472 -j ACCEPT 2>/dev/null || true

# 4. Kubernetes API 서버 포트(6443) 허용
info "4. Allowing Kubernetes API server port (6443)..."
sudo iptables -I INPUT -p tcp --dport 6443 -j ACCEPT 2>/dev/null || true

# 5. NodePort 범위 허용 (30000-32767)
info "5. Allowing NodePort range (30000-32767)..."
sudo iptables -I INPUT -p tcp --dport 30000:32767 -j ACCEPT 2>/dev/null || true
sudo iptables -I INPUT -p udp --dport 30000:32767 -j ACCEPT 2>/dev/null || true

# 6. Flannel 네트워크 대역 허용
info "6. Allowing Flannel network (10.244.0.0/16)..."
sudo iptables -I INPUT -s 10.244.0.0/16 -j ACCEPT 2>/dev/null || true
sudo iptables -I INPUT -d 10.244.0.0/16 -j ACCEPT 2>/dev/null || true
sudo iptables -I FORWARD -s 10.244.0.0/16 -j ACCEPT 2>/dev/null || true
sudo iptables -I FORWARD -d 10.244.0.0/16 -j ACCEPT 2>/dev/null || true

# 7. 노드 간 통신 허용
info "7. Allowing inter-node communication..."
# 마스터 노드 IP 허용
sudo iptables -I INPUT -s 10.10.100.80 -j ACCEPT 2>/dev/null || true
sudo iptables -I INPUT -d 10.10.100.80 -j ACCEPT 2>/dev/null || true

info "✅ Firewall settings fixed"
info ""
info "Verification:"
echo "  FORWARD policy: $(sudo iptables -L FORWARD -n | head -1 | awk '{print $4}')"
echo "  kubelet port (10250): $(sudo iptables -L INPUT -n | grep 10250 | head -1 || echo 'Not found')"
echo "  VXLAN port (8472): $(sudo iptables -L INPUT -n | grep 8472 | head -1 || echo 'Not found')"

