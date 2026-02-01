#!/usr/bin/env bash

set -uo pipefail  # -e 제거하여 오류 발생 시에도 계속 실행

info() {
  echo "[fix-network] $*"
}

info "Fixing network settings on worker node..."

# 1. FORWARD 정책 확인 및 설정
info "1. Setting iptables FORWARD policy to ACCEPT..."
sudo iptables -P FORWARD ACCEPT

# 2. rp_filter 설정 (reverse path filtering 비활성화)
info "2. Disabling rp_filter for better pod networking..."
sudo sysctl -w net.ipv4.conf.all.rp_filter=0 2>/dev/null || true
sudo sysctl -w net.ipv4.conf.default.rp_filter=0 2>/dev/null || true

# Flannel 및 CNI 인터페이스 확인 후 설정 (오류 무시)
if [ -d /proc/sys/net/ipv4/conf/flannel.1 ] 2>/dev/null; then
  sudo sysctl -w net.ipv4.conf.flannel.1.rp_filter=0 2>/dev/null || true
fi
if [ -d /proc/sys/net/ipv4/conf/cni0 ] 2>/dev/null; then
  sudo sysctl -w net.ipv4.conf.cni0.rp_filter=0 2>/dev/null || true
fi

# 실제 네트워크 인터페이스 확인 및 설정
for iface in $(ls /proc/sys/net/ipv4/conf/ 2>/dev/null | grep -E "^(ens|eth|enp)"); do
  if [ -d "/proc/sys/net/ipv4/conf/$iface" ] 2>/dev/null; then
    sudo sysctl -w "net.ipv4.conf.$iface.rp_filter=0" 2>/dev/null || true
  fi
done

# 3. IP forwarding 확인
info "3. Ensuring IP forwarding is enabled..."
sudo sysctl -w net.ipv4.ip_forward=1

# 4. Flannel 인터페이스 확인
info "4. Checking Flannel interface..."
if ip link show flannel.1 >/dev/null 2>&1; then
  info "   ✅ flannel.1 interface exists"
  ip addr show flannel.1 | grep -E "(inet|state)" | head -2
else
  info "   ⚠️  flannel.1 interface not found"
fi

# 5. Flannel Pod 재시작 (필요시) - 워커 노드에서는 kubectl이 없을 수 있으므로 스킵
info "5. Skipping Flannel Pod restart (run on master node if needed)..."

# 6. 영구 설정 저장
info "6. Saving network settings..."
# sysctl 설정 저장
sudo tee -a /etc/sysctl.conf >/dev/null <<EOF

# Kubernetes networking
net.ipv4.ip_forward=1
net.ipv4.conf.all.rp_filter=0
net.ipv4.conf.default.rp_filter=0
EOF

info "✅ Network settings fixed"
info ""
info "Verification:"
echo "  FORWARD policy: $(sudo iptables -L FORWARD -n | head -1 | awk '{print $4}')"
echo "  IP forwarding: $(sysctl net.ipv4.ip_forward | awk '{print $3}')"
echo "  rp_filter (all): $(sysctl net.ipv4.conf.all.rp_filter | awk '{print $3}')"

