#!/usr/bin/env bash

set -euo pipefail

info() {
  echo "[fix-forward] $*"
}

info "Setting iptables FORWARD policy to ACCEPT on worker node..."

# FORWARD 정책을 ACCEPT로 변경
sudo iptables -P FORWARD ACCEPT

info "✅ FORWARD policy set to ACCEPT"

# 영구적으로 적용하기 위해 iptables-persistent 또는 netfilter-persistent 사용
if command -v netfilter-persistent >/dev/null 2>&1; then
  info "Saving iptables rules with netfilter-persistent..."
  sudo netfilter-persistent save
elif command -v iptables-save >/dev/null 2>&1; then
  info "Saving iptables rules..."
  sudo iptables-save | sudo tee /etc/iptables/rules.v4 >/dev/null || {
    info "⚠️  Could not save to /etc/iptables/rules.v4"
    info "   You may need to manually save iptables rules"
  }
fi

info "✅ iptables FORWARD policy fixed"
info ""
info "Verifying FORWARD policy:"
sudo iptables -L FORWARD -n | head -3

