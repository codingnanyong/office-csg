#!/bin/bash
# Docker 설치 스크립트 (Ubuntu)
# Docker Engine, CLI, Containerd, Buildx, Compose 설치

set -e

if [ "$EUID" -ne 0 ]; then 
    echo "❌ 이 스크립트는 sudo 권한이 필요합니다"
    echo "   실행: sudo ./scripts/install-docker.sh"
    exit 1
fi

echo "🚀 Docker 설치를 시작합니다..."
echo ""

# 1. 기존 Docker 제거 (선택사항)
if command -v docker &> /dev/null; then
    echo "⚠️  Docker가 이미 설치되어 있습니다."
    read -p "   기존 Docker를 제거하고 재설치하시겠습니까? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🗑️  기존 Docker 제거 중..."
        sudo apt-get remove -y docker docker-engine docker.io containerd runc || true
        sudo apt-get purge -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin || true
        sudo rm -rf /var/lib/docker /var/lib/containerd
        echo "✅ 기존 Docker 제거 완료"
    else
        echo "❌ 설치를 취소합니다."
        exit 0
    fi
fi

# 2. 필수 패키지 설치
echo "📦 필수 패키지 설치 중..."
apt-get update
apt-get install -y ca-certificates curl gnupg

# 3. Docker GPG 키 디렉토리 생성
echo "🔑 Docker GPG 키 설정 중..."
sudo install -m 0755 -d /etc/apt/keyrings

# 4. Docker GPG 키 다운로드
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc

# 5. Docker APT 저장소 추가
echo "📚 Docker APT 저장소 추가 중..."
ARCH=$(dpkg --print-architecture)
UBUNTU_CODENAME=$(. /etc/os-release && echo ${UBUNTU_CODENAME:-$(lsb_release -cs)})

echo "deb [arch=${ARCH} signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${UBUNTU_CODENAME} stable" | \
    tee /etc/apt/sources.list.d/docker.list > /dev/null

# 6. 패키지 목록 업데이트
echo "🔄 패키지 목록 업데이트 중..."
apt-get update

# 7. Docker 설치
echo "⬇️  Docker 설치 중..."
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 8. Docker 서비스 활성화 및 시작
echo "🔧 Docker 서비스 설정 중..."
systemctl enable --now docker

# 9. 설치 확인
echo ""
echo "✅ Docker 설치 완료!"
echo ""
echo "📋 설치된 버전:"
docker --version
docker compose version

# 10. 현재 사용자를 docker 그룹에 추가
if [ -n "$SUDO_USER" ]; then
    ACTUAL_USER="$SUDO_USER"
elif [ -n "$USER" ] && [ "$USER" != "root" ]; then
    ACTUAL_USER="$USER"
else
    ACTUAL_USER=$(logname 2>/dev/null || echo "")
fi

if [ -n "$ACTUAL_USER" ]; then
    echo ""
    echo "👤 사용자 '${ACTUAL_USER}'를 docker 그룹에 추가 중..."
    usermod -aG docker "$ACTUAL_USER"
    echo "✅ docker 그룹 추가 완료"
    echo "⚠️  그룹 변경사항을 적용하려면 다음 중 하나를 실행하세요:"
    echo "   - 새 터미널 세션 시작"
    echo "   - 로그아웃 후 다시 로그인"
    echo "   - 다음 명령어 실행: newgrp docker"
fi

# 11. Docker 테스트
echo ""
echo "🧪 Docker 테스트 중..."
if systemctl is-active --quiet docker; then
    # docker 그룹에 속한 경우에만 테스트 (root가 아닌 경우)
    if groups | grep -q docker || [ "$EUID" -eq 0 ]; then
        docker run --rm hello-world && echo "✅ Docker 테스트 성공!" || echo "⚠️  Docker 테스트 실패 (그룹 변경 후 다시 시도하세요)"
    else
        echo "⚠️  Docker 테스트를 건너뜁니다 (그룹 변경 필요)"
        echo "   새 터미널에서 다음 명령어로 테스트하세요:"
        echo "   docker run --rm hello-world"
    fi
else
    echo "❌ Docker 서비스가 실행되지 않았습니다"
    exit 1
fi

echo ""
echo "✅ Docker 설치 및 설정 완료!"
echo ""
echo "📝 다음 단계:"
echo "1. (선택) Docker Registry 설정:"
echo "   sudo ./scripts/setup-docker-registry.sh"
echo ""
echo "2. 그룹 변경사항 적용 (새 터미널 또는 로그아웃/재로그인):"
echo "   newgrp docker"
echo ""
echo "3. Docker Compose 사용:"
echo "   docker compose version"
echo "   docker compose up -d"
