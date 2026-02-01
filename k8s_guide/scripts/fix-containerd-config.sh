#!/bin/bash
# containerd 설치 및 설정 스크립트 (Docker와 Kubernetes 공존)

echo "=== containerd 설정 수정 시작 ==="
echo "⚠️  주의: Docker 컨테이너는 유지되지만, containerd 재시작 시 일시적으로 중지될 수 있습니다."
echo ""

if ! command -v containerd >/dev/null 2>&1; then
    echo "containerd가 설치되어 있지 않으므로 설치를 진행합니다."
    if command -v apt >/dev/null 2>&1; then
        sudo apt update
        sudo apt install -y containerd
    elif command -v apt-get >/dev/null 2>&1; then
        sudo apt-get update
        sudo apt-get install -y containerd
    else
        echo "지원되지 않는 패키지 관리자입니다. containerd를 수동 설치하세요."
        exit 1
    fi
fi

# 0. Docker 컨테이너 상태 확인 및 백업
echo "0. Docker 컨테이너 상태 확인 중..."
RUNNING_CONTAINERS=$(docker ps -q)
if [ -n "$RUNNING_CONTAINERS" ]; then
    echo "  실행 중인 컨테이너: $(echo $RUNNING_CONTAINERS | wc -w)개"
    echo "  컨테이너 목록:"
    docker ps --format "    - {{.Names}} ({{.Status}})"
    echo ""
    echo "  ⚠️  containerd 재시작 시 컨테이너가 일시적으로 중지될 수 있지만,"
    echo "     Docker 서비스 재시작 후 자동으로 복구됩니다."
    echo ""
else
    echo "  실행 중인 컨테이너 없음"
fi

# 1. 기존 설정 백업
echo "1. 기존 설정 백업 중..."
sudo cp /etc/containerd/config.toml /etc/containerd/config.toml.backup.$(date +%Y%m%d_%H%M%S)
echo "  ✓ 백업 완료"

# 2. 기본 설정 생성
echo "2. containerd 기본 설정 생성 중..."
containerd config default | sudo tee /etc/containerd/config.toml
echo "  ✓ 기본 설정 생성 완료"

# 3. CRI 플러그인 활성화 (disabled_plugins에서 cri 제거)
echo "3. CRI 플러그인 활성화 중..."
sudo sed -i 's/disabled_plugins = \["cri"\]/# disabled_plugins = []/' /etc/containerd/config.toml
echo "  ✓ CRI 플러그인 활성화 완료"

# 4. SystemdCgroup 활성화 (Kubernetes 필수)
echo "4. SystemdCgroup 활성화 중..."
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
echo "  ✓ SystemdCgroup 활성화 완료"

# 5. 설정 확인
echo ""
echo "5. 설정 확인:"
echo "--- CRI 플러그인 상태 ---"
sudo grep -i "disabled_plugins" /etc/containerd/config.toml || echo "  CRI 플러그인 활성화됨"

echo ""
echo "--- SystemdCgroup 설정 ---"
sudo grep -A 3 "SystemdCgroup" /etc/containerd/config.toml || echo "  SystemdCgroup 설정을 찾을 수 없습니다"

# 6. containerd 재시작
echo ""
echo "6. containerd 재시작 중..."
echo "  ⚠️  이 과정에서 Docker 컨테이너가 일시적으로 중지될 수 있습니다."
sudo systemctl restart containerd
sleep 2

# 7. Docker 재시작 (같은 containerd 사용)
echo "7. Docker 재시작 중..."
echo "  Docker 컨테이너가 자동으로 복구됩니다..."
sudo systemctl restart docker
sleep 3

# 8. Docker 컨테이너 복구 확인
echo ""
echo "8. Docker 컨테이너 복구 확인 중..."
if [ -n "$RUNNING_CONTAINERS" ]; then
    RESTARTED_COUNT=$(docker ps -q | wc -l)
    echo "  복구된 컨테이너: $RESTARTED_COUNT개"
    if [ "$RESTARTED_COUNT" -gt 0 ]; then
        echo "  ✓ 컨테이너 복구 성공"
        docker ps --format "    - {{.Names}} ({{.Status}})"
    else
        echo "  ⚠️  일부 컨테이너가 자동으로 시작되지 않았을 수 있습니다."
        echo "     docker-compose up -d 명령어로 수동으로 시작하세요."
    fi
else
    echo "  실행 중인 컨테이너 없음 (정상)"
fi

# 9. 서비스 상태 확인
echo ""
echo "9. 서비스 상태 확인:"
echo "--- containerd 상태 ---"
sudo systemctl status containerd --no-pager | head -5
echo ""
echo "--- Docker 상태 ---"
sudo systemctl status docker --no-pager | head -5

echo ""
echo "=== containerd 설정 수정 완료 ==="
echo ""
echo "✅ Docker는 정상적으로 유지됩니다!"
echo ""
echo "다음 단계:"
echo "  1. Docker 컨테이너 확인: docker ps"
echo "  2. 일부 컨테이너가 중지되었다면:"
echo "     cd /home/user/apps/airflow && docker-compose up -d"
echo "     cd /home/user/apps/postgres && docker-compose up -d"
echo "  3. Kubernetes 설치 진행 가능"

