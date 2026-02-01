#!/bin/bash
# Docker 복구 스크립트

echo "=== Docker 복구 시작 ==="

# 0. Docker 설정 파일 확인 및 생성 (data-root: /media/de/docker)
echo "0. Docker 설정 확인 중..."
if [ ! -f /etc/docker/daemon.json ]; then
    echo "  Docker 설정 파일 생성 중..."
    sudo mkdir -p /etc/docker
    sudo tee /etc/docker/daemon.json > /dev/null <<EOF
{
  "data-root": "/media/de/docker",
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "3"
  }
}
EOF
    echo "  ✓ Docker 설정 파일 생성 완료 (data-root: /media/de/docker)"
else
    echo "  ✓ Docker 설정 파일 존재 확인"
    echo "  현재 설정:"
    cat /etc/docker/daemon.json | grep -A 1 "data-root"
fi

# 1. Docker 재설치
echo ""
echo "1. Docker 재설치 중..."
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 2. Docker 서비스 unmask
echo ""
echo "2. Docker 서비스 unmask 중..."
sudo systemctl unmask docker

# 3. Docker 서비스 시작
echo ""
echo "3. Docker 서비스 시작 중..."
sudo systemctl start docker
sudo systemctl enable docker

# 4. Docker 상태 확인
echo ""
echo "4. Docker 상태 확인..."
sudo systemctl status docker --no-pager | head -15

# 5. Docker 데이터 경로 확인
echo ""
echo "5. Docker 데이터 경로 확인..."
docker info 2>/dev/null | grep -i "docker root dir" || echo "  Docker가 아직 시작되지 않았습니다."

# 6. 컨테이너 확인
echo ""
echo "6. 컨테이너 상태 확인..."
docker ps -a

echo ""
echo "=== Docker 복구 완료 ==="
echo ""
echo "컨테이너를 재시작하려면:"
echo "  cd /home/user/apps/airflow && docker-compose up -d"
echo "  cd /home/user/apps/postgres && docker-compose up -d"

