# 🐳 Docker 설치 가이드

이 문서는 Docker와 Docker Compose를 설치하는 방법을 안내합니다.

## 📋 사전 요구사항

- Linux, Windows, 또는 macOS 운영체제
- 관리자 권한 (sudo 또는 root)
- 인터넷 연결

## 🐧 Linux 설치

### 방법 1: 자동 설치 스크립트 사용 (권장)

Registry 서버에서 제공하는 자동 설치 스크립트를 사용할 수 있습니다:

```bash
# 스크립트 다운로드
curl http://{REGISTRY_HOST}:9000/scripts/install-docker.sh -o install-docker.sh

# 실행 권한 부여
chmod +x install-docker.sh

# 스크립트 실행
sudo ./install-docker.sh
```

이 스크립트는 다음을 자동으로 수행합니다:

- 기존 Docker 제거 (선택사항)
- 필수 패키지 설치
- Docker 공식 저장소 추가
- Docker Engine, CLI, Containerd, Buildx, Compose 설치
- Docker 서비스 시작 및 자동 시작 설정
- 현재 사용자를 docker 그룹에 추가

### 방법 2: 공식 Docker 저장소에서 설치

#### Ubuntu/Debian

```bash
# 기존 Docker 제거 (있는 경우)
sudo apt-get remove docker docker-engine docker.io containerd runc

# 필수 패키지 설치
sudo apt-get update
sudo apt-get install ca-certificates curl gnupg lsb-release

# Docker 공식 GPG 키 추가
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Docker 저장소 추가
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Docker 설치
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Docker 서비스 시작 및 자동 시작 설정
sudo systemctl start docker
sudo systemctl enable docker

# 현재 사용자를 docker 그룹에 추가 (sudo 없이 사용하기 위해)
sudo usermod -aG docker $USER
```

**참고:** 그룹 변경 후에는 로그아웃 후 다시 로그인하거나 `newgrp docker` 명령을 실행해야 합니다.

#### CentOS/RHEL

```bash
# 기존 Docker 제거 (있는 경우)
sudo yum remove docker docker-client docker-client-latest docker-common docker-latest docker-latest-logrotate docker-logrotate docker-engine

# 필수 패키지 설치
sudo yum install -y yum-utils

# Docker 저장소 추가
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo

# Docker 설치
sudo yum install docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Docker 서비스 시작 및 자동 시작 설정
sudo systemctl start docker
sudo systemctl enable docker

# 현재 사용자를 docker 그룹에 추가
sudo usermod -aG docker $USER
```

#### Fedora

```bash
# 기존 Docker 제거 (있는 경우)
sudo dnf remove docker docker-client docker-client-latest docker-common docker-latest docker-latest-logrotate docker-logrotate docker-engine

# 필수 패키지 설치
sudo dnf install -y dnf-plugins-core

# Docker 저장소 추가
sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo

# Docker 설치
sudo dnf install docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Docker 서비스 시작 및 자동 시작 설정
sudo systemctl start docker
sudo systemctl enable docker

# 현재 사용자를 docker 그룹에 추가
sudo usermod -aG docker $USER
```

### 방법 3: 간단한 설치 (Ubuntu 기본 저장소)

```bash
sudo apt update
sudo apt install docker.io docker-compose
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
# 로그아웃 후 다시 로그인 필요
```

**참고:** 이 방법은 최신 버전이 아닐 수 있습니다. 가능하면 공식 저장소를 사용하는 것을 권장합니다.

## 🪟 Windows 설치

### Docker Desktop 설치 (Windows)

1. [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop) 다운로드
2. 설치 프로그램 실행 및 설치 완료
3. Docker Desktop 실행
4. 시스템 재시작 (필요한 경우)

### 요구사항 (Windows)

- Windows 10 64-bit: Pro, Enterprise, or Education (Build 15063 이상)
- Windows 11 64-bit: Home 또는 Pro (Build 22000 이상)
- WSL 2 기능 활성화
- 가상화 기능 활성화 (BIOS에서)

### 설치 확인 (Windows)

```powershell
docker --version
docker compose version
docker run hello-world
```

## 🍎 macOS 설치

### Docker Desktop 설치 (macOS)

1. [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop) 다운로드
   - Intel Chip: Intel Chip용 다운로드
   - Apple Silicon: Apple Silicon용 다운로드
2. 설치 프로그램 실행 및 설치 완료
3. Docker Desktop 실행

### 요구사항 (macOS)

- macOS 10.15 이상
- 최소 4GB RAM
- VirtualBox 이전 버전 제거 (있는 경우)

### 설치 확인 (macOS)

```bash
docker --version
docker compose version
docker run hello-world
```

## ✅ 설치 확인

설치가 완료된 후 다음 명령어로 확인하세요:

```bash
# Docker 버전 확인
docker --version

# Docker Compose 버전 확인
docker compose version

# Docker 서비스 상태 확인 (Linux)
sudo systemctl status docker

# 간단한 테스트 실행
docker run hello-world
```

## 🔧 문제 해결

### 권한 오류 발생 시

```bash
# docker 그룹에 사용자 추가
sudo usermod -aG docker $USER

# 그룹 변경 적용 (로그아웃/로그인 또는)
newgrp docker

# 또는 새 터미널 세션 시작
```

### Docker 서비스가 시작되지 않는 경우

```bash
# 서비스 상태 확인
sudo systemctl status docker

# 서비스 재시작
sudo systemctl restart docker

# 로그 확인
sudo journalctl -u docker
```

### Windows에서 WSL 2 오류

1. Windows 기능에서 "Linux용 Windows 하위 시스템" 활성화
2. [WSL 2 Linux 커널 업데이트 패키지](https://aka.ms/wsl2kernel) 다운로드 및 설치
3. PowerShell에서 `wsl --set-default-version 2` 실행

### macOS에서 권한 오류

1. Docker Desktop이 실행 중인지 확인
2. 시스템 환경설정 → 보안 및 개인 정보 보호에서 Docker 허용
3. Docker Desktop 재시작

## 📚 추가 리소스

- [Docker 공식 문서](https://docs.docker.com/)
- [Docker Compose 문서](https://docs.docker.com/compose/)
- [Docker 설치 가이드](https://docs.docker.com/get-docker/)
