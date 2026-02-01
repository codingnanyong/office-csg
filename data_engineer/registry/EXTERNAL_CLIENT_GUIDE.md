# 🌐 외부 클라이언트 설정 가이드

이 가이드는 Docker Private Registry에 외부 클라이언트에서 접근하는 방법을 설명합니다.

## 📋 사전 요구사항

- Docker가 설치된 클라이언트 시스템
- Registry 서버에 네트워크 접근 권한
- Registry 인증서 파일 (`domain.crt`)

## 🔐 Registry 정보

- **Registry URL**: `https://{REGISTRY_HOST}:5000`
- **인증서 위치**: 이 저장소의 `domain.crt` 파일
- **접근 방법**: Git 저장소에서 직접 사용

## 🐧 Linux 클라이언트 설정

### 1. Docker 설치 (아직 설치되지 않은 경우)

```bash
sudo apt update
sudo apt install docker.io
sudo systemctl start docker
sudo usermod -aG docker $USER
# 로그아웃 후 다시 로그인 필요
```

### 2. Registry 인증서 준비

```bash
# 현재 디렉토리에 인증서가 있는지 확인
ls -la domain.crt

# 또는 다른 위치에서 작업하는 경우 인증서 복사
cp domain.crt ./
```

### 3. 인증서 설치

```bash
sudo cp domain.crt /usr/local/share/ca-certificates/registry.crt
sudo update-ca-certificates
```

### 4. Docker 데몬 설정

```bash
sudo mkdir -p /etc/docker
sudo nano /etc/docker/daemon.json
```

다음 내용을 추가하세요:

```json
{
  "insecure-registries": ["{REGISTRY_HOST}:5000"]
}
```

### 5. Docker 재시작

```bash
sudo systemctl restart docker
```

### 6. 연결 테스트

```bash
# Registry 카탈로그 확인
curl https://{REGISTRY_HOST}:5000/v2/_catalog

# 이미지 다운로드 테스트
docker pull {REGISTRY_HOST}:5000/your-image
```

## 🪟 Windows 클라이언트 설정

### 1. Docker Desktop 설치

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) 다운로드 및 설치

### 2. Registry 인증서 준비

```powershell
# 현재 디렉토리에 인증서가 있는지 확인
dir domain.crt

# 또는 다른 위치에서 작업하는 경우 인증서 복사
copy domain.crt .\
```

### 3. 인증서 설치

```powershell
# 관리자 권한으로 PowerShell 실행
certutil -addstore -f "ROOT" domain.crt

# 또는 Git 저장소의 인증서를 직접 사용
certutil -addstore -f "ROOT" domain.crt
```

### 4. Docker Desktop 설정

1. Docker Desktop 열기
2. Settings → Docker Engine로 이동
3. JSON 설정에 다음 내용 추가:

```json
{
  "insecure-registries": ["{REGISTRY_HOST}:5000"]
}
```

4. Apply & Restart 클릭

### 5. 연결 테스트

```cmd
# Registry 카탈로그 확인
curl -k https://{REGISTRY_HOST}:5000/v2/_catalog

# 이미지 다운로드 테스트
docker pull {REGISTRY_HOST}:5000/your-image
```

## 📦 Registry 사용법

### 이미지 다운로드

```bash
# 특정 이미지 다운로드
docker pull {REGISTRY_HOST}:5000/{image name}

# 사용 가능한 이미지 목록 확인
curl https://{REGISTRY_HOST}:5000/v2/_catalog

# 특정 이미지의 태그 목록 확인
curl https://{REGISTRY_HOST}:5000/v2/{image name}/tags/list
```

### 이미지 업로드 (권한이 있는 경우)

```bash
# 이미지 태그 지정
docker tag your-image {REGISTRY_HOST}:5000/your-image

# 이미지 업로드
docker push {REGISTRY_HOST}:5000/your-image

# 또는 기존 이미지에 새 태그 지정 후 업로드
docker tag nginx:latest {REGISTRY_HOST}:5000/nginx:v1.0
docker push {REGISTRY_HOST}:5000/nginx:v1.0
```

## 🔧 문제 해결

### 인증서 오류

```bash
# 인증서 상태 확인
openssl x509 -in domain.crt -text -noout

# 시스템 인증서 재설정
sudo update-ca-certificates --fresh

# Docker 인증서 디렉토리 확인
ls -la /etc/docker/certs.d/{REGISTRY_HOST}:5000/

# 수동으로 인증서 복사 (필요한 경우)
sudo mkdir -p /etc/docker/certs.d/{REGISTRY_HOST}:5000/
sudo cp domain.crt /etc/docker/certs.d/{REGISTRY_HOST}:5000/ca.crt
```

### 연결 거부

```bash
# 네트워크 연결 확인
ping {REGISTRY_HOST}

# 포트 접근 확인
telnet {REGISTRY_HOST} 5000
```

### Docker 권한 문제

```bash
# 사용자 그룹 확인
groups $USER

# Docker 그룹에 사용자 추가
sudo usermod -aG docker $USER
```

## 🔧 환경 변수 설정

이 가이드에서 사용되는 변수들을 사용자의 환경에 맞게 설정하세요:

- `{REGISTRY_HOST}`: Registry 서버 IP 주소 또는 도메인 (예: `registry.example.com`)

## 📁 인증서 파일

이 저장소에는 이미 `domain.crt` 파일이 포함되어 있습니다. 별도로 인증서를 다운로드할 필요 없이 이 파일을 사용하세요.

## 📞 지원

문제가 발생하면 다음 정보와 함께 문의하세요:

- 클라이언트 OS 및 버전
- Docker 버전
- 오류 메시지
- 시도한 해결 방법
