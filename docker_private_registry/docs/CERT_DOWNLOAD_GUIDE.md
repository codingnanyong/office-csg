# 🐳 Docker Registry 인증서 다운로드 가이드

## 🌐 인증서 다운로드 서버

Registry와 함께 인증서 다운로드를 위한 웹 서버가 실행됩니다.

- **URL**: `http://{REGISTRY_HOST}:8080/certs/`
- **인증서 다운로드**: `http://{REGISTRY_HOST}:8080/certs/domain.crt`

## 📥 인증서 다운로드 방법

### 방법 1: curl 사용

```bash
# 인증서 다운로드
curl http://{REGISTRY_HOST}:8080/certs/domain.crt -o domain.crt

# 예시 ({REGISTRY_HOST} 서버의 경우)
curl http://{REGISTRY_HOST}:8080/certs/domain.crt -o domain.crt
```

### 방법 2: wget 사용

```bash
# 인증서 다운로드
wget http://{REGISTRY_HOST}:8080/certs/domain.crt -O domain.crt

# 예시
wget http://{REGISTRY_HOST}:8080/certs/domain.crt -O domain.crt
```

### 방법 3: 웹 브라우저 사용

브라우저에서 다음 URL을 열어 다운로드:
```
http://{REGISTRY_HOST}:8080/certs/domain.crt
```

### 방법 4: 웹 브라우저에서 디렉토리 목록 확인

브라우저에서 다음 URL을 열어 사용 가능한 인증서 파일 확인:
```
http://{REGISTRY_HOST}:8080/certs/
```

## 🔒 보안 고려사항

### 현재 설정

- ✅ **domain.crt**: 다운로드 가능 (공개 인증서)
- ❌ **domain.key**: 다운로드 불가 (보안상 비공개)

### 개인키 보호

`domain.key` 파일은 보안상 웹 서버를 통해 제공하지 않습니다.
필요한 경우 직접 서버에 접속하여 안전하게 전송하세요.

## 🚀 서버 시작

```bash
cd /home/de/apps/registry
docker-compose up -d
```

## 📋 서버 상태 확인

```bash
# 컨테이너 상태 확인
docker-compose ps

# 로그 확인
docker-compose logs cert-server

# 인증서 다운로드 테스트
curl -I http://localhost:8080/certs/domain.crt
```

## 🔧 포트 변경

인증서 서버의 포트를 변경하려면 `docker-compose.yml`에서 수정:

```yaml
cert-server:
  ports:
    - "원하는포트:80"  # 예: "9000:80"
```

## 📝 사용 예시

### 클라이언트에서 인증서 다운로드 및 설치 (Linux)

```bash
# 1. 인증서 다운로드
curl http://{REGISTRY_HOST}:8080/certs/domain.crt -o domain.crt

# 2. 인증서 설치
sudo cp domain.crt /usr/local/share/ca-certificates/registry.crt
sudo update-ca-certificates

# 3. Docker 재시작
sudo systemctl restart docker
```

### 클라이언트에서 인증서 다운로드 및 설치 (Windows)

```powershell
# 1. 인증서 다운로드
Invoke-WebRequest -Uri "http://{REGISTRY_HOST}:8080/certs/domain.crt" -OutFile "domain.crt"

# 2. 인증서 설치 (관리자 권한 필요)
certutil -addstore -f "ROOT" domain.crt
```

## 🌐 외부 접근 설정

외부에서 접근하려면 방화벽에서 포트 8080을 열어야 합니다:

```bash
# UFW 사용 (Ubuntu)
sudo ufw allow 8080/tcp

# firewalld 사용 (CentOS/RHEL)
sudo firewall-cmd --add-port=8080/tcp --permanent
sudo firewall-cmd --reload

# iptables 직접 사용
sudo iptables -A INPUT -p tcp --dport 8080 -j ACCEPT
```

## ✅ 확인 방법

서버에서:

```bash
# 인증서 서버 동작 확인
curl http://localhost:8080/certs/domain.crt | head -5

# HTTP 상태 확인
curl -I http://localhost:8080/certs/domain.crt
```

클라이언트에서:

```bash
# 원격에서 인증서 다운로드 테스트
curl http://{REGISTRY_HOST}:8080/certs/domain.crt -o /tmp/test.crt

# 다운로드한 인증서 확인
openssl x509 -in /tmp/test.crt -text -noout | head -20
```
