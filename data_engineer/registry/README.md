# 🐳**Docker Private Registry 가이드**

[![Docker Registry](https://img.shields.io/badge/Docker%20Registry-Private-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/registry/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Supported-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![TLS](https://img.shields.io/badge/TLS-Secure-009639?logo=openssl&logoColor=white)](https://www.openssl.org/)

이 문서는 SSL/TLS 암호화가 적용된 **Docker Private Registry**를 설치하고 사용하는 방법을 안내합니다.

## ⚙️ 사전 요구사항

- Docker와 Docker Compose 설치
- 기본 터미널 명령어 지식
- SSL 인증서 (`domain.crt` 및 `domain.key`) 보안 접근용
- Registry 서버 네트워크 접근 권한 ({REGISTRY_HOST}:5000)

## 📝 `docker-compose.yml` 생성

다음 내용으로 `docker-compose.yml` 파일을 생성하세요:

```yaml
services:
  registry:
    image: registry:2
    container_name: registry
    ports:
      - "5000:5000"
    volumes:
      - {path}/registry:/var/lib/registry
      - {path}/registry/certs:/certs
      - {path}/registry/config.yml:/etc/docker/registry/config.yml
    restart: always
    environment:
      REGISTRY_HTTP_ADDR: :5000
      REGISTRY_HTTP_TLS_CERTIFICATE: /certs/domain.crt
      REGISTRY_HTTP_TLS_KEY: /certs/domain.key
    command: ["registry", "serve", "/etc/docker/registry/config.yml"]
    healthcheck:
      test: ["CMD-SHELL", "curl --fail https://localhost:5000/v2/_catalog || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s
    networks:
      - {network name}

networks:
  {network name}:
    driver: bridge
    external: true
```

## 🔐 SSL/TLS 인증서

SSL 인증서 (`domain.crt` 및 `domain.key`)가 `{DATA_PATH}/registry/certs` 디렉토리에 위치하는지 확인하세요.

### 자체 서명 인증서 생성

다음 명령어로 자체 서명 인증서를 생성할 수 있습니다 (테스트용):

```bash
openssl req -newkey rsa:4096 -nodes -sha256 -keyout domain.key -x509 -days 365 -out domain.crt
```

### 사용자 정의 SAN 설정 사용

운영 환경에서는 적절한 SAN (Subject Alternative Name) 설정으로 인증서를 생성하세요:

```bash
openssl req -newkey rsa:4096 -nodes -sha256 -keyout domain.key \
  -x509 -days 365 -out domain.crt \
  -config openssl-san.cnf
```

**참고:** `openssl-san.cnf` 파일이 이 저장소에 포함되어 있으며, registry 서버용 적절한 SAN 설정이 되어 있습니다.

### Registry 설정 파일

`config.yml` 파일을 생성하여 Registry 설정을 구성하세요:

```yaml
version: 0.1
log:
  level: debug
storage:
  filesystem:
    rootdirectory: /var/lib/registry
  delete:
    enabled: true
http:
  addr: :5000
  tls:
    certificate: /certs/domain.crt
    key: /certs/domain.key
  headers:
    X-Content-Type-Options: [nosniff]
health:
  storagedriver:
    enabled: true
    interval: 10s
    threshold: 3
```

## 🚀 컨테이너 실행

docker-compose.yml 파일이 있는 디렉토리로 이동한 후 실행하세요:

```bash
docker-compose up -d
```

## 📦 Private Registry 사용

### 로컬 사용 (서버)

1. **이미지 태그 지정**
   Docker 이미지에 private registry용 태그를 지정하세요:

```bash
docker tag your-image localhost:5000/your-image
```

2. **이미지 푸시**
   이미지를 private registry에 푸시하세요:

```bash
docker push localhost:5000/your-image
```

3. **이미지 풀**
   private registry에서 이미지를 풀하세요:

```bash
docker pull localhost:5000/your-image
```

### 외부 클라이언트 사용

외부 클라이언트가 registry에 접근하려면 **[EXTERNAL_CLIENT_GUIDE.md](./EXTERNAL_CLIENT_GUIDE.md)** 파일을 참조하세요.

## 🛑 컨테이너 중지 및 제거

서비스를 중지하려면 실행하세요:

```bash
docker-compose down
```

컨테이너와 볼륨을 제거하려면:

```bash
docker-compose down -v
```

## 🔧 문제 해결

### 일반적인 문제

1. **인증서 오류**:

   - 클라이언트에 인증서가 올바르게 설치되었는지 확인
   - 인증서 만료 날짜 확인
   - SAN 설정에 올바른 IP/DNS가 포함되었는지 확인

2. **연결 거부**:

   - registry 컨테이너가 실행 중인지 확인: `docker ps`
   - 포트 5000이 접근 가능한지 확인
   - 방화벽 설정 확인

3. **Docker 데몬 문제**:

   - daemon.json 문법 검증: `sudo docker daemon --validate`
   - Docker 서비스 상태 확인: `sudo systemctl status docker`
   - Docker 로그 검토: `sudo journalctl -u docker`

4. **권한 거부**:
   - 사용자가 docker 그룹에 있는지 확인: `groups $USER`
   - 그룹 변경 후 Docker 재시작
   - 인증서 파일 권한 확인

### Registry 관리

**모든 이미지 목록**:

```bash
curl https://{REGISTRY_HOST}:5000/v2/_catalog
```

**특정 이미지의 태그 목록**:

```bash
curl https://{REGISTRY_HOST}:5000/v2/{image-name}/tags/list
```

**이미지 삭제 (설정된 경우)**:

```bash
curl -X DELETE https://{REGISTRY_HOST}:5000/v2/{image-name}/manifests/{digest}
```

## 📃 License

Copyright © Changsin Inc. All rights reserved.

## 🤝 Contributing

Contributions are welcome!
Feel free to open issues or submit pull requests to improve the system.

## 📊 모니터링

### Registry 상태 확인

**Registry 상태 확인**:

```bash
curl https://{REGISTRY_HOST}:5000/v2/_catalog
```

**Registry 로그 확인**:

```bash
docker logs registry
```

**디스크 사용량 모니터링**:

```bash
du -sh {DATA_PATH}/registry/docker/
```

## 🔒 보안 고려사항

- 인증서를 안전하게 보관하고 접근을 제한하세요
- 만료 전에 정기적으로 인증서를 업데이트하세요
- registry 접근 로그를 모니터링하세요
- 운영 환경에서는 인증 구현을 고려하세요
- 관리 인터페이스에 강력한 비밀번호를 사용하세요

## ✅ 결론

이 가이드는 SSL/TLS 암호화가 적용된 **Docker Private Registry**를 설치하고 구성하며, 외부 클라이언트 접근을 관리하고 일반적인 문제를 해결하는 방법을 안내합니다. Registry는 `https://{REGISTRY_HOST}:5000`에서 접근 가능하며 안전한 이미지 push/pull 작업을 지원합니다.

**Registry URL**: `https://{REGISTRY_HOST}:5000`
**사용 가능한 이미지**: `curl https://{REGISTRY_HOST}:5000/v2/_catalog`로 확인

## 📋 디렉토리 구조

```
{DATA_PATH}/registry/
├── certs/
│   ├── domain.crt          # SSL 인증서
│   └── domain.key          # SSL 개인키
├── docker/                 # Registry 데이터 저장소
└── config.yml              # Registry 설정 파일
```

## 🔧 사전 설정

Registry를 실행하기 전에 다음 디렉토리와 파일을 생성하세요:

```bash
# 디렉토리 생성
sudo mkdir -p {DATA_PATH}/registry/certs
sudo mkdir -p {DATA_PATH}/registry/docker

# 권한 설정
sudo chown -R $USER:$USER {DATA_PATH}/registry
```

## 🔧 환경 변수 설정

다음 변수들을 사용자의 환경에 맞게 설정하세요:

- `{DATA_PATH}`: Registry 데이터 저장 경로 (예: `/media/de/data`, `/opt/registry`)
- `{REGISTRY_HOST}`: Registry 서버 IP 주소 또는 도메인
- `{NETWORK_NAME}`: Docker 네트워크 이름
