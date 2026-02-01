# 🐳 Docker Private Registry Web Interface

이 문서는 Docker Private Registry 웹 인터페이스에 대한 설명입니다.

## 📋 개요

이 웹 인터페이스는 Docker Private Registry를 쉽게 관리하고 사용할 수 있도록 제공되는 웹 기반 도구입니다.

**접속 URL**: `http://{REGISTRY_WEB_HOST}:9000` (배포 시 실제 호스트로 교체)

## 🌐 주요 기능

### 1. 홈 페이지 (`/`)

Registry 리소스 다운로드 서버의 메인 페이지입니다.

**제공 기능:**

- SSL/TLS 인증서 다운로드
- 설치 및 설정 스크립트 다운로드
- 문서 뷰어 접근
- Registry 이미지 목록 보기

### 2. 인증서 다운로드 (`/certs/`)

Registry에 연결하기 위한 SSL 인증서를 다운로드할 수 있습니다.

**다운로드 방법:**

```bash
curl http://{REGISTRY_WEB_HOST}:9000/certs/domain.crt -o domain.crt
```

**또는 웹 브라우저에서:**

- `http://{REGISTRY_WEB_HOST}:9000/certs/` 접속
- `domain.crt` 파일 다운로드

### 3. 스크립트 다운로드 (`/scripts/`)

Docker 설치 및 Registry 설정을 위한 자동화 스크립트를 다운로드할 수 있습니다.

**제공 스크립트:**

- `install-docker.sh` - Docker 자동 설치 스크립트
- `setup-docker-registry.sh` - Registry 설정 스크립트

**다운로드 방법:**

```bash
curl http://{REGISTRY_WEB_HOST}:9000/scripts/install-docker.sh -o install-docker.sh
curl http://{REGISTRY_WEB_HOST}:9000/scripts/setup-docker-registry.sh -o setup-docker-registry.sh
```

### 4. 문서 뷰어 (`/docs-viewer.html`)

Registry 관련 모든 문서를 웹에서 확인할 수 있습니다.

**제공 문서:**

- `README.md` - 메인 가이드
- `REGISTRY_USAGE_GUIDE.md` - Private Registry 사용법 가이드 ⭐
- `DOCKER_INSTALL_GUIDE.md` - Docker 설치 가이드
- `EXTERNAL_CLIENT_GUIDE.md` - 외부 클라이언트 설정 가이드
- `CERT_DOWNLOAD_GUIDE.md` - 인증서 다운로드 가이드

**접속 방법:**

- `http://{REGISTRY_WEB_HOST}:9000/docs-viewer.html`
- 또는 홈 페이지에서 "View All Documentation" 버튼 클릭

### 5. Registry 이미지 목록 (`/registry-list`)

Registry에 저장된 모든 Docker 이미지 목록을 확인하고 Dockerfile을 다운로드할 수 있습니다.

**주요 기능:**

- 이미지 목록 조회
- 이미지 클릭 시 Dockerfile 다운로드
- 이미지 정보 확인
- Auto-refresh 기능 (30초마다 자동 새로고침)
- 수동 새로고침 버튼
- 실시간 새로고침 상태 표시

**접속 방법:**

- `http://{REGISTRY_WEB_HOST}:9000/registry-list`
- 또는 홈 페이지에서 "View Image List" 버튼 클릭

**Auto-refresh 기능:**

- 우측 상단의 토글 스위치로 자동 새로고침 ON/OFF 설정
- 기본값: ON (30초마다 자동 새로고침)
- 수동 새로고침 버튼 클릭 시 자동으로 OFF로 전환

**Dockerfile 다운로드:**

- 이미지 카드를 클릭하거나 "Download Dockerfile" 버튼 클릭
- 파일명: `Dockerfile` (확장자 없음)
- 자동으로 재구성된 Dockerfile을 브라우저에서 다운로드

## 🔧 기술 스택

- **웹 서버**: Nginx (Alpine)
- **프론트엔드**: HTML5, CSS3, JavaScript (Vanilla)
- **마크다운 렌더링**: Marked.js (CDN)
- **Registry API**: Docker Registry API v2

## 📁 디렉토리 구조

```text
registry/
├── config/                 # 설정 파일
│   ├── nginx-cert-server.conf  # Nginx 설정
│   └── openssl-san.cnf        # OpenSSL 설정
├── docs/                   # 문서
│   ├── DOCKER_INSTALL_GUIDE.md
│   ├── EXTERNAL_CLIENT_GUIDE.md
│   └── CERT_DOWNLOAD_GUIDE.md
├── scripts/                # 스크립트
│   ├── install-docker.sh
│   └── setup-docker-registry.sh
├── web/                    # 웹 파일
│   ├── css/                   # 스타일 시트
│   │   ├── common.css            # 공통 스타일
│   │   ├── registry-list.css     # 이미지 목록 페이지 스타일
│   │   ├── docs-viewer.css       # 문서 뷰어 스타일
│   │   ├── certs-index.css       # 인증서 페이지 스타일
│   │   └── scripts-index.css     # 스크립트 페이지 스타일
│   ├── js/                    # JavaScript 파일
│   │   ├── common.js             # 공통 JavaScript
│   │   ├── index.js              # 홈 페이지 JavaScript
│   │   ├── registry-list.js      # 이미지 목록 페이지 JavaScript
│   │   └── docs-viewer.js        # 문서 뷰어 JavaScript
│   ├── index.html             # 홈 페이지
│   ├── registry-list.html     # 이미지 목록 페이지
│   ├── docs-viewer.html       # 문서 뷰어
│   ├── certs-index.html       # 인증서 목록 페이지
│   └── scripts-index.html     # 스크립트 목록 페이지
├── LICENSE                 # 라이선스 파일
├── README.md              # 웹 인터페이스 설명 (이 파일)
└── docker-compose.yml      # Docker Compose 설정
```

## 🚀 시작하기

### 1. 웹 인터페이스 접속

브라우저에서 다음 URL로 접속:

```text
http://{REGISTRY_WEB_HOST}:9000
```

### 2. 인증서 다운로드

1. 홈 페이지에서 "Browse Certificates Directory" 클릭
2. 또는 직접 `/certs/` 경로 접속
3. `domain.crt` 파일 다운로드

### 3. 스크립트 다운로드

1. 홈 페이지에서 "Browse Scripts Directory" 클릭
2. 또는 직접 `/scripts/` 경로 접속
3. 필요한 스크립트 다운로드

### 4. 이미지 목록 확인

1. 홈 페이지에서 "View Image List" 클릭
2. Registry에 저장된 이미지 목록 확인
3. 이미지 클릭하여 Dockerfile 다운로드
4. 🗑️ Delete 버튼으로 불필요한 이미지 삭제 가능

### 5. Registry 사용법 (Push/Pull) ⭐

**이미지 업로드:**

```bash
# 1. 이미지 태그
docker tag my-app:latest {REGISTRY_HOST}:5000/my-app:latest

# 2. Registry로 업로드
docker push {REGISTRY_HOST}:5000/my-app:latest
```

**이미지 다운로드:**

```bash
# Registry에서 이미지 다운로드
docker pull {REGISTRY_HOST}:5000/my-app:latest
```

자세한 사용법은 [Registry 사용법 가이드](./docs/REGISTRY_USAGE_GUIDE.md)를 참고하세요.

## 📖 문서 읽기

1. 홈 페이지에서 "View All Documentation" 클릭
2. 드롭다운에서 원하는 문서 선택
3. 마크다운 형식으로 렌더링된 문서 확인

## 🔗 관련 링크

- **Registry URL**: `https://{REGISTRY_HOST}:5000`
- **웹 인터페이스**: `http://{REGISTRY_WEB_HOST}:9000`
- **인증서 다운로드**: `http://{REGISTRY_WEB_HOST}:9000/certs/`
- **스크립트 다운로드**: `http://{REGISTRY_WEB_HOST}:9000/scripts/`
- **이미지 목록**: `http://{REGISTRY_WEB_HOST}:9000/registry-list`
- **문서 뷰어**: `http://{REGISTRY_WEB_HOST}:9000/docs-viewer.html`

## 🔒 보안

- SSL/TLS 인증서는 HTTPS를 통해 안전하게 제공됩니다
- 개인키(`domain.key`)는 웹을 통해 제공되지 않습니다
- 모든 리소스는 읽기 전용(ro)으로 마운트됩니다

## 🛠️ 문제 해결

### 페이지가 로드되지 않는 경우

```bash
# 컨테이너 상태 확인
docker ps | grep registry-cert-server

# 컨테이너 재시작
docker restart registry-cert-server

# 로그 확인
docker logs registry-cert-server
```

### API 요청이 실패하는 경우

```bash
# Registry 서비스 상태 확인
docker ps | grep registry

# 네트워크 연결 확인
docker network inspect storage_network
```

## 📚 추가 문서

- [REGISTRY_USAGE_GUIDE.md](./docs/REGISTRY_USAGE_GUIDE.md) - **Private Registry 사용법 가이드** ⭐
- [DOCKER_INSTALL_GUIDE.md](./docs/DOCKER_INSTALL_GUIDE.md) - Docker 설치 가이드
- [EXTERNAL_CLIENT_GUIDE.md](./docs/EXTERNAL_CLIENT_GUIDE.md) - 외부 클라이언트 설정 가이드
- [CERT_DOWNLOAD_GUIDE.md](./docs/CERT_DOWNLOAD_GUIDE.md) - 인증서 다운로드 가이드
