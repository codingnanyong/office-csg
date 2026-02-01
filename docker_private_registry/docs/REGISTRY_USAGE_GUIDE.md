# 🐳 Private Docker Registry 사용 가이드

이 문서는 Private Docker Registry에 이미지를 업로드(push)하고 다운로드(pull)하는 방법을 상세히 설명합니다.

## 📋 Registry 정보

- **Registry URL**: `https://{REGISTRY_HOST}:5000`
- **웹 인터페이스**: `http://{REGISTRY_HOST}:9000`
- **프로토콜**: HTTPS (SSL/TLS 인증서 필요)

## 🔧 사전 준비

### 1. 필수 조건

Registry를 사용하기 전에 다음이 필요합니다:

- ✅ **Docker 설치**: [DOCKER_INSTALL_GUIDE.md](./DOCKER_INSTALL_GUIDE.md) 참조
- ✅ **SSL 인증서 설치**: [CERT_DOWNLOAD_GUIDE.md](./CERT_DOWNLOAD_GUIDE.md) 참조  
- ✅ **외부 클라이언트 설정**: [EXTERNAL_CLIENT_GUIDE.md](./EXTERNAL_CLIENT_GUIDE.md) 참조

### 2. Registry 연결 확인

```bash
# Registry 접근 테스트
curl -k https://{REGISTRY_HOST}:5000/v2/_catalog
```

**응답 예시:**

```json
{"repositories":["nginx","my-app","grafana"]}
```

## 📤 이미지 업로드 (Push)

### 1. 기존 이미지를 Private Registry로 Push

```bash
# 예제: nginx 이미지를 private registry로 업로드

# 1단계: 공식 이미지 다운로드
docker pull nginx:latest

# 2단계: Private registry용으로 태그 설정
docker tag nginx:latest {REGISTRY_HOST}:5000/nginx:latest

# 3단계: Private registry로 업로드
docker push {REGISTRY_HOST}:5000/nginx:latest
```

### 2. 로컬 애플리케이션 이미지 Push

```bash
# Dockerfile이 있는 디렉토리에서

# 1단계: 이미지 빌드 (바로 registry 태그로)
docker build -t {REGISTRY_HOST}:5000/my-app:v1.0 .

# 2단계: Private registry로 업로드
docker push {REGISTRY_HOST}:5000/my-app:v1.0
```

### 3. 여러 태그로 Push

```bash
# 같은 이미지를 여러 태그로 업로드
docker tag my-app:latest {REGISTRY_HOST}:5000/my-app:latest
docker tag my-app:latest {REGISTRY_HOST}:5000/my-app:v1.0
docker tag my-app:latest {REGISTRY_HOST}:5000/my-app:stable

# 모든 태그 업로드
docker push {REGISTRY_HOST}:5000/my-app:latest
docker push {REGISTRY_HOST}:5000/my-app:v1.0
docker push {REGISTRY_HOST}:5000/my-app:stable
```

## 📥 이미지 다운로드 (Pull)

### 1. 기본 Pull 방법

```bash
# 특정 태그 다운로드
docker pull {REGISTRY_HOST}:5000/nginx:latest

# 다른 태그 다운로드
docker pull {REGISTRY_HOST}:5000/my-app:v1.0
```

### 2. 다운로드 후 사용

```bash
# 이미지 다운로드
docker pull {REGISTRY_HOST}:5000/my-app:latest

# 컨테이너 실행
docker run -d --name my-container {REGISTRY_HOST}:5000/my-app:latest

# 또는 로컬 태그로 변경 후 사용
docker tag {REGISTRY_HOST}:5000/my-app:latest my-app:latest
docker run -d --name my-container my-app:latest
```

## 🏷️ 태그 관리

### 1. 이미지 태그 확인

```bash
# Registry의 특정 이미지 태그 목록 확인
curl -k https://{REGISTRY_HOST}:5000/v2/my-app/tags/list

# 로컬 이미지 목록 확인
docker images | grep {REGISTRY_HOST}:5000
```

### 2. 태그 전략

```bash
# 개발 환경
docker tag my-app:latest {REGISTRY_HOST}:5000/my-app:dev
docker push {REGISTRY_HOST}:5000/my-app:dev

# 테스트 환경
docker tag my-app:latest {REGISTRY_HOST}:5000/my-app:test
docker push {REGISTRY_HOST}:5000/my-app:test

# 프로덕션 환경
docker tag my-app:latest {REGISTRY_HOST}:5000/my-app:prod
docker push {REGISTRY_HOST}:5000/my-app:prod

# 버전 태그
docker tag my-app:latest {REGISTRY_HOST}:5000/my-app:v$(date +%Y%m%d)
docker push {REGISTRY_HOST}:5000/my-app:v$(date +%Y%m%d)
```

## 🔄 실제 사용 시나리오

### 시나리오 1: Node.js 애플리케이션 배포

```bash
# 1. Dockerfile 작성
cat > Dockerfile << EOF
FROM node:16-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3000
CMD ["node", "server.js"]
EOF

# 2. 이미지 빌드
docker build -t {REGISTRY_HOST}:5000/my-node-app:latest .

# 3. Registry로 업로드
docker push {REGISTRY_HOST}:5000/my-node-app:latest

# 4. 다른 서버에서 다운로드 및 실행
docker pull {REGISTRY_HOST}:5000/my-node-app:latest
docker run -d -p 3000:3000 --name my-app {REGISTRY_HOST}:5000/my-node-app:latest
```

### 시나리오 2: 기존 이미지 Private Registry로 마이그레이션

```bash
# 1. Docker Hub에서 이미지 목록 가져오기
docker images --format "table {{.Repository}}:{{.Tag}}" | grep -v {REGISTRY_HOST}

# 2. 각 이미지를 Private Registry로 복사
for image in $(docker images --format "{{.Repository}}:{{.Tag}}" | grep -v {REGISTRY_HOST} | grep -v "<none>"); do
    # Private Registry 태그 생성
    private_tag="{REGISTRY_HOST}:5000/${image}"

    echo "Copying $image to $private_tag"
    docker tag "$image" "$private_tag"
    docker push "$private_tag"
done
```

### 시나리오 3: CI/CD 파이프라인에서 사용

```bash
#!/bin/bash
# build-and-deploy.sh

APP_NAME="my-app"
VERSION=$(git rev-parse --short HEAD)
REGISTRY="{REGISTRY_HOST}:5000"

echo "Building $APP_NAME:$VERSION"

# 이미지 빌드
docker build -t $REGISTRY/$APP_NAME:$VERSION .
docker tag $REGISTRY/$APP_NAME:$VERSION $REGISTRY/$APP_NAME:latest

# Registry로 업로드
docker push $REGISTRY/$APP_NAME:$VERSION
docker push $REGISTRY/$APP_NAME:latest

echo "Deploy completed: $REGISTRY/$APP_NAME:$VERSION"
```

## 🛠️ 유용한 명령어 모음

### Registry 정보 확인

```bash
# Registry에 있는 모든 이미지 목록
curl -k https://{REGISTRY_HOST}:5000/v2/_catalog | jq '.repositories[]'

# 특정 이미지의 태그 목록
curl -k https://{REGISTRY_HOST}:5000/v2/nginx/tags/list | jq '.tags[]'

# 이미지 manifest 정보 확인
curl -k -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
  https://{REGISTRY_HOST}:5000/v2/nginx/manifests/latest
```

### 로컬 정리

```bash
# Private Registry 이미지만 표시
docker images | grep {REGISTRY_HOST}:5000

# 사용하지 않는 Registry 이미지 삭제
docker images | grep {REGISTRY_HOST}:5000 | awk '{print $1":"$2}' | xargs docker rmi

# 전체 시스템 정리
docker system prune -a
```

### 배치 작업

```bash
# 모든 로컬 이미지를 Private Registry로 백업
#!/bin/bash
REGISTRY="{REGISTRY_HOST}:5000"

for image in $(docker images --format "{{.Repository}}:{{.Tag}}" | grep -v "$REGISTRY" | grep -v "<none>"); do
    backup_name="$REGISTRY/backup-$(echo $image | tr '/:' '-')"
    echo "Backing up $image as $backup_name"

    docker tag "$image" "$backup_name"
    docker push "$backup_name"
done
```

## 🐞 Registry 사용 중 문제 해결

### 1. Push 실패

```bash
# 오류: repository does not exist

# 해결: 이미지 태그 확인
docker tag my-app:latest {REGISTRY_HOST}:5000/my-app:latest
docker push {REGISTRY_HOST}:5000/my-app:latest
```

### 2. Pull 실패

```bash
# 오류: pull access denied for xxx, repository does not exist

# 해결: 정확한 이미지 이름 확인
curl -k https://{REGISTRY_HOST}:5000/v2/_catalog
docker pull {REGISTRY_HOST}:5000/[정확한-이미지-이름]:latest
```

### 3. 태그 충돌

```bash
# 기존 태그와 충돌 시 강제 업데이트
docker push {REGISTRY_HOST}:5000/my-app:latest --force

# 또는 새로운 태그 사용
docker tag my-app:latest {REGISTRY_HOST}:5000/my-app:v$(date +%Y%m%d)
docker push {REGISTRY_HOST}:5000/my-app:v$(date +%Y%m%d)
```

### 4. 기본 설정 문제

SSL 인증서, 연결, 권한 관련 문제는 다음 가이드를 참조하세요:
- **인증서 문제**: [CERT_DOWNLOAD_GUIDE.md](./CERT_DOWNLOAD_GUIDE.md)
- **클라이언트 설정**: [EXTERNAL_CLIENT_GUIDE.md](./EXTERNAL_CLIENT_GUIDE.md)

## 📚 관련 문서

- [CERT_DOWNLOAD_GUIDE.md](./CERT_DOWNLOAD_GUIDE.md) - SSL 인증서 다운로드 및 설치
- [EXTERNAL_CLIENT_GUIDE.md](./EXTERNAL_CLIENT_GUIDE.md) - 외부 클라이언트 설정
- [DOCKER_INSTALL_GUIDE.md](./DOCKER_INSTALL_GUIDE.md) - Docker 설치 가이드

## 📚 외부 자료

- [Docker Registry API 문서](https://docs.docker.com/registry/spec/api/)
- [Docker 명령어 레퍼런스](https://docs.docker.com/engine/reference/commandline/docker/)


## 💡 요약

이 가이드는 **이미지 push/pull에 특화된** 사용법을 다룹니다

- 🔧 **기본 설정**: 다른 가이드 문서들을 먼저 완료하세요
- 📤 **Push**: `docker tag` → `docker push` 순서
- 📥 **Pull**: `docker pull` 직접 사용
- 🏷️ **태그 관리**: 환경별, 버전별 태그 전략 활용

---
이 가이드로 Private Docker Registry를 효과적으로 활용하세요.