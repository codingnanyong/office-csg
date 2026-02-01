# 🐳 Docker 및 Docker Compose 기본 가이드

## 개요

이 문서는 Docker와 Docker Compose의 기본 개념과 주요 명령어를 설명합니다. Docker를 처음 사용하시거나 기본 사용법을 익히고 싶으신 분들을 위한 가이드입니다.

## Docker 기본 개념

### Docker란?

Docker는 **컨테이너 기반 가상화 플랫폼**으로, 애플리케이션을 격리된 환경에서 실행할 수 있게 해줍니다.

### 핵심 용어

- **이미지(Image)**: 컨테이너를 만들기 위한 읽기 전용 템플릿
- **컨테이너(Container)**: 이미지로부터 생성된 실행 가능한 인스턴스
- **Dockerfile**: 이미지를 빌드하기 위한 명령어들을 담은 텍스트 파일
- **Registry**: Docker 이미지를 저장하고 배포하는 저장소

---

## Docker 주요 명령어

### 이미지 관련

```bash
# 이미지 검색
docker search [이미지명]

# 이미지 다운로드
docker pull [이미지명]:[태그]

# 이미지 목록 보기
docker images

# 이미지 삭제
docker rmi [이미지ID 또는 이미지명]

# 이미지 빌드
docker build -t [이미지명]:[태그] .
```

### 컨테이너 관리

```bash
# 컨테이너 실행
docker run [옵션] [이미지명] [명령어]

# 실행 중인 컨테이너 보기
docker ps

# 모든 컨테이너 보기 (중지된 것 포함)
docker ps -a

# 컨테이너 중지
docker stop [컨테이너ID 또는 이름]

# 컨테이너 시작
docker start [컨테이너ID 또는 이름]

# 컨테이너 삭제
docker rm [컨테이너ID 또는 이름]

# 컨테이너 로그 보기
docker logs [컨테이너ID 또는 이름]

# 실행 중인 컨테이너에 접속
docker exec -it [컨테이너ID 또는 이름] /bin/bash
```

### 유용한 옵션들

```bash
# 백그라운드에서 실행
docker run -d [이미지명]

# 포트 매핑
docker run -p [호스트포트]:[컨테이너포트] [이미지명]

# 볼륨 마운트
docker run -v [호스트경로]:[컨테이너경로] [이미지명]

# 환경변수 설정
docker run -e [변수명]=[값] [이미지명]

# 컨테이너에 이름 부여
docker run --name [컨테이너명] [이미지명]
```

---

## Docker Compose 기본 개념

### Docker Compose란?

Docker Compose는 **여러 컨테이너 애플리케이션을 정의하고 실행**하기 위한 도구입니다. YAML 파일을 사용하여 애플리케이션의 서비스들을 구성할 수 있습니다.

### docker-compose.yml 예시

```yaml
version: '3.8'

services:
  web:
    build: .
    ports:
      - "8000:8000"
    volumes:
      - .:/code
    depends_on:
      - db
    environment:
      - DEBUG=1

  db:
    image: postgres:13
    volumes:
      - postgres_data:/var/lib/postgresql/data/
    environment:
      - POSTGRES_DB=myapp
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password

volumes:
  postgres_data:
```

---

## Docker Compose 주요 명령어

### 기본 명령어

```bash
# 서비스 시작 (백그라운드)
docker-compose up -d

# 서비스 중지
docker-compose down

# 서비스 재시작
docker-compose restart

# 서비스 상태 확인
docker-compose ps

# 로그 보기
docker-compose logs

# 특정 서비스 로그 보기
docker-compose logs [서비스명]
```

### 빌드 및 스케일링

```bash
# 이미지 빌드 후 시작
docker-compose up --build

# 특정 서비스만 실행
docker-compose up [서비스명]

# 서비스 스케일링
docker-compose up -d --scale [서비스명]=[개수]

# 컨테이너와 볼륨까지 모두 삭제
docker-compose down -v
```

### 개발 중 유용한 명령어

```bash
# 서비스에 접속
docker-compose exec [서비스명] bash

# 새로운 컨테이너에서 명령 실행
docker-compose run [서비스명] [명령어]

# 구성 파일 검증
docker-compose config

# 구성 파일 검증 및 출력
docker-compose config --services
```

---

## 실용적인 사용 예시

### 1. 간단한 웹 애플리케이션 실행

```bash
# nginx 웹서버 실행
docker run -d -p 80:80 --name my-nginx nginx

# 웹서버 상태 확인
docker ps

# 웹서버 중지 및 제거
docker stop my-nginx
docker rm my-nginx
```

### 2. 개발 환경 구성 (Node.js + MongoDB)

```yaml
# docker-compose.yml
version: '3.8'

services:
  app:
    build: .
    ports:
      - "3000:3000"
    volumes:
      - .:/usr/src/app
      - /usr/src/app/node_modules
    depends_on:
      - mongodb
    environment:
      - NODE_ENV=development

  mongodb:
    image: mongo:4.4
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db

volumes:
  mongodb_data:
```

```bash
# 개발 환경 시작
docker-compose up -d

# 앱 로그 확인
docker-compose logs app

# 개발 환경 중지
docker-compose down
```

### 3. 데이터베이스 백업

```bash
# PostgreSQL 컨테이너에서 덤프 생성
docker exec my-postgres pg_dump -U postgres mydb > backup.sql

# MySQL 컨테이너에서 덤프 생성
docker exec my-mysql mysqldump -u root -p mydb > backup.sql
```

### 4. 이미지 정리

```bash
# 사용하지 않는 이미지 삭제
docker image prune

# 중지된 컨테이너 모두 삭제
docker container prune

# 시스템 전체 정리 (위험!)
docker system prune

# 모든 것 삭제 (매우 위험!)
docker system prune -a
```

---

## 🔗 관련 문서

- **[Docker 설치 가이드](DOCKER_INSTALL_GUIDE.md)** - Docker 설치 방법
- **[Registry 사용 가이드](REGISTRY_USAGE_GUIDE.md)** - Private Registry 사용법
- **[External Client 가이드](EXTERNAL_CLIENT_GUIDE.md)** - 외부 클라이언트 연결 방법

---

## 💡 팁과 모범 사례

### Dockerfile 작성 팁

1. **최소한의 베이스 이미지 사용**: `alpine` 기반 이미지 선택
2. **멀티 스테이지 빌드**: 최종 이미지 크기 최소화
3. **.dockerignore 파일**: 불필요한 파일 제외
4. **캐시 활용**: 자주 변경되는 레이어를 뒤쪽에 배치

### 보안 모범 사례

1. **루트 사용자 피하기**: 별도 사용자 생성
2. **민감한 정보**: 환경 변수나 시크릿 사용
3. **이미지 스캔**: 보안 취약점 정기 검사
4. **최신 베이스 이미지**: 정기적 업데이트

### 성능 최적화

1. **레이어 최소화**: 명령어 통합
2. **볼륨 활용**: 데이터 영속성과 성능 향상
3. **리소스 제한**: 메모리와 CPU 제한 설정
4. **로그 관리**: 로그 순환 정책 설정

---

## ❓ 자주 묻는 질문

### Q: 컨테이너와 가상머신의 차이점은?

A: 컨테이너는 OS 커널을 공유하므로 더 가볍고 빠릅니다. 가상머신은 전체 OS를 포함하므로 더 많은 리소스를 사용합니다.

### Q: Docker 이미지가 너무 큰데 어떻게 줄이나요?

A: 멀티 스테이지 빌드, alpine 기반 이미지 사용, 불필요한 파일 제거 등의 방법을 사용하세요.

### Q: 데이터를 영구적으로 저장하려면?

A: Docker 볼륨이나 바인드 마운트를 사용하여 호스트 시스템에 데이터를 저장하세요.

### Q: 네트워크 문제가 발생하면?

A: `docker network ls`로 네트워크 확인하고, 필요시 커스텀 네트워크를 생성하여 사용하세요.

---

*이 문서는 Docker 기본 사용법에 대한 개요입니다. 더 자세한 내용은 [공식 Docker 문서](https://docs.docker.com/)를 참조하세요.*