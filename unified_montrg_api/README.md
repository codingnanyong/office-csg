# Unified Monitoring API

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115.0-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0.34-1C1C1C?logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Deployed-326CE5?logo=kubernetes&logoColor=white)](https://kubernetes.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Supported-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![MySQL](https://img.shields.io/badge/MySQL-Supported-4479A1?logo=mysql&logoColor=white)](https://www.mysql.com/)

FastAPI 기반의 Kubernetes 클러스터용 모니터링 API 골격입니다.

## 빠른 시작

```bash
# 의존성 설치 (Poetry 사용)
poetry install

# 애플리케이션 실행
poetry run uvicorn app.main:app --reload

# 또는 requirements.txt 기반 가상환경 구성
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

**로컬 개발용:** `.env` 파일을 루트에 생성하여 로컬에서 실행 시 설정을 재정의할 수 있습니다.  
**Kubernetes 배포:** 데이터베이스 연결 정보는 Secret으로 관리되며, `k8s/secret.yaml` 파일에서 설정합니다. 민감한 정보이므로 Git에 커밋하지 마세요.

## 테스트

```bash
poetry run pytest
```

## Docker 이미지 빌드

### Oracle Instant Client 설치 (CMMS 데이터베이스 연결용)

Oracle 데이터베이스(CMMS)에 연결하려면 Oracle Instant Client가 필요합니다.

1. **Oracle Instant Client 다운로드**
   - Oracle 공식 사이트: https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html
   - **Instant Client Basic** 패키지 다운로드 (예: `instantclient-basic-linux.x64-23.7.0.25.01.zip`)
   - Oracle 계정이 필요할 수 있습니다 (무료 등록 가능)

2. **Docker 이미지 빌드**
   ```bash
   # Oracle Instant Client ZIP 파일을 docker/ 디렉토리에 복사
   cp instantclient-basic-linux.x64-23.7.0.25.01.zip docker/instantclient-basic.zip
   
   # 이미지 빌드 (로컬 태그: csg/unified-montrg-api:latest)
   docker build -f docker/Dockerfile -t csg/unified-montrg-api:latest .
   
   # 또는 빌드 인자로 ZIP 파일명 지정
   docker build -f docker/Dockerfile \
     --build-arg INSTANTCLIENT_ZIP=instantclient-basic-linux.x64-23.7.0.25.01.zip \
     -t csg/unified-montrg-api:latest .
   ```

3. **빌드 확인**
   ```bash
   # (kubeadm 환경) containerd에 이미지 적재
   docker save csg/unified-montrg-api:latest -o unified-montrg-api.tar
   sudo ctr -n k8s.io images import unified-montrg-api.tar
   
   # 로컬 실행 예시
   docker run --rm -p 8000:8000 --env-file .env csg/unified-montrg-api:latest
   ```

**참고:**
- Oracle Instant Client ZIP 파일이 없으면 빌드는 성공하지만 경고가 표시됩니다
- 이 경우 thin mode를 사용하려고 시도하지만, 일부 Oracle 서버 버전은 thin mode를 지원하지 않을 수 있습니다
- CMMS 데이터베이스 연결이 필요한 경우 반드시 Oracle Instant Client를 포함하여 빌드해야 합니다

`docker/.dockerignore`에 정의된 패턴은 루트 `.dockerignore` 심볼릭 링크를 통해 빌드 컨텍스트에 적용됩니다.

## Kubernetes 배포

### 사전 준비

1. **Secret 설정**: 데이터베이스 연결 정보를 Secret으로 관리합니다.

```bash
# secret.yaml 파일 생성 (예시는 secret.example.yaml 참고)
# Base64로 인코딩된 데이터베이스 URL을 설정
kubectl apply -f k8s/secret.yaml
```

`k8s/secret.yaml`에는 다음 환경 변수를 Base64 인코딩하여 설정합니다:
- `IP04_DATABASE_URL`: `mysql+aiomysql://<USER>:<PASSWORD>@<HOST>:<PORT>/<DB>`
- `IP12_DATABASE_URL`: `mysql+aiomysql://<USER>:<PASSWORD>@<HOST>:<PORT>/<DB>`
- `IP20_DATABASE_URL`: `mysql+aiomysql://<USER>:<PASSWORD>@<HOST>:<PORT>/<DB>`
- `IP34_DATABASE_URL`: `mysql+aiomysql://<USER>:<PASSWORD>@<HOST>:<PORT>/<DB>`
- `IP37_DATABASE_URL`: `mysql+aiomysql://<USER>:<PASSWORD>@<HOST>:<PORT>/<DB>`
- `MONTRG_DATABASE_URL`: `postgresql+asyncpg://<USER>:<PASSWORD>@<HOST>:<PORT>/<DB>`

**동적 데이터베이스 지원:**
- `IP*_DATABASE_URL` 패턴의 환경 변수를 자동으로 인식합니다
- 예: `IP04_DATABASE_URL` → `ip04`, `IP12_DATABASE_URL` → `ip12` 등
- 새로운 IP 번호를 추가하려면 Secret에 `IP**_DATABASE_URL`만 추가하면 됩니다

### 배포 자동화 스크립트 (권장)

`scripts/deploy.sh` 스크립트를 사용하면 Docker 이미지 빌드부터 Kubernetes 배포까지 전체 프로세스를 자동화합니다.

```bash
cd ~/apps/unified_montrg
./scripts/deploy.sh
```

이 스크립트는 다음 작업을 수행합니다:
1. Docker 이미지 빌드
2. 모든 노드에 이미지 배포 (containerd 환경, 고가용성을 위한 자동 배포)
3. Kubernetes 리소스 적용 (Namespace → Secret → ConfigMap → Service → Deployment → Ingress Controller → Ingress)
4. Deployment 재시작
5. Pod 상태 확인

**고가용성 특징:**
- 애플리케이션 Pod는 **5개의 Replica**로 실행 (기본 설정)
- Ingress Controller는 **DaemonSet**으로 모든 노드에 배포
- 어떤 노드 IP로 접근해도 동일하게 작동하며 **자동 로드밸런싱**
- 노드 또는 Pod 장애 시에도 서비스 계속 제공

**환경 변수 커스터마이징:**
```bash
IMAGE_NAME=my-registry/unified-montrg:1.0.0 \
NAMESPACE=my-namespace \
./scripts/deploy.sh
```

### 수동 배포

스크립트 대신 수동으로 배포하려면 다음 순서로 매니페스트를 적용하세요:

```bash
kf=~/apps/unified_montrg/k8s

# 1. Namespace 생성
kubectl apply -f $kf/namespace.yaml

# 2. Secret (데이터베이스 연결 정보)
kubectl apply -f $kf/secret.yaml

# 3. ConfigMap (애플리케이션 설정)
kubectl apply -f $kf/configmap.yaml

# 4. Service (ClusterIP)
kubectl apply -f $kf/service.yaml

# 5. Deployment
kubectl apply -f $kf/deployment.yaml

# 6. Ingress Controller (DaemonSet)
kubectl apply -f $kf/ingress-controller-daemonset.yaml
kubectl apply -f $kf/ingress-controller-service-patch.yaml

# 7. Ingress
kubectl apply -f $kf/ingress.yaml
```

### 접근 방법

애플리케이션은 **Ingress Controller (DaemonSet)**를 통해 접근할 수 있습니다. Ingress Controller는 모든 노드에 배포되어 있으며, **어떤 노드 IP로 접근해도 자동으로 로드밸런싱**됩니다.

**고가용성 구조:**
- Ingress Controller가 DaemonSet으로 배포되어 **모든 노드**에 실행
- NodePort 30081로 모든 노드에 동일 포트로 접근 가능
- 트래픽이 자동으로 여러 Pod에 분산 처리

**접근 URL 예시:**
- **HTTP**: `http://<노드1 IP>:30081/api` 또는 `http://<노드2 IP>:30081/api` (어느 노드든 접근 가능)
- **API 문서**: `http://<노드 IP>:30081/api/docs`
- **ReDoc**: `http://<노드 IP>:30081/api/redoc`

**예시:**
```bash
# 클러스터의 어떤 노드 IP든 사용 가능
http://10.10.100.80:30081/api     # control-plane 노드
http://10.10.100.81:30081/api     # worker 노드 (동일하게 작동)
```

### API 엔드포인트

- **IP Data API**: `/api/v1/ip-data/{pid}`
  - `db` 쿼리 파라미터로 데이터베이스 선택 (ip04, ip12, ip20, ip34, ip37 등)
  - 예: `/api/v1/ip-data/12345?db=ip04&limit=100`
  
- **Machines API**: `/api/v1/machines`
  - 계층 구조로 기계 정보 조회
  - 필터: `plant_cd`, `process_name`, `op_cd`, `line_no`, `machine_no`
  - 예: `/api/v1/machines?plant_cd=3120&process_name=IP/IU`

- **Health Check**: `/api/healthz`

### 배포 상태 확인

```bash
# Pod 상태 확인
kubectl get pods -n unified-montrg

# Deployment 상태 확인
kubectl get deployment -n unified-montrg

# Service 및 Ingress 확인
kubectl get svc,ingress -n unified-montrg

# Pod 로그 확인
kubectl logs -f deployment/unified-montrg -n unified-montrg

# Deployment 재시작 (코드 변경 후)
kubectl rollout restart deployment/unified-montrg -n unified-montrg
```


