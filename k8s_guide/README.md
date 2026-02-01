# CKA 시험 준비용 Kubeadm 클러스터 구축 가이드

[![Kubernetes](https://img.shields.io/badge/Kubernetes-kubeadm-326CE5?logo=kubernetes&logoColor=white)](https://kubernetes.io/)
[![CKA](https://img.shields.io/badge/CKA-Certified-326CE5?logo=kubernetes&logoColor=white)](https://www.cncf.io/certification/cka/)
[![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![containerd](https://img.shields.io/badge/containerd-Runtime-575757?logo=containerd&logoColor=white)](https://containerd.io/)
[![Flannel](https://img.shields.io/badge/Flannel-CNI-4A90E2?logo=flannel&logoColor=white)](https://github.com/flannel-io/flannel)

> **중요**: CKA(Certified Kubernetes Administrator) 시험은 **kubeadm으로 구축된 클러스터**에서 진행됩니다.
> 따라서 kubeadm으로 클러스터를 직접 구축하고 관리하는 연습이 시험 준비에 필수적입니다.

## 파일 구조

이 가이드와 함께 제공되는 파일들은 다음과 같이 구성되어 있습니다:

```
k8s/
├── README.md                    # 이 가이드 문서 (메인 가이드)
├── config/                      # 실제 사용할 설정 파일
│   ├── kubeadm-config.yaml     # kubeadm 초기화 설정 (실제 IP 포함)
│   └── kube-flannel.yml        # Flannel CNI 설정 (ens15f0 인터페이스 명시)
├── templates/                   # 템플릿 파일 (변수 치환 필요)
│   ├── crictl.yaml             # crictl 설정 템플릿
│   ├── kubelet-config.yaml     # kubelet 설정 템플릿
│   └── kubeadm-config-template.yaml  # kubeadm 설정 템플릿
├── scripts/                     # 실행 스크립트
│   ├── docker-recovery-commands.sh    # Docker 복구 스크립트
│   └── fix-containerd-config.sh      # containerd 설정 수정 스크립트
└── docs/                        # 상세 가이드 문서
    ├── file-structure.md        # 파일 구조 및 사용법 설명
    ├── swap-disabling-guide.md  # Swap 비활성화 상세 가이드
    ├── external-disk-setup.md    # 외장 디스크 설정 가이드
    ├── troubleshooting.md        # 문제 해결 가이드
    └── cka-exam-preparation.md  # CKA 시험 준비 가이드
```

**파일 사용 가이드**:

- **템플릿 파일**: 변수를 실제 값으로 치환하여 사용
- **config 파일**: 실제 환경에 맞게 IP 주소 등을 수정하여 사용
- **스크립트 파일**: 직접 실행 가능 (`bash scripts/script-name.sh`)

자세한 내용은 [파일 구조 가이드](docs/file-structure.md)를 참조하세요.

## 왜 kubeadm인가?

- ✅ CKA 시험 환경과 동일한 kubeadm 기반 클러스터
- ✅ 클러스터 구성 요소를 직접 관리하고 이해할 수 있음
- ✅ 시험에서 나오는 문제들을 실제 환경에서 연습 가능
- ✅ 클러스터 문제 해결 능력 향상

## 사전 요구사항

### 1. 시스템 요구사항 (CKA 시험 환경과 유사하게)

- Ubuntu 20.04+ 또는 다른 Linux 배포판
- **최소 3개 노드 권장** (마스터 1개 + 워커 2개)
  - CKA 시험은 보통 여러 노드가 있는 클러스터에서 작업
- 최소 2GB RAM (노드당)
- 최소 2 CPU 코어 (노드당)
- Swap 비활성화 (필수)

### 2. 학습 환경 구성 옵션

#### 옵션 1: 물리적/VirtualBox/VMware VM (권장)

- 여러 VM을 생성하여 멀티 노드 클러스터 구축
- 시험 환경과 가장 유사

#### 옵션 2: 클라우드 VM (AWS EC2, GCP, Azure 등)

- 각 VM을 노드로 사용

#### 옵션 3: 로컬 머신에 멀티 노드

- 단일 머신에 여러 노드 역할을 하는 환경 (학습용)

### 3. Swap 비활성화

**⚠️ 필수**: Kubernetes는 swap을 비활성화해야 합니다.

```bash
# 현재 swap 상태 확인
sudo swapon --show

# swap 비활성화
sudo swapoff -a

# 영구적으로 비활성화 (재부팅 후에도 유지)
sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab

# 확인
free -h
```

**상세한 설명과 문제 해결 방법은 다음 문서를 참조하세요:**

- [Swap 비활성화 가이드](docs/swap-disabling-guide.md)

## 설치 단계

### 1. 컨테이너 런타임 설치 (containerd)

#### Docker만 설치되어 있는 경우 (containerd 자동 설치 포함)

Docker만 설치되어 있고 containerd는 없는 환경이라면, 아래 스크립트로 containerd 설치부터 설정까지 한 번에 처리할 수 있습니다.

```bash
# containerd 설치 및 설정 (Docker와 Kubernetes 공존)
bash /home/user/apps/k8s/scripts/fix-containerd-config.sh
```

> 스크립트는 containerd가 없으면 `apt`/`apt-get`으로 설치한 뒤 기본 설정을 생성하고 `SystemdCgroup = true` 등 kubeadm에서 요구하는 옵션을 적용합니다. 실행 중 containerd와 Docker가 재시작되므로 영향이 없는 시간에 실행하세요.

**containerd가 이미 설치되어 있는 경우**에는 아래와 같이 설정만 조정하면 됩니다.

```bash
sudo mkdir -p /etc/containerd
containerd config default | sudo tee /etc/containerd/config.toml
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
sudo systemctl restart containerd
sudo systemctl restart docker
sudo systemctl status containerd --no-pager | head -5
sudo systemctl status docker --no-pager | head -5
```

**⚠️ 중요**:

- Docker와 Kubernetes가 같은 containerd를 공유합니다
- Docker 컨테이너와 Kubernetes Pod가 함께 동작합니다
- `/etc/containerd/config.toml` 설정이 Docker와 Kubernetes 모두에 적용됩니다

#### Docker도 containerd도 없는 경우

```bash
sudo apt-get update
sudo apt-get install -y containerd docker.io
sudo mkdir -p /etc/containerd
containerd config default | sudo tee /etc/containerd/config.toml
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
sudo systemctl restart containerd docker
sudo systemctl enable containerd docker
```

#### containerd 설정 확인

```bash
# containerd 설정 확인
sudo cat /etc/containerd/config.toml | grep -A 5 "SystemdCgroup"

# containerd 상태 확인
sudo systemctl status containerd

# containerd 버전 확인
containerd --version
```

### 2. Kubernetes 저장소 추가 및 kubeadm, kubelet, kubectl 설치

**참고**: CKA 시험은 보통 Kubernetes 1.28 또는 1.29 버전을 사용합니다. 최신 안정 버전을 설치하세요.

```bash
# Kubernetes apt 저장소 추가
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.28/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.28/deb/ /' | sudo tee /etc/apt/sources.list.d/kubernetes.list

# 패키지 업데이트
sudo apt-get update

# kubeadm, kubelet, kubectl 설치
sudo apt-get install -y kubelet kubeadm kubectl

# 버전 확인 (중요: CKA 시험에서 kubectl 버전 확인하는 경우가 많음)
kubeadm version
kubectl version --client

# 자동 업그레이드 방지 (의도치 않은 업그레이드 방지)
sudo apt-mark hold kubelet kubeadm kubectl
```

**모든 노드(마스터 + 워커)에서 위 과정을 반복하세요!**

### 2-1. 외장 디스크 설정 (선택사항)

**⚠️ 중요**: 외장 디스크를 사용하려면 **Kubernetes 설치 후, kubeadm init 전**에 설정해야 합니다.

**상세한 설정 방법은 다음 문서를 참조하세요:**

- [외장 디스크 설정 가이드](docs/external-disk-setup.md)

### 3. 커널 모듈 로드

```bash
# 필수 커널 모듈 로드
sudo modprobe overlay
sudo modprobe br_netfilter

# 영구적으로 설정
cat <<EOF | sudo tee /etc/modules-load.d/k8s.conf
overlay
br_netfilter
EOF

# 네트워크 설정
cat <<EOF | sudo tee /etc/sysctl.d/k8s.conf
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF

# 설정 적용
sudo sysctl --system
```

## 마스터 노드 초기화 (마스터 노드에서만 실행)

### 0. 외장 디스크 설정 확인 (선택사항)

**⚠️ 중요**: 외장 디스크 설정은 **Kubernetes 설치 후, kubeadm init 전**에 완료해야 합니다.

**상세한 설정 방법은 다음 문서를 참조하세요:**

- [외장 디스크 설정 가이드](docs/external-disk-setup.md)

#### 빠른 참조

```bash
# kubeadm config 파일 생성 (템플릿 파일 사용)
EXTERNAL_ETCD_PATH="/media/de/k8s/etcd"
MASTER_IP="10.10.100.80"  # 마스터 노드 IP로 변경

sed -e "s|<마스터-노드-IP>|${MASTER_IP}|g" \
    -e "s|/media/de/k8s/etcd|${EXTERNAL_ETCD_PATH}|g" \
  /home/user/apps/k8s/templates/kubeadm-config-template.yaml | \
  sudo tee /tmp/kubeadm-config.yaml

# kubeadm init (config 파일 사용)
sudo kubeadm init --config=/tmp/kubeadm-config.yaml

# 또는 이미 생성된 config 파일 사용
sudo kubeadm init --config=/home/user/apps/k8s/config/kubeadm-config.yaml
```

### 1. kubeadm 초기화

```bash
# 마스터 노드의 IP 주소 확인
ip addr show | grep inet

# 기본 초기화 (Pod 네트워크는 나중에 설치)
sudo kubeadm init --pod-network-cidr=10.244.0.0/16

# 또는 마스터 노드 IP를 명시적으로 지정:
sudo kubeadm init \
  --pod-network-cidr=10.244.0.0/16 \
  --apiserver-advertise-address=<마스터-노드-IP> \
  --control-plane-endpoint=<마스터-노드-IP>:6443

# 또는 외장 디스크에 etcd 데이터 저장 (템플릿 파일 사용):
MASTER_IP="<마스터-노드-IP>"  # 실제 IP로 변경
EXTERNAL_ETCD_PATH="/media/de/k8s/etcd"

sed -e "s|<마스터-노드-IP>|${MASTER_IP}|g" \
    -e "s|/media/de/k8s/etcd|${EXTERNAL_ETCD_PATH}|g" \
  /home/user/apps/k8s/templates/kubeadm-config-template.yaml | \
  sudo tee /tmp/kubeadm-config.yaml

sudo kubeadm init --config=/tmp/kubeadm-config.yaml

# 또는 고가용성(HA) 클러스터를 위한 설정:
sudo kubeadm init \
  --pod-network-cidr=10.244.0.0/16 \
  --apiserver-advertise-address=<마스터-노드-IP> \
  --control-plane-endpoint=<마스터-노드-IP>:6443 \
  --upload-certs
```

**⚠️ 중요**: 초기화 완료 후 출력되는 정보를 **반드시 저장**하세요:

1. `kubeadm join` 명령어 (워커 노드 추가용)
2. `--discovery-token-ca-cert-hash` 값
3. 토큰 (24시간 후 만료됨)

### 2. kubeconfig 설정

```bash
# 일반 사용자용 kubeconfig 설정
mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

# 또는 root가 아닌 사용자로 실행하는 경우:
export KUBECONFIG=$HOME/.kube/config
```

### 3. Pod 네트워크 플러그인 설치 (CNI)

#### Flannel 사용 (권장)

```bash
# 로컬 설정 파일 사용 (ens15f0 인터페이스 명시)
kubectl apply -f /home/user/apps/k8s/config/kube-flannel.yml

# 또는 최신 버전 다운로드 (기본 설정)
# kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml
```

#### 또는 Calico 사용

```bash
kubectl create -f https://raw.githubusercontent.com/projectcalico/calico/v3.26.1/manifests/tigera-operator.yaml
kubectl create -f https://raw.githubusercontent.com/projectcalico/calico/v3.26.1/manifests/custom-resources.yaml
```

### 4. Kubernetes Dashboard 설치 (선택)

간단한 대시보드로 노드/Pod 상태를 확인하고 싶다면 Kubernetes Dashboard를 사용할 수 있습니다. 제공된 스크립트를 이용하면 자동으로 설치됩니다.

```bash
# Dashboard 배포 (기본: v2.7.0)
bash /home/user/apps/k8s/scripts/deploy-dashboard.sh
```

스크립트는 공식 recommended 매니페스트를 적용한 뒤, `cluster-admin` 권한을 가진 `admin-user` ServiceAccount를 생성합니다. 토큰은 다음과 같이 발급받아 로그인에 사용하세요.

```bash
kubectl -n kubernetes-dashboard create token admin-user
```

접속은 프록시를 열고 브라우저에서 Dashboard URL로 접속하면 됩니다.

```bash
kubectl proxy
# 브라우저에서 열기:
# http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/
```

노드에서 직접 배포하고 싶을 경우, 다음 명령을 수동으로 실행해도 같습니다.

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml
kubectl apply -f /home/user/apps/k8s/dashboard/dashboard-admin.yaml
```

### 5. 클러스터 상태 확인

```bash
# 노드 상태 확인
kubectl get nodes

# Pod 상태 확인
kubectl get pods --all-namespaces

# 모든 시스템이 Running 상태가 될 때까지 대기 (약 1-2분)
```

### 6. Docker와 Kubernetes 공존 확인

Docker와 Kubernetes가 같은 containerd를 사용하므로 둘 다 정상 동작해야 합니다:

```bash
# Docker 컨테이너 확인
docker ps

# Kubernetes Pod 확인
kubectl get pods --all-namespaces

# containerd에서 모든 컨테이너 확인
sudo crictl ps -a

# Docker와 Kubernetes가 같은 containerd를 사용하는지 확인
docker info | grep -i containerd
kubectl get nodes -o wide
```

**⚠️ crictl 오류 해결** (dockershim 엔드포인트 오류):

crictl이 기본적으로 여러 엔드포인트를 시도하면서 `dockershim.sock`이 없어서 오류가 발생할 수 있습니다. 템플릿 파일을 사용하여 설정 파일을 생성하세요:

```bash
# 사용자 설정 파일 생성 (템플릿 파일 사용)
cp /home/user/apps/k8s/templates/crictl.yaml ~/.crictl.yaml

# root 사용자용 설정 파일 생성 (sudo 사용 시)
sudo cp /home/user/apps/k8s/templates/crictl.yaml /root/.crictl.yaml

# 또는 시스템 전체 설정 파일 생성
sudo cp /home/user/apps/k8s/templates/crictl.yaml /etc/crictl.yaml

# 설정 확인
crictl ps -a  # 오류 없이 실행되어야 함
```

**참고**:

- Docker 컨테이너와 Kubernetes Pod는 서로 다른 네임스페이스에서 실행됩니다
- Docker는 `moby` 네임스페이스를 사용하고, Kubernetes는 `k8s.io` 네임스페이스를 사용합니다
- 두 시스템이 서로 간섭하지 않습니다

## 워커 노드 추가

### 1. 워커 노드 준비

**워커 노드에서도 다음을 모두 완료해야 합니다:**

- Swap 비활성화
- containerd 설치 및 설정
- kubeadm, kubelet, kubectl 설치
- 커널 모듈 로드

### 2. 워커 노드를 클러스터에 조인

마스터 노드 초기화 시 출력된 `kubeadm join` 명령어를 워커 노드에서 실행:

```bash
# 마스터 노드에서 출력된 join 명령어 실행
sudo kubeadm join <마스터-IP>:6443 --token <token> \
  --discovery-token-ca-cert-hash sha256:<hash>
```

### 3. 토큰이 만료된 경우 (24시간 후)

```bash
# 마스터 노드에서 새 토큰 생성
kubeadm token create --print-join-command

# 출력된 명령어를 워커 노드에서 실행
```

### 4. 노드 확인

```bash
# 마스터 노드에서 확인
kubectl get nodes

# 노드 상세 정보 확인 (CKA 시험에서 자주 사용)
kubectl get nodes -o wide

# 특정 노드 정보 확인
kubectl describe node <node-name>
```

## CKA 시험 준비를 위한 필수 명령어

### 클러스터 관리

```bash
# 토큰 재생성 (토큰이 만료된 경우)
kubeadm token create --print-join-command

# 클러스터 정보 확인 (시험에서 자주 요구)
kubectl cluster-info

# 클러스터 구성 정보 확인
kubectl get nodes -o wide

# 노드 상태 상세 확인
kubectl describe node <node-name>

# 마스터 노드 taint 제거 (마스터에서도 Pod 실행 허용)
# ⚠️ 단일 노드 클러스터나 학습용으로만 사용
kubectl taint nodes --all node-role.kubernetes.io/control-plane-

# 특정 노드에 taint 추가/제거 (CKA 시험 문제)
kubectl taint nodes <node-name> key=value:NoSchedule
kubectl taint nodes <node-name> key:NoSchedule-
```

### 빠른 클러스터 재설정 (CKA 연습용)

시험 준비 시 클러스터를 반복적으로 재구축해야 할 때 유용합니다:

```bash
# 모든 노드에서 실행 (마스터 + 워커 모두)
sudo kubeadm reset -f

# 네트워크 및 설정 파일 정리
sudo rm -rf /etc/cni/net.d
sudo rm -rf /var/lib/cni
sudo rm -rf /var/lib/etcd
sudo rm -rf $HOME/.kube/config
sudo iptables -F && sudo iptables -t nat -F && sudo iptables -t mangle -F && sudo iptables -X

# containerd 재시작
sudo systemctl restart containerd

# 마스터 노드에서 다시 초기화
sudo kubeadm init --pod-network-cidr=10.244.0.0/16
```

### 디버깅 명령어 (CKA 시험에서 중요!)

```bash
# kubelet 로그 확인
sudo journalctl -u kubelet -f

# kubelet 상태 확인
sudo systemctl status kubelet

# 컨테이너 런타임 로그
sudo journalctl -u containerd -f

# etcd 상태 확인
sudo ETCDCTL_API=3 etcdctl --endpoints=https://127.0.0.1:2379 \
  --cacert=/etc/kubernetes/pki/etcd/ca.crt \
  --cert=/etc/kubernetes/pki/etcd/server.crt \
  --key=/etc/kubernetes/pki/etcd/server.key \
  endpoint health

# API 서버 로그
kubectl logs -n kube-system kube-apiserver-<node-name>

# 스케줄러 로그
kubectl logs -n kube-system kube-scheduler-<node-name>

# 컨트롤러 매니저 로그
kubectl logs -n kube-system kube-controller-manager-<node-name>
```

## 문제 해결

자세한 문제 해결 가이드는 다음 문서를 참조하세요:

- [문제 해결 가이드](docs/troubleshooting.md)

### 빠른 참조

```bash
# kubelet 상태 확인
sudo systemctl status kubelet

# kubelet 로그 확인
sudo journalctl -xeu kubelet

# 클러스터 재설정
sudo kubeadm reset -f
sudo rm -rf /etc/cni/net.d /var/lib/cni /var/lib/etcd $HOME/.kube/config
sudo iptables -F && sudo iptables -t nat -F && sudo iptables -t mangle -F && sudo iptables -X
sudo systemctl restart containerd
```

## CKA 시험 준비

**상세한 CKA 시험 준비 가이드는 다음 문서를 참조하세요:**

- [CKA 시험 준비 가이드](docs/cka-exam-preparation.md)

### 빠른 참조

```bash
# kubectl 자동완성 설정 (시험에서 시간 절약!)
echo 'source <(kubectl completion bash)' >>~/.bashrc
source ~/.bashrc

# 또는 zsh 사용 시
echo 'source <(kubectl completion zsh)' >>~/.zshrc
source ~/.zshrc
```

## 추가 학습 리소스

1. **공식 문서**: https://kubernetes.io/docs/
2. **kubeadm 문서**: https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/
3. **CKA 시험 가이드**: https://www.cncf.io/certification/cka/
4. **killer.sh CKA 시뮬레이터**: 실제 시험 환경과 유사한 연습 문제

## 관련 문서

- [파일 구조 가이드](docs/file-structure.md)
- [Swap 비활성화 가이드](docs/swap-disabling-guide.md)
- [외장 디스크 설정 가이드](docs/external-disk-setup.md)
- [문제 해결 가이드](docs/troubleshooting.md)
- [CKA 시험 준비 가이드](docs/cka-exam-preparation.md)
