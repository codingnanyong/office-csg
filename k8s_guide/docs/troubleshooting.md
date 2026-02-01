# Kubernetes 클러스터 문제 해결 가이드

## kubelet이 시작되지 않는 경우

```bash
# kubelet 상태 확인
sudo systemctl status kubelet

# kubelet 로그 확인
sudo journalctl -xeu kubelet
```

## 네트워크 문제

```bash
# CoreDNS Pod 상태 확인
kubectl get pods -n kube-system

# CoreDNS Pod 로그 확인
kubectl logs -n kube-system <coredns-pod-name>
```

## CoreDNS Ready 상태 문제 해결

CoreDNS Pod가 Running이지만 Ready(0/1) 상태가 아닌 경우:

**증상**:
- `kubectl get pods -n kube-system`에서 CoreDNS가 `0/1 Ready`
- 로그에 `Kubernetes API connection failure: dial tcp 10.96.0.1:443: i/o timeout` 오류

**원인**:
- Pod 네트워크에서 서비스 IP(10.96.0.1)로의 연결 문제
- kube-proxy의 iptables 규칙 문제

**해결 방법**:

```bash
# 1. kube-proxy 재시작
kubectl delete pod -n kube-system $(kubectl get pods -n kube-system -l k8s-app=kube-proxy -o jsonpath='{.items[0].metadata.name}')

# 2. CoreDNS Pod 재시작
kubectl delete pod -n kube-system -l k8s-app=kube-dns

# 3. CoreDNS readiness probe 조정 (더 관대한 설정)
kubectl patch deployment -n kube-system coredns -p '{"spec":{"template":{"spec":{"containers":[{"name":"coredns","readinessProbe":{"httpGet":{"path":"/ready","port":8181},"initialDelaySeconds":30,"periodSeconds":10,"timeoutSeconds":5,"failureThreshold":10}}]}}}}'

# 4. 네트워크 연결 확인
curl -k https://10.96.0.1:443/version  # 호스트에서 확인
kubectl get endpoints -n default kubernetes

# 5. iptables 규칙 확인 (root 권한 필요)
sudo iptables -t nat -L | grep -E "10\.96\.0\.1|KUBE-SVC"

# 6. 클러스터 상태 확인
kubectl get nodes
kubectl get cs  # ComponentStatus 확인
```

**참고**:
- CoreDNS가 Ready가 아니어도 기본 DNS 기능은 동작할 수 있습니다
- 클러스터의 다른 기능(API 서버, 스케줄러 등)은 정상 동작합니다
- 시간이 지나면 자동으로 해결될 수 있습니다

## CoreDNS가 API Server에 연결하지 못하는 경우 (Flannel NIC 미지정)

CoreDNS 로그에 `Kubernetes API connection failure: Get "https://10.96.0.1:443/version": i/o timeout`이 반복되는 경우:

**증상**:
- `kubectl get pods -n kube-system`에서 CoreDNS Pod가 `CrashLoopBackOff` 또는 `Running`이지만 기능 이상
- `kubectl logs -n kube-system <coredns-pod>`에서 API 서버 연결 타임아웃 메시지 확인
- `kubectl get all -n kube-flannel`에서 DaemonSet이 `1/1`이 아니거나 Pod 네트워크가 정상 동작하지 않음

**원인**:
- 호스트 기본 NIC가 `eth0`이 아닌데 Flannel이 기본값(`eth0`)으로 기동됨 (예: `ens15f0`)
- `/etc/cni/net.d/10-flannel.conflist`와 같은 이전 Flannel CNI 설정이 남아 있어 kubelet이 잘못된 인터페이스 정보를 계속 로드함

**해결 방법**:

```bash
# 1. 호스트 NIC 이름 확인 (eth0이 아님을 확인)
ip -o -4 addr show

# 2. 남아 있는 Flannel CNI 설정 제거 (zsh의 globbing에 주의)
sudo rm -f /etc/cni/net.d/10-flannel.conflist
sudo rm -f /etc/cni/net.d/.kubernetes-cni-keep

# 3. 남아 있는 CNI 인터페이스 제거 (없는 경우 무시)
sudo ip link delete flannel.1 || true
sudo ip link delete cni0 || true
sudo ip link delete <veth-name> || true  # 예: vethabcd1234

# 4. kubelet 재시작으로 CNI 상태 초기화
sudo systemctl restart kubelet

# 5. Flannel 매니페스트 재적용 (NIC 강제 지정)
# 사용 중인 kube-flannel.yml에 --iface=<NIC명> 옵션이 포함되어 있는지 확인 후 재배포
kubectl apply -f kube-flannel.yml

# 6. 동작 확인
kubectl get all -n kube-flannel
kubectl get pods -n kube-system -l k8s-app=kube-dns
```

**추가 참고**:
- zsh에서 `rm -f /etc/cni/net.d/*` 형태는 globbing 오류(`no matches found`)가 발생할 수 있으므로 파일명을 명시하거나 `setopt nonomatch` 후 실행
- Flannel 매니페스트의 `--iface` 값이 실제 NIC 이름과 일치해야 Pod 네트워크가 복구됨
- 조치 후 CoreDNS Pod가 `1/1 Running` 상태인지 반드시 확인

## Docker와 Kubernetes 충돌 문제

Docker와 Kubernetes가 같은 containerd를 사용할 때 발생할 수 있는 문제:

### 문제 1: containerd 설정 충돌

**자동 해결 스크립트 사용 (권장)**:

```bash
# containerd 설정 수정 스크립트 실행
bash /home/user/apps/k8s/scripts/fix-containerd-config.sh
```

**수동 해결**:

```bash
# containerd 설정 확인
sudo cat /etc/containerd/config.toml | grep -A 10 "SystemdCgroup"

# SystemdCgroup이 true인지 확인 (Kubernetes 필수)
# false라면 다음 명령어 실행:
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
sudo systemctl restart containerd
sudo systemctl restart docker  # Docker도 재시작
```

### 문제 2: Docker 컨테이너가 Kubernetes 초기화 중 문제 발생

```bash
# Docker 컨테이너 일시 중지 (필요한 경우)
docker stop $(docker ps -q)

# Kubernetes 초기화 후 Docker 재시작
sudo systemctl restart docker
docker start $(docker ps -aq)
```

### 문제 3: containerd 소켓 경로 문제

```bash
# containerd 소켓 확인
ls -la /run/containerd/containerd.sock

# kubelet이 올바른 소켓을 사용하는지 확인
sudo cat /var/lib/kubelet/config.yaml | grep -i containerd

# Docker와 Kubernetes가 같은 containerd를 사용하는지 확인
docker info | grep -i containerd
sudo journalctl -u kubelet | grep -i containerd
```

### 문제 4: 리소스 경쟁

Docker와 Kubernetes가 동시에 실행되면 리소스 경쟁이 발생할 수 있습니다:

```bash
# 메모리 사용량 확인
free -h
docker stats --no-stream
kubectl top nodes

# CPU 사용량 확인
top
htop

# 리소스가 부족하면 Docker 컨테이너 수를 줄이거나
# Kubernetes Pod의 resource limits를 설정
```

## 디버깅 명령어

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

## 클러스터 재설정

```bash
# 모든 노드에서 실행
sudo kubeadm reset -f
sudo rm -rf /etc/cni/net.d
sudo rm -rf /var/lib/cni
sudo rm -rf /var/lib/etcd
sudo rm -rf $HOME/.kube/config
sudo iptables -F && sudo iptables -t nat -F && sudo iptables -t mangle -F && sudo iptables -X
sudo systemctl restart containerd
```

## crictl 오류 해결

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

