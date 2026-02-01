# CKA 시험 준비 가이드

## CKA 시험 개요

CKA(Certified Kubernetes Administrator) 시험은 **kubeadm으로 구축된 클러스터**에서 진행됩니다. 
따라서 kubeadm으로 클러스터를 직접 구축하고 관리하는 연습이 시험 준비에 필수적입니다.

## 시험 주제별 연습 가이드

### 1. 클러스터 아키텍처, 설치 및 구성 (25%)

**연습해야 할 내용:**
- [ ] kubeadm으로 클러스터 설치 및 업그레이드
- [ ] 클러스터 업그레이드 (kubeadm upgrade)
- [ ] etcd 백업 및 복원
- [ ] kubeconfig 파일 관리
- [ ] 인증서 관리 및 갱신

**실습 예제:**

```bash
# 클러스터 업그레이드 (1.28 → 1.29 예시)
# 1. kubeadm 업그레이드
sudo apt-mark unhold kubeadm
sudo apt-get update && sudo apt-get install -y kubeadm=1.29.0-00
sudo apt-mark hold kubeadm

# 2. 업그레이드 계획 확인
sudo kubeadm upgrade plan

# 3. 업그레이드 실행
sudo kubeadm upgrade apply v1.29.0

# etcd 백업 (CKA 시험에 자주 나옴!)
sudo ETCDCTL_API=3 etcdctl snapshot save /tmp/etcd-backup.db \
  --endpoints=https://127.0.0.1:2379 \
  --cacert=/etc/kubernetes/pki/etcd/ca.crt \
  --cert=/etc/kubernetes/pki/etcd/server.crt \
  --key=/etc/kubernetes/pki/etcd/server.key

# etcd 복원
sudo systemctl stop kubelet
sudo ETCDCTL_API=3 etcdctl snapshot restore /tmp/etcd-backup.db \
  --data-dir /var/lib/etcd-backup
# (설정 파일 수정 후 etcd 재시작)
```

### 2. 워크로드 및 스케줄링 (15%)

**연습해야 할 내용:**
- [ ] Pod, Deployment, StatefulSet, DaemonSet 생성 및 관리
- [ ] 스케줄링 (nodeSelector, affinity, taints/tolerations)
- [ ] Resource Limits 및 Requests 설정

### 3. 서비스 및 네트워킹 (20%)

**연습해야 할 내용:**
- [ ] Service (ClusterIP, NodePort, LoadBalancer) 생성
- [ ] Ingress 리소스 구성
- [ ] CoreDNS 문제 해결
- [ ] 네트워크 정책 (NetworkPolicy)

**실습 예제:**

```bash
# CoreDNS 문제 해결 (시험에 자주 나옴)
kubectl get pods -n kube-system -l k8s-app=kube-dns
kubectl logs -n kube-system <coredns-pod-name>
kubectl describe pod -n kube-system <coredns-pod-name>

# Service 생성 및 확인
kubectl expose deployment <deployment-name> --port=80 --type=NodePort
kubectl get svc
kubectl get endpoints <service-name>
```

### 4. 스토리지 (10%)

**연습해야 할 내용:**
- [ ] PersistentVolume, PersistentVolumeClaim 생성
- [ ] StorageClass 구성
- [ ] 볼륨 마운트

### 5. 트러블슈팅 (30%) ⭐ 가장 중요!

**연습해야 할 내용:**
- [ ] Pod가 시작되지 않는 경우 진단
- [ ] 노드가 NotReady 상태일 때 해결
- [ ] 네트워크 문제 해결
- [ ] kubelet 문제 해결
- [ ] 컨테이너 런타임 문제 해결

**실습 예제:**

```bash
# Pod 문제 진단 (시험에 거의 항상 나옴!)
kubectl describe pod <pod-name>
kubectl logs <pod-name>
kubectl logs <pod-name> -c <container-name>  # 멀티 컨테이너인 경우
kubectl get events --sort-by=.metadata.creationTimestamp

# 노드 문제 진단
kubectl get nodes
kubectl describe node <node-name>
kubectl get pods -n kube-system -o wide  # 시스템 Pod 확인

# kubelet 문제 해결
sudo systemctl status kubelet
sudo journalctl -u kubelet --no-pager | tail -50
sudo systemctl restart kubelet
```

## CKA 시험 준비 체크리스트

### 클러스터 구축
- [ ] kubeadm으로 마스터 노드 초기화 성공
- [ ] 워커 노드 최소 2개 이상 추가
- [ ] CNI 플러그인 설치 완료
- [ ] 모든 노드가 Ready 상태

### 필수 명령어 숙지
- [ ] `kubectl get/describe/logs/exec` 명령어 자유자재로 사용
- [ ] `kubectl edit`로 리소스 수정
- [ ] `kubectl apply/delete`로 리소스 생성/삭제
- [ ] YAML 파일 생성 및 적용
- [ ] `kubeadm token` 명령어 사용

### 문제 해결 능력
- [ ] Pod가 Pending 상태일 때 원인 파악 및 해결
- [ ] Pod가 CrashLoopBackOff 상태일 때 로그 확인 및 수정
- [ ] 노드가 NotReady 상태일 때 진단 및 복구
- [ ] Service가 작동하지 않을 때 디버깅
- [ ] 네트워크 문제 해결

### 빠른 작업 능력
- [ ] YAML 파일 빠르게 작성 (vim/vi 편집기 사용)
- [ ] kubectl 명령어 자동완성 설정
- [ ] 명령어 단축키(alias) 활용

```bash
# kubectl 자동완성 설정 (시험에서 시간 절약!)
echo 'source <(kubectl completion bash)' >>~/.bashrc
source ~/.bashrc

# 또는 zsh 사용 시
echo 'source <(kubectl completion zsh)' >>~/.zshrc
source ~/.zshrc
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

## 추가 학습 리소스

1. **공식 문서**: https://kubernetes.io/docs/
2. **kubeadm 문서**: https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/
3. **CKA 시험 가이드**: https://www.cncf.io/certification/cka/
4. **killer.sh CKA 시뮬레이터**: 실제 시험 환경과 유사한 연습 문제

