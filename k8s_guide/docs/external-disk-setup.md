# 외장 디스크 설정 가이드

## 개요

Docker처럼 외장 디스크에 Kubernetes 데이터를 저장하려면 **Kubernetes 설치 후, kubeadm init 전**에 설정해야 합니다.

## 외장 디스크 경로 준비

```bash
# 외장 디스크 경로 설정 (예: /media/de/k8s)
EXTERNAL_K8S_PATH="/media/de/k8s"

# 디렉토리 생성
sudo mkdir -p $EXTERNAL_K8S_PATH/kubelet
sudo mkdir -p $EXTERNAL_K8S_PATH/etcd

# 권한 설정
sudo chown root:root $EXTERNAL_K8S_PATH
sudo chmod 755 $EXTERNAL_K8S_PATH
```

## kubelet 데이터 디렉토리 설정

```bash
# kubelet 설정 파일 생성 (템플릿 파일 사용)
sudo mkdir -p /var/lib/kubelet
EXTERNAL_K8S_PATH="/media/de/k8s"
sed "s|/media/de/k8s/kubelet|${EXTERNAL_K8S_PATH}/kubelet|g" \
  /home/user/apps/k8s/templates/kubelet-config.yaml | \
  sudo tee /var/lib/kubelet/config.yaml

# kubelet 서비스 파일 수정 (kubeadm이 생성한 파일 수정)
sudo mkdir -p /etc/systemd/system/kubelet.service.d
sudo cp /usr/lib/systemd/system/kubelet.service /etc/systemd/system/kubelet.service
sudo sed -i "s|ExecStart=/usr/bin/kubelet|ExecStart=/usr/bin/kubelet --root-dir=${EXTERNAL_K8S_PATH}/kubelet|" /etc/systemd/system/kubelet.service
```

**참고**: kubelet의 root-dir 설정은 kubeadm init 전에 해도 되지만, kubeadm이 서비스 파일을 덮어쓸 수 있으므로 init 후에 다시 확인하는 것이 좋습니다.

## containerd root 디렉토리 설정 (Docker와 공유)

Docker가 이미 `/media/de/docker`를 사용 중이라면, containerd도 같은 경로를 사용합니다:

```bash
# containerd 설정 확인
sudo cat /etc/containerd/config.toml | grep -i "root\|state"

# containerd root 디렉토리 변경 (필요한 경우)
sudo sed -i "s|root = \"/var/lib/containerd\"|root = \"/media/de/docker/containerd\"|" /etc/containerd/config.toml
sudo sed -i "s|state = \"/run/containerd\"|state = \"/media/de/docker/containerd/run\"|" /etc/containerd/config.toml

# 디렉토리 생성
sudo mkdir -p /media/de/docker/containerd
sudo mkdir -p /media/de/docker/containerd/run

# containerd 재시작
sudo systemctl restart containerd
```

## etcd 데이터 디렉토리 설정

**⚠️ 중요**: kubeadm 1.28에서는 `--etcd-local-data-dir` 옵션이 지원되지 않습니다. 
kubeadm config 파일을 사용해야 합니다.

```bash
# etcd 데이터 디렉토리를 외장 디스크로 설정
EXTERNAL_ETCD_PATH="/media/de/k8s/etcd"
MASTER_IP="10.10.100.80"  # 마스터 노드 IP로 변경

# kubeadm config 파일 생성 (템플릿 파일 사용)
sed -e "s|<마스터-노드-IP>|${MASTER_IP}|g" \
    -e "s|/media/de/k8s/etcd|${EXTERNAL_ETCD_PATH}|g" \
  /home/user/apps/k8s/templates/kubeadm-config-template.yaml | \
  sudo tee /tmp/kubeadm-config.yaml

# kubeadm init (config 파일 사용)
sudo kubeadm init --config=/tmp/kubeadm-config.yaml
```

또는 이미 생성된 config 파일 사용:

```bash
# config 디렉토리의 파일 사용
sudo kubeadm init --config=/home/user/apps/k8s/config/kubeadm-config.yaml
```

## 확인 및 검증

### kubelet 설정 확인

```bash
# kubelet 설정 확인
sudo cat /var/lib/kubelet/config.yaml | grep rootDir

# 설정이 없다면 다시 생성 (템플릿 파일 사용)
EXTERNAL_K8S_PATH="/media/de/k8s"
sed "s|/media/de/k8s/kubelet|${EXTERNAL_K8S_PATH}/kubelet|g" \
  /home/user/apps/k8s/templates/kubelet-config.yaml | \
  sudo tee /var/lib/kubelet/config.yaml
```

### etcd 데이터 확인

```bash
# etcd 데이터 디렉토리 확인
sudo ls -la /media/de/k8s/etcd

# etcd 데이터 확인
sudo ls -la /media/de/k8s/etcd/member
```

### kubelet 데이터 확인

```bash
# kubelet 데이터 디렉토리 확인
sudo ls -la /media/de/k8s/kubelet
```

## 주의사항

- 외장 디스크가 마운트되어 있어야 합니다
- 디렉토리 권한이 올바르게 설정되어 있어야 합니다
- 재부팅 후에도 자동 마운트되도록 `/etc/fstab`에 설정하는 것을 권장합니다

## 재부팅 후 자동 마운트 설정

```bash
# fstab에 추가 (예시)
# UUID를 실제 디스크 UUID로 변경하세요
UUID=<your-disk-uuid> /media/de ext4 defaults 0 2

# 마운트 확인
sudo mount -a

# 재부팅 후 확인
sudo mount | grep /media/de
```

