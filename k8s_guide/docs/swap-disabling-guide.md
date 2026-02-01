# Swap 비활성화 가이드

## Kubernetes가 Swap을 비활성화해야 하는 이유

Kubernetes는 **예측 가능한 성능**을 위해 swap을 비활성화해야 합니다:

1. **성능 보장**: Swap을 사용하면 디스크 I/O가 발생하여 성능이 크게 저하됩니다
2. **메모리 관리**: Kubernetes의 스케줄러가 Pod의 메모리 사용량을 정확히 예측할 수 없게 됩니다
3. **리소스 보장**: Pod에 할당된 메모리 제한(limits)을 보장할 수 없습니다
4. **OOM(Out of Memory) 처리**: swap이 있으면 OOM Killer가 제대로 작동하지 않을 수 있습니다

## Swap 비활성화 시 발생할 수 있는 문제점

⚠️ **주의사항**:

1. **메모리 부족 시 즉시 OOM 발생**
   - Swap이 없으면 메모리가 부족할 때 프로세스가 즉시 종료될 수 있습니다
   - 시스템이 불안정해질 수 있습니다

2. **제한된 메모리 환경에서의 문제**
   - 특히 개발/학습 환경에서 메모리가 부족한 경우 문제가 될 수 있습니다
   - VM에 할당된 메모리가 적으면 시스템이 멈출 수 있습니다

3. **시스템 프로세스 영향**
   - Kubernetes 외의 다른 프로세스들도 메모리 부족 시 종료될 수 있습니다

## Swap 비활성화 방법 (권장)

**프로덕션 환경 및 CKA 시험 준비용** (swap 완전 비활성화):

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

## Swap을 유지하되 kubelet이 무시하도록 설정 (대안)

**⚠️ 주의**: Kubernetes 1.22+ 버전부터는 swap을 허용할 수 있지만, **CKA 시험 환경에서는 swap이 비활성화되어 있습니다**.

메모리가 부족한 개발/학습 환경에서만 사용하세요:

```bash
# 방법 1: kubelet 설정 파일 수정 (Kubernetes 1.22+)
sudo mkdir -p /var/lib/kubelet
cat <<EOF | sudo tee /var/lib/kubelet/config.yaml
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
failSwapOn: false
EOF

# 방법 2: kubelet 서비스에 파라미터 추가 (이전 버전)
sudo sed -i 's|ExecStart=/usr/bin/kubelet|ExecStart=/usr/bin/kubelet --fail-swap-on=false|' /etc/systemd/system/kubelet.service.d/10-kubeadm.conf
sudo systemctl daemon-reload
sudo systemctl restart kubelet
```

**하지만 CKA 시험 준비를 위해서는 swap을 완전히 비활성화하는 것을 권장합니다.**

## 메모리 부족 대응 방법

Swap 없이 메모리 부족을 해결하는 방법:

1. **충분한 메모리 할당**
   - 최소 2GB RAM (노드당) 이상 권장
   - 마스터 노드: 최소 2GB
   - 워커 노드: 최소 2GB

2. **Resource Limits 설정**
   ```bash
   # Pod의 메모리 제한 설정 예시
   kubectl set resources deployment <deployment-name> \
     --limits=memory=512Mi \
     --requests=memory=256Mi
   ```

3. **불필요한 Pod 제거**
   ```bash
   # 사용하지 않는 Pod 확인 및 삭제
   kubectl get pods --all-namespaces
   kubectl delete pod <pod-name> -n <namespace>
   ```

4. **메모리 모니터링**
   ```bash
   # 노드 메모리 사용량 확인
   kubectl top nodes
   
   # Pod 메모리 사용량 확인
   kubectl top pods --all-namespaces
   
   # 시스템 메모리 확인
   free -h
   ```

## 권장사항 요약

| 환경 | Swap 비활성화 | 이유 |
|------|--------------|------|
| **CKA 시험 준비** | ✅ **필수** | 시험 환경과 동일하게 구성 |
| **프로덕션 환경** | ✅ **권장** | 예측 가능한 성능 보장 |
| **개발/학습 (메모리 충분)** | ✅ **권장** | 실제 환경과 유사하게 |
| **개발/학습 (메모리 부족)** | ⚠️ **선택적** | swap 유지 + failSwapOn=false 설정 가능 |

**결론**: CKA 시험 준비와 프로덕션 환경에서는 **swap을 완전히 비활성화**하는 것이 좋습니다. 
메모리가 부족한 경우 VM/노드의 메모리를 늘리는 것을 권장합니다.

