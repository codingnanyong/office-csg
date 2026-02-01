# 파일 구조 및 사용법

이 디렉토리는 CKA 시험 준비를 위한 Kubernetes 클러스터 구축 가이드와 관련 파일들을 포함합니다.

## 디렉토리 구조

```
k8s/
├── README.md                    # 메인 가이드 문서
├── config/                      # 실제 사용할 설정 파일
│   └── kubeadm-config.yaml     # kubeadm 초기화 설정 (실제 IP 포함)
├── templates/                   # 템플릿 파일 (변수 치환 필요)
│   ├── crictl.yaml             # crictl 설정 템플릿
│   ├── kubelet-config.yaml     # kubelet 설정 템플릿
│   └── kubeadm-config-template.yaml  # kubeadm 설정 템플릿
├── scripts/                     # 실행 스크립트
│   ├── docker-recovery-commands.sh    # Docker 복구 스크립트
│   └── fix-containerd-config.sh      # containerd 설정 수정 스크립트
└── docs/                        # 상세 가이드 문서
    ├── file-structure.md        # 이 파일 (파일 구조 설명)
    ├── swap-disabling-guide.md  # Swap 비활성화 상세 가이드
    ├── external-disk-setup.md   # 외장 디스크 설정 가이드
    ├── troubleshooting.md       # 문제 해결 가이드
    └── cka-exam-preparation.md  # CKA 시험 준비 가이드
```

## 파일 설명

### 설정 파일 (config/)

- **kubeadm-config.yaml**: 실제 마스터 노드 IP가 포함된 kubeadm 초기화 설정 파일
  - 사용법: `sudo kubeadm init --config=/home/user/apps/k8s/config/kubeadm-config.yaml`

### 템플릿 파일 (templates/)

- **crictl.yaml**: crictl 설정 템플릿
  - 사용법: `cp templates/crictl.yaml ~/.crictl.yaml`
  
- **kubelet-config.yaml**: kubelet 설정 템플릿 (외장 디스크 경로 포함)
  - 사용법: 가이드 참조 (경로 치환 필요)
  
- **kubeadm-config-template.yaml**: kubeadm 설정 템플릿
  - 사용법: `<마스터-노드-IP>`를 실제 IP로 치환하여 사용

### 스크립트 파일 (scripts/)

- **docker-recovery-commands.sh**: Docker 복구 스크립트
  - Docker가 삭제되었을 때 복구하는 스크립트
  - 사용법: `bash scripts/docker-recovery-commands.sh`

- **fix-containerd-config.sh**: containerd 설정 수정 스크립트
  - Docker와 Kubernetes가 공존하도록 containerd 설정
  - 사용법: `bash scripts/fix-containerd-config.sh`

## 사용 방법

1. **메인 가이드 읽기**: `README.md` 파일을 먼저 읽어보세요.
   - 기본 설치 및 설정 방법이 포함되어 있습니다
   - 상세한 내용은 `docs/` 디렉토리의 문서를 참조하세요

2. **상세 가이드 문서 (docs/)**:
   - `docs/swap-disabling-guide.md`: Swap 비활성화 상세 가이드
   - `docs/external-disk-setup.md`: 외장 디스크 설정 가이드
   - `docs/troubleshooting.md`: 문제 해결 가이드
   - `docs/cka-exam-preparation.md`: CKA 시험 준비 가이드

3. **템플릿 파일 사용**: 
   - 템플릿 파일은 변수 치환이 필요할 수 있습니다
   - 가이드에서 각 파일의 사용법을 확인하세요

4. **스크립트 실행**:
   - 스크립트는 실행 권한이 필요할 수 있습니다: `chmod +x scripts/*.sh`
   - 또는 `bash scripts/script-name.sh`로 실행

5. **설정 파일 수정**:
   - `config/kubeadm-config.yaml`은 실제 환경에 맞게 IP 주소를 수정하세요

## 주의사항

- 템플릿 파일은 직접 사용하지 말고, 변수를 치환하여 사용하세요
- config 디렉토리의 파일은 실제 환경 설정이 포함되어 있으므로 주의하세요
- 스크립트 실행 전에 백업을 권장합니다

