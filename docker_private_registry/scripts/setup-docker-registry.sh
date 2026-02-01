#!/bin/bash
# Docker Registry 설정 스크립트
# Docker Private Registry 설정 (인증서 다운로드 + daemon.json 업데이트)
# 사용 전 REGISTRY_HOST, REGISTRY_WEB_HOST 환경 변수 설정 또는 아래 값 수정

set -e

DAEMON_JSON="/etc/docker/daemon.json"
BACKUP_FILE="/etc/docker/daemon.json.backup.$(date +%Y%m%d_%H%M%S)"
REGISTRY="${REGISTRY_HOST:-localhost}:5000"
CERT_URL="${REGISTRY_WEB_URL:-http://localhost:9000}/certs/domain.crt"
CERT_DIR="/etc/docker/certs.d/${REGISTRY}"
TEMP_CERT="/tmp/domain.crt"

if [ "$EUID" -ne 0 ]; then 
    echo "❌ 이 스크립트는 sudo 권한이 필요합니다"
    echo "   실행: sudo ./scripts/setup-docker-registry.sh"
    exit 1
fi

echo "🔧 Docker Registry 설정 중..."
echo "Registry: ${REGISTRY}"
echo ""

# 1. 인증서 다운로드
echo "📥 인증서 다운로드 중..."
if curl -f -s "${CERT_URL}" -o "${TEMP_CERT}"; then
    echo "✅ 인증서 다운로드 완료: ${TEMP_CERT}"
else
    echo "❌ 인증서 다운로드 실패: ${CERT_URL}"
    exit 1
fi

# 2. 인증서 디렉토리 생성 및 복사
echo ""
echo "📁 인증서 디렉토리 설정 중..."
mkdir -p "${CERT_DIR}"
cp "${TEMP_CERT}" "${CERT_DIR}/ca.crt"
chmod 644 "${CERT_DIR}/ca.crt"
rm -f "${TEMP_CERT}"
echo "✅ 인증서 설정 완료: ${CERT_DIR}/ca.crt"

# 3. daemon.json 백업
if [ -f "$DAEMON_JSON" ]; then
    cp "$DAEMON_JSON" "$BACKUP_FILE"
    echo "✅ 백업 생성: $BACKUP_FILE"
else
    # daemon.json이 없으면 기본 구조 생성
    echo "{}" > "$DAEMON_JSON"
fi

# 4. daemon.json 업데이트 (insecure-registries 추가)
echo ""
echo "🔧 Docker daemon.json 업데이트 중..."

# jq가 있으면 사용, 없으면 수동으로 처리
if command -v jq &> /dev/null; then
    # jq를 사용하여 insecure-registries 추가
    if ! jq -e '."insecure-registries"' "$DAEMON_JSON" > /dev/null 2>&1; then
        # insecure-registries가 없으면 추가
        jq '. + {"insecure-registries": ["'${REGISTRY}'"]}' "$DAEMON_JSON" > "$DAEMON_JSON.tmp"
    else
        # 이미 있으면 항목 추가 (중복 체크)
        if ! jq -e '."insecure-registries"[] | select(. == "'${REGISTRY}'")' "$DAEMON_JSON" > /dev/null 2>&1; then
            jq '."insecure-registries" += ["'${REGISTRY}'"]' "$DAEMON_JSON" > "$DAEMON_JSON.tmp"
        else
            echo "✅ ${REGISTRY}가 이미 insecure-registries에 등록되어 있습니다"
        fi
    fi
    if [ -f "$DAEMON_JSON.tmp" ]; then
        mv "$DAEMON_JSON.tmp" "$DAEMON_JSON"
    fi
else
    # jq가 없으면 수동으로 JSON 수정
    echo "⚠️  jq가 설치되어 있지 않습니다."
    echo ""
    echo "다음 내용으로 /etc/docker/daemon.json을 수정하세요:"
    echo ""
    cat << EOF
{
  "exec-opts": ["native.cgroupdriver=systemd"],
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m"
  },
  "storage-driver": "overlay2",
  "insecure-registries": ["${REGISTRY}"]
}
EOF
    exit 1
fi

echo "✅ daemon.json 업데이트 완료"
echo "📋 설정 내용:"
cat "$DAEMON_JSON" | jq '.' 2>/dev/null || cat "$DAEMON_JSON"

# 5. Docker daemon 재시작
echo ""
echo "🔄 Docker daemon 재시작 중..."
systemctl restart docker

echo "⏳ Docker daemon 재시작 대기 중..."
sleep 3

if systemctl is-active --quiet docker; then
    echo "✅ Docker daemon 재시작 완료"
    echo ""
    echo "📋 설정 확인:"
    docker info 2>/dev/null | grep -i "Insecure Registries" -A 5 || echo "확인 실패"
    
    echo ""
    echo "📋 인증서 확인:"
    ls -lh "${CERT_DIR}/ca.crt" || echo "인증서 확인 실패"
else
    echo "❌ Docker daemon 재시작 실패"
    echo "백업 파일로 복원하세요:"
    echo "  sudo cp $BACKUP_FILE $DAEMON_JSON"
    echo "  sudo systemctl restart docker"
    exit 1
fi

echo ""
echo "✅ Registry 설정 완료!"
echo ""
echo "💡 사용 방법:"
echo "   # 이미지 pull"
echo "   docker pull ${REGISTRY}/이미지명:태그"
echo ""
echo "   # 이미지 push"
echo "   docker tag 로컬이미지 ${REGISTRY}/이미지명:태그"
echo "   docker push ${REGISTRY}/이미지명:태그"
echo ""
echo "💡 테스트:"
echo "   curl -k https://${REGISTRY}/v2/_catalog"
