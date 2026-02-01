#!/bin/bash

# Sensor Threshold Mapping Service 테스트 스크립트

set -e

echo "🧪 Sensor Threshold Mapping Service 테스트 시작..."

# 가상 환경 활성화 (있는 경우)
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# pip 명령어 확인 (pip3 또는 python3 -m pip)
if command -v pip3 &> /dev/null; then
    PIP_CMD="pip3"
elif command -v python3 &> /dev/null; then
    PIP_CMD="python3 -m pip"
elif command -v pip &> /dev/null; then
    PIP_CMD="pip"
else
    echo "❌ pip 또는 pip3를 찾을 수 없습니다."
    exit 1
fi

# pytest 명령어 확인
if ! command -v pytest &> /dev/null; then
    echo "📦 의존성 설치 중..."
    cd "$(dirname "$0")/.."
    $PIP_CMD install -r requirements.txt
    cd - > /dev/null
fi

# 테스트 실행
echo "🚀 테스트 실행 중..."
pytest tests/ -v

echo "✅ 테스트 완료!"
