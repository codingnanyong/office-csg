#!/bin/bash

# Alert Subscription Service 테스트 실행 스크립트

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🧪 Alert Subscription Service 테스트 시작..."

# 가상환경 확인 및 생성
if [ ! -d "venv" ]; then
    echo "📦 가상환경 생성 중..."
    python3 -m venv venv
fi

# 가상환경 활성화
echo "🔌 가상환경 활성화..."
source venv/bin/activate

# 의존성 설치
echo "📥 의존성 설치 중..."
pip install --upgrade pip
pip install -r requirements.txt

# 테스트 실행
echo "🚀 테스트 실행 중..."
pytest tests/ -v --tb=short --cov=app --cov-report=term-missing --cov-report=html:htmlcov

echo ""
echo "✅ 테스트 완료!"
echo "📊 커버리지 리포트: htmlcov/index.html"
