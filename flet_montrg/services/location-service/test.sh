#!/bin/bash

# Location Service Test Script

set -e

echo "🧪 Location Service 테스트 시작..."

# 가상환경 확인 및 생성
if [ ! -d "venv" ]; then
    echo "📦 가상환경 생성..."
    python3 -m venv venv
fi

# 가상환경 활성화
echo "🔧 가상환경 활성화..."
source venv/bin/activate

# 의존성 설치
echo "📥 의존성 설치..."
pip install -r requirements.txt
pip install -r requirements-test.txt

# 테스트 실행
echo "🚀 테스트 실행..."
pytest tests/ -v --cov=app --cov-report=html

# 코드 포맷팅 (선택사항)
if [ "$1" == "--format" ]; then
    echo "🎨 코드 포맷팅..."
    black app/ tests/
fi

# 린팅 (선택사항)
if [ "$1" == "--lint" ]; then
    echo "🔍 코드 린팅..."
    flake8 app/ tests/
    mypy app/
fi

echo "✅ 테스트 완료!"
echo "📊 커버리지 리포트: htmlcov/index.html"
