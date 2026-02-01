#!/bin/bash

# PLC 데이터 추출 스크립트
# Usage: ./run_plc_extractor.sh

set -e  # Exit on any error

# Set working directory to script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "PLC 데이터 추출기 실행"
echo "=========================================="

# Check if Python3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3가 설치되지 않았습니다. Python3를 먼저 설치해주세요."
    exit 1
fi

echo "✓ Python3 확인: $(python3 --version)"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ 가상환경이 존재하지 않습니다. 먼저 가상환경을 생성해주세요."
    echo "   python3 -m venv venv"
    exit 1
fi

echo "✓ 가상환경 확인됨"

# Activate virtual environment
echo "🔧 가상환경 활성화 중..."
source venv/bin/activate

echo "✓ 가상환경 활성화 완료!"
echo "  Python 버전: $(python --version)"
echo "  Pip 버전: $(pip --version)"

# Set Python path
export PYTHONPATH="$SCRIPT_DIR:$PYTHONPATH"

# Check if main.py exists
if [ ! -f "app/main.py" ]; then
    echo "❌ app/main.py 파일을 찾을 수 없습니다!"
    exit 1
fi

# Check if remote directories exist
if [ ! -d "/media/btx/plc_extractor/logs" ]; then
    echo "📁 원격 로그 디렉토리 생성 중..."
    mkdir -p /media/btx/plc_extractor/logs
    echo "✓ /media/btx/plc_extractor/logs 디렉토리 생성됨"
fi

if [ ! -d "/media/btx/plc_extractor/data" ]; then
    echo "📁 원격 데이터 디렉토리 생성 중..."
    mkdir -p /media/btx/plc_extractor/data
    echo "✓ /media/btx/plc_extractor/data 디렉토리 생성됨"
fi

echo ""
echo "=========================================="
echo "🚀 PLC 데이터 추출 시작"
echo "=========================================="

# Record start time
START_TIME=$(date +%s)

# Run the PLC data extraction
if python3 -m app.main; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    echo "✅ PLC 데이터 추출 성공 (소요시간: ${DURATION}초)"
else
    echo "❌ PLC 데이터 추출 실패"
    exit 1
fi

echo ""
echo "=========================================="
echo "✅ PLC 데이터 추출 완료!"
echo "=========================================="
echo "📁 로그 파일: logs/ 디렉토리 확인"
echo "📁 데이터 파일: data/ 디렉토리 확인"

# 가상환경 자동 비활성화
echo "🔧 가상환경 비활성화 중..."
deactivate
echo "✓ 가상환경 비활성화 완료"