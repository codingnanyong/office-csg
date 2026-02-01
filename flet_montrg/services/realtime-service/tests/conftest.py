"""
Pytest configuration and fixtures for Realtime Service
"""
import pytest
import sys
import os
from pathlib import Path

# 서비스 루트 디렉토리를 Python 경로에 추가
service_root = Path(__file__).parent.parent
if str(service_root) not in sys.path:
    sys.path.insert(0, str(service_root))

pytestmark = pytest.mark.asyncio
