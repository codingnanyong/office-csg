# PLC Extractor

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Modbus](https://img.shields.io/badge/Modbus-Protocol-FF6B35?logo=modbus&logoColor=white)](https://modbus.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Supported-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

PLC 데이터 추출 및 저장 서비스

## 📖 개요

PLC(Programmable Logic Controller)로부터 실시간 데이터를 수집하고 데이터베이스에 저장하는 Python 기반 서비스입니다.

## 🚀 실행

### 환경 변수 설정

```bash
cp env.example .env
# .env 파일을 편집하여 필요한 설정을 입력

# PLC 주소 맵 설정
cp config/plc_addresses.json.example config/plc_addresses.json
# plc_addresses.json을 편집하여 실제 PLC 메모리 맵 설정
```

### 실행

```bash
# 의존성 설치
pip install -r requirements.txt

# 서비스 실행
./plc_extractor.sh
```

## 🧪 테스트

```bash
pytest tests/
```

## 📝 License

Copyright © Changsin Inc. All rights reserved.
