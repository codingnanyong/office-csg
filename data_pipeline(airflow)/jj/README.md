# 🌬️**Airflow Guide**

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Supported-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Celery](https://img.shields.io/badge/Celery-Executor-37814A?logo=celery&logoColor=white)](https://docs.celeryq.dev/)
[![Flower](https://img.shields.io/badge/Flower-Monitoring-FF6B6B?logo=flower&logoColor=white)](https://flower.readthedocs.io/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This document guides you through installing Apache **Airflow** using Docker Compose.

## 📁 Project Structure

```
airflow/
├── dags/                          # DAG 파일 디렉토리
│   ├── dbt/                       # dbt 프로젝트 DAGs
│   │   ├── banbury_anomaly_detection/  # Banbury 이상 탐지 프로젝트
│   │   └── sample_project/        # 샘플 dbt 프로젝트
│   ├── pipeline/                  # 데이터 파이프라인 DAGs
│   │   ├── data_transfer/         # 데이터 전송 파이프라인
│   │   ├── maintenance/           # 유지보수 파이프라인
│   │   ├── ml/                    # 머신러닝 파이프라인
│   │   ├── monitoring/            # 모니터링 파이프라인
│   │   ├── orchestration/         # 오케스트레이션 파이프라인
│   │   ├── production/            # 프로덕션 파이프라인
│   │   ├── quality/               # 데이터 품질 파이프라인
│   │   └── telemetry/             # 텔레메트리 파이프라인
│   └── sample/                    # 샘플 DAGs
├── db/                            # 데이터베이스 관련 스크립트
│   ├── data_quality/              # 데이터 품질 관련 SQL
│   ├── maintenance/               # 유지보수 관련 SQL
│   ├── monitoring/                 # 모니터링 관련 SQL
│   ├── production/                # 프로덕션 관련 SQL
│   ├── quality/                   # 품질 관련 SQL
│   └── telemetry/                 # 텔레메트리 관련 SQL
├── scripts/                       # 유틸리티 스크립트
│   ├── install_pytorch_docker.sh  # PyTorch 설치 스크립트
│   ├── install_tensorflow_docker.sh  # TensorFlow 설치 스크립트
│   ├── monitor_performance.sh     # 성능 모니터링 스크립트
│   └── quick_restart.sh           # 빠른 재시작 스크립트
├── configs/                       # 설정 파일
│   └── airflow.cfg                # Airflow 설정 파일
├── plugins/                       # 커스텀 플러그인
│   ├── hooks/                     # 커스텀 hooks
│   └── models/                    # 커스텀 models
├── auths/                         # 인증 관련 설정
├── models/                        # 모델 파일
├── oracle/                        # Oracle 관련 설정
├── docs/                          # 문서
├── Anomaly-Transformer/           # Anomaly Transformer 모델
├── docker-compose.yml             # Docker Compose 설정
├── requirements.txt               # Python 패키지 의존성
├── README.md                      # 이 파일
├── RESTART_GUIDE.md               # 재시작 가이드
└── PERFORMANCE_MONITORING.md      # 성능 모니터링 가이드
```

## 📚 Related Documentation

- **[RESTART_GUIDE.md](./RESTART_GUIDE.md)**: Airflow 설정 변경 후 안전한 재시작 가이드
- **[PERFORMANCE_MONITORING.md](./PERFORMANCE_MONITORING.md)**: 성능 모니터링 및 최적화 가이드
- **[dags/dbt/README.md](./dags/dbt/README.md)**: dbt 프로젝트 관리 가이드

## ⚙️ Prerequisites

* Docker and Docker Compose installed.
* Basic terminal command knowledge.
* `.env` file configured with necessary environment variables.

## 🛠️**Installation Steps**
### Step 1: Prepare for Installation
 1. 📁 Verify `docker-compose.yml` File Location
    * Ensure you are in the directory containing the `docker-compose.yml` file.
 2. 📄 Check `.env` File
    * Verify that all required environment variables are correctly set in the `.env` file.
 3. 📂 Create Mount Directories
    * Run the following command to create necessary directories:
    ```
    mkdir -p ./dags ./logs ./plugins
    ```
### Step 2: Initialize Airflow
 1. ⚙️ Initialize Airflow Environment
    * Run the following command to set up the Airflow database:
    ```
    docker compose up airflow-init
    ```
 2. 🚀 Start Airflow Services
    * Start WebServer, Scheduler, Triggerer, and optionally Worker and CLI:
    ```
    docker compose up -d
    ```
 3. 🌸 Start Flower (Optional)
    * To monitor the Celery Executor using Flower UI:
    ```
    docker compose up -d airflow-flower
    ```
### Step 3: Generate Security Keys
 1. 🔐 Access Scheduler or WebServer Container
    * Run the following command to access the container:
    ```
    docker exec -it <airflow-scheduler-container> bash
    ```
 2. 🔑 Generate Keys Using Python
    * Fernet Key:
    ```
    from cryptography.fernet import Fernet
    FERNET_KEY = Fernet.generate_key().decode()
    print(FERNET_KEY)
    ```
    * Secret Key:
    ```
    import os
    print(os.urandom(16).hex())
    ```
 3. ✍️ Update `.env` File
    * Add the generated keys to the `.env` file
    ```
    AIRFLOW__CORE__FERNET_KEY=<Generated-Fernet-Key>
    AIRFLOW__WEBSERVER__SECRET_KEY=<Generated-Secret-Key>
    ```
 4. ♻️ Apply Security Keys
    * Rebuild and restart the containers:
    ```
    docker compose up --build --force-recreate -d airflow-init
    docker compose up --build --force-recreate -d
    docker compose up --build --force-recreate -d airflow-flower
    ```
 5. ⬆️ worker Scale Up
    * Rebuild and Created the containers:
    ```
    docker-compose up -d --scale airflow-worker={i} airflow-worker
    ```

### Step 4: Verify Installation
 1. 📋 Check Container Status
    * Ensure all containers are running
    ```
    docker ps
    ```
 2. 🌐 Access Airflow WebServer UI
    * Open http://localhost:8080
 3. 🌸 (Optional) Access Flower UI
    * Open http://localhost:5555

## 📃 License
This project is licensed under the MIT License.

## 🤝 Contributing
Contributions are welcome! Feel free to open issues or submit pull requests. 

## 🚀 Quick Start Scripts

### Performance Monitoring
```bash
./scripts/monitor_performance.sh
```

### Quick Restart
```bash
./scripts/quick_restart.sh
```

### Install ML Libraries
```bash
# PyTorch 설치
./scripts/install_pytorch_docker.sh

# TensorFlow 설치
./scripts/install_tensorflow_docker.sh
```

## 📊 Key Features

- **Multi-Project dbt Support**: 여러 dbt 프로젝트를 독립적으로 관리
- **Pipeline Orchestration**: 데이터 전송, 유지보수, ML, 모니터링 등 다양한 파이프라인 지원
- **Performance Monitoring**: Flower UI 및 커스텀 스크립트를 통한 성능 모니터링
- **Scalable Architecture**: Celery Executor를 통한 수평 확장 지원
- **Custom Plugins**: 커스텀 hooks와 models를 통한 확장성

## ✅ Conclusion
This guide helps you set up **Airflow** with **Flower** using Docker Compose. This setting allows you to efficiently manage **work flow management** and **data collection**.