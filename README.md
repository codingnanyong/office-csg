# Develop Working by CSG

[![.NET](https://img.shields.io/badge/.NET-6.0-512BD4?logo=dotnet&logoColor=white)](https://dotnet.microsoft.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Deployed-326CE5?logo=kubernetes&logoColor=white)](https://kubernetes.io/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-Visualization-F46800?logo=grafana&logoColor=white)](https://grafana.com/)

## About

![Logo](Image/Logo.png)

This repository contains multiple projects and tooling for data engineering, APIs, monitoring and IoT.

## ⚙️ Setup Guide

### Environment Configuration

Copy `.env.example` to `.env` in each service directory and update with actual values:

```bash
cp .env.example .env
# Edit .env with your actual configuration
```

### Configuration Placeholders

Replace these placeholders in configuration files:

- `{DB_HOST}`, `{DB_PORT}`, `{DB_USER}`, `{DB_PASSWORD}` - Database credentials
- `{K8S_HOST}` - Kubernetes cluster host
- `{SMTP_EMAIL}`, `{SMTP_PASSWORD}` - Email account
- `{JWT_SECRET_KEY}` - JWT secret
- `{API_KEY}`, `{TOKEN}` - API credentials

⚠️ **Security Warning:** Never commit real credentials, IP addresses, or sensitive data. See [SECURITY.md](SECURITY.md) for details.

### Highlights

- Automated Blue‑Green deployments for OpenAPI services with Jenkins + Nginx
- Production‑ready observability: Prometheus alerting, Grafana dashboards, exporter suite
- Industrial IoT monitoring (TR) with anomaly alerts and dashboards
- Reusable Docker Compose stacks for DBs and private registry

## 리포지토리 구조

### 📦 데이터 파이프라인 & ETL

- `auto_etl/`: 공장 데이터 수집·적재용 .NET ETL 도구
- `data_pipeline(airflow)/`: Apache Airflow 기반 데이터 파이프라인 (HQ, JJ 공장별)
  - HQ: [`data_pipeline(airflow)/hq/README.md`](<data_pipeline(airflow)/hq/README.md>)
  - JJ: [`data_pipeline(airflow)/jj/README.md`](<data_pipeline(airflow)/jj/README.md>)
- `plc_extractor/`: PLC 데이터 추출 서비스 (Python)
  - 상세: [`plc_extractor/README.md`](plc_extractor/README.md)

### 🏭 FDW & Open API

- `fdw_api/`: FDW(Data Warehouse) .NET 솔루션 모노레포
  - 상세: [`fdw_api/README.md`](fdw_api/README.md)
- `open_api/`: Python FastAPI 기반 Open API 서비스 및 배포 스크립트
  - 상세: [`open_api/README.md`](open_api/README.md)
- `unified_montrg_api/`: Kubernetes 클러스터용 통합 모니터링 API (FastAPI)
  - 상세: [`unified_montrg_api/README.md`](unified_montrg_api/README.md)

### ⚡ IoT 모니터링

- `tr_montrg/`: 변압기 모니터링/이상탐지 .NET 솔루션
  - 상세: [`tr_montrg/README.md`](tr_montrg/README.md)
- `flet_montrg/`: 체감 온도 모니터링 마이크로서비스 (Python + K8s)
  - 상세: [`flet_montrg/README.md`](flet_montrg/README.md)
- `edge-hmi/`: Edge HMI 모니터링·유지보수 (TimescaleDB + FastAPI)
  - 상세: [`edge-hmi/README.md`](edge-hmi/README.md)

### 🗄️ 데이터 인프라

- `data_engineer/`: 데이터베이스/레지스트리 인프라 구성(docker-compose)
  - InfluxDB: [`data_engineer/influxdb/README.md`](data_engineer/influxdb/README.md)
  - MongoDB: [`data_engineer/mongodb/README.md`](data_engineer/mongodb/README.md)
  - Postgres: [`data_engineer/postgres/README.md`](data_engineer/postgres/README.md)
  - Registry: [`data_engineer/registry/README.md`](data_engineer/registry/README.md)
  - Docker 설치: [`data_engineer/docker_install(no enternet)/README.md`](<data_engineer/docker_install(no%20enternet)/README.md>)

### 📊 관측성(Observability)

- `observability/`: 모니터링 스택(Prometheus, Grafana, Exporters)
  - Grafana: [`observability/grafana/README.md`](observability/grafana/README.md)
  - Prometheus: [`observability/prometheus/README.md`](observability/prometheus/README.md)
  - Exporters: [`observability/exporter/README.md`](observability/exporter/README.md)
- `infrawatch/`: 인프라 관측 - Alertmanager/타깃/리버스프록시 등
  - 상세: [`infrawatch/README.md`](infrawatch/README.md)

### ☸️ Infrastructure & DevOps

- `k8s_guide/`: CKA 시험 준비용 Kubeadm 클러스터 구축 가이드
  - 상세: [`k8s_guide/README.md`](k8s_guide/README.md)
- `docker_private_registry/`: Docker Private Registry 웹 UI 및 설치·인증서 가이드
  - 상세: [`docker_private_registry/README.md`](docker_private_registry/README.md)

### 🔧 유틸리티

- `data_editor/`: CSV 등 데이터 편집 스크립트와 샘플 데이터
  - 상세: [`data_editor/README.md`](data_editor/README.md)
- `cooperation/Genetic Algorithm/`: AnyLogic/유전 알고리즘 실험 자산
  - 상세: [`cooperation/Genetic Algorithm/README.md`](cooperation/Genetic%20Algorithm/README.md)
- `Image/`: 문서용 스크린샷 및 아키텍처 이미지

각 디렉터리의 상세 사용법과 설정은 해당 경로의 `README.md`를 참고하세요.

### Do not commit (covered by .gitignore)

- Secrets and environment files: `.env`, `.env.*`, any credentials, tokens
- Certificates/keys: `*.pem`, `*.key`, `*.pfx`, `*.p12`, `*.crt`, `*.cer`, `*.csr`, `*.srl`
- Database dumps/backups: `*.sql`, directories like `bak/`, `backup/`, `backups/`, `dump/`, `dumps/`
- Build artifacts: `.NET` `bin/`, `obj/`; Python `__pycache__/`, `dist/`, `build/`; Java `target/`, `out/`
- IDE settings: `.vscode/`, `.idea/`, `.vs/`

### Maintainer

👤 Codingnanyong (TaeHyeon.Ryu)

For contact, please use repository issues.

### 📃 License

Copyright © Changsin Inc. All rights reserved.  
This is proprietary software developed for internal company use.
