# 🌬️**Airflow Guide**

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Supported-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Celery](https://img.shields.io/badge/Celery-Executor-37814A?logo=celery&logoColor=white)](https://docs.celeryq.dev/)
[![Flower](https://img.shields.io/badge/Flower-Monitoring-FF6B6B?logo=flower&logoColor=white)](https://flower.readthedocs.io/)

This document guides you through installing Apache **Airflow** using Docker Compose.

## ⚙️ Prerequisites

- Docker and Docker Compose installed.
- Basic terminal command knowledge.
- `.env` file configured with necessary environment variables.

## 🛠️**Installation Steps**

### Step 1: Prepare for Installation

1. 📁 Verify `docker-compose.yml` File Location
   - Ensure you are in the directory containing the `docker-compose.yml` file.
2. 📄 Check `.env` File
   - Verify that all required environment variables are correctly set in the `.env` file.
3. 📂 Create Mount Directories

   - Run the following command to create necessary directories:

   ```bash
   mkdir -p ./dags ./logs ./plugins
   ```

### Step 2: Initialize Airflow

1. ⚙️ Initialize Airflow Environment

   - Run the following command to set up the Airflow database:

   ```bash
   docker compose up airflow-init
   ```

2. 🚀 Start Airflow Services

   - Start WebServer, Scheduler, Triggerer, and optionally Worker and CLI:

   ```bash
   docker compose up -d
   ```

3. 🌸 Start Flower (Optional)

   - To monitor the Celery Executor using Flower UI:

   ```bash
   docker compose up -d airflow-flower
   ```

### Step 3: Generate Security Keys

1. 🔐 Access Scheduler or WebServer Container

   - Run the following command to access the container:

   ```bash
   docker exec -it <airflow-scheduler-container> bash
   ```

2. 🔑 Generate Keys Using Python

   - Fernet Key:

   ```bash
   from cryptography.fernet import Fernet
   FERNET_KEY = Fernet.generate_key().decode()
   print(FERNET_KEY)
   ```

   - Secret Key:

   ```bash
   import os
   print(os.urandom(16).hex())
   ```

3. ✍️ Update `.env` File

   - Add the generated keys to the `.env` file

   ```bash
   AIRFLOW__CORE__FERNET_KEY=<Generated-Fernet-Key>
   AIRFLOW__WEBSERVER__SECRET_KEY=<Generated-Secret-Key>
   ```

4. ♻️ Apply Security Keys

   - Rebuild and restart the containers:

   ```bash
   docker compose up --build --force-recreate -d airflow-init
   docker compose up --build --force-recreate -d
   docker compose up --build --force-recreate -d airflow-flower
   ```

5. ⬆️ worker Scale Up

   - Rebuild and Created the containers:

   ```bash
   docker-compose up -d --scale airflow-worker={i} airflow-worker
   ```

### Step 4: Verify Installation

1. 📋 Check Container Status

   - Ensure all containers are running

   ```bash
   docker ps
   ```

2. 🌐 Access Airflow WebServer UI
   - [Open URL](http://localhost:8080)
3. 🌸 (Optional) Access Flower UI
   - [Open URL](http://localhost:5555)

## 📃 License

Copyright © Changsin Inc. All rights reserved.

## 🤝 Contributing

Contributions are welcome!
Feel free to open issues or submit pull requests to improve the system.

## ✅ Conclusion

This guide helps you set up **Airflow** with **Flower** using Docker Compose. This setting allows you to efficiently manage **work flow management** and **data collection**.
