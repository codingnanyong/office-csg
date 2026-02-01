# ⏱️**InfluxDB Guide**

[![InfluxDB](https://img.shields.io/badge/InfluxDB-Time%20Series-22ADF6?logo=influxdb&logoColor=white)](https://www.influxdata.com/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Supported-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)

This document guides you through installing **InfluxDB** with docker compose.

## ⚙ Prerequisites

- Docker and Docker Compose installed.
- Basic terminal command knowledge.

## 📝 Create `docker-compose.yml`

Create a `docker-compose.yml` file with the following content:

```yml
services:
  influxdb2:
    image: influxdb:2
    container_name: influxdb
    env_file: .env
    ports:
      - 8086:8086
    environment:
      DOCKER_INFLUXDB_INIT_MODE: setup
      DOCKER_INFLUXDB_INIT_USERNAME_FILE: /run/secrets/influxdb2-admin-username
      DOCKER_INFLUXDB_INIT_PASSWORD_FILE: /run/secrets/influxdb2-admin-password
      DOCKER_INFLUXDB_INIT_ADMIN_TOKEN_FILE: /run/secrets/influxdb2-admin-token
      DOCKER_INFLUXDB_INIT_ORG: ${org}
      DOCKER_INFLUXDB_INIT_BUCKET: ${bucket}
    secrets:
      - influxdb2-admin-username
      - influxdb2-admin-password
      - influxdb2-admin-token
    volumes:
      - type: bind
        source: /media/de/data/influxdb2-data
        target: /var/lib/influxdb2
      - type: bind
        source: /media/de/data/influxdb2-config
        target: /etc/influxdb2
    healthcheck:
      test: ["CMD-SHELL", "curl --fail http://localhost:8086/health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s
    restart: always
    networks:
      - storage_network

networks:
  storage_network:
    driver: bridge
    external: true

secrets:
  influxdb2-admin-username:
    file: ./.env.influxdb2-admin-username
  influxdb2-admin-password:
    file: ./.env.influxdb2-admin-password
  influxdb2-admin-token:
    file: ./.env.influxdb2-admin-token
```

## 🔐 Create Secrets for Secure Credentials

Create the following secret files in the same directory as docker-compose.yml:

```bash
echo "{admin}" > .env.influxdb2-admin-username
echo "{password}" > .env.influxdb2-admin-password
echo "{token}" > .env.influxdb2-admin-token
```

Ensure that these files are not included in version control.

## 🚀 Running the Container

Navigate to the directory containing the docker-compose.yml file and run:

```bash
docker-compose up -d
```

## 🌐 Accessing InfluxDB

You can access InfluxDB via the web interface by opening:

```bash
http://localhost:8086
```

Alternatively, you can use the InfluxDB CLI inside the container:

```bash
docker exec -it influxdb influx
```

To check database status:

```bash
influx bucket list
```

## 🛑 Stopping and Removing Container

To stop the services, run:

```bash
docker-compose down
```

To remove the containers and volumes:

```bash
docker-compose down -v
```

## 📃 License

Copyright © Changsin Inc. All rights reserved.

## 🤝 Contributing

Contributions are welcome!
Feel free to open issues or submit pull requests to improve the system.

## ✅ Conclusion

This guide helps you install **InfluxDB** using Docker Compose, configure authentication, and manage data persistence. Modify settings as needed to suit your deployment.
