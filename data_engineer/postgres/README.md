# 🐘**PostgreSQL Guide**

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Supported-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)

This document guides you through installing **PostgreSQL** with Docker Compose.

## ⚙️ Prerequisites

- Docker and Docker Compose installed.
- Basic terminal command knowledge.

## 📝 Create `docker-compose.yml`

Create a `docker-compose.yml` file with the following content:

```yml
services:
  postgresql:
    image: postgres:{version}
    container_name: postgres
    env_file: .env
    ports:
      - 5432:5432
    environment:
      - POSTGRES_DB=${POSTGRES_DB}
      - POSTGRES_USER=${POSTGRES_USER}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
    volumes:
      - ${POSTGRES_PATH}/data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s
    restart: always
    networks:
      - { network_name }

networks:
  { network_name }:
    driver: bridge
    external: true
```

## 🚀 Running the Container

Navigate to the directory containing the docker-compose.yml file and run:

```bash
docker-compose up -d
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

This guide helps you install **PostgreSQL** using Docker Compose, configure authentication, and manage data persistence. Modify settings as needed to suit your deployment.
