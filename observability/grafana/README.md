# 📊**Grafana Guide**

[![Grafana](https://img.shields.io/badge/Grafana-Visualization-F46800?logo=grafana&logoColor=white)](https://grafana.com/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Supported-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)

This document guides you through installing **Grafana** and **Grafana Image Renderer** using Docker Compose.

## ⚙️ Prerequisites

- Docker and Docker Compose installed.
- Basic terminal command knowledge.
- `.env` file configured with necessary environment variables.

## 📝 Create `docker-compose.yml`

Create a `docker-compose.yml` file with the following content:

```yml
services:
  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_USER=${GF_SECURITY_ADMIN_USER}
      - GF_SECURITY_ADMIN_PASSWORD=${GF_SECURITY_ADMIN_PASSWORD}
      - GF_PATHS_DATA=${GF_PATHS_DATA}
      - GF_RENDERER_PLUGIN_RENDERING_URL=http://renderer:8081/render
      - GF_RENDERER_PLUGIN_RENDERING_CALLBACK_URL=http://localhost:3000
    volumes:
      - {path}/grafana:/var/lib/grafana
      - {path}/grafana/plugins:/var/lib/grafana/plugins
      - {path}/grafana/provisioning:/etc/grafana/provisioning
      - ./img:/usr/share/grafana/public/img/bg
    restart: always
    networks:
      - grafana-network
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:3000/api/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s

  renderer:
    image: grafana/grafana-image-renderer:latest
    container_name: grafana-renderer
    environment:
      - ENABLE_METRICS=true
    ports:
      - "8081:8081"
    restart: always
    networks:
      - grafana-network
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8081/"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s

networks:
  grafana-network:
    driver: bridge
    external: true
```

## 🔐 Configure Environment Variables

Create a `.env` file in the same directory as your `docker-compose.yml` with the following content:

```bash
GF_SECURITY_ADMIN_USER={user}
GF_SECURITY_ADMIN_PASSWORD={password}
GF_PATHS_DATA=/var/lib/grafana
```

## 🚀 Running the Container

Navigate to the directory containing the docker-compose.yml file and run:

```bash
docker-compose up -d
```

## 🌐 Accessing Grafana

Open your web browser and navigate to:

```bash
http://localhost:3000
```

1. Default Login Credentials:

- Username: as per `GF_SECURITY_ADMIN_USER`
- Password: as per `GF_SECURITY_ADMIN_PASSWORD`

## 🖼️ Verifying Grafana Image Renderer

Ensure that the Grafana Image Renderer is working by exporting a dashboard panel as an image. The Renderer should handle the image generation via:

```bash
http://localhost:8081/
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

This guide helps you set up **Grafana** with Grafana Image Renderer using Docker Compose. With this setup, you can render dashboards as images and manage your visualizations efficiently.
