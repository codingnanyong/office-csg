# OpenAPI Blue-Green Deployment System

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Nginx](https://img.shields.io/badge/Nginx-Reverse%20Proxy-009639?logo=nginx&logoColor=white)](https://www.nginx.com/)
[![Jenkins](https://img.shields.io/badge/Jenkins-CI%2FCD-D24939?logo=jenkins&logoColor=white)](https://www.jenkins.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Supported-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)

This project implements an automated Blue-Green deployment system using Jenkins.

## 🏗️ Architecture

```text
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Jenkins   │───▶│    Nginx    │───▶│ OpenAPI App │
│   Pipeline  │    │   (Proxy)   │    │ (Blue/Green)│
└─────────────┘    └─────────────┘    └─────────────┘
```

## 📁 Project Structure

```text
openapi/
├── app/                          # FastAPI application
│   ├── config/                   # Configuration files
│   │   ├── config.py            # Application configuration
│   │   └── db.json              # Database configuration
│   ├── database/                 # Database connection and setup
│   ├── models/                   # Data models (DAO, Data Access Object)
│   ├── routers/                  # API routes
│   │   └── v1/                   # API version 1
│   │       ├── hq/              # HQ related endpoints
│   │       └── vj/              # VJ related endpoints
│   ├── schemas/                  # Pydantic schemas (DTO, Data Transfer Object)
│   ├── services/                 # Business logic and services
│   │   ├── hq/                  # HQ business services
│   │   ├── vj/                  # VJ business services
│   │   └── helpers/             # Helper services
│   ├── tests/                    # Unit and integration tests
│   └── main.py                   # FastAPI entry point
├── docker/                       # Docker configuration
│   ├── Dockerfile               # Multi-stage Dockerfile for FastAPI
│   └── docker-compose.ops.yml   # Production Blue-Green deployment setup
├── nginx/                        # Nginx reverse proxy configuration
│   └── nginx.conf               # Main Nginx configuration
├── scripts/                      # Deployment and utility scripts
│   └── deploy.sh                # Blue-Green deployment automation script
├── requirements.txt              # Python dependencies
├── Jenkinsfile                   # Jenkins CI/CD pipeline
├── README.md                     # Project documentation
├── SLA.md                        # Service Level Agreement
├── LICENSE                       # MIT License
└── .gitignore                    # Git ignore rules
```

## 🚀 Deployment Process

### 1. Automated Deployment (Jenkins)

When you push to the master branch, Jenkins automatically executes the following stages:

1. **Checkout**: Source code checkout
2. **Docker Permissions**: Docker permissions verification
3. **Tests**: Test execution
4. **Build**: Docker image building
5. **Deploy Blue-Green**: Blue-Green deployment execution
6. **Verify**: Deployment verification
7. **Post-Deployment Tests**: Post-deployment testing

### 2. Manual Deployment

```bash
# Execute deployment script
cd /home/de/apps/openapi
./scripts/deploy.sh
```

## 🔧 Blue-Green Deployment Method

### Current State

- **Blue Container**: Port 8001
- **Green Container**: Port 8002
- **Nginx**: Port 80 (Load Balancer)

### Deployment Process

1. Check current active version (`/deploy` endpoint)
2. Deploy new image to inactive version
3. Health check for new container
4. Update Nginx upstream configuration
5. Traffic switching
6. Clean up previous container

## 📊 Monitoring

### Health Check Endpoints

- `{OPENAPI_HOST}/health` - Nginx status
- `{OPENAPI_HOST}/deploy` - Current deployment version
- `{OPENAPI_HOST}/metrics` - Application metrics

### Log Monitoring

```bash
# Container logs
docker logs openapi_blue
docker logs openapi_green
docker logs nginx

# Real-time logs
docker logs -f openapi_blue
```

## 🛠️ Troubleshooting

### Common Issues

1. **Docker Permission Issues**

   ```bash
   sudo usermod -aG docker jenkins
   sudo systemctl restart jenkins
   ```

2. **Nginx Configuration Errors**

   ```bash
   docker exec nginx nginx -t
   docker exec nginx nginx -s reload
   ```

3. **Container Health Check Failures**

   ```bash
   docker ps
   docker logs openapi_blue --tail 20
   ```

### Rollback Method

```bash
# Manual rollback to previous version
cd /home/de/apps/openapi
docker-compose -f docker/docker-compose.ops.yml up -d blue  # or green
```

## 🔒 Security Considerations

- Security headers added to Nginx
- Docker container isolation
- Log rotation configuration
- Automatic backup file cleanup

## 📈 Performance Optimization

- Nginx Gzip compression enabled
- Docker image layer optimization
- Container resource limit settings
- Load balancing optimization

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📝 License

Copyright © Changsin Inc. All rights reserved.
