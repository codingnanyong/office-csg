# InfraWatch

[![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-Visualization-F46800?logo=grafana&logoColor=white)](https://grafana.com/)
[![Alertmanager](https://img.shields.io/badge/Alertmanager-Alerts-E6522C?logo=prometheus&logoColor=white)](https://prometheus.io/docs/alerting/latest/alertmanager/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Jenkins](https://img.shields.io/badge/Jenkins-CI%2FCD-D24939?logo=jenkins&logoColor=white)](https://www.jenkins.io/)
[![Nginx](https://img.shields.io/badge/Nginx-Reverse%20Proxy-009639?logo=nginx&logoColor=white)](https://www.nginx.com/)

## 📡 Infra Observability System

This is a project to build an infrastructure observation system based on DevOps.  
It configures indicator collection, visualization, notification, and dashboard portal based on Prometheus, Grafana, and Alertmanager.

## 🎯 Composition Goal

Preemptively configure Git, CI/CD, Docker Registry, and Green-Blue deployment structures that form the basis for observation system development and implement an automated system that enables continuous integration and continuous deployment.

## 🛠️ Tech Stack

1. Frontend : ⚛️ React
   - Dashboard : 📊 Grafana
   - Monitoring : 🔥Prometheus, 🚨Alertmanager, 📤Exporter
2. Web Server : 🚀 Nginx
3. CI/CD : ⚙️Jenkins + ☕︎Gitea + 🐳Docker Private Registry
4. Language : 🟨 JavaScript, 🐍Python

## 📁 Project Structure

```text
.
├── docker-compose.yml              # For running the integrated environment
├── prometheus.yml                  # Prometheus settings
├── alert_rules.yml                 # Prometheus notification rules
├── alertmanager.yml                # Alertmanager settings
├── .env                            # Setting environment variables
├── targets/                        # List of targets (JSON files)
├── grafana/                        # Dashboard JSON Backup
│   └── dashboards/
├── nginx/                          # Nginx reverse proxy
│   ├── nginx.conf                  # Nginx main config
│   └── conf.d/                     # Nginx upstream configs
│       ├── upstream.conf           # Active upstream config
│       └── upstream_temp.conf      # Temp upstream config
├── apps/                           # React Apps
├── notifier/                       # Notion Webhook Server (FastAPI...)
│   └── main.py
├── scripts/                        # Deployment scripts
│   └── deploy.sh                   # Blue-Green deployment script
├── Jenkinsfile                     # Deploy Pipeline
└── README.md                       # This file
```

## 🏃‍♂️ How to run

```bash
# Environment startup
docker compose up -d

# Prometheus: http://localhost:9090
# Grafana: http://localhost:3000
# Alertmanager: http://localhost:9093
```

## 📬 Coming Soon

- Alert Webhook Server for Notion integration
- React Portal Status Summary Dashboard
- Exporter Auto-registration Script

## 🤝 Contributing

Contributions are welcome!  
Feel free to open issues or submit pull requests.

### 👥 Maintainer

For questions or issues, please use the repository issue tracker.

## 📃 License

Copyright © Changsin Inc. All rights reserved.

## ✅ Conclusion

This project aims to provide a comprehensive infrastructure **Observability** system based on a fully automated **DevOps pipeline**.
It integrates monitoring, visualization, and notification in a single unified platform to help teams proactively manage and respond to infrastructure status.
