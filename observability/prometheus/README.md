# 🔥**Prometheus Monitoring Guide**

[![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Alertmanager](https://img.shields.io/badge/Alertmanager-Alerts-E6522C?logo=prometheus&logoColor=white)](https://prometheus.io/docs/alerting/latest/alertmanager/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Supported-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)

This project sets up **Prometheus** using Docker Compose for metrics collection and monitoring.

## 📁 Project Structure

```bash
.
├── docker-compose.yml      # Docker Compose configuration
├── prometheus.yml          # Prometheus configuration file
├── alertmanager.yml        # Alertmanager configuration file
├── alert_rules.yml         # Alert Rules configuration file
├── LICENSE                 # License file (MIT)
└── README.md               # This documentation
```

## 🚀 Getting Started

📋 **Prerequisites**

- Docker (v20.x or higher)
- Docker Compose (v2.x or higher)

## ⚙️ Prometheus Configuration

### 1️⃣ prometheus.yml

- Define your scrape jobs and Prometheus settings here.
- Example:

  ```yml
  global:
    scrape_interval: 15s

  alerting:
    alertmanagers:
      - static_configs:
          - targets: ["alertmanager:9093"]

  rule_files:
    - "alert_rules.yml"

  scrape_configs:
    - job_name: "prometheus"
      static_configs:
        - targets: ["localhost:9090"]

    # Add other exporters here
    - job_name: "node_exporter"
      static_configs:
        - targets: ["node_exporter:9100"]
  ```

### 2️⃣ alertmanager.yml

- Define your alert route and Receivers settings here.
- Example:

  ```yml
  global:
    resolve_timeout: 5m

  route:
    receiver: "default"
    group_wait: 10s
    group_interval: 30s
    repeat_interval: 1h

  receivers:
    - name: "default"
      webhook_configs:
        - url: "http://your-webhook-server/alert"
  ```

### 3️⃣ alert_rules.yml

- Alert rules settings here
- Example:

  ```yml
  groups:
    - name: instance-health
      rules:
        - alert: InstanceDown
          expr: up == 0
          for: 30s
          labels:
            severity: critical
          annotations:
            summary: "🔴 {{ $labels.instance }} is down"
            description: "Prometheus could not reach {{ $labels.instance }} for over 30 seconds."
  ```

## 🏃‍♂️ Run the Containers

```bash
# Build and start containers
docker-compose up -d

# Check running containers
docker ps
```

## 🛑 Stop the Containers

```bash
docker-compose down
```

## 📃 License

Copyright © Changsin Inc. All rights reserved.

## 🤝 Contributing

Contributions are welcome!  
Feel free to open issues or submit pull requests to improve the system.

### ✅**Conclusion**

This guide helps you set up **Prometheus** using Docker Compose. With this setup, you can render dashboards as images and manage your visualizations efficiently.
