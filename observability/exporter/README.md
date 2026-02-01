# **Monitoring Exporters Guide**

This project sets up **Prometheus Exporters** for **MongoDB**, **PostgreSQL**, and **InfluxDB** using Docker Compose to collect and monitor metrics.

## 📁 Project Structure

```bash
.
├── docker-compose.yml  # Exporter definitions
├── .env                # Environment file
└── README.md           # Documentation file
```

## 🚀 Getting Started

📋 Prerequisites

- Docker (v20.x or higher)
- Docker Compose (v2.x or higher)
- Prometheus and Grafana (optional, for visualization)

## ⚙️ Environment Variables

Create a `.env` file and add the following:

```bash
# MongoDB
MONGO_USER=exporter_user
MONGO_PASSWORD=your_secure_password

# PostgreSQL
POSTGRES_USER=exporter_user
POSTGRES_PASSWORD=your_secure_password

# InfluxDB
INFLUXDB_TOKEN=your_influxdb_token
INFLUXDB_ORG={influxdb_org}
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

## 📡 Exporter Details

### 1️⃣ MongoDB Exporter (mongodb-exporter)

- Image: bitnami/mongodb-exporter:latest
- Port: 9216
- Environment Variables:
  > `MONGODB_URI`: MongoDB connection string  
  > `MONGODB_COLLECTOR_COLLSTATS`: Collect collection stats (default: true)  
  > `MONGODB_COLLECTOR_DBPSTATS`: Collect DB stats (default: true)  
  > `MONGODB_COLLECTOR_REPLSET`: Collect replica set info (default: true)  
  > `MONGODB_COLLECTOR_TOPMETRICS`: Collect top metrics (default: true)  
  > `MONGODB_COLLECTOR_INDEXUSAGE`: Collect index usage stats (default: true)
- 🔍 Verify Metrics

  ```bash
  curl http://localhost:9216/metrics
  ```

### 2️⃣ PostgreSQL Exporter (postgres-exporter)

- Image: prometheuscommunity/postgres-exporter
- Port: 9187
- Environment Variables:
  > `DATA_SOURCE_NAME`: PostgreSQL connection string
- 🔍 Verify Metrics

  ```bash
  curl http://localhost:9187/metrics
  ```

### 3️⃣ InfluxDB Exporter (influxdb-exporter)

- Image: prom/influxdb-exporter
- Port: 9122
- Environment Variables:
  > `INFLUXDB_ADDR`: InfluxDB URL (e.g., http://influxdb:8086)  
  > `INFLUXDB_TOKEN`: InfluxDB authentication token  
  > `INFLUXDB_ORG`: InfluxDB organization name
- 🔍 Verify Metrics

  ```bash
  curl http://localhost:9122/metrics
  ```

## 📊 Prometheus Configuration

Add the following targets to your prometheus.yml:

```bash
scrape_configs:
  - job_name: 'mongodb-exporter'
    static_configs:
      - targets: ['mongodb-exporter:9216']

  - job_name: 'postgres-exporter'
    static_configs:
      - targets: ['postgres-exporter:9187']

  - job_name: 'influxdb-exporter'
    static_configs:
      - targets: ['influxdb-exporter:9122']
```

### 📈 Recommended Grafana Dashboards

Grafana can visualize the metrics collected by Prometheus.

📂 Grafana.com Dashboard Recommendations:

- MongoDB: ID: 2583
- PostgreSQL: ID: 9628
- InfluxDB: ID: 12481

## 📃 License

Copyright © Changsin Inc. All rights reserved.

## 🤝 Contributing

Contributions are welcome!
Feel free to open issues or submit pull requests to improve the system.

## ✅ Conclusion

This guide helps you set up **Prometheus Exporters** using Docker Compose. With this setup, you can render dashboards as images and manage your visualizations efficiently.
