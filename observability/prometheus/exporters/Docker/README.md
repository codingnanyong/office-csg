# Docker Exporters

This folder contains configuration files for running various exporters using Docker.

## 📁 Included Exporters

### 🐳 Docker Compose Files

| Exporter                | Filename                           | Port | Description                      |
| ----------------------- | ---------------------------------- | ---- | -------------------------------- |
| **Node Exporter**       | `docker-compose.node_exporter.yml` | 9100 | System metrics collection        |
| **MongoDB Exporter**    | `docker-compose.mongodb.yml`       | 9216 | MongoDB metrics collection       |
| **InfluxDB Exporter**   | `docker-compose.influxdb.yml`      | 9122 | InfluxDB metrics collection      |
| **PostgreSQL Exporter** | `docker-compose.postgres.yml`      | 9187 | PostgreSQL metrics collection    |
| **MySQL Exporter**      | `docker-compose.mysql.yml`         | 9104 | MySQL/MariaDB metrics collection |
| **Redis Exporter**      | `docker-compose.redis.yml`         | 9121 | Redis metrics collection         |

## 🚀 Usage

### 1. Individual Exporter Execution

```bash
# Start Node Exporter
docker-compose -f docker-compose.node_exporter.yml up -d

# Start MongoDB Exporter
docker-compose -f docker-compose.mongodb.yml up -d

# Start PostgreSQL Exporter
docker-compose -f docker-compose.postgres.yml up -d
```

### 2. Environment Variable Configuration

Each exporter can be configured through environment variables:

```bash
# Example: PostgreSQL Exporter
export DATA_SOURCE_NAME="postgresql://user:password@localhost:5432/database"
docker-compose -f docker-compose.postgres.yml up -d
```

## ⚙️ Configuration Guide

### Node Exporter

- **Purpose**: System resource monitoring (CPU, Memory, Disk, Network)
- **Port**: 9100
- **Metrics Path**: `/metrics`

### MongoDB Exporter

- **Purpose**: MongoDB database monitoring
- **Port**: 9216
- **Required Environment Variable**: `MONGODB_URI`

### PostgreSQL Exporter

- **Purpose**: PostgreSQL database monitoring
- **Port**: 9187
- **Required Environment Variable**: `DATA_SOURCE_NAME`

### MySQL Exporter

- **Purpose**: MySQL/MariaDB database monitoring
- **Port**: 9104
- **Required Environment Variable**: `DATA_SOURCE_NAME`

## 🔗 Prometheus Integration

Once each exporter is running, you can add it to Prometheus `targets` configuration to collect metrics:

```yaml
# prometheus.yml example
scrape_configs:
  - job_name: "node-exporter"
    static_configs:
      - targets: ["localhost:9100"]

  - job_name: "postgresql-exporter"
    static_configs:
      - targets: ["localhost:9187"]
```

## 📊 Monitoring Dashboards

You can use Grafana dashboard templates for each exporter:

- **Node Exporter**: [1860](https://grafana.com/grafana/dashboards/1860)
- **MongoDB Exporter**: [2583](https://grafana.com/grafana/dashboards/2583)
- **PostgreSQL Exporter**: [9628](https://grafana.com/grafana/dashboards/9628)
- **MySQL Exporter**: [7362](https://grafana.com/grafana/dashboards/7362)

## 🛠️ Troubleshooting

### Common Issues

1. **Port Conflicts**

   ```bash
   # Check for used ports
   netstat -tulpn | grep :9100
   ```

2. **Permission Issues**

   ```bash
   # Check Docker permissions
   docker ps
   docker logs <container_name>
   ```

3. **Network Connectivity**

   ```bash
   # Test exporter connection
   curl http://localhost:9100/metrics
   ```

## 📝 References

- [Prometheus Exporters Official Documentation](https://prometheus.io/docs/instrumenting/exporters/)
- [Docker Compose Official Documentation](https://docs.docker.com/compose/)
- [Node Exporter GitHub](https://github.com/prometheus/node_exporter)
- [PostgreSQL Exporter GitHub](https://github.com/prometheus-community/postgres_exporter)
