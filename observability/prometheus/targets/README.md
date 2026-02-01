# Prometheus Targets

This directory contains target configuration files for Prometheus monitoring.

## ⚠️ Security Notice

**DO NOT commit actual IP addresses or hostnames to this directory.**

All target JSON files in this directory are gitignored. Use template files instead.

## 📋 Setup Instructions

### 1. Create Target Files from Templates

Each subdirectory should contain `.template.json` files with placeholders:

```json
[
  {
    "targets": ["{HOST}:{PORT}"],
    "labels": {
      "job": "{JOB_NAME}",
      "env": "{ENVIRONMENT}"
    }
  }
]
```

### 2. Copy and Configure

```bash
# Example: Platform targets
cd platform/postgres/
cp postgresql.template.json postgresql.json
# Edit postgresql.json with actual values
```

### 3. Required Placeholders

Replace these in your JSON files:

- `{HOST}` - Server hostname or IP
- `{PORT}` - Service port
- `{JOB_NAME}` - Prometheus job identifier
- `{ENVIRONMENT}` - Environment (dev/staging/prod)
- `{LOCATION}` - Site/location identifier

## 📁 Directory Structure

```text
targets/
├── infrastructure/      # OS-level exporters
│   ├── linux/
│   ├── windows/
│   └── blackbox/
├── platform/           # Database exporters
│   ├── postgres/
│   ├── mongodb/
│   ├── mariadb/
│   ├── influxdb/
│   └── airflow/
├── service/            # Application services
│   └── openapi/
└── observability/      # Monitoring tools
    └── grafana/
```

## 🔒 Security Best Practices

1. **Never commit real IPs/hostnames**
2. **Use environment-specific configs**
3. **Keep templates updated**
4. **Document required targets**
5. **Review before commits**

## 📝 Template Example

Create `{service}.template.json`:

```json
[
  {
    "targets": ["{DB_HOST}:9187"],
    "labels": {
      "job": "postgresql",
      "env": "{ENV}",
      "instance": "{INSTANCE_NAME}"
    }
  }
]
```

Then copy and fill actual values in production.
