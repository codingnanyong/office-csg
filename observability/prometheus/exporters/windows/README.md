# Windows Exporters

This folder contains executable files for various exporters that can run in Windows environments.

## 📁 Included Exporters

| Exporter                | Filename                            | Port | Description                       |
| ----------------------- | ----------------------------------- | ---- | --------------------------------- |
| **Windows Exporter**    | `windows_exporter-0.31.0-amd64.msi` | 9182 | Windows system metrics collection |
| **MySQL Exporter**      | `mysqld_exporter.exe`               | 9104 | MySQL/MariaDB metrics collection  |
| **PostgreSQL Exporter** | `postgres_exporter.exe`             | 9187 | PostgreSQL metrics collection     |
| **MQTT Exporter**       | `mqtt2prometheus.exe`               | 9647 | MQTT message collection           |

## 🚀 Installation and Setup

### Windows Exporter (MSI Installation)

#### Installation Steps

1. **Run Installation File** ![Install File](../img/01-install-file.png)

   - Double-click `windows_exporter-0.31.0-amd64.msi` with administrator privileges

2. **Welcome Screen** ![Welcome](../img/02-welcome-screen.png)

   - Click "Next" to proceed

3. **License Agreement** ![License](../img/03-license-agreement.png)

   - Accept the license agreement

4. **Installation Options** ![Options](../img/04-installation-options.png)

   - Ensure collectors are enabled: `cpu`, `memory`, `logical_disk`, `physical_disk`, `net`, `os`, `service`, `system`, `process`, `tcp`, `udp`, `iis`

5. **Installation Progress** ![Progress](../img/05-installation-progress.png)

   - Wait for completion

6. **Installation Complete** ![Complete](../img/06-installation-complete.png)
   - Click "Finish"

#### Post-Installation

```cmd
# Service starts automatically, but you can verify:
net start windows_exporter
sc query windows_exporter
sc config windows_exporter start=auto
```

### Database Exporters (Manual Setup)

#### Quick Start Commands

```cmd
# MySQL Exporter
set DATA_SOURCE_NAME=user:password@(localhost:3306)/
mysqld_exporter.exe --config.my-cnf="C:\mi\mysqld_exporter\my.cnf" --collect.info_schema.tables --collect.info_schema.processlist --collect.perf_schema.tablelocks

# PostgreSQL Exporter
set DATA_SOURCE_NAME=postgresql://user:password@localhost:5432/postgres?sslmode=disable
postgres_exporter.exe

# MQTT Exporter
mqtt2prometheus.exe --config.file=mqtt_config.yml
```

#### Configuration Files

**MySQL (my.cnf)**

```ini
[client]
user=exporter_user
password=your_secure_password
```

## 🔧 Service Registration

### Windows Exporter

- **Automatic**: MSI installation registers service automatically
- **Manual** (if needed):

   ```cmd
  sc.exe create windows_exporter binPath= "\"C:\Program Files\windows_exporter\windows_exporter.exe\" --collectors.enabled=cpu,memory,logical_disk,physical_disk,net,os,service,system,process,tcp,udp,iis"
  sc.exe start windows_exporter
  ```

### Other Exporters (NSSM Required)

Download NSSM from: [https://nssm.cc/download]

#### Generic NSSM Service Registration

```cmd
# Install service
nssm install <ServiceName> "C:\path\to\exporter.exe"

# Set environment variables (if needed)
nssm set <ServiceName> AppEnvironment DATA_SOURCE_NAME=<connection_string>

# Set working directory
nssm set <ServiceName> AppDirectory "C:\path\to\exporter\directory"

# Set command line arguments (if needed)
nssm set <ServiceName> AppParameters <arguments>

# Start and set auto-start
nssm start <ServiceName>
nssm set <ServiceName> Start SERVICE_AUTO_START
```

#### Specific Examples

```cmd
# MySQL Exporter
nssm install MySQLExporter "C:\path\to\mysqld_exporter.exe"
nssm set MySQLExporter AppEnvironment DATA_SOURCE_NAME=user:password@(localhost:3306)/ --config.my-cnf="C:\mi\mysqld_exporter\my.cnf" --collect.info_schema.tables --collect.info_schema.processlist --collect.perf_schema.tablelocks

# PostgreSQL Exporter
nssm install PostgreSQLExporter "C:\path\to\postgres_exporter.exe"
nssm set PostgreSQLExporter AppEnvironment DATA_SOURCE_NAME=postgresql://user:password@localhost:5432/postgres?sslmode=disable

# MQTT Exporter
nssm install MQTTExporter "C:\path\to\mqtt2prometheus.exe"
nssm set MQTTExporter AppParameters --config.file=mqtt_config.yml
```

#### NSSM Management

```cmd
nssm status <ServiceName>    # Check status
nssm stop <ServiceName>      # Stop service
nssm remove <ServiceName> confirm  # Remove service
```

## ⚙️ Configuration and Verification

### Metrics Verification

```cmd
# Windows Exporter
curl http://localhost:9182/metrics

# Database Exporters
curl http://localhost:9104/metrics  # MySQL
curl http://localhost:9187/metrics  # PostgreSQL
curl http://localhost:9647/metrics  # MQTT
```

### Key Metrics Examples

- **Windows**: `windows_cpu_time_total`, `windows_os_physical_memory_total_bytes`
- **MySQL**: `mysql_global_status_connections`, `mysql_up`
- **PostgreSQL**: `pg_up`, `pg_stat_database_size_bytes`

## 🔗 Prometheus Integration

```yaml
scrape_configs:
  - job_name: "windows-exporter"
    static_configs:
      - targets: ["windows-server:9182"]
        labels: { os: "windows", server: "production" }

  - job_name: "database-exporters"
    static_configs:
      - targets: ["windows-server:9104", "windows-server:9187"]
        labels: { server: "production" }
```

## 📊 Monitoring Dashboards

- **Windows Exporter**: [2129](https://grafana.com/grafana/dashboards/2129)
- **MySQL Exporter**: [7362](https://grafana.com/grafana/dashboards/7362)
- **PostgreSQL Exporter**: [9628](https://grafana.com/grafana/dashboards/9628)

## 🛠️ Troubleshooting

### Common Issues

1. **Service Won't Start**

   ```cmd
   Get-EventLog -LogName Application -Source "windows_exporter" -Newest 10
   "C:\Program Files\windows_exporter\windows_exporter.exe"  # Manual test
   ```

2. **Port Conflicts**

   ```cmd
   netstat -ano | findstr 9182
   ```

3. **Firewall Issues**

   ```cmd
   netsh advfirewall firewall add rule name="Exporter" dir=in action=allow protocol=TCP localport=9182
   ```

4. **NSSM Service Issues**

   ```cmd
   nssm status <ServiceName>
   nssm set <ServiceName> AppStdout "C:\logs\service.log"
   nssm set <ServiceName> AppStderr "C:\logs\service_error.log"
   ```

### Debugging

```cmd
# Windows Exporter
windows_exporter.exe --log.level=debug --collectors.enabled=cpu,memory

# Database Exporters
mysqld_exporter.exe --log.level=debug
postgres_exporter.exe --log.level=debug
```

## 📝 References

- [Windows Exporter GitHub](https://github.com/prometheus-community/windows_exporter)
- [MySQL Exporter GitHub](https://github.com/prometheus/mysqld_exporter)
- [PostgreSQL Exporter GitHub](https://github.com/prometheus-community/postgres_exporter)
- [MQTT Exporter GitHub](https://github.com/hikhvar/mqtt2prometheus)
- [NSSM Documentation](https://nssm.cc/)
- [Windows Service Management](https://docs.microsoft.com/en-us/windows/win32/services/services)
