# Flet Monitoring Application Database Schema

Database schema and configuration files for the Flet-based monitoring application, part of the Airflow data pipeline infrastructure.

## Overview

This directory contains the database schema definitions for a Python-based monitoring application built with Flet framework. The application provides real-time data monitoring and alerting capabilities for temperature sensors and other IoT devices.

## Features

- Real-time data monitoring and collection
- Temperature sensor data management
- Alert system with customizable thresholds
- Location-based monitoring and tracking
- Database integration for data persistence
- Integration with Airflow data pipeline

## Project Structure

```
flet_montrg/
├── schema/                     # Database schema files
│   ├── alert_subscriptions.sql # Alert subscription management
│   ├── alerts.sql              # Alert history and notifications
│   ├── location.sql            # Sensor location information
│   ├── sensor.sql              # Device information and configuration
│   ├── temperature.sql         # Temperature readings from sensors
│   └── thresholds.sql          # Customizable alert thresholds
├── hq_flet_montrg_ERD.pdf      # Entity Relationship Diagram
└── README.md                   # This file
```

## Database Schema

The application uses a relational database with the following main entities:

### Core Tables
- **sensor**: Device information and configuration
- **temperature**: Temperature readings from sensors
- **location**: Sensor location information

### Alert System
- **alerts**: Alert history and notifications
- **thresholds**: Customizable alert thresholds
- **alert_subscriptions**: User notification preferences

## Database Setup

1. **Schema Creation**: Execute the SQL files in the `sql/` directory in the following order:
   ```sql
   -- 1. Create base tables
   sensor.sql
   location.sql
   temperature.sql
   
   -- 2. Create alert system tables
   thresholds.sql
   alerts.sql
   alert_subscriptions.sql
   ```

2. **ERD Reference**: Review `hq_flet_montrg_ERD.pdf` for detailed entity relationships

3. **Airflow Integration**: This schema is designed to work with Airflow DAGs for data processing

## Integration with Airflow

This database schema is part of the larger Airflow data pipeline infrastructure:
- **Data Ingestion**: Airflow DAGs collect data from sensors
- **Data Processing**: Transform and validate sensor data
- **Alert Generation**: Trigger alerts based on thresholds
- **Data Storage**: Persist processed data in these tables

## Usage

1. Set up the database using the SQL files in the `schema/` directory
2. Configure Airflow DAGs to use these tables
3. Deploy the Flet monitoring application
4. Monitor data flow through the Airflow pipeline

## Related Components

- **Flet Application**: Frontend monitoring interface
- **Airflow DAGs**: Data pipeline orchestration
- **Sensor Integration**: IoT device connectivity
- **Alert System**: Notification management

