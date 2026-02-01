# 🍃**MongoDB Guide**

[![MongoDB](https://img.shields.io/badge/MongoDB-Database-47A248?logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Supported-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![TLS](https://img.shields.io/badge/TLS-Secure-009639?logo=openssl&logoColor=white)](https://www.openssl.org/)

This document guides you through installing **MongoDB** with docker compose.

## ⚙️ Prerequisites

- Docker and Docker Compose installed.
- Basic terminal command knowledge.

## 🔐 Create SSL/TLS Certificates (ppt reference)

To enable secure connections to MongoDB, you need to create SSL/TLS certificates.

1. Generate a CA certificate:

```bash
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 365 -key ca.key -out ca-cert.pem
```

2. Generate a Server Certificate:

```bash
openssl genrsa -out mongodb.key 4096
openssl req -new -key mongodb.key -out mongodb.csr
openssl x509 -req -in mongodb.csr -CA ca-cert.pem -CAkey ca.key -CAcreateserial -out mongodb.pem -days 365
```

3. Set Proper Permissions:

```bash
chmod 600 mongodb.key mongodb.pem ca-cert.pem
```

Move the generated certificates to {path}.

## 📝 Create `docker-compose.yml`

Create a `docker-compose.yml` file with the following content:

```yml
services:
  mongodb:
    image: mongo:6.0
    container_name: mongodb
    ports:
      - "27017:27017"
    env_file: .env
    environment:
      MONGO_INITDB_ROOT_USERNAME: ${MONGO_USER}
      MONGO_INITDB_ROOT_PASSWORD: ${MONGO_PASSWORD}
    volumes:
      - {path}/mongodb:/data/db
      - {path}/configdb:/data/configdb
      - {path}/mongodb/mongodb.pem:/etc/ssl/mongodb.pem
      - {path}/mongodb/ca-cert.pem:/etc/ssl/ca-cert.pem
      - {path}/mongodb/mongod.conf:/etc/mongod.conf
    command: ["mongod", "--config", "/etc/mongod.conf"]
    restart: always
    healthcheck:
      test: ["CMD-SHELL", "mongosh --tls --tlsCAFile /etc/ssl/ca-cert.pem
      --tlsCertificateKeyFile /etc/ssl/mongodb.pem
      --quiet --eval 'db.runCommand({ ping: 1 }).ok' || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s
    networks:
      - {network name}

networks:
  {network name}:
    driver: bridge
    external: true
```

## 🔑 Create `mongod.conf`

Create a mongod.conf file to enable TLS and authentication:

```yml
net:
  port: 27017
  tls:
    mode: requireTLS
    certificateKeyFile: /etc/ssl/mongodb.pem
    CAFile: /etc/ssl/ca-cert.pem

security:
  authorization: enabled
```

## 🚀 Running the Container

Navigate to the directory containing the docker-compose.yml file and run:

```bash
docker-compose up -d
```

## 🔗 Connecting to MongoDB with SSL/TLS

Use `mongosh` to connect securely to MongoDB:

```bash
mongosh --tls --tlsCAFile /etc/ssl/ca-cert.pem --tlsCertificateKeyFile /etc/ssl/mongodb.pem --host localhost --port 27017 -u ${MONGO_USER} -p ${MONGO_PASSWORD}
```

Once connected, verify the connection:

```bash
db.runCommand({ connectionStatus: 1 })
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

This guide helps you install **MongoDB** using Docker Compose, configure authentication, and manage data persistence. Modify settings as needed to suit your deployment.
