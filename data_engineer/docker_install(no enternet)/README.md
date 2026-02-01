# 🐳**Docker & Docker Compose Installation Guide**

This document guides you through manually installing **Docker** and **Docker Compose** on a Linux system with a restricted Internet environment.

## ⚙️**1. Docker Installation**

### 📥**1.1 Docker File Download**

Run the following command in an internet-connected environment to download the Docker binary file.

```bash
curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-20.10.24.tgz -o docker.tgz
```

### 🛠️**1.2 Docker Installation Procedure**

1. 📂**`docker.tgz` File location**:
   Copy the Docker file to the target system. For example, assume it is stored in `/tmp/docker.tgz`.

2. 📦**Docker Extract files**:

   ```bash
   tar -xvf docker.tgz
   ```

3. 📂**Copy binary files**:
   Copy the extracted Docker file to the executable path (`/usr/bin/`).

   ```bash
   sudo cp docker/* /usr/bin/
   ```

4. 📝**`docker.service` 파일 생성**:
   To register Docker as a system service, create a file `/etc/systemd/system/docker.service` and add the following content.

   ```ini
   [Unit]
   Description=Docker Application Container Engine
   Documentation=https://docs.docker.com
   After=network.target firewalld.service
   Wants=network.target

   [Service]
   ExecStart=/usr/bin/dockerd
   ExecReload=/bin/kill -s HUP $MAINPID
   Restart=on-failure
   StartLimitBurst=3
   StartLimitInterval=60s
   LimitNOFILE=1048576
   LimitNPROC=1048576
   LimitCORE=infinity
   Delegate=yes
   KillMode=process
   OOMScoreAdjust=-500

   [Install]
   WantedBy=multi-user.target
   ```

5. 🔄**Docker daemon reload**:

   ```bash
   sudo systemctl daemon-reload
   ```

6. 🚀**Start and activate Docker service**:

   ```bash
   sudo systemctl start docker
   sudo systemctl enable docker
   ```

7. 👤**Add user to `docker` group**:
   To use Docker without root privileges, add the user to the `docker` group.

   ```bash
   sudo groupadd docker
   sudo usermod -aG docker $USER
   newgrp docker
   ```

## ⚙️**2. Docker Compose Installation**

### 📥**2.1 Download Docker Compose binary**

From your Internet environment, run the following command to download the Docker Compose binary.

```bash
curl -L "https://github.com/docker/compose/releases/download/v2.23.0/docker-compose-$(uname -s)-$(uname -m)" -o docker-compose
```

### 🛠️**2.2 Move binary files and set permissions**

1. Copy the Docker Compose file to the target system.

2. Move the binary to an executable path:

   ```bash
   sudo mv docker-compose /usr/local/bin/docker-compose
   ```

3. Grant execute permission:

   ```bash
   sudo chmod +x /usr/local/bin/docker-compose
   ```

---

## ✅**3. Verify installation**

1. Check Docker installation:

   ```bash
   docker --version
   docker info
   ```

2. Check Docker-Compose installation:

   ```bash
   docker-compose --version
   ```

🎉**Docker** and **Docker Compose** are now successfully installed on your system!
