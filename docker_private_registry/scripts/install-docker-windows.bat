@echo off
REM Docker Desktop Installation Script for Windows
setlocal EnableDelayedExpansion

echo Docker Desktop Installation for Windows
echo ========================================

REM Check admin rights
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Run as Administrator required!
    pause
    exit /b 1
)

REM Check Windows version  
for /f "tokens=4-5 delims=. " %%i in ('ver') do set VERSION=%%i.%%j
if "%VERSION%" lss "10.0" (
    echo ERROR: Windows 10+ required!
    pause
    exit /b 1
)

REM Check WSL2
wsl --status >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing WSL2...
    dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
    dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
    echo WSL2 installed. Reboot required!
    pause
    shutdown /r /t 10
    exit /b 0
)

wsl --set-default-version 2 >nul 2>&1

REM Check existing Docker
docker --version >nul 2>&1
if %errorlevel% equ 0 (
    echo Docker already installed!
    docker --version
    goto :setup_registry
)

REM Download and install Docker Desktop
set TEMP_DIR=%TEMP%\docker-install
mkdir "%TEMP_DIR%" 2>nul
cd /d "%TEMP_DIR%"

echo Downloading Docker Desktop...
curl -L "https://desktop.docker.com/win/main/amd64/Docker%%20Desktop%%20Installer.exe" -o "DockerDesktopInstaller.exe"
if %errorlevel% neq 0 (
    echo Download failed! Check internet connection.
    pause
    exit /b 1
)

echo Installing Docker Desktop...
start /wait "" "DockerDesktopInstaller.exe" install --quiet
if %errorlevel% neq 0 (
    echo Installation failed!
    pause
    exit /b 1
)

cd /d %~dp0
rmdir /s /q "%TEMP_DIR%" 2>nul

echo Starting Docker Desktop...
start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"

echo Waiting for Docker...
set RETRY_COUNT=0
:wait_docker
timeout /t 10 /nobreak >nul
docker --version >nul 2>&1
if %errorlevel% equ 0 goto :docker_ready
set /a RETRY_COUNT+=1
if %RETRY_COUNT% lss 30 goto :wait_docker

echo Docker start timeout! Check manually.
pause
exit /b 1

:docker_ready
echo Docker ready!

:setup_registry
echo Registry Configuration
echo =====================

set REGISTRY_DIR=%USERPROFILE%\docker-registry
mkdir "%REGISTRY_DIR%\certs" 2>nul

echo Downloading certificate...
cd /d "%REGISTRY_DIR%\certs"
curl -k https://%REGISTRY_HOST%:9000/certs/domain.crt -o domain.crt
if %errorlevel% neq 0 (
    echo Certificate download failed!
    pause
    exit /b 1
)

echo Installing certificate...
certlm.msc -import domain.crt -s Root >nul 2>&1

echo Testing Docker...
docker run hello-world >nul 2>&1

echo Testing registry...
docker pull alpine:latest >nul 2>&1
docker tag alpine:latest %REGISTRY_HOST%:5000/test-image:latest >nul 2>&1
docker push %REGISTRY_HOST%:5000/test-image:latest >nul 2>&1

echo.
echo Installation Complete!
echo =====================
echo Registry: https://%REGISTRY_HOST%:5000
echo Web UI: http://%REGISTRY_HOST%:9000
echo Cert: %REGISTRY_DIR%\certs\domain.crt
echo.
pause