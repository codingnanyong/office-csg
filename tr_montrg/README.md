# TR Monitoring Solution

[![.NET](https://img.shields.io/badge/.NET-6.0-512BD4?logo=dotnet&logoColor=white)](https://dotnet.microsoft.com/)
[![ASP.NET Core](https://img.shields.io/badge/ASP.NET%20Core-6.0-512BD4?logo=dotnet&logoColor=white)](https://dotnet.microsoft.com/apps/aspnet)
[![Blazor](https://img.shields.io/badge/Blazor-6.0-512BD4?logo=blazor&logoColor=white)](https://dotnet.microsoft.com/apps/aspnet/web-apps/blazor)
[![Entity Framework Core](https://img.shields.io/badge/EF%20Core-6.0-512BD4?logo=dotnet&logoColor=white)](https://docs.microsoft.com/ef/core/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Supported-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)

변압기(TRansformer) 모니터링 및 이상 탐지 솔루션

## 📖 개요

변압기 상태를 실시간으로 모니터링하고, 이상 발생 시 알림을 제공하는 .NET 기반 통합 솔루션입니다.

### 주요 기능

- 🌡️ 변압기 온도 실시간 모니터링
- 📧 이상 감지 시 이메일 알림
- 📊 기업/위치별 대시보드 제공
- 🔄 IoT 디바이스 자동 재부팅 기능
- 📷 열화상 카메라 통합

## 🖥️ 화면

### TrMontrgSrv.Web

변압기 모니터링 웹 솔루션

![Main](../Image/TrMontgSrv/Main.PNG)

![Plan Example](../Image/TrMontgSrv/Plan_ex.PNG)
![Device Info](../Image/TrMontgSrv/DeviceInfo.PNG)
![Device Detail](../Image/TrMontgSrv/DeviceInfo_detail.PNG)
![Device Chart](../Image/TrMontgSrv/device_chart.PNG)
![Device Issue](../Image/TrMontgSrv/device_issue.PNG)
![Thermal Camera](../Image/TrMontgSrv/temp_camera.PNG)

### TrMontrgSrv.Dashboard

![Dashboard](img/dashboard.png)

## 🏗️ 프로젝트 구조

```text
tr_montrg/
├── TrMontrgSrv.Web/              # ASP.NET Core MVC Web Application
├── TrMontrgSrv.WebApi/           # RESTful Web API
├── TrMontrgSrv.Dashboard/        # Blazor Dashboard
├── TrMontrgSrv.BLL/              # Business Logic Layer
├── TrMontrgSrv.EF/               # Entity Framework Core
├── TrMontrgSrv.Model/            # Data Models
├── TrMontrgSrv.AutoBtg/          # Auto Batch Generator
├── TrDataImporterSvc/            # Data Importer Service
└── TrMontrgSrv.sln               # Solution File
```

## 🤝 Contributing

Contributions, issues and feature requests are welcome.  
Feel free to check issues page if you want to contribute.

## 📝 License

Copyright © Changsin Inc. All rights reserved.
