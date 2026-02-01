# 📊 dbt 프로젝트 디렉토리

[![dbt](https://img.shields.io/badge/dbt-Core-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Cosmos](https://img.shields.io/badge/Cosmos-Integration-FF6B6B?logo=astronomer&logoColor=white)](https://astronomer.github.io/astronomer-cosmos/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![SQL](https://img.shields.io/badge/SQL-Query-CC2927?logo=postgresql&logoColor=white)](https://www.postgresql.org/docs/)

이 디렉토리는 dbt 프로젝트들을 프로젝트 단위로 관리합니다.

## 📁 구조

```
dbt/
├── banbury_anomaly_detection/  # Banbury 공정 이상 감지 dbt 프로젝트
│   ├── dag.py                  # 이 프로젝트를 실행하는 DAG (Incremental & Backfill)
│   ├── dbt_project.yml         # dbt 프로젝트 설정
│   ├── profiles.yml            # dbt 프로필 설정
│   ├── models/                 # dbt 모델 파일들
│   ├── tests/                  # dbt 테스트 파일들
│   ├── macros/                 # dbt 매크로 파일들
│   ├── seeds/                  # dbt 시드 파일들
│   ├── snapshots/              # dbt 스냅샷 파일들
│   ├── analyses/               # dbt 분석 파일들
│   └── README.md               # 프로젝트별 문서
├── unified_montrg/             # Unified Monitoring 배치 dbt (증분/백필)
│   ├── dag.py                  # 증분/백필 DAG
│   ├── dbt_project.yml         # dbt 프로젝트 설정
│   ├── profiles.yml            # dbt 프로필 설정
│   ├── models/                 # dbt 모델 파일들 (productivity/downtime)
│   └── README.md               # 프로젝트별 문서
├── unified_montrg_realtime/    # Unified Monitoring 실시간 dbt (5분 주기)
│   ├── dag.py                  # 실시간 DAG
│   ├── dbt_project.yml         # dbt 프로젝트 설정
│   ├── profiles.yml            # dbt 프로필 설정
│   ├── models/                 # dbt 모델 파일들 (realtime marts)
│   └── README.md               # 프로젝트별 문서
├── sample_project/             # 샘플 dbt 프로젝트
│   ├── dag.py                  # 이 프로젝트를 실행하는 DAG
│   ├── dbt_project.yml         # dbt 프로젝트 설정
│   ├── profiles.yml            # dbt 프로필 설정
│   ├── models/                 # dbt 모델 파일들
│   ├── tests/                  # dbt 테스트 파일들
│   ├── macros/                 # dbt 매크로 파일들
│   └── ...
└── README.md                   # 이 파일 (전체 프로젝트 개요)
```

## 🎯 프로젝트 단위 관리의 장점

1. **프로젝트별 독립성**: 각 dbt 프로젝트가 자체 DAG와 함께 관리됨
2. **명확한 구조**: 프로젝트별로 모든 관련 파일이 한 곳에 모임
3. **확장성**: 새로운 dbt 프로젝트 추가 시 새 디렉토리만 생성
4. **유지보수 용이**: 프로젝트별로 독립적으로 수정 가능
5. **버전 관리 용이**: 프로젝트별로 독립적인 버전 관리 가능

## 📝 새 프로젝트 추가 방법

1. 새 디렉토리 생성:
   ```bash
   mkdir -p dags/dbt/my_new_project
   cd dags/dbt/my_new_project
   ```

2. dbt 프로젝트 초기화:
   ```bash
   dbt init my_new_project
   ```

3. DAG 파일 생성:
   - `sample_project/dag.py` 또는 `banbury_anomaly_detection/dag.py`를 참고하여 새 프로젝트용 DAG 작성
   - 프로젝트 경로와 프로필 이름 수정
   - 필요에 따라 Incremental/Backfill DAG 추가

4. Airflow Connection 설정:
   - Airflow UI에서 해당 프로젝트용 Connection 설정
   - `profiles.yml`에 정의된 Connection ID와 일치시킴

5. 프로젝트 문서 작성:
   - 프로젝트별 `README.md` 작성 (선택사항)

## 🔍 현재 프로젝트

### banbury_anomaly_detection
- **DAG ID**: 
  - `dbt_banbury_anomaly_detection_incremental` (증분 처리)
  - `dbt_banbury_anomaly_detection_backfill` (백필 처리)
- **경로**: `/opt/airflow/dags/dbt/banbury_anomaly_detection`
- **프로필**: `banbury_anomaly_detection`
- **Connection**: `pg_jj_telemetry_dw`
- **스키마**: `silver` (dbt 모델), `gold` (결과 저장)
- **설명**: Banbury 공정의 이상 감지를 위한 CNN 모델 기반 분석 파이프라인
- **특징**: 
  - 증분 처리: 매일 07:00 실행 (06:30 데이터 처리)
  - 백필 처리: 수동 실행 (2025-01-01부터 오늘-2일까지)
  - CNN 모델을 사용한 이상 감지 추론 포함

### unified_montrg
- **DAG ID**:
  - `dbt_unified_montrg_incremental` (증분 처리)
  - `dbt_unified_montrg_backfill` (백필 처리)
- **경로**: `/opt/airflow/dags/dbt/unified_montrg`
- **프로필**: `unified_montrg`
- **Connection**: `pg_jj_unified_montrg_dw`
- **스키마**: `silver`
- **설명**: 생산성/다운타임 마트 생성 및 백필을 위한 배치 dbt 파이프라인
- **특징**: 모델별 기본 시작일 관리, Cosmos 기반 incremental/backfill TaskGroup 사용

### unified_montrg_realtime
- **DAG ID**: `dbt_unified_montrg_realtime`
- **경로**: `/opt/airflow/dags/dbt/unified_montrg_realtime`
- **프로필**: `unified_montrg_realtime`
- **Connection**: Oracle `orc_jj_gmes` → PostgreSQL `pg_jj_unified_montrg_dw`
- **스키마**: `bronze`(staging), `silver`(dbt 모델)
- **설명**: GMES(IP/PH) 실시간 데이터를 5분 주기로 적재 후 dbt로 가공
- **특징**: Oracle 추출 → staging → dbt mart(materialization)까지 단일 DAG

### sample_project
- **DAG ID**: `dbt_sample_project`
- **경로**: `/opt/airflow/dags/dbt/sample_project`
- **프로필**: `sample_project`
- **Connection**: `postgres_default`
- **스키마**: `public`
- **설명**: dbt 프로젝트 구조 및 설정 예시
- **특징**: 수동 실행 (테스트용)

## ✅ TODO: OSS로 강화하는 데이터 파이프라인

- 데이터 품질: dbt elementary로 모델별 체크 및 데이터 품질 모니터링, Airflow 태스크로 연동
- 계보/메타데이터: OpenLineage로 데이터 계보 추적 및 메타데이터 관리
- 관측/알림: Grafana/Prometheus로 SLA·실패율 모니터링, Slack 알림 표준화
- CI/CD: PR에서 `dbt build --select state:modified` 실행, sqlfluff·ruff pre-commit
- 테스트: dbt unit tests 활성화, pytest로 서비스 레이어 단위 테스트 확장
- 신뢰성: 증분/백필 윈도우 자동 확장 로직, idempotent 태스크 점검
- 보안: Secrets Vault 이관, RBAC·감사 로그 수집, 민감 데이터 마스킹 플로우
- 성능: 머티리얼라이즈/파티션 키 표준화, 쿼리 비용·통계 관리 자동화
- 운영: 실패 재처리 플레이북, DAG/모델 네이밍 컨벤션과 린트 정리

## 📚 참고 자료

- 각 프로젝트 디렉토리 내의 `README.md`에서 프로젝트별 상세 정보 확인 가능
- dbt 공식 문서: https://docs.getdbt.com/
- Cosmos (Airflow dbt 통합): https://astronomer.github.io/astronomer-cosmos/
