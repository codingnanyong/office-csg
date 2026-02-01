# Unified Monitoring dbt 프로젝트

[![dbt](https://img.shields.io/badge/dbt-Core-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Cosmos](https://img.shields.io/badge/Cosmos-Integration-FF6B6B?logo=astronomer&logoColor=white)](https://astronomer.github.io/astronomer-cosmos/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![SQL](https://img.shields.io/badge/SQL-Query-CC2927?logo=postgresql&logoColor=white)](https://www.postgresql.org/docs/)


`unified_montrg` 서비스의 **신규 기능들**을 dbt 모델로 구현하는 프로젝트입니다.

## 📋 목적

- 기존 기능은 Python 서비스로 유지
- **신규 모니터링/분석 기능**은 dbt 모델로 구현
- 데이터 변환 로직을 SQL로 관리하여 유지보수성 향상

## 📁 구조

```
unified_montrg/
├── dag.py                  # Airflow DAG (증분/백필 처리)
├── dbt_project.yml         # dbt 프로젝트 설정
├── profiles.yml            # dbt 프로필 설정
├── models/
│   ├── staging/           # 원본 데이터 정제
│   ├── marts/             # 비즈니스 로직 적용
│   └── analytics/         # 분석 및 집계 모델
├── tests/                 # dbt 테스트
├── macros/                # dbt 매크로
└── README.md              # 이 파일
```

## 🔧 설정

### 데이터베이스 연결

- **Connection ID**: 각 데이터베이스별 Connection 사용
  - `pg_jj_maintenance_dw` (maintenance 데이터베이스)
  - `pg_jj_production_dw` (production 데이터베이스)
  - `pg_jj_quality_dw` (quality 데이터베이스)
- **데이터베이스**: `maintenance`, `production`, `quality` (monitoring, telemetry 제외)
- **스키마**: 각 데이터베이스마다 `bronze`, `silver`, `gold` 스키마 지원

### 프로필 설정

`profiles.yml`에서 각 데이터베이스와 스키마 조합을 설정할 수 있습니다:

#### Maintenance 데이터베이스
- `maintenance_bronze`: 원본 데이터
- `maintenance_silver`: 처리된 데이터
- `maintenance_gold`: 최종 분석 데이터

#### Production 데이터베이스
- `production_bronze`: 원본 데이터
- `production_silver`: 처리된 데이터
- `production_gold`: 최종 분석 데이터

#### Quality 데이터베이스
- `quality_bronze`: 원본 데이터
- `quality_silver`: 처리된 데이터
- `quality_gold`: 최종 분석 데이터

### 사용 방법

```bash
# 기본 (maintenance_silver)
dbt run --target dev

# Maintenance 데이터베이스
dbt run --target maintenance_bronze
dbt run --target maintenance_silver
dbt run --target maintenance_gold

# Production 데이터베이스
dbt run --target production_bronze
dbt run --target production_silver
dbt run --target production_gold

# Quality 데이터베이스
dbt run --target quality_bronze
dbt run --target quality_silver
dbt run --target quality_gold
```

## 🚀 사용 방법

### 1. 신규 기능 추가 시

1. **Staging 모델 생성** (필요시)
   ```sql
   -- models/staging/stg_new_feature.sql
   SELECT ...
   FROM bronze.source_table
   ```

2. **Marts/Analytics 모델 생성**
   ```sql
   -- models/analytics/mart_new_feature.sql
   SELECT ...
   FROM {{ ref('stg_new_feature') }}
   ```

3. **DAG 실행**
   - 증분 처리: `dbt_unified_montrg_incremental`
   - 백필 처리: `dbt_unified_montrg_backfill`

### 2. 모델 실행

```bash
# 전체 모델 실행 (기본: maintenance_silver)
dbt run --project-dir /opt/airflow/dags/dbt/unified_montrg

# 특정 데이터베이스/스키마로 실행
dbt run --target maintenance_silver
dbt run --target production_silver
dbt run --target quality_gold

# 특정 모델만 실행
dbt run --select staging.*
dbt run --select analytics.*

# 태그로 실행
dbt run --select tag:analytics
```

### 3. 다른 스키마/데이터베이스의 데이터 참조

모델에서 같은 데이터베이스 내 다른 스키마의 테이블을 참조할 때:

```sql
-- 같은 데이터베이스 내 Bronze 스키마의 테이블 참조
SELECT * FROM bronze.mc_master

-- 같은 데이터베이스 내 Gold 스키마의 테이블 참조
SELECT * FROM gold.machine_grade
```

**참고**: 다른 데이터베이스의 테이블을 참조하려면 `pg_fdw`를 통한 foreign table을 사용하거나, 해당 데이터베이스를 target으로 지정하여 별도로 실행해야 합니다.

## 📝 모델 Materialization 전략

- **Staging**: `view` (경량, 빠른 반영)
- **Marts**: `table` (성능 최적화)
- **Analytics**: `table` (집계 데이터, 성능 중요)

## 🔍 기존 기능 vs 신규 기능

### 기존 기능 (Python 서비스로 유지)
- `wo_analysis` - 작업 지시 분석
- `mc_master` - 기계 마스터 조회
- `machine_grade` - 기계 등급 조회
- `economin_lifespan` - 경제적 수명
- `spare_part_inventory` - 부품 재고
- `ipi_quality_temperature` - IPI 품질 온도
- `rtf_data` - IP HMI 데이터 조회

### 신규 기능 (dbt 모델로 구현)
- 향후 추가될 모니터링/분석 기능들

## 📚 참고

- dbt 공식 문서: https://docs.getdbt.com/
- Cosmos (Airflow dbt 통합): https://astronomer.github.io/astronomer-cosmos/

