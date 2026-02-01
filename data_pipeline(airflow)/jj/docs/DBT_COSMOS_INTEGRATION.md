# dbt와 Astronomer Cosmos 통합 가이드

## 📋 개요

이 가이드는 현재 운영 중인 Airflow 환경에 **dbt (Data Build Tool)**와 **Astronomer Cosmos**를 통합하는 방법을 설명합니다.

### dbt란?
- 데이터 변환(Transform) 작업을 SQL 기반으로 관리하는 오픈소스 도구
- 데이터 웨어하우스에서 ELT 파이프라인 구축에 사용
- 모델, 테스트, 문서화 기능 제공

### Astronomer Cosmos란?
- Airflow와 dbt를 통합하는 오픈소스 라이브러리
- dbt 프로젝트를 Airflow DAG로 자동 변환
- dbt 모델을 Airflow 태스크로 실행 가능하게 해줌

## 💰 비용 정보

### ✅ 무료 (오픈소스)
- **dbt Core**: 완전 무료 오픈소스
- **Astronomer Cosmos**: 완전 무료 오픈소스 (Apache 2.0 라이선스)

### 💳 유료 옵션 (선택사항)
- **dbt Cloud**: dbt Labs의 관리형 서비스 (유료, 선택사항)
- **Astronomer Platform**: Astronomer의 관리형 Airflow 서비스 (유료, 선택사항)

**결론**: 기본 통합은 **100% 무료**입니다. 자체 호스팅 환경에서는 추가 비용이 없습니다.

## 🚀 설치 및 설정

### 1단계: requirements.txt 업데이트

`requirements.txt`에 다음 패키지가 추가되어야 합니다:

```txt
# dbt 및 Cosmos 통합
astronomer-cosmos>=1.0.0
dbt-core>=1.5.0
dbt-postgres>=1.5.0  # PostgreSQL 사용 시
# 또는 다른 어댑터:
# dbt-snowflake>=1.5.0  # Snowflake 사용 시
# dbt-bigquery>=1.5.0   # BigQuery 사용 시
# dbt-redshift>=1.5.0   # Redshift 사용 시
```

### 2단계: dbt 프로젝트 구조 생성

Airflow의 `dags` 디렉토리 내에 dbt 프로젝트를 생성합니다:

```bash
cd /home/user/apps/airflow/dags
mkdir -p dbt/my_dbt_project
cd dbt/my_dbt_project
```

또는 Airflow 컨테이너 내에서:

```bash
docker exec -it airflow-scheduler bash
cd /opt/airflow/dags
mkdir -p dbt/my_dbt_project
cd dbt/my_dbt_project
dbt init my_dbt_project
```

### 3단계: dbt 프로젝트 설정

`dbt/my_dbt_project/dbt_project.yml` 파일을 생성/수정:

```yaml
name: 'my_dbt_project'
version: '1.0.0'
config-version: 2

profile: 'my_dbt_project'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  my_dbt_project:
    +materialized: view
```

`dbt/my_dbt_project/profiles.yml` 파일 생성:

```yaml
my_dbt_project:
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DBT_HOST', '10.10.100.80') }}"  # 실제 데이터 저장소 호스트
      port: "{{ env_var('DBT_PORT', '5432') }}"
      user: "{{ env_var('DBT_USER', 'your_user') }}"
      password: "{{ env_var('DBT_PASSWORD', 'your_password') }}"
      dbname: "{{ env_var('DBT_DATABASE', 'telemetry') }}"  # 실제 데이터베이스 이름
      schema: dbt  # dbt가 사용할 스키마
    prod:
      type: postgres
      host: "{{ env_var('DBT_HOST', '10.10.100.80') }}"
      port: "{{ env_var('DBT_PORT', '5432') }}"
      user: "{{ env_var('DBT_USER', 'your_user') }}"
      password: "{{ env_var('DBT_PASSWORD', 'your_password') }}"
      dbname: "{{ env_var('DBT_DATABASE', 'telemetry') }}"
      schema: dbt
  target: dev
```

**⚠️ 중요**: 
- `host`: **실제 데이터 저장소**의 호스트 주소 (예: `10.10.100.80` 또는 외부 DB)
- `dbname`: **실제 데이터베이스 이름** (예: `telemetry`, `monitoring` 등)
- `airflow-postgres`를 사용하려면 `host: postgres`로 설정하되, 일반적으로는 실제 데이터 저장소를 사용합니다

### 4단계: Airflow DAG 작성

`dags/dbt_dag_example.py` 파일 생성:

```python
from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Profile 설정 (PostgreSQL 사용 예시)
profile_config = ProfileConfig(
    profile_name="my_dbt_project",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",  # Airflow Connection ID
        profile_args={"schema": "dbt"}
    ),
)

# Execution 설정
execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/bin/dbt",  # 또는 dbt가 설치된 경로
)

# DAG 정의
with DAG(
    dag_id="dbt_transform_pipeline",
    default_args=default_args,
    description="dbt를 사용한 데이터 변환 파이프라인",
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "transform"],
) as dag:
    
    # dbt TaskGroup 생성
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dags/dbt/my_dbt_project",
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "install_deps": True,  # dbt 의존성 자동 설치
        },
    )
```

### 5단계: Airflow Connection 설정

**⚠️ 중요**: dbt는 **실제 데이터가 저장된 데이터베이스**에 연결해야 합니다. 
- `airflow-postgres`는 Airflow 메타데이터용이므로 일반적으로 dbt 작업 대상이 **아닙니다**
- 사용자가 주로 사용하는 **실제 데이터 저장소** 를 연결해야 합니다

Airflow UI에서 Connection을 설정하거나 CLI로 설정:

```bash
# 예시: 실제 데이터 저장소 연결 (사용자 환경에 맞게 수정)
docker exec -it airflow-webserver airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host '10.10.100.80' \
    --conn-schema 'telemetry' \
    --conn-login 'your_user' \
    --conn-password 'your_password' \
    --conn-port 5432
```

또는 Airflow UI에서:
1. Admin → Connections
2. `+` 버튼 클릭
3. Connection ID: `postgres_default` (또는 원하는 이름)
4. Connection Type: `Postgres`
5. **실제 데이터 저장소 정보 입력**:
   - **Host**: 실제 데이터베이스 호스트 (예: `10.10.100.80` 또는 외부 DB 주소)
   - **Schema**: 데이터베이스 이름 (예: `telemetry`, `monitoring` 등)
   - **Login**: 데이터베이스 사용자명
   - **Password**: 데이터베이스 비밀번호
   - **Port**: 데이터베이스 포트 (일반적으로 `5432`)

**참고**: 
- `airflow-postgres`를 사용하려면 Host를 `postgres` (docker-compose 서비스 이름)로 설정
- 하지만 일반적으로는 실제 데이터 저장소를 사용하는 것이 권장됩니다

### 6단계: Docker Compose 업데이트 (선택사항)

dbt 프로젝트 디렉토리를 볼륨에 추가하려면 `docker-compose.yml`의 volumes 섹션에 추가:

```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ./dags/dbt:/opt/airflow/dags/dbt  # dbt 프로젝트 디렉토리
```

## 📝 사용 예시

### 간단한 dbt 모델 예시

`dbt/my_dbt_project/models/staging/stg_customers.sql`:

```sql
{{ config(materialized='view') }}

select
    id,
    name,
    email,
    created_at
from {{ source('raw', 'customers') }}
where is_active = true
```

`dbt/my_dbt_project/models/marts/dim_customers.sql`:

```sql
{{ config(materialized='table') }}

select
    id as customer_id,
    name as customer_name,
    email,
    created_at as customer_since
from {{ ref('stg_customers') }}
```

### DAG에서 특정 모델만 실행

```python
dbt_transform = DbtTaskGroup(
    group_id="dbt_transform",
    project_config=ProjectConfig(
        dbt_project_path="/opt/airflow/dags/dbt/my_dbt_project",
    ),
    profile_config=profile_config,
    execution_config=execution_config,
    select=["models/staging/*"],  # 특정 경로의 모델만 실행
    # 또는
    # select=["tag:staging"],  # 태그로 필터링
)
```

## 🔧 고급 설정

### 커스텀 dbt 실행 경로

`execution_config`에서 dbt 실행 파일 경로 지정:

```python
execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/bin/dbt",
    # 또는 Python 패키지로 설치된 경우
    # dbt_executable_path="dbt",
)
```

### 환경 변수 사용

`.env` 파일에 dbt 관련 변수 추가:

```env
DBT_PROJECT_PATH=/opt/airflow/dags/dbt/my_dbt_project
DBT_PROFILE_NAME=my_dbt_project
DBT_TARGET=dev
```

### 다른 데이터베이스 어댑터 사용

PostgreSQL 외 다른 데이터베이스 사용 시:

```python
# Snowflake 예시
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="my_dbt_project",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={"schema": "dbt", "database": "analytics"}
    ),
)
```

그리고 `requirements.txt`에 해당 어댑터 추가:
```txt
dbt-snowflake>=1.5.0
```

## ✅ 검증 및 테스트

### 1. 패키지 설치 확인

```bash
docker exec -it airflow-scheduler pip list | grep -E "cosmos|dbt"
```

### 2. dbt 프로젝트 검증

```bash
docker exec -it airflow-scheduler bash
cd /opt/airflow/dags/dbt/my_dbt_project
dbt debug
dbt parse
```

### 3. DAG 로드 확인

Airflow UI에서 DAG가 정상적으로 로드되는지 확인:
- DAG 목록에 `dbt_transform_pipeline`이 표시되는지 확인
- DAG를 클릭하여 태스크 구조 확인

## 🐛 문제 해결

### 문제: dbt 명령어를 찾을 수 없음
**해결**: `execution_config`의 `dbt_executable_path` 확인 또는 `install_deps=True` 설정

### 문제: Connection 오류
**해결**: Airflow Connection이 올바르게 설정되었는지 확인

### 문제: 모델이 실행되지 않음
**해결**: `dbt_project.yml`과 `profiles.yml` 설정 확인

## 📚 참고 자료

- [Astronomer Cosmos 공식 문서](https://astronomer.github.io/astronomer-cosmos/)
- [dbt Core 문서](https://docs.getdbt.com/docs/introduction)
- [Airflow dbt 통합 가이드](https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/index.html)

## 🎯 다음 단계

1. `requirements.txt` 업데이트
2. dbt 프로젝트 초기화
3. 샘플 DAG 생성 및 테스트
4. 실제 데이터 모델 개발

