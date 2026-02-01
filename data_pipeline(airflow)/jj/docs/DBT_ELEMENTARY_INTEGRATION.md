# dbt Elementary 데이터 품질 모니터링 통합 가이드

## 📋 개요

이 가이드는 **dbt Elementary**를 사용하여 dbt 모델별 데이터 품질 체크 및 모니터링을 설정하고, Airflow 태스크로 연동하는 방법을 설명합니다.

### dbt Elementary란?
- dbt 프로젝트의 데이터 품질을 모니터링하는 오픈소스 도구
- 모델별 자동 데이터 품질 체크 생성
- 데이터 드리프트, 이상치, 스키마 변경 감지
- 웹 대시보드를 통한 시각화 및 알림
- **완전 무료 오픈소스** (Apache 2.0 라이선스)

### 주요 기능
1. **자동 데이터 품질 체크**: 모델별 freshness, volume, schema 변경 감지
2. **데이터 드리프트 감지**: 통계적 이상치 및 패턴 변화 감지
3. **테스트 결과 추적**: dbt 테스트 실행 결과 모니터링
4. **웹 대시보드**: 실시간 데이터 품질 상태 시각화
5. **알림 통합**: Slack, Email 등으로 알림 전송

## 🚀 설치 및 설정

### 1단계: dbt 패키지 추가

각 dbt 프로젝트의 `packages.yml` 파일에 elementary 패키지를 추가합니다:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
  - package: elementary-data/elementary
    version: 0.12.0  # 최신 버전 확인: https://hub.getdbt.com/elementary-data/elementary
```

### 2단계: dbt 프로젝트 설정

`dbt_project.yml`에 elementary 설정을 추가합니다:

```yaml
name: 'your_project'
version: '1.0.0'
config-version: 2

profile: 'your_project'

# Elementary 설정
vars:
  elementary:
    # 데이터 품질 체크 활성화
    data_quality_enabled: true
    # 모델 freshness 체크 활성화
    freshness_enabled: true
    # 스키마 변경 감지 활성화
    schema_changes_enabled: true
    # 데이터 드리프트 감지 활성화
    anomaly_detection_enabled: true

models:
  your_project:
    +materialized: view
```

### 3단계: Elementary 프로필 설정

`profiles.yml`에 elementary 전용 설정을 추가합니다 (선택사항):

```yaml
your_project:
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DBT_HOST', 'localhost') }}"
      port: "{{ env_var('DBT_PORT', '5432') }}"
      user: "{{ env_var('DBT_USER', 'user') }}"
      password: "{{ env_var('DBT_PASSWORD', 'password') }}"
      dbname: "{{ env_var('DBT_DATABASE', 'database') }}"
      schema: dbt
  target: dev
```

### 4단계: 패키지 설치

dbt 프로젝트 디렉토리에서 패키지를 설치합니다:

```bash
cd /opt/airflow/dags/dbt/your_project
dbt deps
```

또는 Airflow 컨테이너 내에서:

```bash
docker exec -it airflow-scheduler bash
cd /opt/airflow/dags/dbt/your_project
dbt deps
```

### 5단계: Elementary 초기화

Elementary를 초기화하여 필요한 테이블과 뷰를 생성합니다:

```bash
dbt run-operation elementary.create_elementary_tables
```

또는 Airflow에서 실행:

```bash
dbt run --select elementary
```

## 📊 데이터 품질 체크 설정

### 모델별 Freshness 체크

`models/schema.yml` 또는 별도 파일에 freshness 체크를 정의합니다:

```yaml
version: 2

models:
  - name: stg_orders
    description: "주문 스테이징 테이블"
    config:
      # Elementary freshness 체크
      meta:
        elementary:
          freshness:
            warn_after: {count: 1, period: hour}
            error_after: {count: 2, period: hour}
            filter: "updated_at >= CURRENT_DATE - INTERVAL '1 day'"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
```

### 데이터 드리프트 감지

```yaml
models:
  - name: dim_customers
    description: "고객 차원 테이블"
    config:
      meta:
        elementary:
          # 데이터 드리프트 감지 설정
          anomaly_detection:
            timestamp_column: updated_at
            # 통계적 이상치 감지
            statistical_anomalies: true
            # 값 분포 변화 감지
            distribution_anomalies: true
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
```

### 스키마 변경 감지

Elementary는 자동으로 스키마 변경을 감지합니다. 추가 설정은 필요 없습니다.

## 🔄 Airflow 통합

### 방법 1: dbt TaskGroup에 Elementary 체크 포함

기존 DAG에 Elementary 체크를 추가합니다:

```python
from datetime import datetime, timedelta
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.operators.bash import BashOperator

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Profile 설정
profile_config = ProfileConfig(
    profile_name="your_project",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "dbt"}
    ),
)

# Execution 설정
execution_config = ExecutionConfig(
    dbt_executable_path="dbt",
)

# DAG 정의
with DAG(
    dag_id="dbt_your_project_with_quality",
    default_args=default_args,
    description="dbt 프로젝트 실행 및 데이터 품질 체크",
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "data-quality", "elementary"],
) as dag:
    
    # 1. dbt 모델 실행
    dbt_run = DbtTaskGroup(
        group_id="dbt_run",
        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dags/dbt/your_project",
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "install_deps": True,
            "select": "path:models",  # elementary 모델 제외
        },
    )
    
    # 2. Elementary 데이터 품질 체크 실행
    elementary_checks = BashOperator(
        task_id="elementary_quality_checks",
        bash_command="""
        cd /opt/airflow/dags/dbt/your_project && \
        dbt run --select elementary && \
        dbt test --select elementary
        """,
    )
    
    # 3. dbt 테스트 실행 (일반 테스트)
    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command="""
        cd /opt/airflow/dags/dbt/your_project && \
        dbt test --select test_type:data
        """,
    )
    
    # 태스크 의존성 설정
    dbt_run >> elementary_checks >> dbt_tests
```

### 방법 2: 별도 데이터 품질 DAG 생성

데이터 품질 체크를 별도 DAG로 분리하여 주기적으로 실행:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,  # 데이터 품질 이슈 시 알림
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Profile 설정
profile_config = ProfileConfig(
    profile_name="your_project",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "dbt"}
    ),
)

# Execution 설정
execution_config = ExecutionConfig(
    dbt_executable_path="dbt",
)

# DAG 정의
with DAG(
    dag_id="dbt_data_quality_monitoring",
    default_args=default_args,
    description="데이터 품질 모니터링 및 체크",
    schedule_interval="0 */6 * * *",  # 6시간마다 실행
    catchup=False,
    tags=["dbt", "data-quality", "elementary", "monitoring"],
) as dag:
    
    # Elementary 테이블 생성/업데이트
    elementary_setup = BashOperator(
        task_id="elementary_setup",
        bash_command="""
        cd /opt/airflow/dags/dbt/your_project && \
        dbt run --select elementary
        """,
    )
    
    # 데이터 품질 체크 실행
    quality_checks = BashOperator(
        task_id="run_quality_checks",
        bash_command="""
        cd /opt/airflow/dags/dbt/your_project && \
        dbt test --select elementary
        """,
    )
    
    # Elementary 리포트 생성 (선택사항)
    generate_report = BashOperator(
        task_id="generate_quality_report",
        bash_command="""
        cd /opt/airflow/dags/dbt/your_project && \
        edr report  # Elementary CLI 리포트 생성
        """,
    )
    
    # 태스크 의존성
    elementary_setup >> quality_checks >> generate_report
```

### 방법 3: 모델별 체크를 각 모델 실행 후 실행

각 모델 실행 직후 해당 모델의 품질 체크를 실행:

```python
from datetime import datetime, timedelta
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.operators.bash import BashOperator

# ... (기본 설정 동일)

with DAG(
    dag_id="dbt_incremental_with_quality",
    default_args=default_args,
    description="증분 모델 실행 및 품질 체크",
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "incremental", "data-quality"],
) as dag:
    
    # 모델 실행
    dbt_models = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dags/dbt/your_project",
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "install_deps": True,
        },
    )
    
    # 실행된 모델에 대한 품질 체크
    quality_checks = BashOperator(
        task_id="post_model_quality_checks",
        bash_command="""
        cd /opt/airflow/dags/dbt/your_project && \
        # 최근 실행된 모델에 대한 Elementary 체크
        dbt run --select elementary && \
        dbt test --select elementary
        """,
    )
    
    dbt_models >> quality_checks
```

## 📈 Elementary 대시보드 실행

Elementary는 웹 대시보드를 제공합니다. 별도 서버로 실행할 수 있습니다:

### 로컬 실행

```bash
cd /opt/airflow/dags/dbt/your_project
edr run
```

기본적으로 `http://localhost:8080`에서 대시보드를 확인할 수 있습니다.

### Docker로 실행 (선택사항)

```yaml
# docker-compose.yml에 추가
elementary:
  image: elementary/elementary:latest
  ports:
    - "8080:8080"
  environment:
    - DBT_PROJECT_DIR=/opt/airflow/dags/dbt/your_project
    - DBT_PROFILES_DIR=/opt/airflow/dags/dbt/your_project
  volumes:
    - ./dags/dbt/your_project:/opt/airflow/dags/dbt/your_project
  command: edr run
```

### Airflow에서 대시보드 실행 태스크 (선택사항)

```python
from airflow.operators.bash import BashOperator

# DAG 내에 추가
elementary_dashboard = BashOperator(
    task_id="start_elementary_dashboard",
    bash_command="""
    cd /opt/airflow/dags/dbt/your_project && \
    edr run --port 8080 &
    """,
    # 주의: 백그라운드 실행은 프로덕션에서는 별도 서비스로 관리 권장
)
```

## 🔔 알림 설정

### Slack 알림

`dbt_project.yml`에 Slack 설정 추가:

```yaml
vars:
  elementary:
    slack:
      token: "{{ env_var('ELEMENTARY_SLACK_TOKEN') }}"
      channel: "#data-quality"
      notify_on: ["test_failure", "anomaly_detected"]
```

### Email 알림

Airflow의 기본 email 설정을 활용하거나, Elementary의 email 설정 사용:

```yaml
vars:
  elementary:
    email:
      smtp_host: "{{ env_var('SMTP_HOST') }}"
      smtp_port: "{{ env_var('SMTP_PORT', '587') }}"
      smtp_user: "{{ env_var('SMTP_USER') }}"
      smtp_password: "{{ env_var('SMTP_PASSWORD') }}"
      to_emails: ["data-team@example.com"]
```

## 📝 실제 적용 예시

### 예시 1: sample_project에 Elementary 적용

1. `packages.yml` 업데이트:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
  - package: elementary-data/elementary
    version: 0.12.0
```

2. `dbt_project.yml`에 설정 추가:

```yaml
vars:
  elementary:
    data_quality_enabled: true
    freshness_enabled: true
    schema_changes_enabled: true
    anomaly_detection_enabled: true
```

3. DAG 업데이트 (`dag.py`):

```python
# 기존 DbtTaskGroup 다음에 Elementary 체크 추가
elementary_quality = BashOperator(
    task_id="elementary_quality_checks",
    bash_command="""
    cd /opt/airflow/dags/dbt/sample_project && \
    dbt run --select elementary && \
    dbt test --select elementary
    """,
)

dbt_transform >> elementary_quality
```

### 예시 2: unified_montrg에 Elementary 적용

증분 처리 DAG에 품질 체크 추가:

```python
# unified_montrg/dag.py에 추가
with DAG(...) as dag:
    # ... 기존 태스크들 ...
    
    # 데이터 품질 체크 태스크 추가
    quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command="""
        cd /opt/airflow/dags/dbt/unified_montrg && \
        dbt run --select elementary && \
        dbt test --select elementary
        """,
    )
    
    # 의존성 설정
    dbt_incremental >> quality_checks
```

## ✅ 체크리스트

- [ ] `packages.yml`에 elementary 패키지 추가
- [ ] `dbt deps` 실행하여 패키지 설치
- [ ] `dbt_project.yml`에 elementary 설정 추가
- [ ] `dbt run --select elementary` 실행하여 테이블 생성
- [ ] Airflow DAG에 Elementary 체크 태스크 추가
- [ ] 테스트 실행 및 결과 확인
- [ ] (선택) Elementary 대시보드 실행
- [ ] (선택) 알림 설정 (Slack/Email)

## 🔍 문제 해결

### 패키지 설치 실패
```bash
# dbt 프로젝트 디렉토리에서 실행
dbt deps --profiles-dir .
```

### Elementary 테이블 생성 실패
```bash
# 프로필 확인
dbt debug --profiles-dir .

# Elementary 테이블 수동 생성
dbt run-operation elementary.create_elementary_tables
```

### 대시보드 접근 불가
- 포트가 이미 사용 중인지 확인
- 방화벽 설정 확인
- Docker 네트워크 설정 확인

## 📚 참고 자료

- Elementary 공식 문서: https://docs.elementary-data.com/
- dbt 패키지 허브: https://hub.getdbt.com/elementary-data/elementary
- GitHub: https://github.com/elementary-data/elementary
- 데이터 품질 모범 사례: https://docs.elementary-data.com/guides/data-quality-best-practices

## 💡 모범 사례

1. **점진적 적용**: 모든 모델에 한 번에 적용하지 말고, 중요한 모델부터 시작
2. **알림 설정**: 중요한 체크 실패 시 즉시 알림 받도록 설정
3. **대시보드 모니터링**: 정기적으로 대시보드를 확인하여 데이터 품질 트렌드 파악
4. **테스트 결과 저장**: Airflow XCom 또는 별도 저장소에 테스트 결과 저장
5. **문서화**: 각 모델의 품질 체크 기준을 명확히 문서화
