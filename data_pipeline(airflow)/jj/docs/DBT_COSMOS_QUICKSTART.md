# dbt + Cosmos 빠른 시작 가이드

## ✅ 무료 여부 확인

**100% 무료입니다!**
- `dbt-core`: 오픈소스 (무료)
- `astronomer-cosmos`: 오픈소스 (Apache 2.0 라이선스, 무료)
- 자체 호스팅 환경에서는 추가 비용 없음

## 🚀 빠른 시작 (5단계)

### 1단계: 패키지 설치

`requirements.txt`가 이미 업데이트되었습니다. Airflow 재시작 시 자동 설치됩니다:

```bash
cd /home/user/apps/airflow
docker compose restart airflow-scheduler airflow-webserver airflow-worker
```

또는 수동 설치:

```bash
docker exec -it airflow-scheduler pip install astronomer-cosmos dbt-core dbt-postgres
```

### 2단계: dbt 프로젝트 생성

```bash
# Airflow 컨테이너 내에서
docker exec -it airflow-scheduler bash
cd /opt/airflow/dags
mkdir -p dbt/my_dbt_project
cd dbt/my_dbt_project

# dbt 프로젝트 초기화
dbt init my_dbt_project
```

### 3단계: dbt 프로필 설정

**⚠️ 중요**: dbt는 **실제 데이터가 저장된 데이터베이스**에 연결해야 합니다.
- `airflow-postgres`는 Airflow 메타데이터용이므로 일반적으로 dbt 작업 대상이 아닙니다
- 사용자가 주로 사용하는 **실제 데이터 저장소**를 연결해야 합니다

`/opt/airflow/dags/dbt/my_dbt_project/profiles.yml` 파일 수정:

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
  target: dev
```

### 4단계: Airflow Connection 설정

**실제 데이터 저장소**에 대한 Connection을 설정합니다:

Airflow UI에서:
1. **Admin** → **Connections**
2. `+` 버튼 클릭
3. 설정:
   - **Connection Id**: `postgres_default` (또는 원하는 이름)
   - **Connection Type**: `Postgres`
   - **Host**: 실제 데이터베이스 호스트 (예: `10.10.100.80` 또는 외부 DB 주소)
   - **Schema**: 데이터베이스 이름 (예: `telemetry`, `monitoring` 등)
   - **Login**: 데이터베이스 사용자명
   - **Password**: 데이터베이스 비밀번호
   - **Port**: `5432` (또는 실제 포트)

**참고**: `airflow-postgres`를 사용하려면 Host를 `postgres`로 설정하되, 일반적으로는 실제 데이터 저장소를 사용합니다.

### 5단계: DAG 확인

1. Airflow UI에서 DAG 목록 확인
2. `dbt_transform_pipeline` DAG가 보이는지 확인
3. DAG 활성화 후 테스트 실행

## 📝 간단한 dbt 모델 예시

`/opt/airflow/dags/dbt/my_dbt_project/models/example.sql` 생성:

```sql
{{ config(materialized='view') }}

select
    current_timestamp as run_time,
    'Hello from dbt!' as message
```

## 🔍 검증

### 패키지 설치 확인
```bash
docker exec -it airflow-scheduler pip list | grep -E "cosmos|dbt"
```

### dbt 프로젝트 검증
```bash
docker exec -it airflow-scheduler bash
cd /opt/airflow/dags/dbt/my_dbt_project
dbt debug
```

### DAG 로드 확인
- Airflow UI → DAG 목록에서 `dbt_transform_pipeline` 확인

## 📚 상세 가이드

더 자세한 내용은 `DBT_COSMOS_INTEGRATION.md` 파일을 참고하세요.

## ⚠️ 주의사항

1. **경로 확인**: DAG 파일의 `dbt_project_path`가 실제 경로와 일치하는지 확인
2. **Connection 설정**: Airflow Connection이 올바르게 설정되었는지 확인
3. **스키마 생성**: dbt가 사용할 스키마가 데이터베이스에 존재하는지 확인

```sql
-- PostgreSQL에서 스키마 생성 예시
CREATE SCHEMA IF NOT EXISTS dbt;
```

## 🆘 문제 해결

### dbt 명령어를 찾을 수 없음
```bash
# dbt 설치 확인
docker exec -it airflow-scheduler which dbt
# 없으면 재설치
docker exec -it airflow-scheduler pip install dbt-core dbt-postgres
```

### Connection 오류
- Airflow UI에서 Connection 설정 확인
- 데이터베이스 접근 권한 확인

### DAG가 로드되지 않음
- DAG 파일에 문법 오류가 없는지 확인
- Airflow 로그 확인: `docker logs airflow-scheduler`

