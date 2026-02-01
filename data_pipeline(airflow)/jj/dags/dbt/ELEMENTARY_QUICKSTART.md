# dbt Elementary 빠른 시작 가이드

이 가이드는 dbt Elementary를 기존 dbt 프로젝트에 빠르게 적용하는 방법을 설명합니다.

## 🚀 3단계 빠른 시작

### 1단계: 패키지 추가

프로젝트의 `packages.yml` 파일에 elementary 패키지를 추가:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
  - package: elementary-data/elementary
    version: 0.12.0
```

### 2단계: 설정 추가

`dbt_project.yml`에 Elementary 설정 추가:

```yaml
vars:
  elementary:
    data_quality_enabled: true
    freshness_enabled: true
    schema_changes_enabled: true
    anomaly_detection_enabled: true
```

### 3단계: 패키지 설치 및 초기화

```bash
cd /opt/airflow/dags/dbt/your_project
dbt deps
dbt run --select elementary
```

## 📝 프로젝트별 적용 방법

### sample_project에 적용

1. **packages.yml 업데이트**:
   ```bash
   cd /opt/airflow/dags/dbt/sample_project
   # packages.yml.example을 참고하여 packages.yml 수정
   ```

2. **dbt_project.yml 업데이트**:
   ```bash
   # dbt_project.yml.example을 참고하여 vars 섹션 추가
   ```

3. **패키지 설치**:
   ```bash
   dbt deps --profiles-dir .
   ```

4. **Elementary 초기화**:
   ```bash
   dbt run --select elementary --profiles-dir .
   ```

5. **DAG 사용**:
   - `dag_with_elementary.py`를 사용하거나
   - 기존 `dag.py`에 Elementary 체크 태스크 추가

### unified_montrg에 적용

1. **packages.yml에 elementary 추가**:
   ```yaml
   packages:
     - package: elementary-data/elementary
       version: 0.12.0
   ```

2. **dbt_project.yml에 vars 추가**:
   ```yaml
   vars:
     elementary:
       data_quality_enabled: true
       freshness_enabled: true
   ```

3. **DAG에 품질 체크 태스크 추가**:
   ```python
   # unified_montrg/dag.py에 추가
   quality_checks = BashOperator(
       task_id="data_quality_checks",
       bash_command="""
       cd /opt/airflow/dags/dbt/unified_montrg && \
       dbt run --select elementary --profiles-dir . && \
       dbt test --select elementary --profiles-dir .
       """,
   )
   
   # 의존성 설정
   dbt_incremental >> quality_checks
   ```

### banbury_anomaly_detection에 적용

동일한 방식으로 적용:

1. `packages.yml`에 elementary 추가
2. `dbt_project.yml`에 vars 추가
3. DAG에 품질 체크 태스크 추가

## 🔍 체크 실행 방법

### Airflow에서 실행

1. **DAG 실행**: `dbt_sample_project_with_quality` DAG 실행
2. **별도 품질 체크 DAG**: 주기적으로 품질만 체크하는 별도 DAG 생성 가능

### 수동 실행

```bash
# Elementary 테이블 업데이트
dbt run --select elementary

# 품질 체크 실행
dbt test --select elementary

# 특정 모델의 품질 체크만 실행
dbt test --select elementary --select stg_orders
```

## 📊 대시보드 확인

Elementary 대시보드를 실행하여 결과를 시각화:

```bash
cd /opt/airflow/dags/dbt/your_project
edr run
```

브라우저에서 `http://localhost:8080` 접속

## ✅ 확인 사항

- [ ] `dbt deps` 실행 성공
- [ ] `dbt run --select elementary` 실행 성공
- [ ] Airflow DAG에서 Elementary 체크 태스크 실행 성공
- [ ] 대시보드에서 데이터 품질 메트릭 확인 가능

## 🐛 문제 해결

### 패키지 설치 실패
```bash
# 프로필 경로 명시
dbt deps --profiles-dir .
```

### Elementary 테이블 생성 실패
```bash
# 수동으로 테이블 생성
dbt run-operation elementary.create_elementary_tables --profiles-dir .
```

### 테스트 실패
- `dbt debug`로 프로필 확인
- 데이터베이스 연결 확인
- 스키마 권한 확인

## 📚 더 자세한 정보

- 전체 가이드: `/opt/airflow/docs/DBT_ELEMENTARY_INTEGRATION.md`
- Elementary 문서: https://docs.elementary-data.com/
- 예시 파일: `sample_project/*.example` 파일들 참고
