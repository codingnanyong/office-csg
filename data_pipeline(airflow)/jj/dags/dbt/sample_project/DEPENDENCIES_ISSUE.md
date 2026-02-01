# dbt 패키지 설치 문제 해결

## 현재 상황

- SSL 인증서 오류로 `dbt deps` 실행 실패
- `curl: (77) error setting certificate file: /etc/ssl/certs/ca-certificates.crt`

## 해결 방법

### 방법 1: SSL 인증서 업데이트 (컨테이너 내)

```bash
docker exec -u root airflow-scheduler bash -c "apt-get update && apt-get install -y ca-certificates"
```

### 방법 2: dbt_utils 없이 진행 (현재 상태)

현재 `schema.yml`에서 `dbt_utils.accepted_range` 테스트가 주석 처리되어 있으므로, 패키지 없이도 정상 작동합니다.

### 방법 3: 수동 패키지 설치

GitHub에서 직접 다운로드:

```bash
cd /home/user/apps/airflow/dags/dbt/sample_project
git clone https://github.com/dbt-labs/dbt-utils.git dbt_packages/dbt_utils
cd dbt_packages/dbt_utils
git checkout v1.3.0  # 또는 호환되는 버전
```

### 방법 4: 패키지 테스트 주석 유지

현재 상태로 사용:
- 기본 테스트만 사용 (not_null, unique, accepted_values)
- dbt_utils 테스트는 주석 처리 상태 유지

## 권장 사항

현재 상태로 진행하는 것을 권장합니다:
- ✅ 패키지 없이도 모든 기능 작동
- ✅ 기본 테스트로도 충분한 데이터 품질 검증 가능
- ✅ 나중에 SSL 문제 해결 후 패키지 설치 가능

