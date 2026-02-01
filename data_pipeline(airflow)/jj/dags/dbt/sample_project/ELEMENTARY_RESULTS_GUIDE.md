# Elementary 결과 해석 가이드

## 📊 Elementary 실행 결과 이해하기

Elementary가 정상적으로 실행되면 다음과 같은 결과를 확인할 수 있습니다.

> 💡 **빠른 시작**: 각 테이블/뷰의 역할과 무엇을 봐야 하는지는 `ELEMENTARY_TABLES_GUIDE.md`를 참고하세요.  
> 💡 **실용 쿼리**: 바로 사용할 수 있는 SQL 쿼리는 `elementary_quick_queries.sql`을 참고하세요.

## 1. 실행 결과 해석

### `dbt run --select elementary` 결과

```
Finished running 15 incremental models, 1 table model, 14 view models, 2 hooks
Completed successfully
Done. PASS=30 WARN=0 ERROR=0 SKIP=0 TOTAL=30
```

**의미:**
- ✅ **PASS=30**: 30개의 Elementary 모델/테이블이 성공적으로 생성/업데이트됨
- ⚠️ **WARN=0**: 경고 없음
- ❌ **ERROR=0**: 오류 없음
- ⏭️ **SKIP=0**: 건너뛴 항목 없음

**생성된 주요 테이블/뷰:**
- `dbt_models`: dbt 모델 메타데이터
- `dbt_sources`: dbt 소스 메타데이터
- `dbt_tests`: dbt 테스트 정의
- `elementary_test_results`: 모든 테스트 결과
- `dbt_run_results`: dbt 실행 결과
- `model_run_results`: 모델 실행 결과
- `alerts_*`: 다양한 알림 뷰들 (anomaly, schema changes, freshness 등)

### `dbt test --select elementary` 결과

```
Completed successfully
Done. PASS=X WARN=Y ERROR=Z SKIP=0 TOTAL=N
```

**의미:**
- ✅ **PASS**: 통과한 테스트 수
- ⚠️ **WARN**: 경고를 발생시킨 테스트 수
- ❌ **ERROR**: 실패한 테스트 수
- **TOTAL**: 전체 테스트 수

## 2. 데이터베이스에서 결과 확인하기

Elementary는 데이터베이스에 여러 테이블을 생성합니다. SQL로 직접 조회하여 확인할 수 있습니다.

### 주요 확인 테이블

#### 1. 데이터 품질 테스트 결과
```sql
-- 모든 Elementary 테스트 결과 확인
SELECT 
    test_name,
    test_type,
    status,
    table_name,
    test_time,
    test_result
FROM public.elementary_test_results
ORDER BY test_time DESC
LIMIT 100;
```

#### 2. 모델 실행 결과
```sql
-- 최근 모델 실행 결과 확인
SELECT 
    model_name,
    status,
    run_time,
    rows_affected,
    execution_time
FROM public.model_run_results
ORDER BY run_time DESC
LIMIT 50;
```

#### 3. 데이터 이상치 알림
```sql
-- 이상치 감지 알림 확인
SELECT 
    alert_type,
    table_name,
    alert_time,
    alert_value,
    alert_message
FROM public.alerts_anomaly_detection
ORDER BY alert_time DESC;
```

#### 4. 스키마 변경 알림
```sql
-- 스키마 변경 감지 확인
SELECT 
    table_name,
    column_name,
    change_type,
    change_time,
    description
FROM public.alerts_schema_changes
ORDER BY change_time DESC;
```

#### 5. 데이터 Freshness 알림
```sql
-- 데이터 최신성 체크 결과
SELECT 
    source_name,
    table_name,
    last_updated,
    freshness_status,
    alert_time
FROM public.alerts_dbt_source_freshness
ORDER BY alert_time DESC;
```

## 3. Airflow에서 결과 확인하기

### DAG 실행 로그 확인

1. **Airflow UI** → DAG 선택 → Task Instance 클릭
2. **Log 탭**에서 다음을 확인:
   - `install_dbt_dependencies`: 패키지 설치 성공 여부
   - `elementary_setup`: Elementary 테이블 생성 성공 여부
   - `elementary_quality_checks`: 데이터 품질 체크 결과
   - `dbt_tests`: 일반 테스트 결과

### 성공/실패 판단 기준

- ✅ **성공**: 모든 테스트가 PASS 또는 WARN=0, ERROR=0
- ⚠️ **경고**: WARN > 0 (일부 테스트가 경고 발생, 하지만 실행은 성공)
- ❌ **실패**: ERROR > 0 (테스트 실패 또는 실행 오류)

## 4. Elementary 대시보드 사용하기

더 시각적으로 결과를 확인하려면 Elementary 대시보드를 사용할 수 있습니다.

### 대시보드 실행 방법

```bash
# Airflow 컨테이너 내에서
docker exec -it airflow-scheduler bash
cd /opt/airflow/dags/dbt/sample_project

# Elementary CLI 설치 (아직 설치되지 않은 경우)
pip install elementary-data[postgres]

# 대시보드 실행
edr run --profiles-dir .
```

브라우저에서 `http://localhost:8080` 접속 (포트는 다를 수 있음)

### 대시보드에서 확인할 수 있는 정보

1. **모델 실행 현황**: 각 모델의 실행 상태 및 성능
2. **테스트 결과**: 모든 테스트의 통과/실패 현황
3. **데이터 품질 메트릭**: 
   - 데이터 볼륨 변화
   - 이상치 감지
   - 스키마 변경
   - 데이터 Freshness
4. **알림 내역**: 발생한 모든 알림 및 경고

## 5. 일반적인 상황별 해석

### 상황 1: 모든 테스트 통과
```
PASS=30 WARN=0 ERROR=0
```
**의미**: 모든 데이터 품질 체크가 정상입니다. 데이터에 문제가 없습니다.

### 상황 2: 일부 경고 발생
```
PASS=28 WARN=2 ERROR=0
```
**의미**: 대부분 정상이지만 일부 경고가 있습니다. 경고 내용을 확인하고 필요시 조치가 필요할 수 있습니다.

### 상황 3: 테스트 실패
```
PASS=25 ERROR=5
```
**의미**: 일부 데이터 품질 체크가 실패했습니다. 실패한 테스트를 확인하고 데이터 또는 모델 로직을 점검해야 합니다.

## 6. 실제 활용 예시

### 예시 1: 일일 데이터 품질 리포트 생성

```sql
-- 오늘 실행된 모든 테스트 결과 요약
SELECT 
    DATE(test_time) as test_date,
    COUNT(*) as total_tests,
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) as warned
FROM public.elementary_test_results
WHERE DATE(test_time) = CURRENT_DATE
GROUP BY DATE(test_time);
```

### 예시 2: 문제가 있는 모델 찾기

```sql
-- 최근 7일간 실패한 테스트가 있는 모델 찾기
SELECT 
    table_name,
    test_name,
    COUNT(*) as failure_count,
    MAX(test_time) as last_failure
FROM public.elementary_test_results
WHERE status = 'fail'
  AND test_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY table_name, test_name
ORDER BY failure_count DESC;
```

### 예시 3: 데이터 이상치 모니터링

```sql
-- 오늘 발생한 이상치 알림 확인
SELECT 
    alert_type,
    table_name,
    alert_value,
    alert_message,
    alert_time
FROM public.alerts_anomaly_detection
WHERE DATE(alert_time) = CURRENT_DATE
ORDER BY alert_time DESC;
```

## 7. 다음 단계

1. **정기적으로 모니터링**: DAG를 정기적으로 실행하여 데이터 품질을 지속적으로 모니터링
2. **알림 설정**: 중요한 실패 시 Slack/Email 알림 설정 (dbt_project.yml 참고)
3. **대시보드 활용**: Elementary 대시보드를 사용하여 시각적으로 모니터링
4. **커스터마이징**: 모델별로 필요한 Elementary 테스트 추가 (schema.yml 참고)

## 8. 추가 자료

- Elementary 공식 문서: https://docs.elementary-data.com/
- dbt 패키지 허브: https://hub.getdbt.com/elementary-data/elementary
- 전체 통합 가이드: `/opt/airflow/docs/DBT_ELEMENTARY_INTEGRATION.md`

