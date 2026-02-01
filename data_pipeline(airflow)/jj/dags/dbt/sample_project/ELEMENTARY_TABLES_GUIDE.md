# Elementary 테이블/뷰 가이드 - 무엇을 봐야 하나?

데이터베이스에 생성된 Elementary 테이블과 뷰들의 역할과 주요 확인 사항을 정리했습니다.

## 📊 우선 확인해야 할 핵심 뷰 (TOP 5)

### 1. `alerts_dbt_tests` ⭐⭐⭐
**역할**: dbt 테스트 실패/경고 알림  
**언제 봐야 하나**: 데이터 품질 문제가 발생했을 때  
**확인 방법**:
```sql
-- 최근 테스트 실패 확인
SELECT 
    test_name,
    table_name,
    status,
    test_time,
    message
FROM public.alerts_dbt_tests
WHERE status = 'fail'
ORDER BY test_time DESC
LIMIT 20;
```

### 2. `model_run_results` ⭐⭐⭐
**역할**: 각 dbt 모델의 실행 결과 및 성능  
**언제 봐야 하나**: 모델 실행 상태와 성능 확인  
**확인 방법**:
```sql
-- 최근 실행된 모델 상태 확인
SELECT 
    model_name,
    status,
    run_time,
    execution_time,
    rows_affected
FROM public.model_run_results
ORDER BY run_time DESC
LIMIT 20;
```

### 3. `alerts_anomaly_detection` ⭐⭐
**역할**: 데이터 이상치(Anomaly) 감지 알림  
**언제 봐야 하나**: 데이터 패턴이 비정상적으로 변했을 때  
**확인 방법**:
```sql
-- 최근 이상치 감지 확인
SELECT 
    alert_type,
    table_name,
    alert_value,
    alert_message,
    alert_time
FROM public.alerts_anomaly_detection
ORDER BY alert_time DESC
LIMIT 20;
```

### 4. `alerts_schema_changes` ⭐⭐
**역할**: 스키마 변경 감지 (컬럼 추가/삭제/타입 변경)  
**언제 봐야 하나**: 테이블 구조가 예상치 못하게 변경되었을 때  
**확인 방법**:
```sql
-- 최근 스키마 변경 확인
SELECT 
    table_name,
    column_name,
    change_type,
    change_time,
    description
FROM public.alerts_schema_changes
ORDER BY change_time DESC;
```

### 5. `alerts_dbt_source_freshness` ⭐
**역할**: 소스 데이터의 최신성(Freshness) 체크  
**언제 봐야 하나**: 데이터 업데이트가 지연되었을 때  
**확인 방법**:
```sql
-- 데이터 최신성 확인
SELECT 
    source_name,
    table_name,
    last_updated,
    freshness_status,
    alert_time
FROM public.alerts_dbt_source_freshness
WHERE freshness_status != 'pass'
ORDER BY alert_time DESC;
```

---

## 📋 모든 테이블/뷰 상세 설명

### 📦 Tables (테이블)

#### dbt 메타데이터 테이블들

| 테이블 이름 | 역할 | 주요 컬럼 | 언제 확인? |
|-----------|------|----------|----------|
| **`dbt_models`** | 모든 dbt 모델의 메타데이터 | model_name, schema, materialized, tags | 모델 목록 확인, 모델 정보 조회 |
| **`dbt_sources`** | 소스 테이블 메타데이터 | source_name, table_name, schema | 소스 데이터 정보 확인 |
| **`dbt_tests`** | 테스트 정의 메타데이터 | test_name, test_type, table_name | 설정된 테스트 목록 확인 |
| **`dbt_columns`** | 모든 컬럼 메타데이터 | table_name, column_name, data_type | 테이블 구조 확인 |
| **`dbt_run_results`** | dbt 실행 결과 상세 | run_id, status, execution_time | 실행 이력 상세 분석 |

#### Elementary 모니터링 테이블들

| 테이블 이름 | 역할 | 주요 컬럼 | 언제 확인? |
|-----------|------|----------|----------|
| **`elementary_test_results`** | 모든 테스트 실행 결과 | test_name, status, test_time, table_name | 테스트 결과 상세 조회 |
| **`data_monitoring_metrics`** | 데이터 모니터링 메트릭 | metric_name, metric_value, timestamp | 데이터 품질 메트릭 추이 분석 |
| **`schema_columns_snapshot`** | 스키마 컬럼 스냅샷 | table_name, column_name, snapshot_time | 스키마 변경 이력 확인 |
| **`test_result_rows`** | 테스트 결과의 실제 행 데이터 | test_name, table_name, failed_rows | 실패한 테스트의 실제 데이터 확인 |

#### 기타 테이블들

| 테이블 이름 | 역할 | 주요 컬럼 | 언제 확인? |
|-----------|------|----------|----------|
| **`dbt_invocations`** | dbt 실행 호출 이력 | invocation_id, run_started_at | 실행 이력 확인 |
| **`metadata`** | Elementary 메타데이터 | key, value | Elementary 설정 확인 |
| **`dim_products`** | 실제 비즈니스 데이터 | (프로젝트 모델) | 실제 데이터 확인 |

### 👁️ Views (뷰)

#### 알림 관련 뷰 (Alerts) - 가장 중요! ⭐

| 뷰 이름 | 역할 | 주요 확인 사항 |
|--------|------|--------------|
| **`alerts_dbt_tests`** | dbt 테스트 실패 알림 | 실패한 테스트, 원인 |
| **`alerts_anomaly_detection`** | 이상치 감지 알림 | 비정상 데이터 패턴 |
| **`alerts_schema_changes`** | 스키마 변경 알림 | 예상치 못한 스키마 변경 |
| **`alerts_dbt_source_freshness`** | 데이터 최신성 알림 | 지연된 데이터 업데이트 |
| **`alerts_dbt_models`** | 모델 관련 알림 | 모델 실행 실패 등 |

#### 실행 결과 뷰

| 뷰 이름 | 역할 | 주요 확인 사항 |
|--------|------|--------------|
| **`model_run_results`** | 모델 실행 결과 | 실행 상태, 성능, 영향받은 행 수 |
| **`job_run_results`** | 작업 실행 결과 | 전체 작업 실행 현황 |
| **`snapshot_run_results`** | 스냅샷 실행 결과 | 스냅샷 실행 현황 |

#### 분석 및 모니터링 뷰

| 뷰 이름 | 역할 | 주요 확인 사항 |
|--------|------|--------------|
| **`metrics_anomaly_score`** | 이상치 점수 | 데이터 이상 정도 측정 |
| **`monitors_runs`** | 모니터 실행 이력 | 모니터링 실행 현황 |
| **`anomaly_threshold_sensitivity`** | 이상치 임계값 민감도 | 이상치 감지 설정 |

#### 메타데이터 뷰

| 뷰 이름 | 역할 | 주요 확인 사항 |
|--------|------|--------------|
| **`enriched_columns`** | 보강된 컬럼 정보 | 컬럼 상세 메타데이터 |
| **`information_schema_columns`** | 정보 스키마 컬럼 | 데이터베이스 컬럼 정보 |
| **`dbt_artifacts_hashes`** | dbt 아티팩트 해시 | dbt 파일 변경 감지 |

---

## 🎯 실전 사용 시나리오

### 시나리오 1: "오늘 데이터 품질 문제가 있었나?"

```sql
-- 1단계: 테스트 실패 확인
SELECT 
    test_name,
    table_name,
    status,
    test_time,
    message
FROM public.alerts_dbt_tests
WHERE DATE(test_time) = CURRENT_DATE
  AND status = 'fail'
ORDER BY test_time DESC;

-- 2단계: 이상치 확인
SELECT 
    alert_type,
    table_name,
    alert_message,
    alert_time
FROM public.alerts_anomaly_detection
WHERE DATE(alert_time) = CURRENT_DATE
ORDER BY alert_time DESC;

-- 3단계: 스키마 변경 확인
SELECT *
FROM public.alerts_schema_changes
WHERE DATE(change_time) = CURRENT_DATE;
```

### 시나리오 2: "특정 모델이 정상 실행되고 있나?"

```sql
-- 모델 실행 상태 확인
SELECT 
    model_name,
    status,
    run_time,
    execution_time,
    rows_affected
FROM public.model_run_results
WHERE model_name = 'dim_products'  -- 모델 이름 변경
ORDER BY run_time DESC
LIMIT 10;
```

### 시나리오 3: "데이터가 최신 상태인가?"

```sql
-- 소스 데이터 최신성 확인
SELECT 
    source_name,
    table_name,
    last_updated,
    freshness_status,
    alert_time
FROM public.alerts_dbt_source_freshness
ORDER BY alert_time DESC;
```

### 시나리오 4: "최근 7일간 데이터 품질 트렌드는?"

```sql
-- 일별 테스트 통과율
SELECT 
    DATE(test_time) as test_date,
    COUNT(*) as total_tests,
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate
FROM public.elementary_test_results
WHERE test_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(test_time)
ORDER BY test_date DESC;
```

### 시나리오 5: "어떤 모델이 가장 자주 실패하나?"

```sql
-- 실패 빈도가 높은 모델 찾기
SELECT 
    table_name,
    test_name,
    COUNT(*) as failure_count,
    MAX(test_time) as last_failure
FROM public.elementary_test_results
WHERE status = 'fail'
  AND test_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY table_name, test_name
ORDER BY failure_count DESC
LIMIT 10;
```

---

## 📊 일일 점검 체크리스트

매일 아침 다음 순서로 확인하세요:

```sql
-- 1. 테스트 실패 확인 (가장 중요!)
SELECT COUNT(*) as failed_tests
FROM public.alerts_dbt_tests
WHERE DATE(test_time) = CURRENT_DATE
  AND status = 'fail';

-- 2. 이상치 알림 확인
SELECT COUNT(*) as anomalies
FROM public.alerts_anomaly_detection
WHERE DATE(alert_time) = CURRENT_DATE;

-- 3. 스키마 변경 확인
SELECT COUNT(*) as schema_changes
FROM public.alerts_schema_changes
WHERE DATE(change_time) = CURRENT_DATE;

-- 4. 데이터 최신성 확인
SELECT COUNT(*) as stale_data
FROM public.alerts_dbt_source_freshness
WHERE DATE(alert_time) = CURRENT_DATE
  AND freshness_status != 'pass';

-- 5. 최근 모델 실행 상태 확인
SELECT 
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failed
FROM public.model_run_results
WHERE DATE(run_time) = CURRENT_DATE;
```

---

## 🔍 테이블 구조 빠른 확인

### 주요 테이블의 컬럼 확인 방법

```sql
-- PostgreSQL에서 테이블 구조 확인
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'alerts_dbt_tests'  -- 테이블 이름 변경
ORDER BY ordinal_position;
```

---

## 💡 요약: 처음에는 이것만 보세요!

**우선순위 1 (매일 확인)**:
- `alerts_dbt_tests` - 테스트 실패
- `model_run_results` - 모델 실행 상태

**우선순위 2 (문제 발생 시)**:
- `alerts_anomaly_detection` - 이상치
- `alerts_schema_changes` - 스키마 변경
- `alerts_dbt_source_freshness` - 데이터 최신성

**우선순위 3 (상세 분석 시)**:
- `elementary_test_results` - 테스트 결과 상세
- `dbt_run_results` - 실행 이력 상세
- `data_monitoring_metrics` - 메트릭 분석

---

## 📚 관련 문서

- 전체 Elementary 가이드: `ELEMENTARY_RESULTS_GUIDE.md`
- Elementary 공식 문서: https://docs.elementary-data.com/

