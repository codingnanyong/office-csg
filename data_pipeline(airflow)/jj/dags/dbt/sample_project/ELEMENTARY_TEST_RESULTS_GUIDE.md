# elementary_test_results 테이블 해석 가이드

## 📊 테이블 개요

`elementary_test_results` 테이블은 **모든 dbt 테스트 실행 결과**를 저장하는 Elementary의 핵심 테이블입니다.

## 🔍 주요 컬럼 설명

사용자가 보여준 데이터 구조를 기반으로 설명합니다:

| 컬럼 이름 | 설명 | 예시 |
|----------|------|------|
| `test_name` | 테스트 이름 | `accepted_values`, `not_null`, `unique` 등 |
| `test_type` | 테스트 타입 | `ERROR` (테스트 타입 분류) |
| `status` | 테스트 상태 | `pass` (통과), `fail` (실패), `warn` (경고) |
| `table_name` | 테스트 대상 테이블 | `stg_sample_data`, `dim_products` 등 |
| `column_name` | 테스트 대상 컬럼 | `id`, `price`, `status_normalized` 등 |
| `model` | 모델 참조 정보 | `{{ get_where_subquery(ref('stg_sample_data')) }}` |
| `test_result` | 테스트 결과 상세 (JSON) | 테스트별 설정 값들 |

## ✅ 현재 상태 해석

보여주신 데이터를 분석하면:

```
✅ 모든 테스트가 status = "pass" (통과)
✅ 총 12개의 테스트가 실행됨
✅ 3개의 모델에 대한 테스트:
   - stg_sample_data: 6개 테스트
   - dim_products: 4개 테스트  
   - agg_product_summary: 2개 테스트
```

**의미**: 현재 데이터 품질이 정상입니다! 🎉

## 📋 테스트별 설명

### 1. `not_null` 테스트
**목적**: 컬럼에 NULL 값이 없는지 확인

```sql
-- 예시: id 컬럼에 NULL이 없어야 함
SELECT COUNT(*) 
FROM stg_sample_data 
WHERE id IS NULL;  -- 결과가 0이어야 함
```

**현재 상태**: ✅ 모두 통과

### 2. `unique` 테스트
**목적**: 컬럼 값이 고유한지 확인 (중복 없음)

```sql
-- 예시: id 컬럼 값이 고유해야 함
SELECT id, COUNT(*) 
FROM stg_sample_data 
GROUP BY id 
HAVING COUNT(*) > 1;  -- 결과가 없어야 함
```

**현재 상태**: ✅ 모두 통과

### 3. `accepted_values` 테스트
**목적**: 컬럼 값이 허용된 값 목록에 있는지 확인

```sql
-- 예시: status_normalized는 'active' 또는 'inactive'만 허용
SELECT DISTINCT status_normalized 
FROM stg_sample_data 
WHERE status_normalized NOT IN ('active', 'inactive');  -- 결과가 없어야 함
```

**현재 상태**: ✅ 모두 통과

### 4. `accepted_range` 테스트 (dbt_utils)
**목적**: 숫자 값이 허용된 범위 내에 있는지 확인

```sql
-- 예시: price는 0 이상이어야 함
SELECT COUNT(*) 
FROM stg_sample_data 
WHERE price < 0;  -- 결과가 0이어야 함
```

**현재 상태**: ✅ 모두 통과

## 🔍 유용한 쿼리 예제

### 1. 최근 테스트 결과 요약

```sql
-- 전체 테스트 통과율
SELECT 
    status,
    COUNT(*) as test_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM public.elementary_test_results
WHERE test_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY status
ORDER BY 
    CASE status 
        WHEN 'pass' THEN 1 
        WHEN 'warn' THEN 2 
        WHEN 'fail' THEN 3 
    END;
```

### 2. 테이블별 테스트 현황

```sql
-- 각 테이블별 테스트 통과/실패 현황
SELECT 
    table_name,
    COUNT(*) as total_tests,
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) as warned,
    MAX(test_time) as last_test_time
FROM public.elementary_test_results
WHERE test_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY table_name
ORDER BY failed DESC, table_name;
```

### 3. 실패한 테스트 상세 확인

```sql
-- 실패한 테스트의 상세 정보
SELECT 
    test_name,
    table_name,
    column_name,
    status,
    test_time,
    test_result
FROM public.elementary_test_results
WHERE status = 'fail'
ORDER BY test_time DESC;
```

### 4. 특정 모델의 테스트 이력

```sql
-- 특정 테이블의 최근 테스트 결과
SELECT 
    test_name,
    column_name,
    status,
    test_time
FROM public.elementary_test_results
WHERE table_name = 'stg_sample_data'  -- 테이블 이름 변경
ORDER BY test_time DESC, test_name;
```

### 5. 테스트 타입별 통계

```sql
-- 테스트 타입별 통과/실패 현황
SELECT 
    test_name,
    COUNT(*) as total_count,
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate
FROM public.elementary_test_results
WHERE test_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY test_name
ORDER BY failed DESC, test_name;
```

### 6. 최근 24시간 테스트 결과

```sql
-- 최근 24시간 동안의 테스트 결과
SELECT 
    DATE_TRUNC('hour', test_time) as test_hour,
    COUNT(*) as total_tests,
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as failed
FROM public.elementary_test_results
WHERE test_time >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', test_time)
ORDER BY test_hour DESC;
```

### 7. 실패 빈도가 높은 테스트 찾기

```sql
-- 가장 자주 실패하는 테스트 Top 10
SELECT 
    table_name,
    test_name,
    column_name,
    COUNT(*) as failure_count,
    MAX(test_time) as last_failure,
    MIN(test_time) as first_failure
FROM public.elementary_test_results
WHERE status = 'fail'
  AND test_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY table_name, test_name, column_name
ORDER BY failure_count DESC
LIMIT 10;
```

## 📊 일일 모니터링 쿼리

### 오늘의 테스트 현황 (한눈에 보기)

```sql
SELECT 
    '오늘 총 테스트 수' as metric,
    COUNT(*)::text as value
FROM public.elementary_test_results
WHERE DATE(test_time) = CURRENT_DATE

UNION ALL

SELECT 
    '오늘 통과한 테스트',
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)::text
FROM public.elementary_test_results
WHERE DATE(test_time) = CURRENT_DATE

UNION ALL

SELECT 
    '오늘 실패한 테스트',
    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)::text
FROM public.elementary_test_results
WHERE DATE(test_time) = CURRENT_DATE

UNION ALL

SELECT 
    '오늘 테스트 통과율 (%)',
    ROUND(100.0 * SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) / 
          NULLIF(COUNT(*), 0), 2)::text || '%'
FROM public.elementary_test_results
WHERE DATE(test_time) = CURRENT_DATE;
```

## 🚨 문제 발생 시 확인 순서

### Step 1: 전체 현황 파악
```sql
SELECT status, COUNT(*) 
FROM public.elementary_test_results 
WHERE DATE(test_time) = CURRENT_DATE
GROUP BY status;
```

### Step 2: 실패한 테스트 확인
```sql
SELECT 
    table_name,
    test_name,
    column_name,
    test_time,
    test_result
FROM public.elementary_test_results
WHERE status = 'fail'
  AND DATE(test_time) = CURRENT_DATE
ORDER BY test_time DESC;
```

### Step 3: 문제가 있는 테이블 확인
```sql
SELECT 
    table_name,
    COUNT(*) as failed_tests
FROM public.elementary_test_results
WHERE status = 'fail'
  AND DATE(test_time) = CURRENT_DATE
GROUP BY table_name
ORDER BY failed_tests DESC;
```

### Step 4: 상세 분석
실패한 테스트의 `test_result` 컬럼을 확인하여 어떤 값이 문제인지 파악합니다.

## 💡 주요 포인트

1. **`status = 'pass'`**: ✅ 정상 - 테스트 통과
2. **`status = 'fail'`**: ❌ 문제 - 즉시 확인 필요
3. **`status = 'warn'`**: ⚠️ 경고 - 주의 필요

4. **`test_type = 'ERROR'`**: 이것은 테스트 타입 분류일 뿐, 실제 오류를 의미하는 것이 아닙니다. `status` 컬럼을 확인하세요!

5. **`test_result` 컬럼**: JSON 형식으로 테스트별 설정 값이 저장됩니다:
   - `accepted_values`: `{"values": ["active", "inactive"], ...}`
   - `accepted_range`: `{"min_value": 0, "inclusive": true, ...}`
   - `not_null`, `unique`: 설정 없음 (NULL 또는 빈 객체)

## 📝 현재 프로젝트의 테스트 현황

보여주신 데이터를 기반으로:

| 모델 | 테스트 수 | 상태 |
|------|----------|------|
| `stg_sample_data` | 6개 | ✅ 모두 통과 |
| `dim_products` | 4개 | ✅ 모두 통과 |
| `agg_product_summary` | 2개 | ✅ 모두 통과 |

**결론**: 현재 모든 데이터 품질 테스트가 정상적으로 통과하고 있습니다! 🎉

## 🔗 관련 문서

- 테이블/뷰 전체 가이드: `ELEMENTARY_TABLES_GUIDE.md`
- 빠른 쿼리 모음: `elementary_quick_queries.sql`
- 결과 해석 가이드: `ELEMENTARY_RESULTS_GUIDE.md`

