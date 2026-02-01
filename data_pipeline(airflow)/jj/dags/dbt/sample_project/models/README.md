# dbt 모델 구조

이 디렉토리는 dbt 모델 파일들을 포함합니다.

## 📁 디렉토리 구조

```
models/
├── staging/          # Staging 레이어: 원본 데이터 정제
│   └── stg_sample_data.sql
├── marts/           # Marts 레이어: 비즈니스 로직 적용
│   ├── dim_products.sql
│   └── agg_product_summary.sql
├── example.sql      # 간단한 예시 모델 (테스트용)
└── schema.yml       # 모델 문서화 및 테스트 정의
```

## 🔄 데이터 흐름

```
원본 데이터 (Source)
    ↓
stg_sample_data (Staging) - 데이터 정제 및 표준화
    ↓
dim_products (Marts) - 비즈니스 로직 적용
    ↓
agg_product_summary (Marts) - 집계 및 요약
```

## 📝 모델 설명

### Staging 레이어

#### `stg_sample_data`
- **목적**: 원본 데이터를 정제하고 표준화
- **Materialization**: View
- **기능**:
  - 데이터 정제 (status 소문자 변환)
  - 계산된 필드 추가 (price_with_tax)
  - 타임스탬프 추가

### Marts 레이어

#### `dim_products`
- **목적**: 제품 차원 테이블 (비즈니스 로직 적용)
- **Materialization**: Table
- **기능**:
  - 가격대 분류 (Low, Medium, High)
  - 활성 여부 플래그
  - `stg_sample_data` 참조

#### `agg_product_summary`
- **목적**: 제품 요약 통계
- **Materialization**: View
- **기능**:
  - 가격대별, 상태별 집계
  - 통계 지표 (count, sum, avg, min, max)
  - `dim_products` 참조

## 🧪 테스트

모델에 대한 테스트는 `schema.yml`에 정의되어 있습니다:

- **고유성 테스트**: id, product_id
- **NULL 체크**: 필수 필드
- **값 검증**: status, price_category
- **범위 검증**: price >= 0

테스트 실행:
```bash
dbt test
```

## 🚀 실행 방법

### 전체 모델 실행
```bash
dbt run
```

### 특정 모델만 실행
```bash
dbt run --select stg_sample_data
dbt run --select marts
dbt run --select staging
```

### 태그로 실행
```bash
dbt run --select tag:staging
dbt run --select tag:marts
```

## 📊 결과 확인

모델 실행 후 데이터베이스에서 확인:

```sql
-- Staging 모델 확인
SELECT * FROM public.stg_sample_data;

-- Marts 모델 확인
SELECT * FROM public.dim_products;
SELECT * FROM public.agg_product_summary;
```

## 🔧 실제 데이터 사용하기

현재 모델은 테스트용 샘플 데이터를 사용합니다. 실제 데이터를 사용하려면:

1. `stg_sample_data.sql`에서 실제 테이블 참조:
```sql
select * from {{ source('raw', 'your_table') }}
```

2. `sources.yml` 파일 생성하여 소스 정의:
```yaml
version: 2

sources:
  - name: raw
    database: test_db
    schema: public
    tables:
      - name: your_table
```

