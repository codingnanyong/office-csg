# Banbury Anomaly Detection - 데이터 저장 위치 매핑

## 데이터베이스 및 스키마
- **Database**: `telemetry`
- **Schema**: `silver` (dbt 모델 저장 위치)
- **Connection**: `pg_jj_telemetry_dw`

## dbt 모델 저장 위치

### 1. Staging 레이어 (silver 스키마)
| 모델명 | 파일 경로 | Materialization | 저장 위치 | 설명 |
|--------|----------|----------------|-----------|------|
| `stg_banbury_plc_raw` | `models/staging/stg_banbury_plc_raw.sql` | **incremental** (table) | `silver.stg_banbury_plc_raw` | PLC 원시 데이터 필터링 및 전처리 |

**제공된 코드 매핑**: `get_plc_data()` 함수 결과

### 2. Marts 레이어 (silver 스키마)
| 모델명 | 파일 경로 | Materialization | 저장 위치 | 설명 |
|--------|----------|----------------|-----------|------|
| `mart_banbury_plc_pivot` | `models/marts/mart_banbury_plc_pivot.sql` | **incremental** (table) | `silver.mart_banbury_plc_pivot` | PLC 데이터 피벗 변환 및 값 정규화 |

**제공된 코드 매핑**: `convert_to_pivot()` 함수 결과

### 3. Anomaly 레이어 (silver 스키마)
| 모델명 | 파일 경로 | Materialization | 저장 위치 | 설명 |
|--------|----------|----------------|-----------|------|
| `anomaly_banbury_segments` | `models/anomaly/anomaly_banbury_segments.sql` | **table** | `silver.anomaly_banbury_segments` | 유효한 생산 세그먼트 분할 |
| `anomaly_banbury_cycles` | `models/anomaly/anomaly_banbury_cycles.sql` | **table** | `silver.anomaly_banbury_cycles` | 사이클 경계 탐지 및 요약 |
| `anomaly_banbury_plc_segments` | `models/anomaly/anomaly_banbury_plc_segments.sql` | **table** | `silver.anomaly_banbury_plc_segments` | 사이클별 PLC 신호 세그먼트 |

**제공된 코드 매핑**:
- `anomaly_banbury_segments`: `split_valid_segments()` 함수 결과
- `anomaly_banbury_cycles`: `compare_peak()` 함수 결과
- `anomaly_banbury_plc_segments`: `build_plc_segments()` 함수 결과

## Python 처리 결과 저장 위치

### CNN 추론 결과 (Gold 레이어)
| 결과 데이터 | 처리 위치 | 저장 위치 | 설명 |
|------------|----------|-----------|------|
| CNN 추론 결과 | DAG의 `run_cnn_inference` 함수 | **`gold.banbury_anomaly_detection_result`** | 사이클별 이상 확률 및 이상 여부 (최종 분석 결과) |

**제공된 코드 매핑**: `run.py`의 `main()` 함수 최종 결과 (`df_pred`)

**저장 컬럼**:
- 사이클 정보: `prod_set_id`, `cycle_id`, `cycle_start`, `cycle_end`
- 사이클 특성: `run_mode_start`, `mix_duration_sec`, `max_temp`, `peak_count`, `is_3_stage`
- CNN 추론 결과: `anomaly_prob`, `is_anomaly`
- ETL 메타데이터: `etl_extract_time`, `etl_ingest_time`

**Primary Key**: `(prod_set_id, cycle_id, cycle_start)`

**레이어 구분**:
- **Bronze**: 원본 데이터 (`bronze.os_banb_plc_raw_data`)
- **Silver**: 정제/변환된 데이터 (dbt 모델들 - `silver.stg_*`, `silver.mart_*`, `silver.anomaly_*`)
- **Gold**: 분석/비즈니스 결과 (CNN 추론 결과 - `gold.banbury_anomaly_detection_result`)

## 데이터 흐름

```
bronze.os_banb_plc_raw_data (원본)
    ↓ [dbt: stg_banbury_plc_raw]
silver.stg_banbury_plc_raw (incremental table)
    ↓ [dbt: mart_banbury_plc_pivot]
silver.mart_banbury_plc_pivot (incremental table)
    ↓ [dbt: anomaly_banbury_segments]
silver.anomaly_banbury_segments (table)
    ↓ [dbt: anomaly_banbury_cycles]
silver.anomaly_banbury_cycles (table)
    ↓ [dbt: anomaly_banbury_plc_segments]
silver.anomaly_banbury_plc_segments (table)
    ↓ [Python: build_cycle_matrix + CNN 추론]
gold.banbury_anomaly_detection_result (table) ← CNN 추론 결과 저장 (Gold 레이어)
```

## 모델별 상세 정보

### Incremental 모델 (증분 처리)
- `stg_banbury_plc_raw`: Airflow Variable 기반 증분 처리
- `mart_banbury_plc_pivot`: 증분 처리 지원

### Table 모델 (전체 재생성)
- `anomaly_banbury_segments`: 매 실행 시 전체 재생성
- `anomaly_banbury_cycles`: 매 실행 시 전체 재생성
- `anomaly_banbury_plc_segments`: 매 실행 시 전체 재생성

## 참고사항

1. **스키마**: 
   - **Bronze**: 원본 데이터 (`bronze.os_banb_plc_raw_data`)
   - **Silver**: dbt 모델들은 `silver` 스키마에 저장 (dbt 프로필 설정)
   - **Gold**: CNN 추론 결과는 `gold` 스키마에 저장
2. **데이터베이스**: `telemetry` 데이터베이스 사용
3. **CNN 추론 결과**: `gold.banbury_anomaly_detection_result` 테이블에 저장
   - Primary Key: `(prod_set_id, cycle_id, cycle_start)`
   - 중복 시 업데이트 (ON CONFLICT)
4. **레이어 구조**:
   - Bronze → Silver (dbt 모델들) → Gold (CNN 추론 결과)

