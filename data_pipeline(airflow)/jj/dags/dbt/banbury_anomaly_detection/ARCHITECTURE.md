# Banbury 이상 감지 시스템 아키텍처

## 개요

이 프로젝트는 Banbury 공정의 이상을 감지하기 위해 **dbt**와 **원본 Python 알고리즘**을 결합한 하이브리드 아키텍처를 사용합니다.

### 핵심 설계 원칙

1. **원본 로직 보존**: `/banb` 폴더의 원본 Python 로직을 그대로 재현
2. **dbt와 Python의 역할 분리**:
   - **dbt**: 데이터 전처리 및 스테이징 (staging, intermediate 모델)
   - **Python (algorithm)**: 핵심 알고리즘 로직 (사이클 탐지, 피크 계산, CNN 추론)
3. **데이터 흐름**: 원시 데이터 → dbt 전처리 → Python 알고리즘 → 결과 저장

---

## 아키텍처 다이어그램

```
┌─────────────────────────────────────────────────────────────────┐
│                    Airflow DAG                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. get_date_range (날짜 범위 계산)                              │
│     ↓                                                            │
│  2. prepare_dbt_vars (dbt 변수 준비)                            │
│     ↓                                                            │
│  3. dbt_task (dbt 모델 실행)                                    │
│     ├─ stg_banbury_plc_raw (증분 적재)                          │
│     ├─ mart_banbury_plc_pivot (피벗 변환)                        │
│     └─ banbury_segments (세그먼트 분할)                          │
│     ↓                                                            │
│  4. cnn_inference (Python 알고리즘 실행)                         │
│     ├─ algorithm/data_io_transform                              │
│     │  ├─ get_plc_data_from_db (원시 데이터 로드)               │
│     │  ├─ convert_to_pivot (피벗 변환)                           │
│     │  └─ split_valid_segments (세그먼트 분할)                  │
│     ├─ algorithm/cycle_features                                  │
│     │  └─ compare_peak (사이클 경계 탐지, 피크 계산)             │
│     ├─ algorithm/segments                                        │
│     │  ├─ build_plc_segments (PLC 세그먼트 생성)                 │
│     │  └─ build_cycle_matrix (사이클 행렬 생성)                  │
│     └─ algorithm/inference                                      │
│        ├─ predict_cycles_cnn (CNN 추론)                          │
│        └─ convert_prob_to_result (결과 변환)                      │
│     ↓                                                            │
│  5. update_variable (진행 상태 저장)                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 디렉토리 구조

```
banbury_anomaly_detection/
├── dag.py                    # Airflow DAG 정의
├── algorithm/               # 원본 Python 로직 모듈
│   ├── __init__.py
│   ├── data_io_transform.py # 데이터 로드 및 전처리
│   ├── cycle_features.py     # 사이클 경계 탐지 및 피크 계산
│   ├── segments.py           # PLC 세그먼트 생성 및 행렬 변환
│   └── inference.py          # CNN 모델 추론
└── models/                   # dbt 모델
    ├── staging/
    │   └── stg_banbury_plc_raw.sql
    ├── marts/
    │   └── mart_banbury_plc_pivot.sql
    ├── intermediate/
    │   ├── banbury_segments.sql
    │   ├── banbury_cycles.sql (enabled=false)
    │   └── banbury_plc_segments.sql (enabled=false)
    └── result/
        └── banbury_anomaly_result.sql
```

---

## dbt 모델 구조

### 1. Staging Layer

#### `stg_banbury_plc_raw`
- **목적**: 원시 PLC 데이터 증분 적재
- **소스**: `bronze.os_banb_plc_raw_data`
- **기능**:
  - Airflow Variable 기반 증분 처리
  - PLC 키 필터링 (`D207`, `D256`, `D540`, `D542`, `D544`, `D546`)
  - 중복 제거

### 2. Marts Layer

#### `mart_banbury_plc_pivot`
- **목적**: 원시 데이터를 피벗 형식으로 변환
- **기능**:
  - PLC 키를 컬럼으로 변환
  - 값 정규화 (D540: 0/1 → STOP/RUN, D542: 0/1/2 → manual/Semi/Auto 등)
  - 밀리초 정밀도 유지

### 3. Intermediate Layer

#### `banbury_segments`
- **목적**: 생산 세그먼트 분할 및 필터링
- **기능**:
  - 10분 이상 간격으로 세그먼트 분할
  - 유휴 세그먼트 제거 (motor <= 50)
  - 원본 `split_valid_segments` 로직과 동일

#### `banbury_cycles` (enabled=false)
- **상태**: 비활성화 (Python에서 직접 처리)
- **이유**: 원본 Python 로직(`cycle_features.compare_peak`)을 사용하여 정확도 보장

#### `banbury_plc_segments` (enabled=false)
- **상태**: 비활성화 (Python에서 직접 처리)
- **이유**: 원본 Python 로직(`segments.build_plc_segments`)을 사용

### 4. Result Layer

#### `banbury_anomaly_result`
- **목적**: 최종 이상 감지 결과 테이블 스키마 정의
- **기능**: Python에서 직접 데이터 삽입

---

## Algorithm 모듈 구조

### `data_io_transform.py`

원본 `banb/data_io_transform.py`를 그대로 재현.

#### 주요 함수

1. **`get_plc_data_from_db(pg_helper, context, sql)`**
   - 데이터베이스에서 원시 PLC 데이터 로드
   - 원본 `get_plc_data()`와 동일한 로직

2. **`convert_to_pivot(df_plc_data)`**
   - 원시 데이터를 피벗 형식으로 변환
   - PLC 키를 컬럼으로 변환
   - 값 정규화 (D540, D542, D544, D546)

3. **`split_valid_segments(df)`**
   - 생산 세그먼트 분할
   - 10분 이상 간격으로 분할
   - 유휴 세그먼트 제거

### `cycle_features.py`

원본 `banb/cycle_features.py`를 그대로 재현.

#### 주요 함수

1. **`_find_cycle_boundaries(df)`**
   - 문 상태 전환 (close → open)으로 사이클 경계 탐지
   - 믹서 시작/정지 이벤트 감지

2. **`_mix_duration_seconds(seg)`**
   - 믹스 지속 시간 계산
   - 여러 번의 mix 구간 합산

3. **`_summarize_cycle(seg, start, end)`**
   - 사이클 요약 정보 생성
   - 피크 개수 계산 (`scipy.signal.find_peaks`)
   - 최대 온도, 믹스 지속 시간 등

4. **`compare_peak(df)`**
   - 세그먼트별 사이클 경계 탐지 및 요약
   - `mix_duration_sec > 60` 필터링

### `segments.py`

원본 `banb/segments.py`를 그대로 재현.

#### 주요 함수

1. **`build_plc_segments(df_filtered, df_result, ...)`**
   - 사이클 윈도우에 맞춰 PLC 신호 슬라이싱
   - 타임존 일치 처리
   - Forward/Backward fill

2. **`build_cycle_matrix(df_cycles, ...)`**
   - 각 사이클을 고정 길이(500 포인트)로 리샘플링
   - `np.interp`를 사용한 보간
   - Motor와 Temperature를 결합한 행렬 생성

### `inference.py`

원본 `banb/inference.py`를 그대로 재현.

#### 주요 함수

1. **`reshape_for_cnn(X)`**
   - CNN 모델 입력 형태로 변환
   - (n_cycles, 1000) → (n_cycles, 500, 2)

2. **`predict_cycles_cnn(model, X_array)`**
   - CNN 모델 추론 실행
   - 이상 확률 반환

3. **`convert_prob_to_result(val, threshold)`**
   - 확률을 이상 여부로 변환
   - 기본 임계값: 0.1

---

## 데이터 흐름

### 1. 데이터 로드 단계

```python
# 원시 PLC 데이터 로드 (bronze.os_banb_plc_raw_data)
sql = _build_plc_data_query(start_date, end_date)
df_plc_data = data_io_transform.get_plc_data_from_db(pg, context, sql)
```

### 2. 전처리 단계

```python
# 세그먼트 분할
filtered_sets = data_io_transform.split_valid_segments(df_plc_data)
df_filtered = pd.concat(filtered_sets).reset_index(drop=True)
```

### 3. 사이클 계산 단계

```python
# 각 세그먼트별로 사이클 계산
for segment in filtered_sets:
    df_cycle = cycle_features.compare_peak(segment)
    # peak_count, is_3_stage 등 계산
```

### 4. PLC 세그먼트 생성 단계

```python
# 사이클 윈도우에 맞춰 PLC 신호 슬라이싱
df_plc_seg = segments.build_plc_segments(df_filtered, df_result)
```

### 5. CNN 추론 단계

```python
# 사이클 행렬 생성
X = segments.build_cycle_matrix(df_plc_seg)

# CNN 모델 추론
prob = inference.predict_cycles_cnn(cnn_model, X)

# 결과 변환
is_anomaly = [inference.convert_prob_to_result(p) for p in prob]
```

### 6. 결과 저장 단계

```python
# 결과를 gold.banbury_anomaly_result에 저장
pg.insert_data(
    schema_name="gold",
    table_name="banbury_anomaly_result",
    data=insert_data,
    ...
)
```

---

## 주요 설계 결정 사항

### 1. 왜 dbt와 Python을 함께 사용하는가?

- **dbt의 장점**: SQL 기반 데이터 전처리, 증분 처리, 재사용성
- **Python의 장점**: 복잡한 알고리즘 구현 (피크 탐지, CNN 추론)
- **하이브리드 접근**: 각 도구의 장점을 활용

### 2. 왜 일부 dbt 모델을 비활성화했는가?

- `banbury_cycles`: 원본 Python 로직(`cycle_features.compare_peak`)을 사용하여 정확도 보장
- `banbury_plc_segments`: 원본 Python 로직(`segments.build_plc_segments`)을 사용하여 일관성 유지
- SQL로 복잡한 알고리즘을 정확히 재현하기 어려움

### 3. 데이터 흐름 최적화

- **이전**: dbt 모델 → Python (테이블 읽기)
- **현재**: 원시 데이터 → Python (처음부터 실행)
- **이유**: dbt 모델과 Python 로직 간 불일치 방지, 정확도 향상

---

## 핵심 개선 사항

### 1. 원본 로직 보존

- `/banb` 폴더의 원본 로직을 `algorithm/` 모듈로 분리
- 함수별로 독립적인 모듈 구성
- 원본 로직과 100% 동일한 결과 보장

### 2. 코드 모듈화

- `data_io_transform` → `cycle_features` → `segments` → `inference` 순서 유지
- 각 모듈이 단일 책임 원칙 준수
- 테스트 및 유지보수 용이

### 3. 불필요한 코드 제거

- 사용되지 않는 헬퍼 함수 제거 (약 238줄)
- 중복 로직 제거
- 코드 가독성 향상

### 4. 에러 처리 개선

- 타임존 처리 개선
- dtype 변환 처리 (`Decimal` → `float`)
- 명확한 에러 메시지

---

## 실행 흐름

### Incremental DAG

```
get_date_range
  → prepare_dbt_vars
    → dbt_task (stg_banbury_plc_raw, mart_banbury_plc_pivot, banbury_segments)
      → cnn_inference (Python 알고리즘 전체 실행)
        → update_variable
```

### Backfill DAG

```
get_backfill_date_range
  → prepare_backfill_vars
    → dbt_backfill_task
      → cnn_inference_backfill
        → update_backfill_variable
```

---

## 주요 설정

### 상수 정의

```python
# CNN 모델 설정
MODEL_PATH = "/opt/airflow/models/cnn_anomaly_classifier.h5"
ANOMALY_THRESHOLD = 0.1
SEQUENCE_LENGTH = 500

# 데이터베이스 설정
POSTGRES_CONN_ID = "pg_jj_telemetry_dw"
SCHEMA = "silver"
```

### 컬럼명 정의

```python
TIME_COL = "collection_timestamp"
MOTOR_COL = "motor_current"
TEMP_COL = "chamber_temperature"
MIXER_COL = "mixer_run"
RUN_MODE_COL = "run_mode"
PROCESS_STAGE_COL = "process_stage"
DROP_DOOR_COL = "drop_door_position"
```

---

## 결과 테이블 스키마

### `gold.banbury_anomaly_result`

```sql
CREATE TABLE gold.banbury_anomaly_result (
    prod_set_id INTEGER NOT NULL,
    cycle_id INTEGER NOT NULL,
    cycle_start TIMESTAMPTZ NOT NULL,
    cycle_end TIMESTAMPTZ NOT NULL,
    run_mode_start VARCHAR(10),
    mix_duration_sec NUMERIC(10, 1) NOT NULL,
    max_temp NUMERIC(10, 2),
    peak_count INTEGER NOT NULL,
    is_3_stage BOOLEAN NOT NULL,
    anomaly_prob NUMERIC(10, 6) NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    etl_extract_time TIMESTAMPTZ NOT NULL,
    etl_ingest_time TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_banbury_anomaly_result PRIMARY KEY (prod_set_id, cycle_id, cycle_start)
);
```

---

## 참고 사항

1. **원본 로직 위치**: `/opt/airflow/banb/`
2. **dbt 프로젝트 위치**: `/opt/airflow/dags/dbt/banbury_anomaly_detection/`
3. **모델 파일 위치**: `/opt/airflow/models/cnn_anomaly_classifier.h5`
4. **증분 처리**: Airflow Variable `last_extract_time_banbury_anomaly_detection` 사용

---

## 향후 개선 방향

1. **테스트 코드 추가**: 각 algorithm 모듈에 대한 단위 테스트
2. **로깅 개선**: 구조화된 로깅 (JSON 형식)
3. **모니터링**: Prometheus 메트릭 수집
4. **에러 복구**: 실패한 사이클 재처리 로직

