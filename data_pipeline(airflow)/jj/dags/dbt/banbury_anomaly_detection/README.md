# 🔍 Banbury 공정 이상 감지 알고리즘 (dbt 프로젝트)

[![dbt](https://img.shields.io/badge/dbt-Core-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![TensorFlow](https://img.shields.io/badge/TensorFlow-Keras-FF6F00?logo=tensorflow&logoColor=white)](https://www.tensorflow.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![CNN](https://img.shields.io/badge/CNN-Model-FF6B6B?logo=deep-learning&logoColor=white)](https://en.wikipedia.org/wiki/Convolutional_neural_network)

이 프로젝트는 Banbury 공정의 이상 감지를 위한 dbt 프로젝트입니다. CNN 모델을 사용하여 PLC 데이터에서 이상 패턴을 탐지합니다.

## 📁 프로젝트 구조

```
banbury_anomaly_detection/
├── dag.py                  # Airflow DAG (Incremental & Backfill)
├── dbt_project.yml         # dbt 프로젝트 설정
├── profiles.yml            # 데이터베이스 연결 설정
├── packages.yml            # 패키지 의존성
├── models/                 # SQL 모델 파일
│   ├── staging/            # 스테이징 모델
│   ├── marts/              # 마트 모델
│   └── intermediate/       # 중간 처리 모델
├── macros/                 # 매크로 함수
├── tests/                  # 테스트 파일
├── seeds/                  # 시드 데이터
├── analyses/               # 분석 파일
├── snapshots/              # 스냅샷 파일
└── README.md               # 이 파일
```

## 🚀 사용 방법

### 로컬 개발 환경

1. **패키지 설치**:
   ```bash
   cd airflow/dags/dbt/banbury_anomaly_detection
   dbt deps
   ```

2. **모델 실행**:
   ```bash
   dbt run
   ```

3. **테스트 실행**:
   ```bash
   dbt test
   ```

### Airflow에서 실행

- **증분 처리**: `dbt_banbury_anomaly_detection_incremental` DAG (매일 07:00 자동 실행)
- **백필 처리**: `dbt_banbury_anomaly_detection_backfill` DAG (수동 실행)

## 📊 데이터 소스

### 데이터베이스 연결
- **Connection ID**: `pg_jj_telemetry_dw`
- **Database**: `telemetry`
- **Source Schema**: `bronze`
- **Target Schema**: `silver` (dbt 모델), `gold` (CNN 결과)

### 사용 테이블
- `bronze.os_banb_plc_raw_data`: Banbury PLC 원시 데이터
  - **필터링 조건**:
    - `collection_timestamp >= (current_date - interval '1 day' + time '06:30')`
    - `collection_timestamp < (current_date + time '06:30')`
    - `plc_key in ('D207', 'D256', 'D540', 'D542', 'D544', 'D546')`

### PLC 키 매핑
- **D207**: Motor (모터 전류)
- **D256**: Temperature (온도, 1000 초과 시 NULL)
- **D540**: Mixer (0=STOP, 1=RUN)
- **D542**: Run Mode (0=manual, 1=Semi, 2=Auto)
- **D544**: Process Stage (0=load, 1=mix, 2=discharge)
- **D546**: Drop Door (0=open, 1=close)

## 🤖 CNN 모델

- **모델 경로**: `/opt/airflow/models/cnn_anomaly_classifier.h5`
- **모델 타입**: 1D Convolutional Neural Network
- **입력 형식**: 
  - 시퀀스 길이: 500 포인트
  - 채널 수: 2 (motor, temperature)
  - Shape: `(n_cycles, 500, 2)`
- **임계값**: 0.1 (이하일 경우 이상으로 판단)
- **출력**: 이상 확률 (0.0 ~ 1.0)

## ⚙️ 증분 처리 (Incremental)

dbt는 `incremental` materialization을 사용하여 증분 처리를 지원합니다.

### 📅 Incremental DAG
- **DAG ID**: `dbt_banbury_anomaly_detection_incremental`
- **스케줄**: 매일 07:00 UTC (06:30 데이터 처리)
- **동작 방식**:
  1. Airflow Variable `last_extract_time_banbury_anomaly_detection`에서 마지막 처리 시간 읽기
  2. 마지막 처리 시간의 다음 날 06:30부터 **하루씩** 처리
  3. **최대 제한**: 오늘 날짜 - 1일까지만 처리 (안전 마진)
  4. 처리 완료 후 Variable 업데이트

### 🔄 Backfill DAG
- **DAG ID**: `dbt_banbury_anomaly_detection_backfill`
- **스케줄**: 수동 실행
- **동작 방식**:
  1. Airflow Variable에서 마지막 처리 시간 읽기 (없으면 2025-01-01 06:30:00부터 시작)
  2. **2025-01-01 06:30:00**부터 **오늘 날짜 - 2일 06:30:00**까지 자동 처리
  3. 처리 완료 후 Variable 업데이트
- **주의**: Variable이 없으면 초기 날짜(2025-01-01)부터 시작

### 📝 dbt 변수
- `start_date`: 증분 처리 시작 날짜 (Airflow Variable에서 자동 계산)
- `end_date`: 증분 처리 종료 날짜 (Airflow Variable에서 자동 계산)
- `backfill_start_date`: 백필 시작 날짜 (DAG Run Config에서 지정)
- `backfill_end_date`: 백필 종료 날짜 (DAG Run Config에서 지정)

### 🔧 증분 처리 전략
- **Materialization**: `incremental`
- **Strategy**: `merge` (중복 방지)
- **Unique Key**: `collection_timestamp || '_' || plc_key`

## 🔬 알고리즘 로직

### 1. 데이터 스테이징 (`stg_banbury_plc_raw`)
- Bronze 레이어의 `os_banb_plc_raw_data`에서 PLC 원시 데이터 추출
- 필터링: 지정된 PLC 키 (D207, D256, D540, D542, D544, D546)만 선택
- 불필요한 컬럼 제거 (raw_value, boolean_value, data_quality_score)
- 시간 범위 필터링: 전일 06:30 ~ 당일 06:30

### 2. 피벗 변환 (`mart_banbury_plc_pivot`)
Python의 `convert_to_pivot` 함수를 SQL로 구현:
- **피벗 변환**: PLC 키를 컬럼으로 변환 (각 timestamp당 하나의 행)
- **값 정규화**:
  - **D207 → motor**: 그대로 사용
  - **D256 → temperature**: 1000 초과 시 NULL
  - **D540 → mixer**: 0=STOP, 1=RUN
  - **D542 → run_mode**: 0=manual, 1=Semi, 2=Auto
  - **D544 → process_stage**: 0=load, 1=mix, 2=discharge
  - **D546 → drop_door**: 0=open, 1=close

### 3. 세그먼트 분할 (`anomaly_banbury_segments`)
Python의 `split_valid_segments` 함수를 SQL로 구현:
- **세그먼트 분할**: 10분 이상 시간 간격으로 세그먼트 구분
- **유휴 세그먼트 제거**:
  - Motor 값이 없는 세그먼트 제거
  - Motor 값이 모두 50 이하인 세그먼트 제거 (유휴 상태)
- **결과**: 유효한 생산 세그먼트만 반환

### 4. 사이클 경계 탐지 및 요약 (`anomaly_banbury_cycles`)
Python의 `compare_peak` 함수를 SQL로 구현:
- **사이클 경계 찾기** (`_find_cycle_boundaries`):
  - 문 상태 전환: close → open 감지
  - 믹서 시작/정지 이벤트 감지
  - 사이클 경계 배열 생성
- **믹스 지속 시간 계산** (`_mix_duration_seconds`):
  - 프로세스 스테이지 전환 감지 (load → mix, mix → load)
  - 믹스 구간별 지속 시간 합계
- **피크 탐지** (`find_peaks`):
  - Motor 값 정규화 (NaN을 median으로 대체)
  - prominence >= 100, distance >= 40 조건으로 피크 탐지
- **사이클 요약** (`_summarize_cycle`):
  - 시작 시 실행 모드
  - 믹스 지속 시간 (초)
  - 최대 온도 (30초 이후)
  - 피크 개수
  - 3단계 공정 여부 (peak_count > 5 and max_temp > 105)
- **필터링**: mix_duration_sec > 60인 사이클만 반환

### 5. PLC 사이클별 신호 세그먼트 (`anomaly_banbury_plc_segments`)
Python의 `build_plc_segments` 함수를 SQL로 구현:
- **사이클 윈도우 슬라이싱**: 각 사이클의 start/end 시간에 맞춰 PLC 신호 추출
- **NaN 값 제거**: motor와 temperature가 모두 null인 행 제거
- **Forward/Backward Fill**: 사이클별로 motor와 temperature의 null 값을 채움
- **결과**: CNN 모델 입력을 위한 사이클별 시계열 데이터

### 6. 사이클 행렬 변환 및 CNN 추론 (`build_cycle_matrix` + `run_cnn_inference`)
**참고**: 이 단계는 Python으로 처리됩니다 (Airflow PythonOperator).
- `anomaly_banbury_plc_segments` 모델의 데이터를 읽어서:
  - 각 사이클을 고정 길이(500 포인트)로 리샘플링 (NumPy interpolation)
  - motor와 temperature를 concatenate하여 행렬 생성
  - CNN 모델 입력 형식으로 변환: `(n_cycles, 500, 2)`
- CNN 모델 추론 실행
- 이상 확률 계산 및 결과 저장 (`gold.banbury_anomaly_result`)

## 📈 모델 의존성

```
stg_banbury_plc_raw (staging)
    ↓
mart_banbury_plc_pivot (marts)
    ↓
anomaly_banbury_segments (anomaly)
    ↓
anomaly_banbury_cycles (anomaly)
    ↓
anomaly_banbury_plc_segments (anomaly)
    ↓
[Python 처리: build_cycle_matrix]
    ↓
[CNN 모델 추론]
    ↓
gold.banbury_anomaly_result
```

## 📋 결과 테이블

### `gold.banbury_anomaly_result`
CNN 모델 추론 결과가 저장되는 테이블:
- `prod_set_id`: 생산 세트 ID
- `cycle_id`: 사이클 ID
- `cycle_start`: 사이클 시작 시간
- `cycle_end`: 사이클 종료 시간
- `run_mode_start`: 시작 시 실행 모드
- `mix_duration_sec`: 믹스 지속 시간 (초)
- `max_temp`: 최대 온도
- `peak_count`: 피크 개수
- `is_3_stage`: 3단계 공정 여부
- `anomaly_prob`: 이상 확률 (0.0 ~ 1.0)
- `is_anomaly`: 이상 여부 (anomaly_prob < 0.1)
- `etl_extract_time`: 추출 시간
- `etl_ingest_time`: 적재 시간

## 🔗 관련 문서

- [dbt 프로젝트 디렉토리 README](../README.md)
- [Airflow 메인 README](../../../README.md)
- [dbt 공식 문서](https://docs.getdbt.com/)
- [Cosmos (Airflow dbt 통합)](https://astronomer.github.io/astronomer-cosmos/)
