# IPI Temperature Anomaly Detection - Model Setup Guide

## 모델 파일 설정 방법

`ipi_temperature_anomaly_detection` DAG를 실행하기 위해서는 학습된 Anomaly Transformer 모델 파일이 필요합니다.

## 1. 모델 파일 위치

기본 경로: `/opt/airflow/models/best_anomaly_transformer.pth`

## 2. 모델 파일 설정 방법

### 방법 1: Airflow Variable 사용 (권장)

Airflow UI에서 다음 Variable을 설정:

**Variable Key**: `ipi_temperature_model_path`  
**Variable Value**: `/opt/airflow/models/best_anomaly_transformer.pth` (또는 실제 모델 경로)

또는 CLI로 설정:
```bash
docker exec airflow-webserver airflow variables set ipi_temperature_model_path /opt/airflow/models/best_anomaly_transformer.pth
```

### 방법 2: 모델 파일 직접 업로드

학습된 모델 파일을 컨테이너로 복사:

```bash
# 모든 Airflow 컨테이너에 모델 디렉토리 생성 및 파일 복사
cd /home/user/apps/airflow

# 모델 디렉토리 생성
for container in $(docker ps --format "{{.Names}}" | grep -E "^airflow-(scheduler|webserver|triggerer|airflow-worker-)"); do
  docker exec -u root "$container" mkdir -p /opt/airflow/models
done

# 모델 파일 복사 (호스트의 모델 파일 경로를 YOUR_MODEL_FILE.pth로 변경)
YOUR_MODEL_FILE="/path/to/your/best_anomaly_transformer.pth"
for container in $(docker ps --format "{{.Names}}" | grep -E "^airflow-(scheduler|webserver|triggerer|airflow-worker-)"); do
  docker cp "$YOUR_MODEL_FILE" "${container}:/opt/airflow/models/best_anomaly_transformer.pth"
  docker exec -u root "$container" chown airflow:airflow /opt/airflow/models/best_anomaly_transformer.pth
done
```

### 방법 3: Docker Volume Mount

`docker-compose.yml`에 volume 추가:

```yaml
volumes:
  - ./models:/opt/airflow/models
```

그리고 호스트의 `./models/best_anomaly_transformer.pth` 파일을 준비합니다.

## 3. 초기화된 모델로 테스트 (비권장)

⚠️ **주의**: 학습되지 않은 초기화 모델은 부정확한 결과를 생성할 수 있습니다.

테스트 목적으로만 사용하려면:

```bash
docker exec airflow-webserver airflow variables set ipi_temperature_allow_uninitialized_model true
```

이 경우 모델 파일이 없어도 초기화된 모델로 실행되지만, 이상치 탐지 결과가 부정확할 수 있습니다.

## 4. 모델 학습 (필요한 경우)

모델을 학습하려면:

1. Anomaly Transformer 학습 스크립트 실행
2. 학습된 모델을 `.pth` 파일로 저장
3. 위 방법 중 하나로 모델 파일을 Airflow 컨테이너에 복사

## 5. 확인

모델 파일이 올바르게 설정되었는지 확인:

```bash
# 컨테이너에서 확인
docker exec airflow-scheduler ls -lh /opt/airflow/models/best_anomaly_transformer.pth

# Airflow Variable 확인
docker exec airflow-webserver airflow variables get ipi_temperature_model_path
```

## 6. 문제 해결

### 모델 파일을 찾을 수 없음

**에러**: `FileNotFoundError: 모델 파일을 찾을 수 없습니다`

**해결**:
1. 모델 파일이 올바른 경로에 있는지 확인
2. Airflow Variable `ipi_temperature_model_path` 설정 확인
3. 모든 컨테이너(worker, scheduler 등)에 모델 파일이 복사되었는지 확인

### AnomalyTransformer 모듈을 찾을 수 없음

**에러**: `ImportError: AnomalyTransformer 모듈을 찾을 수 없습니다`

**해결**:
1. `/opt/airflow/Anomaly-Transformer-main` 디렉토리가 존재하는지 확인
2. `INSTALL_ANOMALY_TRANSFORMER.md` 가이드 참고하여 모듈 설치

