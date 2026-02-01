# Airflow Models Directory

이 디렉토리는 Airflow DAG에서 사용하는 머신러닝 모델 파일을 저장하는 디렉토리입니다.

## 디렉토리 구조

```
models/
├── best_anomaly_transformer.pth  # Anomaly Transformer 모델 (PyTorch)
│                                  # 용도: IPI 플레이트 온도 이상치 탐지
│                                  # 사용 DAG: ipi_anomaly_transformer_*
│                                  # 프레임워크: PyTorch
├── cnn_anomaly_classifier.h5     # CNN Anomaly Classifier 모델 (TensorFlow/Keras)
│                                  # 용도: 이상치 분류 (테스트용)
│                                  # 사용 DAG: test_tensorflow_model_loading
│                                  # 프레임워크: TensorFlow/Keras
└── README.md                     # 이 파일
```

## 모델 파일 상세 정보

### 1. best_anomaly_transformer.pth
- **형식**: PyTorch 모델 파일 (.pth)
- **용도**: IPI 플레이트 온도 데이터의 이상치 탐지 및 제거
- **사용 DAG**: `ipi_anomaly_transformer_incremental`, `ipi_anomaly_transformer_backfill`
- **Airflow Variable**: `ipi_temperature_model_path` (기본값: `/opt/airflow/models/best_anomaly_transformer.pth`)
- **프레임워크**: PyTorch
- **입력**: 시계열 온도 데이터 (window_size=60)
- **출력**: 이상치 점수 및 정제된 데이터

### 2. cnn_anomaly_classifier.h5
- **형식**: TensorFlow/Keras 모델 파일 (.h5)
- **용도**: CNN 기반 이상치 분류 (테스트/검증용)
- **사용 DAG**: `test_tensorflow_model_loading`
- **프레임워크**: TensorFlow/Keras
- **설치 필요**: TensorFlow 2.15.x - 2.16.x (CPU 버전)

## 모델 파일 업로드 방법

### 1. 호스트에서 직접 복사
```bash
# PyTorch 모델 파일 복사
cp /path/to/your/model.pth ./models/best_anomaly_transformer.pth

# TensorFlow/Keras 모델 파일 복사
cp /path/to/your/model.h5 ./models/cnn_anomaly_classifier.h5

# 또는 심볼릭 링크 생성
ln -s /path/to/your/model.pth ./models/best_anomaly_transformer.pth
ln -s /path/to/your/model.h5 ./models/cnn_anomaly_classifier.h5
```

### 2. 컨테이너 내부에서 확인
```bash
# 모든 컨테이너에 모델이 마운트되어 있는지 확인
docker exec airflow-scheduler ls -lh /opt/airflow/models/
docker exec airflow-worker ls -lh /opt/airflow/models/

# 파일 존재 및 크기 확인
docker exec airflow-scheduler file /opt/airflow/models/*.pth
docker exec airflow-scheduler file /opt/airflow/models/*.h5
```

## Airflow Variable 설정

모델 경로를 변경하려면 Airflow Variable을 설정하세요:

```bash
# Anomaly Transformer 모델 경로 설정
docker exec airflow-webserver airflow variables set ipi_temperature_model_path /opt/airflow/models/best_anomaly_transformer.pth

# 또는 Airflow UI에서 설정
# Admin > Variables > ipi_temperature_model_path
```

**참고**: `cnn_anomaly_classifier.h5`는 현재 DAG에서 하드코딩된 경로를 사용합니다.

## 필수 라이브러리 설치

### PyTorch 모델 (best_anomaly_transformer.pth)
- **필수**: PyTorch 2.0.x - 2.9.x (CPU 버전), NumPy
- **설치 방법**: `scripts/install_pytorch_docker.sh` 실행
```bash
cd /home/user/apps/airflow
bash scripts/install_pytorch_docker.sh
```
- **설치 확인**: `docker exec airflow-worker python3 -c "import torch; print(torch.__version__)"`

### TensorFlow/Keras 모델 (cnn_anomaly_classifier.h5)
- **필수**: TensorFlow 2.15.x - 2.16.x (CPU 버전)
- **설치 방법**: `scripts/install_tensorflow_docker.sh` 실행
```bash
cd /home/user/apps/airflow
bash scripts/install_tensorflow_docker.sh
```
- **설치 확인**: `docker exec airflow-worker python3 -c "import tensorflow as tf; print(tf.__version__)"`

## 주의사항

1. **파일 크기**: 
   - `.pth` 파일: 보통 수백 MB ~ 수 GB
   - `.h5` 파일: 보통 수십 MB ~ 수백 MB
2. **버전 관리**: 큰 파일이므로 Git에 포함하지 마세요 (`.gitignore`에 추가)
3. **권한**: 컨테이너 내부에서 읽을 수 있도록 권한 확인
4. **프레임워크 호환성**: 
   - PyTorch 모델은 PyTorch 버전과 호환되어야 함
   - TensorFlow 모델은 TensorFlow 2.15.x - 2.16.x와 호환되어야 함

## 권한 설정

모델 파일이 올바르게 읽히도록 권한 설정:

```bash
# 호스트에서 (모든 모델 파일)
chmod 644 ./models/*.pth ./models/*.h5

# 또는 컨테이너 내부에서 (필요한 경우)
docker exec -u root airflow-scheduler chown airflow:airflow /opt/airflow/models/*.pth
docker exec -u root airflow-scheduler chown airflow:airflow /opt/airflow/models/*.h5
docker exec -u root airflow-scheduler chmod 644 /opt/airflow/models/*.pth
docker exec -u root airflow-scheduler chmod 644 /opt/airflow/models/*.h5
```

## 모델 테스트

### PyTorch 모델 테스트
```bash
# DAG 실행: ipi_anomaly_transformer_incremental
# 또는 Airflow UI에서 DAG 트리거
```

### TensorFlow/Keras 모델 테스트
```bash
# DAG 실행: test_tensorflow_model_loading
# 또는 Airflow UI에서 DAG 트리거
```

## 문제 해결

### 모델 파일을 찾을 수 없음
1. 파일이 `/opt/airflow/models/` 경로에 있는지 확인
2. 파일 권한 확인 (`ls -lh /opt/airflow/models/`)
3. 컨테이너 내부에서 직접 확인: `docker exec airflow-scheduler ls -lh /opt/airflow/models/`

### TensorFlow 모델 로딩 실패
1. TensorFlow 설치 확인: `docker exec airflow-worker python3 -c "import tensorflow as tf; print(tf.__version__)"`
2. TensorFlow 재설치: `bash scripts/install_tensorflow_docker.sh`
3. 모델 파일 형식 확인: `.h5` 파일이 올바른 Keras 모델인지 확인

### PyTorch 모델 로딩 실패
1. PyTorch 설치 확인: `docker exec airflow-worker python3 -c "import torch; print(torch.__version__)"`
2. PyTorch 재설치: `bash scripts/install_pytorch_docker.sh`
3. 모델 파일 형식 확인: `.pth` 파일이 올바른 PyTorch 모델인지 확인
4. NumPy 버전 확인: PyTorch와 호환되는 NumPy 버전인지 확인

