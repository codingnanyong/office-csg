"""
TensorFlow Model Loading Test DAG
=================================
CNN Anomaly Classifier 모델이 정상적으로 로딩되는지 확인하는 테스트 DAG

테스트 항목:
1. TensorFlow/Keras 임포트 확인
2. 모델 파일 존재 확인
3. 모델 로딩 테스트
4. 모델 정보 출력 (입력/출력 shape, 레이어 수 등)
5. 더미 데이터로 예측 테스트 (선택사항)

Schedule: 수동 실행 (테스트용)
"""

import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import tensorflow as tf
from tensorflow import keras
import numpy as np

# ════════════════════════════════════════════════════════════════
# Configuration
# ════════════════════════════════════════════════════════════════

MODEL_PATH = "/opt/airflow/models/cnn_anomaly_classifier.h5"

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# ════════════════════════════════════════════════════════════════
# Test Functions
# ════════════════════════════════════════════════════════════════

def test_tensorflow_import(**kwargs) -> dict:
    """TensorFlow/Keras 임포트 테스트"""
    logging.info("=" * 60)
    logging.info("1️⃣ TensorFlow/Keras 임포트 테스트")
    logging.info("=" * 60)
    
    try:
        logging.info(f"✅ TensorFlow 버전: {tf.__version__}")
        

        logging.info(f"✅ Keras 버전: {keras.__version__}")
        
        # GPU 사용 가능 여부 확인
        gpus = tf.config.list_physical_devices('GPU')
        if gpus:
            logging.info(f"✅ GPU 사용 가능: {len(gpus)}개")
            for i, gpu in enumerate(gpus):
                logging.info(f"   GPU {i}: {gpu.name}")
        else:
            logging.info("ℹ️ GPU 사용 불가 (CPU 모드)")
        
        return {
            "status": "success",
            "tensorflow_version": tf.__version__,
            "keras_version": keras.__version__,
            "gpu_available": len(gpus) > 0,
            "gpu_count": len(gpus)
        }
    except ImportError as e:
        error_msg = f"❌ TensorFlow/Keras 임포트 실패: {str(e)}"
        logging.error(error_msg)
        raise ImportError(error_msg) from e


def check_model_file(**kwargs) -> dict:
    """모델 파일 존재 및 접근 가능 여부 확인"""
    logging.info("=" * 60)
    logging.info("2️⃣ 모델 파일 확인")
    logging.info("=" * 60)
    
    if not os.path.exists(MODEL_PATH):
        error_msg = f"❌ 모델 파일이 존재하지 않습니다: {MODEL_PATH}"
        logging.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # 파일 정보
    file_size = os.path.getsize(MODEL_PATH)
    file_size_mb = file_size / (1024 * 1024)
    
    logging.info(f"✅ 모델 파일 존재: {MODEL_PATH}")
    logging.info(f"   파일 크기: {file_size_mb:.2f} MB ({file_size:,} bytes)")
    
    # 읽기 권한 확인
    if not os.access(MODEL_PATH, os.R_OK):
        error_msg = f"❌ 모델 파일 읽기 권한이 없습니다: {MODEL_PATH}"
        logging.error(error_msg)
        raise PermissionError(error_msg)
    
    logging.info("✅ 모델 파일 읽기 권한 확인 완료")
    
    return {
        "status": "success",
        "model_path": MODEL_PATH,
        "file_size_bytes": file_size,
        "file_size_mb": round(file_size_mb, 2),
        "readable": True
    }


def load_model(**kwargs) -> dict:
    """모델 로딩 테스트"""
    logging.info("=" * 60)
    logging.info("3️⃣ 모델 로딩 테스트")
    logging.info("=" * 60)
    
    try:
        
        logging.info(f"모델 로딩 시작: {MODEL_PATH}")
        model = keras.models.load_model(MODEL_PATH)
        logging.info("✅ 모델 로딩 성공!")
        
        # 모델 정보 출력
        logging.info("=" * 60)
        logging.info("📊 모델 정보")
        logging.info("=" * 60)
        
        # 모델 요약
        logging.info("모델 구조:")
        model.summary(print_fn=logging.info)
        
        # 입력/출력 shape
        if hasattr(model, 'input_shape'):
            logging.info(f"입력 Shape: {model.input_shape}")
        if hasattr(model, 'output_shape'):
            logging.info(f"출력 Shape: {model.output_shape}")
        
        # 레이어 정보
        logging.info(f"총 레이어 수: {len(model.layers)}")
        logging.info("레이어 목록:")
        for i, layer in enumerate(model.layers):
            layer_type = type(layer).__name__
            layer_config = layer.get_config() if hasattr(layer, 'get_config') else {}
            logging.info(f"  [{i+1}] {layer_type}: {layer.name}")
            if hasattr(layer, 'output_shape'):
                logging.info(f"      Output Shape: {layer.output_shape}")
        
        # 모델 컴파일 정보
        if hasattr(model, 'optimizer') and model.optimizer:
            logging.info(f"Optimizer: {type(model.optimizer).__name__}")
        if hasattr(model, 'loss'):
            logging.info(f"Loss: {model.loss}")
        if hasattr(model, 'metrics'):
            logging.info(f"Metrics: {model.metrics}")
        
        return {
            "status": "success",
            "model_loaded": True,
            "input_shape": str(model.input_shape) if hasattr(model, 'input_shape') else None,
            "output_shape": str(model.output_shape) if hasattr(model, 'output_shape') else None,
            "layer_count": len(model.layers),
            "layers": [type(layer).__name__ for layer in model.layers]
        }
        
    except Exception as e:
        error_msg = f"❌ 모델 로딩 실패: {str(e)}"
        logging.error(error_msg)
        logging.exception("상세 에러 정보:")
        raise Exception(error_msg) from e


def test_model_prediction(**kwargs) -> dict:
    """더미 데이터로 모델 예측 테스트"""
    logging.info("=" * 60)
    logging.info("4️⃣ 모델 예측 테스트 (더미 데이터)")
    logging.info("=" * 60)
    
    try:

        # 모델 로딩
        model = keras.models.load_model(MODEL_PATH)
        
        # 입력 shape 확인
        if not hasattr(model, 'input_shape') or not model.input_shape:
            logging.warning("⚠️ 모델의 입력 shape를 확인할 수 없어 예측 테스트를 건너뜁니다.")
            return {
                "status": "skipped",
                "reason": "input_shape not available"
            }
        
        # 입력 shape에서 None (배치 크기) 제거
        input_shape = model.input_shape[1:] if model.input_shape[0] is None else model.input_shape
        
        logging.info(f"입력 Shape (배치 제외): {input_shape}")
        
        # 더미 데이터 생성
        batch_size = 1
        dummy_input = np.random.randn(batch_size, *input_shape).astype(np.float32)
        
        logging.info(f"더미 입력 데이터 생성: shape={dummy_input.shape}")
        
        # 예측 실행
        logging.info("예측 실행 중...")
        prediction = model.predict(dummy_input, verbose=0)
        
        logging.info(f"✅ 예측 성공!")
        logging.info(f"예측 결과 Shape: {prediction.shape}")
        logging.info(f"예측 결과 (첫 번째 샘플): {prediction[0]}")
        
        # 예측 결과 통계
        if prediction.size > 0:
            logging.info(f"예측 값 범위: [{prediction.min():.6f}, {prediction.max():.6f}]")
            logging.info(f"예측 값 평균: {prediction.mean():.6f}")
            logging.info(f"예측 값 표준편차: {prediction.std():.6f}")
        
        return {
            "status": "success",
            "prediction_shape": list(prediction.shape),
            "prediction_sample": prediction[0].tolist() if prediction.size > 0 else None,
            "prediction_stats": {
                "min": float(prediction.min()) if prediction.size > 0 else None,
                "max": float(prediction.max()) if prediction.size > 0 else None,
                "mean": float(prediction.mean()) if prediction.size > 0 else None,
                "std": float(prediction.std()) if prediction.size > 0 else None,
            }
        }
        
    except Exception as e:
        error_msg = f"❌ 예측 테스트 실패: {str(e)}"
        logging.error(error_msg)
        logging.exception("상세 에러 정보:")
        raise Exception(error_msg) from e


# ════════════════════════════════════════════════════════════════
# DAG Definition
# ════════════════════════════════════════════════════════════════

with DAG(
    dag_id="test_tensorflow_model_loading",
    default_args=DEFAULT_ARGS,
    description="TensorFlow CNN Anomaly Classifier 모델 로딩 테스트",
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["ml", "tensorflow", "keras", "model", "test", "cnn", "anomaly"],
    max_active_runs=1,
) as dag:
    
    # 1. TensorFlow 임포트 테스트
    test_import = PythonOperator(
        task_id="test_tensorflow_import",
        python_callable=test_tensorflow_import,
    )
    
    # 2. 모델 파일 확인
    check_file = PythonOperator(
        task_id="check_model_file",
        python_callable=check_model_file,
    )
    
    # 3. 모델 로딩 테스트
    load_model_task = PythonOperator(
        task_id="load_model",
        python_callable=load_model,
    )
    
    # 4. 예측 테스트
    test_prediction = PythonOperator(
        task_id="test_model_prediction",
        python_callable=test_model_prediction,
    )
    
    # Task 의존성 설정
    test_import >> check_file >> load_model_task >> test_prediction

