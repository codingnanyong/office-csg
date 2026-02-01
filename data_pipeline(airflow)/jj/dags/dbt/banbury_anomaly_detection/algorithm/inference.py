"""
원본 banb/inference.py를 그대로 재현
CNN 추론 관련 함수
"""
import numpy as np

# 상수 정의 (원본 config.py와 동일)
ANOMALY_THRESHOLD = 0.1
NUM_CHANNELS = 2
SEQUENCE_LENGTH = 500


def reshape_for_cnn(X: np.ndarray) -> np.ndarray:
    """Reshape 2-channel sequences for 1D CNN input.
    
    원본 로직 그대로 재현.
    """
    if X is None or len(X) == 0:
        return np.empty((0, SEQUENCE_LENGTH, NUM_CHANNELS))
    return np.stack([X[:, :SEQUENCE_LENGTH], X[:, SEQUENCE_LENGTH : SEQUENCE_LENGTH * 2]], axis=-1)


def predict_cycles_cnn(model, X_array: np.ndarray) -> np.ndarray:
    """Run CNN inference and return anomaly probabilities.
    
    원본 로직 그대로 재현.
    """
    if X_array is None or len(X_array) == 0:
        return np.array([])
    X_seq = reshape_for_cnn(X_array)
    return model.predict(X_seq, verbose=0)[:, 0]


def convert_prob_to_result(val: float, threshold: float = ANOMALY_THRESHOLD) -> bool:
    """Convert probability to anomaly result.
    
    원본 로직 그대로 재현.
    """
    return val < threshold

