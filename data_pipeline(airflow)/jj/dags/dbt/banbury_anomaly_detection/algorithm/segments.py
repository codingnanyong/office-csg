"""
원본 banb/segments.py를 그대로 재현
PLC 세그먼트 슬라이싱 및 사이클 행렬 생성
"""
import numpy as np
import pandas as pd

# 컬럼명 정의 (원본 config.py와 동일)
CYCLE_ID_COL = "idx"
MOTOR_COL = "motor_current"
TEMP_COL = "chamber_temperature"
TIME_COL = "collection_timestamp"
SEQUENCE_LENGTH = 500


def build_plc_segments(
    df_filtered: pd.DataFrame,
    df_result: pd.DataFrame,
    current_col: str = MOTOR_COL,
    temp_col: str = TEMP_COL,
    cycle_col: str = CYCLE_ID_COL,
) -> pd.DataFrame:
    """Slice PLC signals to the cycle windows in df_result.
    
    원본 로직 그대로 재현.
    """
    cols = [TIME_COL, current_col, temp_col]
    segments: list[pd.DataFrame] = []

    # 타임존 일치: df_filtered의 TIME_COL과 df_result의 start/end 타임존 맞추기
    filtered_tz_aware = pd.api.types.is_datetime64tz_dtype(df_filtered[TIME_COL])
    result_tz_aware = pd.api.types.is_datetime64tz_dtype(df_result["start"])
    
    if not filtered_tz_aware and result_tz_aware:
        # df_filtered가 tz-naive이고 df_result가 tz-aware인 경우
        df_filtered = df_filtered.copy()
        result_tz = df_result["start"].dt.tz
        df_filtered[TIME_COL] = pd.to_datetime(df_filtered[TIME_COL]).dt.tz_localize(result_tz)
    elif filtered_tz_aware and not result_tz_aware:
        # df_filtered가 tz-aware이고 df_result가 tz-naive인 경우
        df_result = df_result.copy()
        df_result["start"] = pd.to_datetime(df_result["start"]).dt.tz_localize(None)
        df_result["end"] = pd.to_datetime(df_result["end"]).dt.tz_localize(None)
    elif filtered_tz_aware and result_tz_aware:
        # 둘 다 tz-aware인 경우, 타임존이 다르면 변환
        filtered_tz = df_filtered[TIME_COL].dt.tz
        result_tz = df_result["start"].dt.tz
        if filtered_tz != result_tz:
            df_filtered = df_filtered.copy()
            df_filtered[TIME_COL] = df_filtered[TIME_COL].dt.tz_convert(result_tz)

    for idx, row in df_result.iterrows():
        window_mask = (df_filtered[TIME_COL] >= row["start"]) & (df_filtered[TIME_COL] <= row["end"])
        sliced = df_filtered.loc[window_mask, cols].copy()
        if sliced.empty:
            continue
        sliced[cycle_col] = idx
        segments.append(sliced)

    if not segments:
        return pd.DataFrame(columns=[cycle_col] + cols)

    df_plc = pd.concat(segments).reset_index(drop=True)
    df_plc = df_plc[[cycle_col] + cols]

    mask = df_plc[current_col].isna() & df_plc[temp_col].isna()
    df_plc = df_plc.loc[~mask].reset_index(drop=True)
    df_plc[current_col] = df_plc.groupby(cycle_col)[current_col].transform(lambda s: s.ffill().bfill())
    df_plc[temp_col] = df_plc.groupby(cycle_col)[temp_col].transform(lambda s: s.ffill().bfill())
    return df_plc


def build_cycle_matrix(
    df_cycles: pd.DataFrame,
    idx_col: str = CYCLE_ID_COL,
    current_col: str = MOTOR_COL,
    temp_col: str = TEMP_COL,
    n_points: int = SEQUENCE_LENGTH,
) -> np.ndarray:
    """Resample each cycle to a fixed length and stack current/temp features.
    
    원본 로직 그대로 재현 (banb/segments.py와 동일).
    """
    X = []
    for _, df_cycle in df_cycles.groupby(idx_col):
        # 데이터베이스에서 가져온 Decimal 타입을 float64로 변환 (np.interp 요구사항)
        # pd.to_numeric은 기본적으로 float64를 반환하지만, 명시적으로 지정하여 안전성 확보
        y1 = pd.to_numeric(df_cycle[current_col], errors='coerce').to_numpy(dtype=np.float64)
        y2 = pd.to_numeric(df_cycle[temp_col], errors='coerce').to_numpy(dtype=np.float64)
        if len(y1) == 0 or len(y2) == 0:
            continue
        x_old = np.linspace(0, 1, len(y1))
        x_new = np.linspace(0, 1, n_points)
        current_new = np.interp(x_new, x_old, y1)
        temp_new = np.interp(x_new, x_old, y2)
        X.append(np.concatenate([current_new, temp_new]))
    return np.array(X)

