"""
원본 banb/cycle_features.py를 그대로 재현
사이클 경계 탐지 및 피크 계산
"""
import numpy as np
import pandas as pd
from scipy.signal import find_peaks

# 컬럼명 정의 (원본 config.py와 동일)
TIME_COL = "collection_timestamp"
MOTOR_COL = "motor_current"
TEMP_COL = "chamber_temperature"
MIXER_COL = "mixer_run"
RUN_MODE_COL = "run_mode"
PROCESS_STAGE_COL = "process_stage"
DROP_DOOR_COL = "drop_door_position"


def _find_cycle_boundaries(df: pd.DataFrame) -> tuple[list[pd.Timestamp], pd.Series, pd.Series]:
    """Detect start/end boundaries using door open and mixer run transitions.
    
    원본 로직 그대로 재현.
    """
    door = df[DROP_DOOR_COL].where(df[DROP_DOOR_COL].isin(["close", "open"]))
    door_ffill = door.ffill()
    open_after_close = (door_ffill == "open") & (door_ffill.shift() == "close")
    cycle_marks = df.loc[open_after_close, TIME_COL].dropna().sort_values().to_list()

    mixer = df[MIXER_COL].where(df[MIXER_COL].isin(["RUN", "STOP"]))
    mixer_prev = mixer.ffill().shift()
    run_starts = df.loc[(mixer == "RUN") & (mixer_prev.isin(["STOP", np.nan])), TIME_COL].dropna().sort_values()
    run_stops = df.loc[(mixer == "STOP") & (mixer_prev == "RUN"), TIME_COL].dropna().sort_values()

    start_edge = run_starts.iloc[0] if len(run_starts) else df[TIME_COL].min()
    if cycle_marks:
        stop_after_last = run_stops[run_stops > cycle_marks[-1]]
        end_edge = stop_after_last.iloc[0] if len(stop_after_last) else df[TIME_COL].max()
    else:
        end_edge = run_stops.iloc[0] if len(run_stops) else df[TIME_COL].max()

    boundaries = sorted([start_edge] + cycle_marks + [end_edge])
    return boundaries, run_starts, run_stops


def _mix_duration_seconds(seg: pd.DataFrame) -> float:
    """Calculate mix duration in seconds.
    
    원본 로직 그대로 재현.
    """
    stage = seg[PROCESS_STAGE_COL].where(seg[PROCESS_STAGE_COL].isin(["load", "mix"])).ffill()
    prev_stage = stage.shift()

    mix_start_mask = (stage == "mix") & ((prev_stage != "mix") | prev_stage.isna())
    mix_start_times = seg.loc[mix_start_mask, TIME_COL]
    mix_end_times = seg.loc[(prev_stage == "mix") & (stage == "load"), TIME_COL]

    mix_duration = pd.Timedelta(0)
    end_idx = 0
    for start in mix_start_times:
        while end_idx < len(mix_end_times) and mix_end_times.iloc[end_idx] <= start:
            end_idx += 1
        if end_idx < len(mix_end_times):
            end_time = mix_end_times.iloc[end_idx]
            end_idx += 1
        else:
            end_time = seg[TIME_COL].iloc[-1]
        mix_duration += end_time - start

    return mix_duration.total_seconds()


def _summarize_cycle(seg: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> dict | None:
    """Return a feature summary for one cycle window.
    
    원본 로직 그대로 재현.
    """
    motor = seg[MOTOR_COL].to_numpy()

    peaks, _ = find_peaks(
        np.nan_to_num(motor, nan=np.nanmedian(motor)),
        prominence=100,
        distance=40,
    )
    peak_count = len(peaks)

    threshold = seg[TIME_COL].min() + pd.Timedelta(seconds=30)
    max_temp = seg.loc[seg[TIME_COL] >= threshold, TEMP_COL].max()

    mix_duration_sec = _mix_duration_seconds(seg)
    if mix_duration_sec <= 60:
        return None

    run_mode_series = seg[RUN_MODE_COL].ffill().bfill()
    run_mode_start = run_mode_series.iloc[0] if len(run_mode_series) else None

    return {
        "start": start,
        "run_mode_start": run_mode_start,
        "end": end,
        "mix_duration_sec": round(mix_duration_sec, 1),
        "max_temp": max_temp,
        "peak_count": peak_count,
        "is_3_stage": (peak_count > 5) and (max_temp > 105),
    }


def compare_peak(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize cycles using peak count, duration, temperature.
    
    원본 로직 그대로 재현.
    """
    df = df.copy()
    df[TIME_COL] = pd.to_datetime(df[TIME_COL], errors="coerce")
    df[MOTOR_COL] = pd.to_numeric(df[MOTOR_COL], errors="coerce")

    boundaries, run_starts, _ = _find_cycle_boundaries(df)
    if len(boundaries) < 2:
        return pd.DataFrame()

    guided_cycles: list[dict] = []

    for i in range(len(boundaries) - 1):
        start, end = boundaries[i], boundaries[i + 1]
        if len(run_starts):
            seg_run_starts = run_starts[(run_starts >= start) & (run_starts < end)]
            if len(seg_run_starts):
                start = seg_run_starts.iloc[-1]
        seg = df[(df[TIME_COL] >= start) & (df[TIME_COL] < end)].copy().reset_index(drop=True)
        if seg.empty:
            continue

        summary = _summarize_cycle(seg, start, end)
        if summary:
            guided_cycles.append(summary)

    return pd.DataFrame(guided_cycles)

