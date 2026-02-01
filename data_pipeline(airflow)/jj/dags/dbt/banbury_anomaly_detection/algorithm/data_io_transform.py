"""
원본 banb/data_io_transform.py를 그대로 재현
PLC 데이터 로드 및 피벗 변환, 세그먼트 분할
"""
import numpy as np
import pandas as pd
from typing import List

# 컬럼명 정의 (원본 config.py와 동일)
TIME_COL = "collection_timestamp"
MOTOR_COL = "motor_current"
TEMP_COL = "chamber_temperature"
MIXER_COL = "mixer_run"
RUN_MODE_COL = "run_mode"
PROCESS_STAGE_COL = "process_stage"
DROP_DOOR_COL = "drop_door_position"


def get_plc_data_from_db(pg_helper, context, sql: str) -> pd.DataFrame:
    """Load PLC data from database (원본 get_plc_data와 동일한 로직).
    
    Args:
        pg_helper: PostgresHelper instance
        context: Airflow context (for ti)
        sql: SQL query string
    
    Returns:
        DataFrame with PLC data
    """
    records = pg_helper.execute_query(sql, task_id="get_plc_data", xcom_key=None, ti=context.get('ti'))
    
    if not records:
        return pd.DataFrame()
    
    # 원본 로직: 기본 컬럼명 사용 (실제 쿼리 결과와 일치)
    # 쿼리: plc_key, numeric_value, collection_timestamp, raw_value, boolean_value, data_quality_score
    columns = ['plc_key', 'numeric_value', 'collection_timestamp', 'raw_value', 'boolean_value', 'data_quality_score']
    
    df = pd.DataFrame(records, columns=columns[:len(records[0])] if records else columns)
    
    # 원본 로직: 정렬 및 불필요한 컬럼 제거
    if TIME_COL in df.columns:
        df = df.sort_values(TIME_COL)
    df = df.drop(columns=["raw_value", "boolean_value", "data_quality_score"], errors="ignore")
    df = df.reset_index(drop=True)
    
    return df


def convert_to_pivot(df_plc_data: pd.DataFrame) -> pd.DataFrame:
    """Pivot raw PLC rows to a single row per timestamp and normalize values.
    
    원본 로직 그대로 재현.
    """
    df_plc_data = df_plc_data.copy()

    df_plc_data[TIME_COL] = pd.to_datetime(df_plc_data[TIME_COL], errors="coerce")
    df_plc_data = df_plc_data.sort_values(TIME_COL).reset_index(drop=True)

    df_pivot = df_plc_data.pivot_table(
        index=TIME_COL,
        columns="plc_key",
        values="numeric_value",
        aggfunc="first",
    ).reset_index()

    df_pivot.loc[df_pivot["D256"] > 1000, "D256"] = pd.NA

    df_pivot["D540"] = df_pivot["D540"].map({0: "STOP", 1: "RUN"})
    df_pivot["D542"] = df_pivot["D542"].map({0: "manual", 1: "Semi", 2: "Auto"})
    df_pivot["D544"] = df_pivot["D544"].map({0: "load", 1: "mix", 2: "discharge"})
    df_pivot["D546"] = df_pivot["D546"].map({0: "open", 1: "close"})

    col_mapping = {
        "D207": MOTOR_COL,
        "D256": TEMP_COL,
        "D540": MIXER_COL,
        "D542": RUN_MODE_COL,
        "D544": PROCESS_STAGE_COL,
        "D546": DROP_DOOR_COL,
    }
    df = df_pivot.rename(columns=col_mapping).sort_values(TIME_COL).reset_index(drop=True)
    return df


def split_valid_segments(df: pd.DataFrame) -> List[pd.DataFrame]:
    """Split into production sets and drop all-idle segments.
    
    원본 로직 그대로 재현.
    """
    df = convert_to_pivot(df)

    gaps = df[TIME_COL].diff() >= pd.Timedelta(minutes=10)
    df["prod_set_id"] = gaps.cumsum()
    prod_sets = [g.copy() for _, g in df.groupby("prod_set_id")]

    filtered = []
    for g in prod_sets:
        vals = pd.to_numeric(g[MOTOR_COL], errors="coerce").dropna()
        if vals.empty:
            continue
        if (vals <= 50).all():
            continue
        filtered.append(g)

    if not filtered:
        raise ValueError("No usable production segments were found.")

    return filtered

