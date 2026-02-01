"""공통 함수 모듈 - IPI Temperature Matching"""
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple, List, Dict, Optional
from plugins.hooks.postgres_hook import PostgresHelper


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

QUALITY_POSTGRES_CONN_ID = "pg_jj_quality_dw"
GOOD_PRODUCT_SCHEMA = "silver"
GOOD_PRODUCT_TABLE = "ipi_good_product"
OSND_CROSS_VALIDATED_SCHEMA = "silver"
OSND_CROSS_VALIDATED_TABLE = "ipi_defective_cross_validated"
TEMPERATURE_SCHEMA = "silver"
TEMPERATURE_TABLE = "ipi_anomaly_transformer_result"
DEFECT_CODE_SCHEMA = "silver"
DEFECT_CODE_TABLE = "ip_defect_code"
TARGET_SCHEMA = "gold"
TARGET_TABLE = "ipi_temperature_matching"
TARGET_DETAIL_TABLE = "ipi_temperature_matching_detail"
TEMPERATURE_LOOKBACK_MINUTES = 7
ALLOWED_REASON_CDS = ['good', 'Burning', 'Sink mark']

# Machine Configuration
# 처리할 machine_no 리스트 (ipi_anomaly_transformer_common.py와 동일하게 설정)
# 예: MACHINE_NO_LIST = ["MCA34", "MCA20", "MCA37"]  # 여러 개 처리
MACHINE_NO_LIST = ["MCA34"]  # 현재 MCA34만 처리 (온도 데이터와 일치시켜야 함)


# ════════════════════════════════════════════════════════════════
# 2️⃣ Data Extraction
# ════════════════════════════════════════════════════════════════

def extract_good_product_data(pg: PostgresHelper, start_date: str, end_date: str) -> pd.DataFrame:
    """양품 데이터 추출 (머신 필터링 적용)"""
    logging.info(f"1️⃣ 양품 데이터 추출: {start_date} ~ {end_date}")
    
    # 머신 필터링 조건 추가
    if MACHINE_NO_LIST and len(MACHINE_NO_LIST) > 0:
        machine_list_str = ','.join([f"'{mno}'" for mno in MACHINE_NO_LIST])
        machine_filter = f"AND mc_cd IN ({machine_list_str})"
        logging.info(f"🏭 머신 필터링 적용: {', '.join(MACHINE_NO_LIST)}")
    else:
        machine_filter = ""
        logging.warning("⚠️ 머신 필터링이 설정되지 않았습니다. 모든 머신 데이터를 사용합니다.")
    
    sql = f"""SELECT so_id, rst_ymd, mc_cd, st_num, st_side, mold_id
              FROM {GOOD_PRODUCT_SCHEMA}.{GOOD_PRODUCT_TABLE}
              WHERE DATE(rst_ymd) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
              {machine_filter}"""
    data = pg.execute_query(sql, task_id="extract_good_product", xcom_key=None)
    df = pd.DataFrame(data, columns=['so_id', 'rst_ymd', 'mc_cd', 'st_num', 'st_side', 'mold_id']) if data else pd.DataFrame(columns=['so_id', 'rst_ymd', 'mc_cd', 'st_num', 'st_side', 'mold_id'])
    df.columns = df.columns.str.lower()
    logging.info(f"✅ 양품 데이터 추출 완료: {len(df):,} rows")
    return df


def extract_osnd_data(pg: PostgresHelper, start_date: str, end_date: str) -> pd.DataFrame:
    """OSND 데이터 추출 (머신 필터링 적용)"""
    logging.info(f"2️⃣ OSND 데이터 추출: {start_date} ~ {end_date}")
    col_sql = f"""SELECT column_name FROM information_schema.columns
                 WHERE table_schema = '{OSND_CROSS_VALIDATED_SCHEMA}' AND table_name = '{OSND_CROSS_VALIDATED_TABLE}'
                 ORDER BY ordinal_position"""
    columns = pg.execute_query(col_sql, task_id="get_osnd_columns", xcom_key=None)
    column_names = [col[0].lower() for col in columns] if columns else []
    
    # 머신 필터링 조건 추가
    if MACHINE_NO_LIST and len(MACHINE_NO_LIST) > 0:
        machine_list_str = ','.join([f"'{mno}'" for mno in MACHINE_NO_LIST])
        machine_filter = f"AND machine_cd IN ({machine_list_str})"
        logging.info(f"🏭 머신 필터링 적용: {', '.join(MACHINE_NO_LIST)}")
    else:
        machine_filter = ""
        logging.warning("⚠️ 머신 필터링이 설정되지 않았습니다. 모든 머신 데이터를 사용합니다.")
    
    sql = f"""SELECT * FROM {OSND_CROSS_VALIDATED_SCHEMA}.{OSND_CROSS_VALIDATED_TABLE}
              WHERE DATE(osnd_dt) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
              {machine_filter}"""
    data = pg.execute_query(sql, task_id="extract_osnd_data", xcom_key=None)
    df = pd.DataFrame(data, columns=column_names) if data else pd.DataFrame(columns=column_names)
    logging.info(f"✅ OSND 데이터 추출 완료: {len(df):,} rows")
    return df


def extract_temperature_data(pg: PostgresHelper, start_date: str, end_date: str) -> pd.DataFrame:
    """온도 데이터 추출 (시간 범위 및 machine_code 기준)"""
    logging.info(f"3️⃣ 온도 데이터 추출: {start_date} ~ {end_date}")
    start_dt = datetime.strptime(start_date, '%Y-%m-%d') - timedelta(minutes=TEMPERATURE_LOOKBACK_MINUTES)
    end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
    
    # machine_code 필터링 조건 추가
    if MACHINE_NO_LIST and len(MACHINE_NO_LIST) > 0:
        machine_list_str = ','.join([f"'{mno}'" for mno in MACHINE_NO_LIST])
        machine_filter = f"AND machine_code IN ({machine_list_str})"
        logging.info(f"🏭 머신 필터링 적용: {', '.join(MACHINE_NO_LIST)}")
    else:
        machine_filter = ""
        logging.warning("⚠️ 머신 필터링이 설정되지 않았습니다. 모든 머신 데이터를 사용합니다.")
    
    # 시간 범위 및 machine_code로 필터링
    sql = f"""SELECT machine_code, mc, prop, measurement_time, temperature
              FROM {TEMPERATURE_SCHEMA}.{TEMPERATURE_TABLE}
              WHERE measurement_time BETWEEN TIMESTAMP '{start_dt}' AND TIMESTAMP '{end_dt}'
              {machine_filter}"""
    data = pg.execute_query(sql, task_id="extract_temperature", xcom_key=None)
    df = pd.DataFrame(data, columns=['machine_code', 'mc', 'prop', 'measurement_time', 'temperature']) if data else pd.DataFrame(columns=['machine_code', 'mc', 'prop', 'measurement_time', 'temperature'])
    df.columns = df.columns.str.lower()
    logging.info(f"✅ 온도 데이터 추출 완료: {len(df):,} rows")
    return df


def extract_defect_code_data(pg: PostgresHelper) -> pd.DataFrame:
    """불량 코드 마스터 추출"""
    logging.info("4️⃣ 불량 코드 마스터 추출 중...")
    sql = f"SELECT defect_cd, defect_name FROM {DEFECT_CODE_SCHEMA}.{DEFECT_CODE_TABLE}"
    data = pg.execute_query(sql, task_id="extract_defect_code", xcom_key=None)
    df = pd.DataFrame(data, columns=['defect_cd', 'defect_name']) if data else pd.DataFrame(columns=['defect_cd', 'defect_name'])
    df.columns = df.columns.str.lower()
    logging.info(f"✅ 불량 코드 마스터 추출 완료: {len(df):,} rows")
    return df


# ════════════════════════════════════════════════════════════════
# 3️⃣ Data Transformation
# ════════════════════════════════════════════════════════════════

def transform_good_to_osnd_format(df_good: pd.DataFrame) -> pd.DataFrame:
    """양품 데이터를 OSND 스키마로 변환"""
    if len(df_good) == 0:
        return pd.DataFrame()
    osnd_cols = ["osnd_id", "osnd_dt", "machine_cd", "station", "station_rl", "mold_id", "reason_cd", "size_cd", "lr_cd", "osnd_bt_qty"]
    df = df_good.rename(columns={"so_id": "osnd_id", "rst_ymd": "osnd_dt", "mc_cd": "machine_cd", "st_num": "station", "st_side": "station_rl"}).copy()
    df["reason_cd"], df["size_cd"], df["lr_cd"], df["osnd_bt_qty"] = "good", None, None, None
    df = df[osnd_cols].copy()
    df["osnd_dt"], df["station"] = pd.to_datetime(df["osnd_dt"], errors="coerce"), df["station"].astype(str)
    logging.info(f"✅ 양품 데이터 변환 완료: {len(df):,} rows")
    return df


def merge_osnd_data(df_osnd: pd.DataFrame, df_good_as_osnd: pd.DataFrame) -> pd.DataFrame:
    """OSND 데이터와 양품 데이터 통합"""
    osnd_cols = ["osnd_id", "osnd_dt", "machine_cd", "station", "station_rl", "mold_id", "reason_cd", "size_cd", "lr_cd", "osnd_bt_qty"]
    if len(df_good_as_osnd) > 0:
        df_osnd_sel = df_osnd[osnd_cols].copy() if all(col in df_osnd.columns for col in osnd_cols) else pd.DataFrame(columns=osnd_cols)
        if len(df_osnd_sel) > 0:
            df_osnd_sel["osnd_dt"], df_osnd_sel["station"] = pd.to_datetime(df_osnd_sel["osnd_dt"], errors="coerce"), df_osnd_sel["station"].astype(str)
        df_result = pd.concat([df_osnd_sel, df_good_as_osnd], ignore_index=True)
    else:
        df_result = df_osnd[osnd_cols].copy() if all(col in df_osnd.columns for col in osnd_cols) else pd.DataFrame(columns=osnd_cols)
        if len(df_result) > 0:
            df_result["osnd_dt"], df_result["station"] = pd.to_datetime(df_result["osnd_dt"], errors="coerce"), df_result["station"].astype(str)
    df_result = df_result.sort_values("osnd_dt", ascending=True).reset_index(drop=True)
    df_result["station"] = df_result["station"].astype(int)
    logging.info(f"✅ OSND 데이터 통합 완료: {len(df_result):,} rows")
    return df_result


def map_defect_codes(df_osnd: pd.DataFrame, df_defect_code: pd.DataFrame) -> pd.DataFrame:
    """불량 코드 매핑"""
    if len(df_defect_code) == 0:
        return df_osnd
    defect_dict = df_defect_code.set_index('defect_cd')['defect_name'].to_dict()
    df_osnd['reason_cd'] = df_osnd['reason_cd'].map(defect_dict).fillna(df_osnd['reason_cd'])
    df_filtered = df_osnd[df_osnd['reason_cd'].isin(ALLOWED_REASON_CDS)].reset_index(drop=True)
    logging.info(f"✅ 불량 코드 매핑 완료: {len(df_filtered):,} rows")
    return df_filtered


def match_temperature_data(df_osnd: pd.DataFrame, df_temp: pd.DataFrame) -> List[Dict]:
    """온도 데이터 매칭"""
    if len(df_temp) == 0:
        return []
    df_temp['mc_prop'] = df_temp['mc'] + '_' + df_temp['prop']
    df_temp['measurement_time'] = pd.to_datetime(df_temp['measurement_time'], errors="coerce")
    df_osnd['osnd_dt'] = pd.to_datetime(df_osnd['osnd_dt'], errors="coerce")
    df_osnd['station'], df_osnd['station_rl'] = df_osnd['station'].astype(int), df_osnd['station_rl'].astype(str)
    df_valid = df_osnd[df_osnd['osnd_dt'].notna() & df_osnd['station'].notna() & df_osnd['station_rl'].notna()].copy()
    if len(df_valid) == 0:
        return []
    df_valid['time_start'] = df_valid['osnd_dt'] - pd.Timedelta(minutes=TEMPERATURE_LOOKBACK_MINUTES)
    # station_rl을 대문자로 변환하여 일관성 유지 (원래 코드와 동일하게)
    df_valid['station_rl_upper'] = df_valid['station_rl'].str.upper()
    df_valid['mc_prop_L'] = 'st_' + df_valid['station'].astype(str) + '_Plate Temperature L' + df_valid['station_rl_upper']
    df_valid['mc_prop_U'] = 'st_' + df_valid['station'].astype(str) + '_Plate Temperature U' + df_valid['station_rl_upper']
    temp_dict = {mc_prop: df_temp[df_temp['mc_prop'] == mc_prop][['measurement_time', 'temperature']].sort_values('measurement_time') 
                 for mc_prop in df_temp['mc_prop'].unique()}
    
    # 디버깅: 매칭 가능한 mc_prop 목록 로깅
    available_mc_props = set(temp_dict.keys())
    logging.info(f"📊 사용 가능한 온도 센서 mc_prop 개수: {len(available_mc_props)}")
    if len(available_mc_props) > 0:
        sample_props = list(available_mc_props)[:5]
        logging.info(f"📊 샘플 mc_prop: {sample_props}")
    
    combined_rows = []
    matched_count = 0
    unmatched_count = 0
    for idx, row in df_valid.iterrows():
        try:
            osnd_time, time_start = row['osnd_dt'], row['time_start']
            mc_prop_L = row['mc_prop_L']
            mc_prop_U = row['mc_prop_U']
            
            # temp_dict에서 직접 조회 (더 안전한 방식)
            temp_low_df = temp_dict.get(mc_prop_L, pd.DataFrame())
            temp_upper_df = temp_dict.get(mc_prop_U, pd.DataFrame())
            
            # 시간 범위 필터링
            if len(temp_low_df) > 0:
                temp_low_filtered = temp_low_df[
                    (temp_low_df['measurement_time'] >= time_start) & 
                    (temp_low_df['measurement_time'] <= osnd_time)
                ]
                temp_low_list = temp_low_filtered.to_dict('records')
            else:
                temp_low_list = []
                if idx < 10:  # 처음 10개만 로깅
                    logging.debug(f"⚠️ 매칭 실패 (L): {mc_prop_L} (station={row['station']}, station_rl={row['station_rl']})")
            
            if len(temp_upper_df) > 0:
                temp_upper_filtered = temp_upper_df[
                    (temp_upper_df['measurement_time'] >= time_start) & 
                    (temp_upper_df['measurement_time'] <= osnd_time)
                ]
                temp_upper_list = temp_upper_filtered.to_dict('records')
            else:
                temp_upper_list = []
                if idx < 10:  # 처음 10개만 로깅
                    logging.debug(f"⚠️ 매칭 실패 (U): {mc_prop_U} (station={row['station']}, station_rl={row['station_rl']})")
            
            if len(temp_low_list) > 0 or len(temp_upper_list) > 0:
                matched_count += 1
            else:
                unmatched_count += 1
            combined_row = row.drop(['time_start', 'mc_prop_L', 'mc_prop_U', 'station_rl_upper']).to_dict()
            combined_row['temp_L'], combined_row['temp_U'] = temp_low_list, temp_upper_list
            combined_rows.append(combined_row)
        except Exception as e:
            logging.warning(f"❗ Index {idx} 오류: {e}")
            unmatched_count += 1
    
    logging.info(f"✅ 온도 데이터 매칭 완료: {len(combined_rows):,} rows")
    logging.info(f"📊 매칭 성공: {matched_count:,} rows, 매칭 실패: {unmatched_count:,} rows")
    if unmatched_count > 0:
        match_ratio = matched_count / (matched_count + unmatched_count) * 100
        logging.warning(f"⚠️ 매칭 성공률: {match_ratio:.2f}% ({matched_count}/{matched_count + unmatched_count})")
    
    return combined_rows


# ════════════════════════════════════════════════════════════════
# 4️⃣ Data Loading
# ════════════════════════════════════════════════════════════════

def calculate_temperature_stats(temp_list: List[Dict]) -> Tuple[int, Optional[float], Optional[float], Optional[float]]:
    """온도 통계 계산"""
    if not temp_list:
        return 0, None, None, None
    temps = [float(item.get('temperature', 0)) for item in temp_list if item.get('temperature') is not None]
    return (len(temps), float(np.mean(temps)), float(np.min(temps)), float(np.max(temps))) if temps else (0, None, None, None)


def load_data(pg: PostgresHelper, combined_rows: List[Dict], extract_time: datetime) -> dict:
    """Gold 테이블에 데이터 적재"""
    if len(combined_rows) == 0:
        return {"main_rows": 0, "detail_rows": 0}
    main_data = [(
        row.get('osnd_id'), row.get('osnd_dt'), row.get('machine_cd'), row.get('station'), row.get('station_rl'),
        row.get('mold_id'), row.get('reason_cd'), row.get('size_cd'), row.get('lr_cd'), row.get('osnd_bt_qty'),
        *calculate_temperature_stats(row.get('temp_L', [])), *calculate_temperature_stats(row.get('temp_U', [])), extract_time
    ) for row in combined_rows]
    main_cols = ["osnd_id", "osnd_dt", "machine_cd", "station", "station_rl", "mold_id", "reason_cd", "size_cd", "lr_cd", "osnd_bt_qty",
                 "temp_L_count", "temp_L_avg", "temp_L_min", "temp_L_max", "temp_U_count", "temp_U_avg", "temp_U_min", "temp_U_max", "etl_extract_time"]
    logging.info(f"📦 메인 테이블 적재: {len(main_data):,} rows")
    pg.insert_data(TARGET_SCHEMA, TARGET_TABLE, main_data, main_cols, ["osnd_id", "osnd_dt"], chunk_size=1000)
    detail_data = []
    for row in combined_rows:
        for temp_list, temp_type in [(row.get('temp_L', []), 'L'), (row.get('temp_U', []), 'U')]:
            for seq_no, item in enumerate(sorted(temp_list, key=lambda x: x.get('measurement_time')), 1):
                detail_data.append((row.get('osnd_id'), row.get('osnd_dt'), temp_type, pd.to_datetime(item.get('measurement_time')), float(item.get('temperature', 0)), seq_no, extract_time))
    if detail_data:
        detail_cols = ["osnd_id", "osnd_dt", "temp_type", "measurement_time", "temperature", "seq_no", "etl_extract_time"]
        logging.info(f"📦 상세 테이블 적재: {len(detail_data):,} rows")
        pg.insert_data(TARGET_SCHEMA, TARGET_DETAIL_TABLE, detail_data, detail_cols, ["osnd_id", "osnd_dt", "temp_type", "measurement_time"], chunk_size=1000)
    return {"main_rows": len(main_data), "detail_rows": len(detail_data)}


# ════════════════════════════════════════════════════════════════
# 5️⃣ Main ETL Logic
# ════════════════════════════════════════════════════════════════

def process_single_date(target_date: str) -> dict:
    """단일 날짜 전체 처리"""
    extract_time = datetime.utcnow()
    pg = PostgresHelper(conn_id=QUALITY_POSTGRES_CONN_ID)
    try:
        df_good = extract_good_product_data(pg, target_date, target_date)
        df_osnd = extract_osnd_data(pg, target_date, target_date)
        df_temp = extract_temperature_data(pg, target_date, target_date)
        df_defect = extract_defect_code_data(pg)
        df_osnd_mapped = map_defect_codes(merge_osnd_data(df_osnd, transform_good_to_osnd_format(df_good)), df_defect)
        if len(df_osnd_mapped) == 0:
            return {"status": "success", "rows_processed": 0, "rows_inserted": 0, "processed_date": target_date}
        combined_rows = match_temperature_data(df_osnd_mapped, df_temp)
        if len(combined_rows) == 0:
            return {"status": "success", "rows_processed": 0, "rows_inserted": 0, "processed_date": target_date}
        load_result = load_data(pg, combined_rows, extract_time)
        logging.info(f"✅ [{target_date}] 완료: {len(combined_rows):,} rows, 메인={load_result['main_rows']:,}, 상세={load_result['detail_rows']:,}")
        return {"status": "success", "rows_processed": len(combined_rows), "main_rows": load_result['main_rows'], "detail_rows": load_result['detail_rows'], "processed_date": target_date}
    except Exception as e:
        logging.error(f"❌ [{target_date}] 실패: {e}", exc_info=True)
        return {"status": "failed", "error": str(e), "processed_date": target_date}

