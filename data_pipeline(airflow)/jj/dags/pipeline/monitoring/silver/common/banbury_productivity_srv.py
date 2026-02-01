"""공통 함수 모듈 - MSBP_ROLL Shift Summary (교대별 Roll 계획/실적 집계)"""
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.mysql_hook import MySQLHelper

# ════════════════════════════════════════════════════════════════
	# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Source Configuration (추출)
SOURCE_ORACLE_CONN_ID = "orc_jj_gmes"  # 추출용 Oracle 연결

# Target Configuration (적재)
TARGET_TABLE = "banbury_productivity"  # 테이블명은 나중에 정리해서 알려줄 것
TARGET_SCHEMA = "ccs_rtf"

# 적재 대상 MySQL 연결 목록
TARGET_MYSQL_CONNECTIONS = [
    {"conn_id": "maria_jj_os_banb_3", "enabled": True},  # 현재 활성화
]

# 백필 설정
INITIAL_START_DATE = "20240101"  # 초기 시작 날짜 (YYYYMMDD 형식)

# ════════════════════════════════════════════════════════════════
	# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════

def parse_date(date_str: str) -> datetime:
    """YYYYMMDD 형식의 날짜 문자열을 datetime으로 변환"""
    return datetime.strptime(date_str, '%Y%m%d')


def format_date(dt: datetime) -> str:
    """datetime을 YYYYMMDD 형식의 문자열로 변환"""
    return dt.strftime('%Y%m%d')


def get_realtime_date_range() -> dict | None:
    """실시간 처리용 날짜 범위 계산 (인도네시아 시간 기준, 오전 6시 기준 날짜 변경)
    
    교대별 데이터 처리:
    - 1교대: DATE_1 (전일) - 1교대는 전일부터 시작
    - 2/3교대: DATE_2 (당일) - 2/3교대는 당일
    
    날짜 기준:
    - 오전 6시 이전: 전일 기준으로 처리
    - 오전 6시 이후: 당일 기준으로 처리
    - 시간대: 인도네시아 시간 (Asia/Jakarta, UTC+7) - 반드시 인도네시아 시간 기준으로 계산해야 함
    
    예시:
    - 현재 시간 (인도네시아): 2026-01-08 05:30 → DATE_1=20260106, DATE_2=20260107, process_date=20260107 (전일 기준)
    - 현재 시간 (인도네시아): 2026-01-08 06:30 → DATE_1=20260107, DATE_2=20260108, process_date=20260108 (당일 기준)
    
    주의: UTC로 계산하면 인도네시아와 7시간 차이로 날짜가 잘못 계산될 수 있음
    
    Returns:
        Dictionary with 'v_p_date_1', 'v_p_date_2', and 'process_date' strings (YYYYMMDD), or None if no data to process
    """
    # 인도네시아 시간대 (WIB - Western Indonesian Time, UTC+7)
    # 반드시 인도네시아 시간 기준으로 계산해야 함 (UTC로 계산하면 날짜가 잘못될 수 있음)
    indonesia_tz = ZoneInfo("Asia/Jakarta")
    now = datetime.now(indonesia_tz)
    current_hour = now.hour
    
    # 오전 6시 이전이면 전일 기준, 6시 이후면 당일 기준
    if current_hour < 6:
        # 오전 6시 이전: 전일 기준 처리
        base_date = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = base_date - timedelta(days=1)  # 전전일
        today = base_date  # 전일
    else:
        # 오전 6시 이후: 당일 기준 처리
        base_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = base_date - timedelta(days=1)  # 전일
        today = base_date  # 당일
    
    # DATE_1은 전일(1교대용), DATE_2는 당일(2/3교대용)
    # process_date는 UI의 "Working Date"와 동일 (DATE_2)
    # timezone 정보 제거 (날짜만 사용)
    v_p_date_1 = format_date(yesterday.replace(tzinfo=None))
    v_p_date_2 = format_date(today.replace(tzinfo=None))
    process_date_str = format_date(today.replace(tzinfo=None))
    
    time_status = "오전 6시 이전 (전일 기준)" if current_hour < 6 else "오전 6시 이후 (당일 기준)"
    logging.info(f"📅 실시간 처리 날짜 (인도네시아 시간 기준, {time_status}): 현재 시간={now.strftime('%Y-%m-%d %H:%M %Z')}, DATE_1={v_p_date_1} (1교대), DATE_2={v_p_date_2} (2/3교대), process_date={process_date_str}")
    
    return {
        "v_p_date_1": v_p_date_1,
        "v_p_date_2": v_p_date_2,
        "process_date": process_date_str
    }


def get_backfill_date_range() -> dict | None:
    """백필 처리용 날짜 범위 계산 (인도네시아 시간 기준)
    
    종료 날짜: 오늘 -1일 (전일까지)
    Variable 없이 매번 INITIAL_START_DATE부터 전일까지 모든 날짜를 처리
    시간대: 인도네시아 시간 (Asia/Jakarta, UTC+7)
    
    처리 로직:
    - 각 날짜(DATE_2)에 대해 DATE_1은 전일, DATE_2는 당일로 설정
    - 예: 2024-01-01 처리 시 → DATE_1 = 2023-12-31, DATE_2 = 2024-01-01
    - 예: 2024-01-02 처리 시 → DATE_1 = 2024-01-01, DATE_2 = 2024-01-02
    
    Returns:
        Dictionary with 'date_pairs' (list of dicts with 'date_1', 'date_2', 'process_date'), 
        'backfill_start_date', 'backfill_end_date', or None if no data to process
    """
    # INITIAL_START_DATE부터 시작
    start_date = parse_date(INITIAL_START_DATE)
    
    # 종료 날짜: 오늘 -1일 (전일까지) - 인도네시아 시간 기준
    indonesia_tz = ZoneInfo("Asia/Jakarta")
    now = datetime.now(indonesia_tz)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # timezone 정보 제거 후 계산
    today_naive = today.replace(tzinfo=None)
    end_date = today_naive - timedelta(days=1)
    
    if start_date > end_date:
        logging.info(f"⚠️ 처리할 데이터 없음: {format_date(start_date)} > {format_date(end_date)}")
        return None
    
    logging.info(f"📅 백필 처리 날짜 범위: 시작={format_date(start_date)}, 종료={format_date(end_date)} (전일까지, Variable 없이 매번 전체 처리)")
    
    # 여러 날짜를 처리해야 하므로, 시작일부터 종료일까지의 모든 날짜 리스트 반환
    # 각 날짜(DATE_2)에 대해 DATE_1(전일)과 DATE_2(당일) 쌍을 생성
    date_pairs = []
    current_date = start_date
    while current_date <= end_date:
        # 각 날짜에 대해 DATE_1은 전일, DATE_2는 당일
        # 예: current_date = 2024-01-01 → DATE_1 = 2023-12-31, DATE_2 = 2024-01-01
        prev_date = current_date - timedelta(days=1)
        date_pairs.append({
            "date_1": format_date(prev_date),
            "date_2": format_date(current_date),
            "process_date": format_date(current_date)  # 처리 기준 날짜 (DATE_2)
        })
        current_date += timedelta(days=1)
    
    logging.info(f"📋 생성된 날짜 쌍 수: {len(date_pairs)}개")
    if date_pairs:
        first_pair = date_pairs[0]
        last_pair = date_pairs[-1]
        logging.info(f"   첫 번째: DATE_1={first_pair['date_1']}, DATE_2={first_pair['date_2']}")
        logging.info(f"   마지막: DATE_1={last_pair['date_1']}, DATE_2={last_pair['date_2']}")
    
    return {
        "date_pairs": date_pairs,
        "backfill_start_date": format_date(start_date),
        "backfill_end_date": format_date(end_date)
    }

def build_oracle_query(v_p_date_1: str, v_p_date_2: str) -> str:
    """Oracle 쿼리 생성
    
    Args:
        v_p_date_1: 날짜1 (YYYYMMDD 형식, 1교대용)
        v_p_date_2: 날짜2 (YYYYMMDD 형식, 2/3교대용)
    
    Returns:
        Oracle SQL query string
    """
    return f"""
    WITH base AS (
        SELECT
            b.mc_cd,
            a.mcs_cd        AS mcs_name,
            a.mcs_color     AS mcs_color_name,
            b.shift,
            COUNT(*)                    AS prs_qty,
            COUNT(c.status)             AS rst_qty,
            COUNT(*) * 70               AS p_mat,
            COUNT(c.status) * 70        AS act_mat
        FROM msbp_roll_plan a
        JOIN msbp_roll_so b
          ON a.so_id = b.so_id
        LEFT JOIN msbp_roll_lot c
          ON b.so_id = c.so_id
         AND b.so_seq = c.so_seq
         AND c.roll_op_cd = 'COMP'
         AND c.status = '7'
        WHERE a.area_cd = 'B1'
          AND a.op_cd = 'OS'
          AND a.upd_user <> 'OSD'
          AND (
                (a.cfm_date = '{v_p_date_1}' AND b.shift = '1')
             OR (a.cfm_date = '{v_p_date_2}' AND b.shift IN ('2','3'))
              )
        GROUP BY
            b.mc_cd, a.mcs_cd, a.mcs_color, b.shift
    ),

    pivoted AS (
        SELECT
            mc_cd,
            mcs_name,
            mcs_color_name,
            SUM(CASE WHEN shift='1' THEN prs_qty ELSE 0 END) AS s1_prs,
            SUM(CASE WHEN shift='1' THEN rst_qty ELSE 0 END) AS s1_rst,
            SUM(CASE WHEN shift='1' THEN p_mat   ELSE 0 END) AS s1_pmat,
            SUM(CASE WHEN shift='1' THEN act_mat ELSE 0 END) AS s1_amat,
            SUM(CASE WHEN shift='2' THEN prs_qty ELSE 0 END) AS s2_prs,
            SUM(CASE WHEN shift='2' THEN rst_qty ELSE 0 END) AS s2_rst,
            SUM(CASE WHEN shift='2' THEN p_mat   ELSE 0 END) AS s2_pmat,
            SUM(CASE WHEN shift='2' THEN act_mat ELSE 0 END) AS s2_amat,
            SUM(CASE WHEN shift='3' THEN prs_qty ELSE 0 END) AS s3_prs,
            SUM(CASE WHEN shift='3' THEN rst_qty ELSE 0 END) AS s3_rst,
            SUM(CASE WHEN shift='3' THEN p_mat   ELSE 0 END) AS s3_pmat,
            SUM(CASE WHEN shift='3' THEN act_mat ELSE 0 END) AS s3_amat
        FROM base
        GROUP BY mc_cd, mcs_name, mcs_color_name
    )

    SELECT
        CASE WHEN SUBSTR(mc_cd, 4, 2) IS NULL THEN 'Total'
        ELSE SUBSTR(mc_cd, 4, 2) END AS machine_no,   -- 설비 번호 (NULL인 경우 'Total')

        /* ===================== 1교대 ===================== */
        SUM(s1_prs)  AS s1_plan_qty,    -- 1교대 계획 Roll 수
        SUM(s1_rst)  AS s1_actual_qty,  -- 1교대 실적 Roll 수
        SUM(s1_pmat) AS s1_plan_material,    -- 1교대 계획 자재량
        SUM(s1_amat) AS s1_actual_material,  -- 1교대 실적 자재량

        /* ===================== 2교대 ===================== */
        SUM(s2_prs)  AS s2_plan_qty,
        SUM(s2_rst)  AS s2_actual_qty,
        SUM(s2_pmat) AS s2_plan_material,
        SUM(s2_amat) AS s2_actual_material,

        /* ===================== 3교대 ===================== */
        SUM(s3_prs)  AS s3_plan_qty,
        SUM(s3_rst)  AS s3_actual_qty,
        SUM(s3_pmat) AS s3_plan_material,
        SUM(s3_amat) AS s3_actual_material,

        /* ===================== 전체 합계 ===================== */
        SUM(s1_prs + s2_prs + s3_prs) AS total_plan_roll_qty,   -- 전체 계획 Roll
        SUM(s1_rst + s2_rst + s3_rst) AS total_actual_roll_qty  -- 전체 실적 Roll

    FROM pivoted
    GROUP BY ROLLUP(mc_cd, mcs_name, mcs_color_name)
    HAVING mcs_color_name IS NULL
        AND GROUPING(mcs_name) = 1
        AND GROUPING(mcs_color_name) = 1 
    ORDER BY mc_cd
    """


def create_target_table_if_not_exists(mysql: MySQLHelper, conn_id: str) -> None:
    """타겟 테이블이 없으면 생성 (MySQL/MariaDB)
    
    Args:
        mysql: MySQLHelper instance
        conn_id: Connection ID (로깅용)
    """
    # 먼저 테이블 존재 여부 확인 (디스크 공간 부족 에러 방지)
    try:
        check_table_sql = f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = '{TARGET_SCHEMA}' 
            AND table_name = '{TARGET_TABLE}'
        """
        with mysql.hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(check_table_sql)
            table_exists = cursor.fetchone()[0] > 0
            
            if table_exists:
                logging.info(f"✅ 테이블 이미 존재: {conn_id} - {TARGET_SCHEMA}.{TARGET_TABLE}")
                return
            
            logging.info(f"📋 테이블이 존재하지 않음. 생성 시도: {conn_id} - {TARGET_SCHEMA}.{TARGET_TABLE}")
    except Exception as e:
        logging.warning(f"⚠️ 테이블 존재 여부 확인 실패: {conn_id} - {str(e)}")
        # 확인 실패해도 계속 진행 (테이블이 없을 수 있음)
    
    # 테이블이 없으면 생성 (mcs_color_name 제거된 구조)
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{TARGET_SCHEMA}`.`{TARGET_TABLE}` (
            process_date DATE NOT NULL COMMENT '처리 기준 날짜 (DATE_2)',
            machine_no VARCHAR(10) NOT NULL,  -- CASE 문으로 항상 값이 있으므로 NOT NULL 가능
            
            -- 1교대
            s1_plan_qty DECIMAL(18, 0) DEFAULT 0,
            s1_actual_qty DECIMAL(18, 0) DEFAULT 0,
            s1_plan_material DECIMAL(18, 2) DEFAULT 0,
            s1_actual_material DECIMAL(18, 2) DEFAULT 0,
            
            -- 2교대
            s2_plan_qty DECIMAL(18, 0) DEFAULT 0,
            s2_actual_qty DECIMAL(18, 0) DEFAULT 0,
            s2_plan_material DECIMAL(18, 2) DEFAULT 0,
            s2_actual_material DECIMAL(18, 2) DEFAULT 0,
            
            -- 3교대
            s3_plan_qty DECIMAL(18, 0) DEFAULT 0,
            s3_actual_qty DECIMAL(18, 0) DEFAULT 0,
            s3_plan_material DECIMAL(18, 2) DEFAULT 0,
            s3_actual_material DECIMAL(18, 2) DEFAULT 0,
            
            -- 전체 합계
            total_plan_roll_qty DECIMAL(18, 0) DEFAULT 0,
            total_actual_roll_qty DECIMAL(18, 0) DEFAULT 0,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            PRIMARY KEY (process_date, machine_no),
            INDEX idx_process_date (process_date),
            INDEX idx_machine_no (machine_no)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    
    try:
        with mysql.hook.get_conn() as conn, conn.cursor() as cursor:
            # SCHEMA 생성 (디스크 공간 부족 시 실패할 수 있음)
            try:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
                conn.commit()
                logging.info(f"✅ SCHEMA 확인/생성 완료: {conn_id} - {TARGET_SCHEMA}")
            except Exception as schema_error:
                # SCHEMA 생성 실패해도 계속 진행 (이미 존재할 수 있음)
                logging.warning(f"⚠️ SCHEMA 생성 시도 실패 (무시하고 계속): {conn_id} - {str(schema_error)}")
            
            # 테이블 생성 시도
            cursor.execute(create_table_sql)
            conn.commit()
            logging.info(f"✅ 테이블 확인/생성 완료: {conn_id} - {TARGET_SCHEMA}.{TARGET_TABLE}")
    except Exception as e:
        error_msg = str(e)
        # 디스크 공간 부족 에러인 경우 특별 처리
        if "No space left on device" in error_msg or "Errcode: 28" in error_msg:
            logging.error(f"❌ 테이블 생성 실패 (디스크 공간 부족): {conn_id} - {error_msg}")
            logging.error(f"💡 해결 방법: MariaDB 서버의 디스크 공간을 확보해야 합니다.")
            # 디스크 공간 부족은 치명적이므로 예외 발생
            raise Exception(f"디스크 공간 부족으로 테이블 생성 실패: {error_msg}") from e
        else:
            logging.error(f"❌ 테이블 생성 실패: {conn_id} - {error_msg}")
            raise


def build_insert_sql_mysql() -> str:
    """MySQL/MariaDB 적재용 INSERT ... ON DUPLICATE KEY UPDATE 쿼리 생성
    
    Returns:
        SQL query string (MySQL/MariaDB)
    """
    return f"""
        INSERT INTO `{TARGET_SCHEMA}`.`{TARGET_TABLE}` (
            process_date,
            machine_no,
            s1_plan_qty,
            s1_actual_qty,
            s1_plan_material,
            s1_actual_material,
            s2_plan_qty,
            s2_actual_qty,
            s2_plan_material,
            s2_actual_material,
            s3_plan_qty,
            s3_actual_qty,
            s3_plan_material,
            s3_actual_material,
            total_plan_roll_qty,
            total_actual_roll_qty
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            s1_plan_qty = VALUES(s1_plan_qty),
            s1_actual_qty = VALUES(s1_actual_qty),
            s1_plan_material = VALUES(s1_plan_material),
            s1_actual_material = VALUES(s1_actual_material),
            s2_plan_qty = VALUES(s2_plan_qty),
            s2_actual_qty = VALUES(s2_actual_qty),
            s2_plan_material = VALUES(s2_plan_material),
            s2_actual_material = VALUES(s2_actual_material),
            s3_plan_qty = VALUES(s3_plan_qty),
            s3_actual_qty = VALUES(s3_actual_qty),
            s3_plan_material = VALUES(s3_plan_material),
            s3_actual_material = VALUES(s3_actual_material),
            total_plan_roll_qty = VALUES(total_plan_roll_qty),
            total_actual_roll_qty = VALUES(total_actual_roll_qty),
            updated_at = CURRENT_TIMESTAMP
    """


# ════════════════════════════════════════════════════════════════
	# 3️⃣ Data Processing
# ════════════════════════════════════════════════════════════════

def process_roll_shift_summary(v_p_date_1: str, v_p_date_2: str) -> dict:
    """MSBP_ROLL 교대별 집계 처리
    
    Args:
        v_p_date_1: 날짜1 (YYYYMMDD 형식, 1교대용)
        v_p_date_2: 날짜2 (YYYYMMDD 형식, 2/3교대용)
    
    Returns:
        Processing result dictionary
    """
    total_rows_processed = 0
    target_results = {}
    
    try:
        # 1. Oracle에서 데이터 추출 (OP_CD는 항상 'OS'로 고정)
        logging.info(f"📥 Oracle 데이터 추출 시작: OP_CD=OS, DATE_1={v_p_date_1}, DATE_2={v_p_date_2}")
        
        oracle_query = build_oracle_query(v_p_date_1, v_p_date_2)
        logging.info(f"Oracle 쿼리:\n{oracle_query[:500]}...")  # 처음 500자만 로깅
        
        with OracleHelper(conn_id=SOURCE_ORACLE_CONN_ID) as oracle:
            oracle_conn = oracle.get_conn()
            cursor = oracle_conn.cursor()
            cursor.execute(oracle_query)
            
            # 컬럼 정보 가져오기
            columns = [desc[0] for desc in cursor.description]
            logging.info(f"📊 컬럼: {columns}")
            
            # 데이터 가져오기
            rows = cursor.fetchall()
            logging.info(f"✅ 데이터 추출 완료: {len(rows)} rows")
        
        if not rows:
            logging.warning("⚠️ 추출된 데이터가 없습니다.")
            return {
                "status": "success",
                "v_p_op_cd": "OS",
                "v_p_date_1": v_p_date_1,
                "v_p_date_2": v_p_date_2,
                "rows_processed": 0,
                "extract_time": datetime.utcnow().isoformat(),
                "targets": {}
            }
        
        # 2. 활성화된 MySQL 연결에 데이터 적재
        insert_sql = build_insert_sql_mysql()
        enabled_targets = [t for t in TARGET_MYSQL_CONNECTIONS if t.get("enabled", False)]
        
        if not enabled_targets:
            logging.warning("⚠️ 활성화된 적재 대상이 없습니다.")
            return {
                "status": "success",
                "v_p_op_cd": "OS",
                "v_p_date_1": v_p_date_1,
                "v_p_date_2": v_p_date_2,
                "rows_processed": 0,
                "extract_time": datetime.utcnow().isoformat(),
                "targets": {}
            }
        
        for target_config in enabled_targets:
            conn_id = target_config["conn_id"]
            try:
                logging.info(f"💾 데이터 적재 시작: {conn_id}")
                mysql_target = MySQLHelper(conn_id=conn_id)
                
                # 테이블 생성 (없으면)
                create_target_table_if_not_exists(mysql_target, conn_id)
                
                # 데이터를 튜플 리스트로 변환
                # Oracle 결과는 (machine_no, s1_plan_qty, ...) 형태 (mcs_name 제거됨)
                # process_date는 UI의 "Working Date"와 동일 (DATE_2)
                process_date_obj = datetime.strptime(v_p_date_2, '%Y%m%d').date()
                
                insert_data = []
                for row in rows:
                    # row는 튜플 형태: (machine_no, s1_plan_qty, ...)
                    # process_date를 맨 앞에 추가
                    # None 값은 None으로 유지, 숫자는 float로 변환, 문자열은 그대로
                    processed_values = [
                        float(val) if isinstance(val, (int, float)) and val is not None else val
                        for val in row
                    ]
                    
                    # machine_no가 빈 문자열인 경우 'Total'로 변환 (NOT NULL 제약 때문)
                    if isinstance(processed_values[0], str) and processed_values[0].strip() == '':
                        processed_values[0] = 'Total'
                    
                    # process_date를 맨 앞에 추가
                    processed_row = (process_date_obj,) + tuple(processed_values)
                    insert_data.append(processed_row)
                
                # 배치로 INSERT 실행
                rows_to_process = len(insert_data)
                logging.info(f"📝 처리할 데이터: {rows_to_process}개 행")
                
                with mysql_target.hook.get_conn() as conn, conn.cursor() as cursor:
                    cursor.executemany(insert_sql, insert_data)
                    rows_affected = cursor.rowcount  # 실제로 INSERT/UPDATE된 행 수 (값 변경이 없으면 0일 수 있음)
                    conn.commit()
                
                # 결과 확인을 위한 COUNT 쿼리
                count_sql = f"""
                    SELECT COUNT(*) 
                    FROM `{TARGET_SCHEMA}`.`{TARGET_TABLE}`
                """
                count_result = mysql_target.execute_query(count_sql, task_id=f"count_records_{conn_id}", xcom_key=None)
                row_count = count_result[0][0] if count_result and len(count_result) > 0 else 0
                
                total_rows_processed = max(total_rows_processed, row_count)
                target_results[conn_id] = {
                    "status": "success",
                    "rows_processed": rows_to_process,  # 처리한 행 수 (Oracle에서 추출한 행 수)
                    "rows_affected": rows_affected,  # 실제로 INSERT/UPDATE된 행 수 (값 변경이 없으면 0일 수 있음)
                    "total_rows": row_count  # 전체 테이블 레코드 수
                }
                
                logging.info(f"✅ 데이터 적재 완료: {conn_id} - {rows_to_process}개 행 처리 완료 (실제 영향받은 행: {rows_affected}개, 전체 테이블 레코드 수: {row_count}개)")
                
            except Exception as e:
                logging.error(f"❌ 데이터 적재 실패: {conn_id} - {str(e)}", exc_info=True)
                target_results[conn_id] = {
                    "status": "failed",
                    "error": str(e)
                }
                # 한 타겟 실패해도 다른 타겟은 계속 처리
        
        return {
            "status": "success",
            "v_p_op_cd": "OS",
            "v_p_date_1": v_p_date_1,
            "v_p_date_2": v_p_date_2,
            "rows_processed": total_rows_processed,
            "extract_time": datetime.utcnow().isoformat(),
            "targets": target_results
        }
        
    except Exception as e:
        logging.error(f"❌ 집계 처리 실패: {str(e)}", exc_info=True)
        return {
            "status": "failed",
            "error": str(e),
            "v_p_op_cd": "OS",
            "v_p_date_1": v_p_date_1,
            "v_p_date_2": v_p_date_2,
            "targets": target_results
        }