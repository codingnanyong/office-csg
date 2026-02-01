"""
Unified Monitoring Realtime - Oracle GMES Realtime DAG

Oracle GMES(IP, PH 공정)에서 실시간 데이터를 추출하고
PostgreSQL bronze 스키마에 적재한 뒤,
dbt를 통해 silver 테이블(view/table)로 materialize하는 DAG.
매 5분마다 실행됩니다.
"""

from datetime import datetime, timedelta
import logging
import sys

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.postgres_hook import PostgresHelper

# ============================================================================
# 상수 정의
# ============================================================================

# 경로 설정
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/unified_montrg_realtime"
if DBT_PROJECT_DIR not in sys.path:
    sys.path.insert(0, DBT_PROJECT_DIR)

# 데이터베이스 설정
ORACLE_CONN_ID = "orc_jj_gmes"  # Oracle GMES 연결
POSTGRES_CONN_ID = "pg_jj_unified_montrg_dw"  # PostgreSQL 연결
STAGING_SCHEMA = "bronze"  # Oracle 데이터 staging 저장 스키마
SCHEMA = "silver"  # dbt 모델 저장 스키마

# DAG 기본 설정
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# dbt Profile 설정
PROFILE_CONFIG = ProfileConfig(
    profile_name="unified_montrg_realtime",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={"schema": SCHEMA}
    ),
)

# dbt Execution 설정
EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path="dbt",
)

# ============================================================================
# Oracle GMES IP 데이터 추출 함수
# ============================================================================

def extract_oracle_to_staging(**context):
    """Oracle GMES(IP 공정)에서 데이터를 추출하여 PostgreSQL staging 테이블에 저장.
    
    사용자가 제공한 쿼리를 Variable에서 가져오거나 기본 쿼리를 사용합니다.
    Variable 키: 'oracle_gmes_staging_query' (설정 시 우선 사용)
    
    저장 위치: bronze.production_ip_staging_raw
    """
    try:
        # 기본 쿼리 (IP 공정 교대 근무 시간대별 생산성 데이터)
        default_query = """
            WITH shift_window AS (
                SELECT
                    SYSDATE AS now_dt,
                    CASE
                        WHEN TO_CHAR(SYSDATE,'DY','NLS_DATE_LANGUAGE=ENGLISH')
                            IN ('MON','TUE','WED','THU') THEN
                            CASE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)-1+INTERVAL '22:30' HOUR TO MINUTE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '14:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '22:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '14:30' HOUR TO MINUTE
                                ELSE
                                    TRUNC(SYSDATE)+INTERVAL '22:30' HOUR TO MINUTE
                            END
                        WHEN TO_CHAR(SYSDATE,'DY','NLS_DATE_LANGUAGE=ENGLISH') = 'FRI' THEN
                            CASE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)-1+INTERVAL '22:30' HOUR TO MINUTE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '15:00' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '23:00' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '15:00' HOUR TO MINUTE
                                ELSE
                                    TRUNC(SYSDATE)+INTERVAL '22:30' HOUR TO MINUTE
                            END
                        WHEN TO_CHAR(SYSDATE,'DY','NLS_DATE_LANGUAGE=ENGLISH') = 'SAT' THEN
                            CASE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                    THEN NULL
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '11:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '16:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '11:30' HOUR TO MINUTE
                                ELSE
                                    TRUNC(SYSDATE)+INTERVAL '16:30' HOUR TO MINUTE
                            END
                    END AS start_dt
                FROM dual
            ),
            time_window AS (
                SELECT
                    start_dt,
                    CASE
                        WHEN start_dt IS NULL THEN NULL
                        WHEN start_dt = TRUNC(start_dt)+INTERVAL '6:30'  HOUR TO MINUTE THEN start_dt + INTERVAL '8' HOUR
                        WHEN start_dt = TRUNC(start_dt)+INTERVAL '14:30' HOUR TO MINUTE THEN start_dt + INTERVAL '8' HOUR
                        WHEN start_dt = TRUNC(start_dt)+INTERVAL '15:00' HOUR TO MINUTE THEN start_dt + INTERVAL '8' HOUR
                        WHEN start_dt = TRUNC(start_dt)+INTERVAL '11:30' HOUR TO MINUTE THEN start_dt + INTERVAL '5' HOUR
                        WHEN start_dt = TRUNC(start_dt)+INTERVAL '16:30' HOUR TO MINUTE THEN start_dt + INTERVAL '5' HOUR
                        ELSE start_dt + INTERVAL '8' HOUR
                    END AS end_dt
                FROM shift_window
            )

            SELECT
                COALESCE(p.zone_cd, r.zone_cd, d.zone_cd)           AS zone_cd,
                COALESCE(p.machine_cd, r.machine_cd, d.machine_cd) AS machine_cd,
                NVL(p.plan_qty,0)   AS plan_qty,
                NVL(r.prod_qty,0)   AS prod_qty,
                NVL(d.defect_qty,0) AS defect_qty
            FROM
            /* ================= PLAN ================= */
            (
                SELECT
                    zone_cd,
                    machine_cd,
                    SUM(NVL(prs_qty,0)) AS plan_qty
                FROM SSS_IPP_SO sis
                CROSS JOIN time_window t
                WHERE t.start_dt IS NOT NULL
                AND (
                        (sis.ymd = TO_CHAR(t.start_dt,'YYYYMMDD')
                        AND TO_NUMBER(sis.hh) >= TO_NUMBER(TO_CHAR(t.start_dt,'HH24')))
                    OR
                        (sis.ymd = TO_CHAR(t.end_dt,'YYYYMMDD')
                        AND TO_NUMBER(sis.hh) <  TO_NUMBER(TO_CHAR(t.end_dt,'HH24')))
                    )
                GROUP BY zone_cd, machine_cd
            ) p

            FULL OUTER JOIN
            /* ================= PROD ================= */
            (
                SELECT
                    zone_cd,
                    machine_cd,
                    SUM(NVL(prs_qty,0)) AS prod_qty
                FROM SMP_SS_IPI_RST@jjedif r
                CROSS JOIN time_window t
                WHERE t.start_dt IS NOT NULL
                AND r.start_date >= t.start_dt
                AND r.end_date   <  t.end_dt
                GROUP BY zone_cd, machine_cd
            ) r
            ON p.zone_cd = r.zone_cd
            AND p.machine_cd = r.machine_cd

            FULL OUTER JOIN
            /* ================= DEFECT ================= */
            (
                SELECT
                    miobi.ref_value03 AS zone_cd,
                    miobi.machine_cd,
                    SUM(NVL(miobi.osnd_bt_qty,0)) AS defect_qty
                FROM mspq_in_osnd_bt_ipi miobi
                CROSS JOIN time_window t
                WHERE t.start_dt IS NOT NULL
                AND miobi.osnd_date = TO_CHAR(TRUNC(t.start_dt),'YYYYMMDD')
                GROUP BY miobi.ref_value03, miobi.machine_cd
            ) d
            ON COALESCE(p.zone_cd, r.zone_cd) = d.zone_cd
            AND COALESCE(p.machine_cd, r.machine_cd) = d.machine_cd

            ORDER BY zone_cd, machine_cd
        """
        
        # 사용자 쿼리 가져오기 (Variable에서, 없으면 기본 쿼리 사용)
        user_query = Variable.get("oracle_gmes_ip_staging_query", default_var=default_query)
        
        logging.info(f"📝 Oracle 쿼리 실행 시작")
        logging.info(f"📝 쿼리: {user_query[:200]}...")  # 처음 200자만 로깅
        
        # Oracle에서 데이터 추출
        with OracleHelper(conn_id=ORACLE_CONN_ID) as oracle:
            oracle_conn = oracle.get_conn()
            cursor = oracle_conn.cursor()
            cursor.execute(user_query)
            
            # 컬럼 정보 가져오기
            columns = [desc[0] for desc in cursor.description]
            logging.info(f"📊 컬럼: {columns}")
            
            # 데이터 가져오기
            rows = cursor.fetchall()
            logging.info(f"📊 추출된 행 수: {len(rows)}")
            
            if not rows:
                logging.warning("⚠️ 추출된 데이터가 없습니다.")
                return
            
            # PostgreSQL에 저장
            pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
            pg_hook = pg.hook
            
            # 테이블이 없으면 생성 (IP 공정용)
            table_name = "production_ip_staging_raw"
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.{table_name} (
                    {', '.join([f'"{col}" TEXT' for col in columns])},
                    etl_extract_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            
            with pg_hook.get_conn() as conn, conn.cursor() as pg_cursor:
                pg_cursor.execute(create_table_sql)
                conn.commit()
                logging.info(f"✅ 테이블 생성/확인 완료: {STAGING_SCHEMA}.{table_name}")
                
                # 기존 데이터 삭제 (전체 갱신)
                pg_cursor.execute(f"TRUNCATE TABLE {STAGING_SCHEMA}.{table_name}")
                conn.commit()
                logging.info(f"🧹 기존 데이터 삭제 완료")
                
                # 데이터 삽입
                from psycopg2.extras import execute_values
                values = [tuple(str(val) if val is not None else None for val in row) for row in rows]
                insert_sql = f"""
                    INSERT INTO {STAGING_SCHEMA}.{table_name} 
                    ({', '.join([f'"{col}"' for col in columns])})
                    VALUES %s
                """
                execute_values(pg_cursor, insert_sql, values)
                conn.commit()
            logging.info(f"✅ {len(rows)}행 삽입 완료: {STAGING_SCHEMA}.{table_name}")
        
    except Exception as e:
        logging.error(f"❌ Oracle IP staging 작업 실패: {e}", exc_info=True)
        raise


# ============================================================================
# Oracle GMES PH 데이터 추출 함수
# ============================================================================


def extract_oracle_ph_to_staging(**context):
    """Oracle GMES(PH 공정)에서 데이터를 추출하여 PostgreSQL staging 테이블에 저장.
    
    사용자가 제공한 쿼리를 Variable에서 가져오거나 기본 쿼리를 사용합니다.
    Variable 키: 'oracle_gmes_ph_staging_query' (설정 시 우선 사용)
    
    저장 위치: bronze.production_ph_staging_raw
    """
    try:
        # 기본 쿼리 (PH 공정 교대 근무 시간대별 생산성 데이터)
        default_query = """
            WITH shift_window AS (
                SELECT
                    SYSDATE AS now_dt,

                    /* ================= 근무조 시작 시각 ================= */
                    CASE
                        /* 월~목 */
                        WHEN TO_CHAR(SYSDATE,'DY','NLS_DATE_LANGUAGE=ENGLISH')
                            IN ('MON','TUE','WED','THU') THEN
                            CASE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)-1+INTERVAL '22:30' HOUR TO MINUTE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '14:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '22:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '14:30' HOUR TO MINUTE
                                ELSE
                                    TRUNC(SYSDATE)+INTERVAL '22:30' HOUR TO MINUTE
                            END

                        /* 금요일 */
                        WHEN TO_CHAR(SYSDATE,'DY','NLS_DATE_LANGUAGE=ENGLISH') = 'FRI' THEN
                            CASE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)-1+INTERVAL '22:30' HOUR TO MINUTE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '15:00' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '23:00' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '15:00' HOUR TO MINUTE
                                ELSE
                                    TRUNC(SYSDATE)+INTERVAL '22:30' HOUR TO MINUTE
                            END

                        /* 토요일 */
                        WHEN TO_CHAR(SYSDATE,'DY','NLS_DATE_LANGUAGE=ENGLISH') = 'SAT' THEN
                            CASE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                    THEN NULL
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '11:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '6:30' HOUR TO MINUTE
                                WHEN SYSDATE < TRUNC(SYSDATE)+INTERVAL '16:30' HOUR TO MINUTE
                                    THEN TRUNC(SYSDATE)+INTERVAL '11:30' HOUR TO MINUTE
                                ELSE
                                    TRUNC(SYSDATE)+INTERVAL '16:30' HOUR TO MINUTE
                            END
                    END AS start_dt
                FROM dual
            ),
            time_window AS (
                SELECT
                    start_dt,

                    /* ================= 근무조 종료 시각 ================= */
                    CASE
                        WHEN start_dt IS NULL THEN NULL
                        WHEN start_dt = TRUNC(start_dt)+INTERVAL '6:30'  HOUR TO MINUTE THEN start_dt + INTERVAL '8' HOUR
                        WHEN start_dt = TRUNC(start_dt)+INTERVAL '14:30' HOUR TO MINUTE THEN start_dt + INTERVAL '8' HOUR
                        WHEN start_dt = TRUNC(start_dt)+INTERVAL '15:00' HOUR TO MINUTE THEN start_dt + INTERVAL '8' HOUR
                        WHEN start_dt = TRUNC(start_dt)+INTERVAL '11:30' HOUR TO MINUTE THEN start_dt + INTERVAL '5' HOUR
                        WHEN start_dt = TRUNC(start_dt)+INTERVAL '16:30' HOUR TO MINUTE THEN start_dt + INTERVAL '5' HOUR
                        ELSE start_dt + INTERVAL '8' HOUR
                    END AS end_dt
                FROM shift_window
            )

            /* ===================== 최종 집계 ===================== */
            SELECT
                COALESCE(p.line_cd, r.line_cd, d.line_cd)           AS line_cd,
                COALESCE(p.machine_cd, r.machine_cd, d.machine_cd) AS machine_cd,

                NVL(p.plan_qty,   0) AS plan_qty,
                NVL(r.prod_qty,   0) AS prod_qty,
                NVL(d.defect_qty, 0) AS defect_qty
            FROM

            /* ================= PLAN ================= */
            (
                SELECT
                    CASE
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 1  AND 8  THEN 'LINE 1'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 9  AND 16 THEN 'LINE 2'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 17 AND 24 THEN 'LINE 3'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 25 AND 30 THEN 'LINE 4'
                        ELSE 'UNKNOWN'
                    END AS line_cd,
                    'PH'||SUBSTR(resource_cd,2,2) AS machine_cd,
                    SUM(NVL(prs_qty,0)) AS plan_qty
                FROM LMES.sss_php_so sps
                CROSS JOIN time_window t
                WHERE
                    t.start_dt IS NOT NULL
                    AND TO_DATE(sps.plan_date||LPAD(sps.plan_hour,2,'0'),
                                'YYYYMMDDHH24')
                        >= t.start_dt
                    AND TO_DATE(sps.plan_date||LPAD(sps.plan_hour,2,'0'),
                                'YYYYMMDDHH24')
                        <  t.end_dt
                GROUP BY
                    CASE
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 1  AND 8  THEN 'LINE 1'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 9  AND 16 THEN 'LINE 2'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 17 AND 24 THEN 'LINE 3'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 25 AND 30 THEN 'LINE 4'
                        ELSE 'UNKNOWN'
                    END,
                    'PH'||SUBSTR(resource_cd,2,2)
            ) p

            FULL OUTER JOIN
            /* ================= PROD ================= */
            (
                SELECT
                    CASE
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 1  AND 8  THEN 'LINE 1'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 9  AND 16 THEN 'LINE 2'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 17 AND 24 THEN 'LINE 3'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 25 AND 30 THEN 'LINE 4'
                        ELSE 'UNKNOWN'
                    END AS line_cd,
                    'PH'||SUBSTR(resource_cd,2,2) AS machine_cd,
                    SUM(NVL(prod_qty,0)) AS prod_qty
                FROM LMES.SMP_SS_PHP_RST sspr
                CROSS JOIN time_window t
                WHERE
                    t.start_dt IS NOT NULL
                    AND TO_DATE(sspr.work_date || sspr.hms, 'YYYYMMDDHH24MISS') >= t.start_dt
                    AND TO_DATE(sspr.work_date || sspr.hms, 'YYYYMMDDHH24MISS') <  t.end_dt
                GROUP BY
                    CASE
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 1  AND 8  THEN 'LINE 1'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 9  AND 16 THEN 'LINE 2'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 17 AND 24 THEN 'LINE 3'
                        WHEN TO_NUMBER(SUBSTR(resource_cd,2,2)) BETWEEN 25 AND 30 THEN 'LINE 4'
                        ELSE 'UNKNOWN'
                    END,
                    'PH'||SUBSTR(resource_cd,2,2)
            ) r

            ON p.line_cd = r.line_cd
            AND p.machine_cd = r.machine_cd

            FULL OUTER JOIN

            /* ================= DEFECT ================= */
            (
                SELECT
                    CASE
                        WHEN TO_NUMBER(SUBSTR(sub_wc_cd,-2)) BETWEEN 1  AND 8  THEN 'LINE 1'
                        WHEN TO_NUMBER(SUBSTR(sub_wc_cd,-2)) BETWEEN 9  AND 16 THEN 'LINE 2'
                        WHEN TO_NUMBER(SUBSTR(sub_wc_cd,-2)) BETWEEN 17 AND 24 THEN 'LINE 3'
                        WHEN TO_NUMBER(SUBSTR(sub_wc_cd,-2)) BETWEEN 25 AND 30 THEN 'LINE 4'
                        ELSE 'UNKNOWN'
                    END AS line_cd,
                    'PH'||SUBSTR(sub_wc_cd,4,2) AS machine_cd,
                    SUM(NVL(osnd_bt_qty,0)) AS defect_qty
                FROM LMES.MSPQ_IN_OSND_BT miob
                CROSS JOIN time_window t
                WHERE
                    t.start_dt IS NOT NULL
                    AND miob.osnd_date = TO_CHAR(TRUNC(t.start_dt), 'YYYYMMDD')
                    AND miob.op_cd = 'PHH'
                    AND SUBSTR(miob.sub_wc_cd, 4, 2) IN ('01','02','03','04','05','06','07','08')
                    AND miob.reason_cd IN
                        ('PHH01','PHH02','PHH03','PHH04','PHH05','PHH06',
                        'PHH07','PHH08','PHH09','PHH10','PHH11','PHH20')
                GROUP BY
                    CASE
                        WHEN TO_NUMBER(SUBSTR(sub_wc_cd,-2)) BETWEEN 1  AND 8  THEN 'LINE 1'
                        WHEN TO_NUMBER(SUBSTR(sub_wc_cd,-2)) BETWEEN 9  AND 16 THEN 'LINE 2'
                        WHEN TO_NUMBER(SUBSTR(sub_wc_cd,-2)) BETWEEN 17 AND 24 THEN 'LINE 3'
                        WHEN TO_NUMBER(SUBSTR(sub_wc_cd,-2)) BETWEEN 25 AND 30 THEN 'LINE 4'
                        ELSE 'UNKNOWN'
                    END,
                    'PH'||SUBSTR(sub_wc_cd,4,2)
            ) d

            ON COALESCE(p.line_cd, r.line_cd) = d.line_cd
            AND COALESCE(p.machine_cd, r.machine_cd) = d.machine_cd

            ORDER BY line_cd, machine_cd
        """

        # 사용자 쿼리 가져오기 (Variable에서, 없으면 기본 쿼리 사용)
        user_query = Variable.get("oracle_gmes_ph_staging_query", default_var=default_query)

        logging.info(f"📝 Oracle PH 쿼리 실행 시작")
        logging.info(f"📝 쿼리: {user_query[:200]}...")

        # Oracle에서 데이터 추출
        with OracleHelper(conn_id=ORACLE_CONN_ID) as oracle:
            oracle_conn = oracle.get_conn()
            cursor = oracle_conn.cursor()
            cursor.execute(user_query)

            columns = [desc[0] for desc in cursor.description]
            logging.info(f"📊 PH 컬럼: {columns}")

            rows = cursor.fetchall()
            logging.info(f"📊 PH 추출된 행 수: {len(rows)}")

            if not rows:
                logging.warning("⚠️ PH 추출된 데이터가 없습니다.")
                return

            pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
            pg_hook = pg.hook

            table_name = "production_ph_staging_raw"
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.{table_name} (
                    {', '.join([f'"{col}" TEXT' for col in columns])},
                    etl_extract_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """

            with pg_hook.get_conn() as conn, conn.cursor() as pg_cursor:
                pg_cursor.execute(create_table_sql)
                conn.commit()
                logging.info(f"✅ PH 테이블 생성/확인 완료: {STAGING_SCHEMA}.{table_name}")

                pg_cursor.execute(f"TRUNCATE TABLE {STAGING_SCHEMA}.{table_name}")
                conn.commit()
                logging.info("🧹 기존 PH 데이터 삭제 완료")

                from psycopg2.extras import execute_values
                values = [tuple(str(val) if val is not None else None for val in row) for row in rows]
                insert_sql = f"""
                    INSERT INTO {STAGING_SCHEMA}.{table_name} 
                    ({', '.join([f'"{col}"' for col in columns])})
                    VALUES %s
                """
                execute_values(pg_cursor, insert_sql, values)
                conn.commit()
                logging.info(f"✅ {len(rows)}행 PH 데이터 삽입 완료: {STAGING_SCHEMA}.{table_name}")

    except Exception as e:
        logging.error(f"❌ Oracle PH staging 작업 실패: {e}", exc_info=True)
        raise


# ============================================================================
# Oracle GMES OP 그룹 데이터 추출 함수
# ============================================================================


def extract_oracle_opgroup_to_staging(**context):
    """Oracle GMES(OP 그룹: IP/OS/PH) 데이터를 추출하여 PostgreSQL staging 테이블에 저장."""
    try:
        default_query = """
            WITH shift_window AS (
                SELECT
                    SYSDATE AS now_dt,
                    CASE
                        WHEN TO_CHAR(SYSDATE,'DY','NLS_DATE_LANGUAGE=ENGLISH') IN ('MON','TUE','WED','THU') THEN
                            CASE
                                WHEN SYSDATE < TRUNC(SYSDATE)+NUMTODSINTERVAL(6,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                    THEN TRUNC(SYSDATE)-1+NUMTODSINTERVAL(22,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                WHEN SYSDATE < TRUNC(SYSDATE)+NUMTODSINTERVAL(14,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                    THEN TRUNC(SYSDATE)+NUMTODSINTERVAL(6,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                WHEN SYSDATE < TRUNC(SYSDATE)+NUMTODSINTERVAL(22,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                    THEN TRUNC(SYSDATE)+NUMTODSINTERVAL(14,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                ELSE TRUNC(SYSDATE)+NUMTODSINTERVAL(22,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                            END
                        WHEN TO_CHAR(SYSDATE,'DY','NLS_DATE_LANGUAGE=ENGLISH') = 'FRI' THEN
                            CASE
                                WHEN SYSDATE < TRUNC(SYSDATE)+NUMTODSINTERVAL(6,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                    THEN TRUNC(SYSDATE)-1+NUMTODSINTERVAL(22,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                WHEN SYSDATE < TRUNC(SYSDATE)+NUMTODSINTERVAL(15,'HOUR')
                                    THEN TRUNC(SYSDATE)+NUMTODSINTERVAL(6,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                WHEN SYSDATE < TRUNC(SYSDATE)+NUMTODSINTERVAL(23,'HOUR')
                                    THEN TRUNC(SYSDATE)+NUMTODSINTERVAL(15,'HOUR')
                                ELSE TRUNC(SYSDATE)+NUMTODSINTERVAL(22,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                            END
                        WHEN TO_CHAR(SYSDATE,'DY','NLS_DATE_LANGUAGE=ENGLISH') = 'SAT' THEN
                            CASE
                                WHEN SYSDATE < TRUNC(SYSDATE)+NUMTODSINTERVAL(6,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                    THEN NULL
                                WHEN SYSDATE < TRUNC(SYSDATE)+NUMTODSINTERVAL(11,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                    THEN TRUNC(SYSDATE)+NUMTODSINTERVAL(6,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                WHEN SYSDATE < TRUNC(SYSDATE)+NUMTODSINTERVAL(16,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                    THEN TRUNC(SYSDATE)+NUMTODSINTERVAL(11,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                                ELSE TRUNC(SYSDATE)+NUMTODSINTERVAL(16,'HOUR')+NUMTODSINTERVAL(30,'MINUTE')
                            END
                    END AS start_dt
                FROM dual
            ),
            base_date AS (
                SELECT
                    start_dt,
                    TRUNC(start_dt)       AS base_dt,
                    TRUNC(start_dt, 'MM') AS month_start,
                    LAST_DAY(start_dt)    AS month_end
                FROM shift_window
                WHERE start_dt IS NOT NULL
            ),
            plan_data AS (
                SELECT
                    CASE WHEN OP_CD='IPI' THEN 'IP' WHEN OP_CD='OSP' THEN 'OS' WHEN OP_CD IN ('PHH','PHM') THEN 'PH' ELSE OP_CD END AS op_group,
                    op_cd,
                    SUM(PCARD_QTY) AS plan_qty
                FROM LMES.MSPD_PCARD_RESULT p
                CROSS JOIN base_date b
                WHERE p.FA_DATE BETWEEN TO_CHAR(b.month_start,'YYYYMMDD') AND TO_CHAR(b.month_end,'YYYYMMDD')
                AND p.PROD_MOVE_TYPE = 'PROD'
                AND p.PLAN_PROD_DATE = TO_CHAR(b.base_dt,'YYYYMMDD')
                AND p.OP_CD IN ('IPI','OSP','PHH','PHM')
                GROUP BY CASE WHEN OP_CD='IPI' THEN 'IP' WHEN OP_CD='OSP' THEN 'OS' WHEN OP_CD IN ('PHH','PHM') THEN 'PH' ELSE OP_CD END, op_cd
            ),
            prod_data AS (
                SELECT
                    CASE WHEN OP_CD='IPI' THEN 'IP' WHEN OP_CD='OSP' THEN 'OS' WHEN OP_CD IN ('PHH','PHM') THEN 'PH' ELSE OP_CD END AS op_group,
                    op_cd,
                    SUM(PCARD_QTY) AS prod_qty
                FROM LMES.MSPD_PCARD_RESULT p
                CROSS JOIN base_date b
                WHERE p.FA_DATE BETWEEN TO_CHAR(b.month_start,'YYYYMMDD') AND TO_CHAR(b.month_end,'YYYYMMDD')
                AND p.PROD_MOVE_TYPE = 'PROD'
                AND p.PROD_DATE = TO_CHAR(b.base_dt,'YYYYMMDD')
                AND p.OP_CD IN ('IPI','OSP','PHH','PHM')
                GROUP BY CASE WHEN OP_CD='IPI' THEN 'IP' WHEN OP_CD='OSP' THEN 'OS' WHEN OP_CD IN ('PHH','PHM') THEN 'PH' ELSE OP_CD END, op_cd
            ),
            defect_data AS (
                SELECT
                    CASE WHEN OP_CD='IPI' THEN 'IP' WHEN OP_CD='OSP' THEN 'OS' WHEN OP_CD IN ('PHH','PHM') THEN 'PH' ELSE OP_CD END AS op_group,
                    op_cd,
                    SUM(OSND_BT_QTY) AS defect_qty
                FROM MSPQ_IN_OSND_BT d
                CROSS JOIN base_date b
                WHERE d.OSND_DATE = TO_CHAR(b.base_dt,'YYYYMMDD')
                AND d.OP_CD IN ('IPI','OSP','PHH','PHM')
                GROUP BY CASE WHEN OP_CD='IPI' THEN 'IP' WHEN OP_CD='OSP' THEN 'OS' WHEN OP_CD IN ('PHH','PHM') THEN 'PH' ELSE OP_CD END, op_cd
            )
            SELECT
                COALESCE(p.op_group, r.op_group, d.op_group) AS op_group,
                COALESCE(p.op_cd,    r.op_cd,    d.op_cd)    AS op_cd,
                NVL(p.plan_qty,0)   AS plan_qty,
                NVL(r.prod_qty,0)   AS prod_qty,
                NVL(d.defect_qty,0) AS defect_qty
            FROM plan_data p
            FULL OUTER JOIN prod_data r
                ON  p.op_group = r.op_group
                AND p.op_cd    = r.op_cd
            FULL OUTER JOIN defect_data d
                ON  COALESCE(p.op_group, r.op_group) = d.op_group
                AND COALESCE(p.op_cd,    r.op_cd)    = d.op_cd
            ORDER BY op_group, op_cd
        """

        user_query = Variable.get("oracle_gmes_opgroup_staging_query", default_var=default_query)

        logging.info(f"📝 Oracle OP 그룹 쿼리 실행 시작")
        logging.info(f"📝 쿼리: {user_query[:200]}...")

        with OracleHelper(conn_id=ORACLE_CONN_ID) as oracle:
            oracle_conn = oracle.get_conn()
            cursor = oracle_conn.cursor()
            cursor.execute(user_query)

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            logging.info(f"📊 OP 그룹 추출된 행 수: {len(rows)}")

            if not rows:
                logging.warning("⚠️ OP 그룹 데이터가 없습니다.")
                return

            pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)
            pg_hook = pg.hook

            table_name = "production_op_group_staging_raw"
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.{table_name} (
                    {', '.join([f'"{col}" TEXT' for col in columns])},
                    etl_extract_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """

            with pg_hook.get_conn() as conn, conn.cursor() as pg_cursor:
                pg_cursor.execute(create_table_sql)
                conn.commit()
                logging.info(f"✅ OP 그룹 테이블 생성/확인 완료: {STAGING_SCHEMA}.{table_name}")

                pg_cursor.execute(f"TRUNCATE TABLE {STAGING_SCHEMA}.{table_name}")
                conn.commit()
                logging.info("🧹 기존 OP 그룹 데이터 삭제 완료")

                from psycopg2.extras import execute_values
                values = [tuple(str(val) if val is not None else None for val in row) for row in rows]
                insert_sql = f"""
                    INSERT INTO {STAGING_SCHEMA}.{table_name} 
                    ({', '.join([f'"{col}"' for col in columns])})
                    VALUES %s
                """
                execute_values(pg_cursor, insert_sql, values)
                conn.commit()
                logging.info(f"✅ {len(rows)}행 OP 그룹 데이터 삽입 완료: {STAGING_SCHEMA}.{table_name}")

    except Exception as e:
        logging.error(f"❌ Oracle OP 그룹 staging 작업 실패: {e}", exc_info=True)
        raise
# DAG 정의
# ============================================================================

with DAG(
    dag_id="dbt_unified_montrg_realtime",
    default_args=DEFAULT_ARGS,
    description="Unified Monitoring Realtime - Oracle GMES Realtime (IP/PH, 매 5분 실행)",
    schedule_interval="*/3 * * * *",  # 매 3분마다 실행
    catchup=False,
    tags=["dbt", "unified_montrg", "realtime", "oracle_gmes"],
) as dag:
    
    # Oracle(IP)에서 데이터 추출하여 PostgreSQL staging 테이블에 저장
    extract_ip_task = PythonOperator(
        task_id="extract_oracle_ip_to_staging",
        python_callable=extract_oracle_to_staging,
    )

    # Oracle(PH)에서 데이터 추출하여 PostgreSQL staging 테이블에 저장
    extract_ph_task = PythonOperator(
        task_id="extract_oracle_ph_to_staging",
        python_callable=extract_oracle_ph_to_staging,
    )
    
    # dbt 변수 준비
    def prepare_dbt_vars(**context):
        """dbt 실행에 필요한 변수 준비."""
        return {}
    
    prepare_vars = PythonOperator(
        task_id="prepare_dbt_vars",
        python_callable=prepare_dbt_vars,
    )
    
    # dbt 모델 실행 (staging과 marts 모델 모두 실행)
    dbt_task = DbtTaskGroup(
        group_id="dbt_unified_montrg_realtime",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        operator_args={
            "vars": "{{ ti.xcom_pull(task_ids='prepare_dbt_vars') }}",
            "select": "tag:oracle_gmes tag:realtime",  # oracle_gmes와 realtime 태그가 있는 모델 실행
        },
    )
    
    # dbt test 실행 (run 이후 태그 기준으로 테스트)
    def run_dbt_tests(**context):
        """dbt test 실행 (realtime 태그 전체 테스트)"""
        import subprocess
        cmd = [
            "dbt", "test",
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROJECT_DIR,
            "--profile", "unified_montrg_realtime",
            "--target", "dev",
            "--select", "tag:realtime",  # realtime 태그 전체 테스트
        ]
        logging.info(f"🔍 dbt test 실행: {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=DBT_PROJECT_DIR, capture_output=True, text=True)
        logging.info(f"dbt test stdout:\n{result.stdout}")
        if result.stderr:
            logging.warning(f"dbt test stderr:\n{result.stderr}")
        # 테스트 실패해도 DAG 전체를 막지 않도록 warning만 남깁니다.
        if result.returncode != 0:
            logging.warning(f"⚠️ dbt test 실패 (return code={result.returncode}). DAG는 계속 진행합니다.")
            # 실패 시 stdout/stderr 일부를 추가로 기록하여 원인 파악에 도움
            logging.warning("⚠️ dbt test failure details (stdout tail):\n" + result.stdout[-2000:])
            if result.stderr:
                logging.warning("⚠️ dbt test failure details (stderr tail):\n" + result.stderr[-2000:])
        return {"returncode": result.returncode, "output": result.stdout}

    dbt_test_task = PythonOperator(
        task_id="dbt_tests",
        python_callable=run_dbt_tests,
    )
    
    # Oracle(OP 그룹)에서 데이터 추출하여 PostgreSQL staging 테이블에 저장
    extract_opgroup_task = PythonOperator(
        task_id="extract_oracle_opgroup_to_staging",
        python_callable=extract_oracle_opgroup_to_staging,
    )
    
    # 작업 플로우: IP/PH/OP그룹 추출을 병렬로 실행한 후 dbt run -> dbt test
    [extract_ip_task, extract_ph_task, extract_opgroup_task] >> prepare_vars >> dbt_task >> dbt_test_task
