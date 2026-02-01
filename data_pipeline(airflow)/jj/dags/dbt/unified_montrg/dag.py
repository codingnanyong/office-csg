"""
Unified Monitoring 신규 기능 dbt 프로젝트를 실행하는 Airflow DAG

신규 모니터링/분석 기능들을 dbt 모델로 구현합니다.
증분 처리 및 백필 처리를 지원합니다.
"""

from datetime import datetime, timedelta, timezone, time
from typing import Optional, Dict, Any
import logging
import sys
import json
import subprocess

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# ============================================================================
# 상수 정의
# ============================================================================

# 경로 설정
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/unified_montrg"
if DBT_PROJECT_DIR not in sys.path:
    sys.path.insert(0, DBT_PROJECT_DIR)

# Airflow 설정
INCREMENT_KEY = "last_extract_time_unified_montrg"  # JSON 형태로 모든 모델의 날짜 저장
# JSON 형식: {"mart_productivity_by_op_cd": "20251203", "mart_productivity_by_op_group": "20251203", "mart_productivity_by_ip_zone": "20241201"}
KST_TZ = timezone(timedelta(hours=9))  # KST (UTC+9)
INITIAL_START_DATE = datetime(2024, 1, 1, 6, 30, 0, tzinfo=KST_TZ)  # 06:30:00 시작
DAILY_START_HOUR = 6  # 일일 데이터 시작 시간 (시)
DAILY_START_MINUTE = 30  # 일일 데이터 시작 시간 (분)
DAYS_OFFSET_FOR_INCREMENTAL = 1  # 증분 처리: 실행 날짜 - 1일까지만
DAYS_OFFSET_FOR_BACKFILL = 2  # 백필 처리: 실행 날짜 - 2일까지만

# 모델별 기본 날짜 (새 모델 추가 시 여기에 정의)
MODEL_DEFAULT_DATES = {
    "mart_productivity_by_op_cd": INITIAL_START_DATE.date(),
    "mart_productivity_by_op_group": INITIAL_START_DATE.date(),
    "mart_productivity_by_ip_zone": INITIAL_START_DATE.date(),
    "mart_productivity_by_ph_zone": INITIAL_START_DATE.date(),
    "mart_productivity_by_ip_machine": INITIAL_START_DATE.date(),
    "mart_productivity_by_ph_machine": INITIAL_START_DATE.date(),
    "mart_downtime_by_line": INITIAL_START_DATE.date(),
    "mart_downtime_by_machine": INITIAL_START_DATE.date(),
}

# Incremental DAG에서 실행할 모델 목록 (productivity 모델들만)
INCREMENTAL_MODELS = [
    "mart_productivity_by_op_cd",
    "mart_productivity_by_op_group",
    "mart_productivity_by_ip_zone",
    "mart_productivity_by_ph_zone",
    "mart_productivity_by_ip_machine",
    "mart_productivity_by_ph_machine",
    "mart_downtime_by_line",
    "mart_downtime_by_machine",
]

# Backfill DAG에서 실행할 모델 목록 (select 옵션과 일치해야 함)
BACKFILL_MODELS = [
    "mart_productivity_by_op_cd",
    "mart_productivity_by_op_group",
    "mart_productivity_by_ip_zone",
    "mart_productivity_by_ph_zone",
    "mart_productivity_by_ip_machine",
    "mart_productivity_by_ph_machine",
    "mart_downtime_by_line",
    "mart_downtime_by_machine",
]  # downtime 모델들만 backfill

# 데이터베이스 설정
# 주의: 실제 연결은 profiles.yml의 target 설정에 따라 결정됨
POSTGRES_CONN_ID = "pg_jj_unified_montrg_dw"  # 기본 Connection (실제로는 profiles.yml의 target이 우선)
SCHEMA = "silver"  # dbt 모델 저장 스키마

# DAG 기본 설정
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  # timezone-naive (UTC 기준)
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# dbt Profile 설정
# unified_montrg_realtime과 동일하게 profile_mapping 사용
# profiles.yml 파일을 직접 사용하려면 profiles_yml_filepath를 사용할 수 있지만,
# profile_mapping이 더 안정적이고 권장되는 방법입니다
PROFILE_CONFIG = ProfileConfig(
    profile_name="unified_montrg",
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
# 날짜 범위 계산 함수
# ============================================================================

def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string and ensure timezone is set."""
    from dateutil.parser import parse
    dt = parse(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST_TZ)
    return dt


def get_model_date_from_variable(model_name: str) -> Optional[datetime.date]:
    """Variable에서 특정 모델의 마지막 처리 날짜를 가져옴 (JSON 형태 또는 기존 단일 날짜).
    
    기존 단일 날짜 문자열 형식도 자동으로 JSON으로 마이그레이션합니다.
    
    Args:
        model_name: 모델 이름 (예: "mart_productivity_by_op_cd")
    
    Returns:
        마지막 처리 날짜 (date 객체), 없으면 None
    """
    try:
        var_value = Variable.get(INCREMENT_KEY, default_var=None)
        if not var_value:
            return None
        
        # 기존 단일 날짜 문자열 형식인지 확인 (yyyyMMdd 또는 yyyy-mm-dd)
        if isinstance(var_value, str):
            # JSON 형식인지 확인 (중괄호로 시작)
            if var_value.strip().startswith('{'):
                try:
                    var_dict = json.loads(var_value)
                except json.JSONDecodeError:
                    # JSON 파싱 실패 시 기존 형식으로 처리
                    var_dict = None
            else:
                # 기존 단일 날짜 문자열 형식
                var_dict = None
        else:
            var_dict = var_value
        
        # 기존 단일 날짜 형식인 경우, 모든 모델에 동일한 날짜 적용하고 JSON으로 마이그레이션
        if var_dict is None:
            # 기존 날짜 파싱
            if isinstance(var_value, str) and len(var_value) == 8 and var_value.isdigit():
                # yyyyMMdd 형식
                old_date = datetime.strptime(var_value, "%Y%m%d").date()
            else:
                # 기존 형식 (yyyy-mm-dd 또는 datetime string)
                old_date = parse_datetime(var_value).date()
            
            # 모든 모델에 동일한 날짜로 JSON 생성 및 저장 (마이그레이션)
            var_dict = {}
            for model in MODEL_DEFAULT_DATES.keys():
                var_dict[model] = old_date.strftime("%Y%m%d")
            Variable.set(INCREMENT_KEY, json.dumps(var_dict, ensure_ascii=False))
            logging.info(f"🔄 Variable 마이그레이션 완료: 기존 날짜 '{old_date}'를 JSON 형식으로 변환")
        
        # 모델별 날짜 가져오기
        model_date_str = var_dict.get(model_name)
        if not model_date_str:
            return None
        
        # yyyyMMdd 형식 파싱
        if isinstance(model_date_str, str) and len(model_date_str) == 8 and model_date_str.isdigit():
            return datetime.strptime(model_date_str, "%Y%m%d").date()
        else:
            # 기존 형식 (yyyy-mm-dd)
            return parse_datetime(model_date_str).date()
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logging.warning(f"⚠️ Variable 파싱 오류 (모델: {model_name}): {e}")
        return None


def update_model_date_in_variable(model_name: str, processed_date: datetime.date) -> None:
    """Variable에 특정 모델의 처리 날짜를 업데이트 (JSON 형태).
    
    Args:
        model_name: 모델 이름 (예: "mart_productivity_by_op_cd")
        processed_date: 처리 완료 날짜
    """
    try:
        # 기존 Variable 가져오기
        var_value = Variable.get(INCREMENT_KEY, default_var=None)
        if var_value:
            if isinstance(var_value, str):
                var_dict = json.loads(var_value)
            else:
                var_dict = var_value
        else:
            var_dict = {}
        
        # 모델별 날짜 업데이트 (yyyyMMdd 형식)
        var_dict[model_name] = processed_date.strftime("%Y%m%d")
        
        # Variable 저장
        Variable.set(INCREMENT_KEY, json.dumps(var_dict, ensure_ascii=False))
        logging.info(f"✅ Variable '{INCREMENT_KEY}' 업데이트: {model_name} = {processed_date.strftime('%Y%m%d')}")
    except Exception as e:
        logging.error(f"❌ Variable 업데이트 오류 (모델: {model_name}): {e}")
        raise


def get_incremental_date_range(**context) -> Optional[Dict[str, str]]:
    """증분 처리용 날짜 범위 계산 - Variable 날짜부터 다음날까지 처리.
    
    Variable에 저장된 날짜를 기준으로, 해당 날짜 06:30:00부터 다음날 06:30:00까지의 데이터를 처리합니다.
    예: Variable이 2025-12-03이면, 2025-12-04 값을 계산하기 위해 2025-12-03 06:30:00 ~ 2025-12-04 06:30:00 범위를 처리합니다.
    최대 (실제 오늘 날짜 - 1일)까지만 처리합니다.
    실행 시점과 무관하게, 실제 오늘 날짜(KST) 기준으로 계산됩니다.
    일요일 휴무일 등 데이터가 없어도 0 값으로 레코드를 생성합니다.
    
    Returns:
        Dictionary with 'start_date' and 'end_date' strings (yyyyMMdd format), or None if no data to process
    """
    # DAG 실행 날짜 기준으로 계산 (data_interval_start 사용, 없으면 execution_date)
    dag_run_date = context.get('data_interval_start') or context.get('execution_date')
    
    if dag_run_date:
        # data_interval_start/execution_date가 timezone-aware가 아니면 KST로 설정
        if isinstance(dag_run_date, str):
            dag_run_date = parse_datetime(dag_run_date)
        if dag_run_date.tzinfo is None:
            dag_run_date = dag_run_date.replace(tzinfo=KST_TZ)
        # UTC에서 KST로 변환 (필요시)
        if dag_run_date.tzinfo != KST_TZ:
            dag_run_date = dag_run_date.astimezone(KST_TZ)
        execution_date_kst = dag_run_date.date()
    else:
        # Fallback: 현재 시간 기준 (하지만 이 경우는 드묾)
        execution_date_kst = datetime.now(KST_TZ).date()
    
    logging.info(f"📅 DAG 실행 날짜 (KST): {execution_date_kst}")
    
    # Incremental DAG에서 실행할 모델들의 최소 날짜를 찾아서 처리 (가장 뒤처진 모델 기준)
    # Incremental DAG는 INCREMENTAL_MODELS에 정의된 모델들만 실행하므로, 해당 모델들의 가장 오래된 날짜부터 처리
    all_model_dates = {}
    min_date = None
    
    for model_name in INCREMENTAL_MODELS:
        model_date = get_model_date_from_variable(model_name)
        if model_date:
            all_model_dates[model_name] = model_date
            if min_date is None or model_date < min_date:
                min_date = model_date
        else:
            # Variable에 없으면 기본 날짜 사용
            default_date = MODEL_DEFAULT_DATES[model_name]
            all_model_dates[model_name] = default_date
            if min_date is None or default_date < min_date:
                min_date = default_date
    
    if min_date is None:
        min_date = INITIAL_START_DATE.date()
    
    logging.info(f"📅 모델별 날짜: {all_model_dates}")
    logging.info(f"📅 최소 날짜 (처리 기준): {min_date}")
    
    # 처리할 날짜: 최소 날짜 그대로 사용 (다음날이 아님)
    # min_date가 2025-12-03이면, 2025-12-04 값을 계산하기 위해 2025-12-03 ~ 2025-12-04 범위 처리
    target_date = min_date
    
    # 최대 제한: (실제 오늘 날짜 - 1일)까지만 처리
    # 실행 시점과 무관하게, 실제 오늘 날짜 기준으로 계산
    today_kst = datetime.now(KST_TZ).date()
    max_end_date = today_kst - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)
    
    logging.info(f"📅 오늘 날짜 (KST): {today_kst}, 최대 처리 가능 날짜: {max_end_date}")
    
    # target_date가 max_end_date보다 크면 처리할 데이터 없음
    # Variable이 2025-12-04이고 max_end_date가 2025-12-04이면, 2025-12-04 데이터를 처리할 수 있음
    if target_date > max_end_date:
        logging.info(f"⚠️ 처리할 데이터 없음: target_date({target_date}) > max_end_date({max_end_date})")
        return None
    
    # 06:30:00 기준으로 날짜 범위 설정
    # Variable이 2025-12-03이면, 2025-12-03 06:30:00 ~ 2025-12-04 06:30:00
    start_date = datetime.combine(target_date, time(DAILY_START_HOUR, DAILY_START_MINUTE)).replace(tzinfo=KST_TZ)
    end_date = start_date + timedelta(days=1)
    
    logging.info(f"📅 증분 처리: {target_date} 06:30:00 ~ {target_date + timedelta(days=1)} 06:30:00 데이터 처리 (최소 날짜: {min_date}, DAG 실행 날짜: {execution_date_kst})")
    
    # yyyyMMdd 형식으로 반환
    return {
        "start_date": target_date.strftime("%Y%m%d"),
        "end_date": (target_date + timedelta(days=1)).strftime("%Y%m%d")
    }


def get_backfill_date_range(**context) -> Optional[Dict[str, str]]:
    """백필 처리용 날짜 범위 계산 - 초기 날짜부터 (실행 날짜-2일)까지.
    
    Backfill DAG는 select된 모델만 실행하므로, 해당 모델의 Variable만 사용.
    모델 이름은 dbt select에서 추출하거나, 기본값으로 mart_productivity_by_ip_zone 사용.
    
    Returns:
        Dictionary with 'backfill_start_date' and 'backfill_end_date' strings (yyyyMMdd format), or None if no data to process
    """
    # DAG 실행 날짜 기준으로 계산
    dag_run_date = context.get('data_interval_start') or context.get('execution_date')
    
    if dag_run_date:
        if isinstance(dag_run_date, str):
            dag_run_date = parse_datetime(dag_run_date)
        if dag_run_date.tzinfo is None:
            dag_run_date = dag_run_date.replace(tzinfo=KST_TZ)
        if dag_run_date.tzinfo != KST_TZ:
            dag_run_date = dag_run_date.astimezone(KST_TZ)
        execution_date_kst = dag_run_date.date()
    else:
        execution_date_kst = datetime.now(KST_TZ).date()
    
    # Backfill DAG는 BACKFILL_MODELS에 정의된 모델들만 실행
    # 모든 모델의 최소 날짜를 찾아서 처리 (가장 뒤처진 모델 기준)
    all_model_dates = {}
    min_date = None
    min_model_name = None
    
    for model_name in BACKFILL_MODELS:
        model_date = get_model_date_from_variable(model_name)
        if not model_date:
            # Variable이 없으면 기본 날짜 사용
            model_date = MODEL_DEFAULT_DATES.get(model_name, INITIAL_START_DATE.date())
            logging.info(f"📅 Variable이 없음. 모델 '{model_name}' 기본 날짜 사용: {model_date}")
        else:
            logging.info(f"📅 Variable에서 모델 '{model_name}' 마지막 처리 날짜 가져옴: {model_date}")
        
        all_model_dates[model_name] = model_date
        if min_date is None or model_date < min_date:
            min_date = model_date
            min_model_name = model_name
    
    if min_date is None:
        min_date = INITIAL_START_DATE.date()
        min_model_name = BACKFILL_MODELS[0] if BACKFILL_MODELS else None
    
    # 종료 날짜: (실행 날짜 - 2일)
    end_date = execution_date_kst - timedelta(days=DAYS_OFFSET_FOR_BACKFILL)
    
    if min_date >= end_date:
        logging.info(f"⚠️ 처리할 데이터 없음: {min_date} >= {end_date}")
        return None
    
    logging.info(f"📅 백필 처리 범위 (모델: {BACKFILL_MODELS}, 최소 날짜: {min_date} from {min_model_name}): {min_date} ~ {end_date}")
    
    return {
        "backfill_start_date": min_date.strftime("%Y%m%d"),
        "backfill_end_date": end_date.strftime("%Y%m%d"),
        "model_names": BACKFILL_MODELS  # 모델 이름 목록 반환
    }


# ============================================================================
# Variable 업데이트 함수
# ============================================================================

def update_variable_after_run(**context) -> None:
    """처리 완료 후 Variable 업데이트 (Incremental).
    
    Incremental DAG는 INCREMENTAL_MODELS에 정의된 모델들만 실행하므로, 해당 모델들의 날짜만 업데이트합니다.
    처리한 날짜의 다음날로 각 모델의 Variable을 업데이트합니다.
    예: 2025-12-03 ~ 2025-12-04를 처리했다면, productivity 모델들의 Variable을 2025-12-04로 업데이트
    데이터가 없어도 (예: 일요일 휴무) Variable을 업데이트하여 다음날 정상 처리되도록 함.
    """
    date_range = context['ti'].xcom_pull(task_ids='get_date_range')
    if date_range:
        # 처리한 날짜는 end_date (다음날 날짜)
        # yyyyMMdd 형식으로 반환되므로 파싱
        end_date_str = date_range["end_date"]
        if len(end_date_str) == 8 and end_date_str.isdigit():
            # yyyyMMdd 형식
            processed_date = datetime.strptime(end_date_str, "%Y%m%d").date()
        else:
            # 기존 형식 (yyyy-mm-dd)
            processed_date = parse_datetime(end_date_str).date()
        
        # Incremental DAG에서 실행한 모델들의 날짜만 업데이트
        for model_name in INCREMENTAL_MODELS:
            update_model_date_in_variable(model_name, processed_date)
        
        logging.info(f"✅ Incremental 모델들 Variable 업데이트 완료: {processed_date.strftime('%Y%m%d')}")
    else:
        # 날짜 범위가 없으면 Variable 업데이트하지 않음 (처리할 데이터가 없었음)
        logging.warning(f"⚠️ 날짜 범위가 없어 Variable을 업데이트하지 않음")


def update_backfill_variable(**context) -> None:
    """백필 처리 완료 후 Variable 업데이트.
    
    Backfill DAG는 BACKFILL_MODELS에 정의된 모델들만 실행하므로, 해당 모델들의 Variable만 업데이트합니다.
    데이터가 없어도 (예: 일요일 휴무) Variable을 업데이트하여 다음날 정상 처리되도록 함.
    """
    date_range = context['ti'].xcom_pull(task_ids='get_backfill_date_range')
    if date_range:
        # 모델 이름 목록 가져오기
        model_names = date_range.get("model_names", BACKFILL_MODELS)
        
        # yyyyMMdd 형식으로 파싱
        end_date_str = date_range["backfill_end_date"]
        if len(end_date_str) == 8 and end_date_str.isdigit():
            processed_date = datetime.strptime(end_date_str, "%Y%m%d").date()
        else:
            processed_date = parse_datetime(end_date_str).date()
        
        # 모든 backfill 모델의 Variable 업데이트
        for model_name in model_names:
            update_model_date_in_variable(model_name, processed_date)
        
        logging.info(f"✅ Backfill 모델들 Variable 업데이트 완료: {model_names} = {processed_date.strftime('%Y%m%d')}")
    else:
        logging.warning(f"⚠️ 날짜 범위가 없어 Variable을 업데이트하지 않음")


# ============================================================================
# DAG 정의
# ============================================================================

# Incremental DAG
with DAG(
    dag_id="dbt_unified_montrg_incremental",
    default_args=DEFAULT_ARGS,
    description="Unified Monitoring 신규 기능 - 증분 처리",
    schedule_interval="30 0 * * *",  # 매일 00:30 (UTC) 실행 
    catchup=False,
    tags=["dbt", "unified_montrg", "monitoring", "incremental"],
) as incremental_dag:
    
    get_date_range = PythonOperator(
        task_id="get_date_range",
        python_callable=get_incremental_date_range,
    )
    
    def prepare_dbt_vars(**context):
        """dbt 실행에 필요한 변수 준비."""
        date_range = context['ti'].xcom_pull(task_ids='get_date_range')
        if date_range:
            return {
                "start_date": date_range["start_date"],
                "end_date": date_range["end_date"]
            }
        return {}
    
    prepare_vars = PythonOperator(
        task_id="prepare_dbt_vars",
        python_callable=prepare_dbt_vars,
    )
    
    dbt_task = DbtTaskGroup(
        group_id="dbt_unified_montrg",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        operator_args={
            "vars": "{{ ti.xcom_pull(task_ids='prepare_dbt_vars') }}",
            "select": INCREMENTAL_MODELS,  # Productivity 모델들만 실행
        },
    )
    
    # dbt test 실행 (모델 실행 후 테스트)
    def run_dbt_test(**context):
        """dbt test 실행 (출력 로깅 포함)"""
        import subprocess
        test_models_str = ' '.join(INCREMENTAL_MODELS)
        cmd = [
            "dbt", "test",
            "--profiles-dir", DBT_PROJECT_DIR,
            "--profile", "unified_montrg",
            "--target", "dev",
            "--select", test_models_str
        ]
        
        logging.info(f"🔍 dbt test 실행: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                cwd=DBT_PROJECT_DIR,
                capture_output=True,
                text=True,
                check=False  # 실패해도 예외 발생시키지 않음
            )
            
            # 출력 로깅
            if result.stdout:
                logging.info(f"✅ dbt test stdout:\n{result.stdout}")
            if result.stderr:
                logging.warning(f"⚠️ dbt test stderr:\n{result.stderr}")
            
            # 반환 코드 확인
            if result.returncode != 0:
                logging.error(f"❌ dbt test 실패 (return code: {result.returncode})")
                # 테스트 실패 시에도 계속 진행 (선택사항)
                # raise Exception(f"dbt test 실패: {result.stderr}")
            else:
                logging.info("✅ dbt test 성공")
                
            return {"returncode": result.returncode, "output": result.stdout}
        except Exception as e:
            logging.error(f"❌ dbt test 실행 중 오류: {e}")
            raise
    
    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_test,
    )
    
    update_var = PythonOperator(
        task_id="update_variable",
        python_callable=update_variable_after_run,
    )
    
    get_date_range >> prepare_vars >> dbt_task >> dbt_test >> update_var


# Backfill DAG
with DAG(
    dag_id="dbt_unified_montrg_backfill",
    default_args=DEFAULT_ARGS,
    description="Unified Monitoring 신규 기능 - 백필 처리 (초기 날짜부터 오늘-2일까지)",
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["dbt", "unified_montrg", "monitoring", "backfill"],
) as backfill_dag:
    
    get_backfill_range = PythonOperator(
        task_id="get_backfill_date_range",
        python_callable=get_backfill_date_range,
    )
    
    def prepare_backfill_vars(**context):
        """dbt 실행에 필요한 변수 준비."""
        date_range = context['ti'].xcom_pull(task_ids='get_backfill_date_range')
        if date_range:
            return {
                "backfill_start_date": date_range["backfill_start_date"],
                "backfill_end_date": date_range["backfill_end_date"]
            }
        return {}
    
    prepare_backfill_vars_task = PythonOperator(
        task_id="prepare_backfill_vars",
        python_callable=prepare_backfill_vars,
    )
    
    dbt_backfill_task = DbtTaskGroup(
        group_id="dbt_unified_montrg_backfill",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        operator_args={
            "vars": "{{ ti.xcom_pull(task_ids='prepare_backfill_vars') }}",
            "full_refresh": True,
            # BACKFILL_MODELS에 정의된 모델들만 backfill 실행
            "select": BACKFILL_MODELS,  # 새 모델만 실행
        },
    )
    
    # dbt test 실행 (모델 실행 후 테스트)
    def run_dbt_backfill_test(**context):
        """dbt test 실행 (출력 로깅 포함)"""
        import subprocess
        test_models_str = ' '.join(BACKFILL_MODELS)
        cmd = [
            "dbt", "test",
            "--profiles-dir", DBT_PROJECT_DIR,
            "--profile", "unified_montrg",
            "--target", "dev",
            "--select", test_models_str
        ]
        
        logging.info(f"🔍 dbt test 실행 (backfill): {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                cwd=DBT_PROJECT_DIR,
                capture_output=True,
                text=True,
                check=False  # 실패해도 예외 발생시키지 않음
            )
            
            # 출력 로깅
            if result.stdout:
                logging.info(f"✅ dbt test stdout:\n{result.stdout}")
            if result.stderr:
                logging.warning(f"⚠️ dbt test stderr:\n{result.stderr}")
            
            # 반환 코드 확인
            if result.returncode != 0:
                logging.error(f"❌ dbt test 실패 (return code: {result.returncode})")
                # 테스트 실패 시에도 계속 진행 (선택사항)
                # raise Exception(f"dbt test 실패: {result.stderr}")
            else:
                logging.info("✅ dbt test 성공")
                
            return {"returncode": result.returncode, "output": result.stdout}
        except Exception as e:
            logging.error(f"❌ dbt test 실행 중 오류: {e}")
            raise
    
    dbt_backfill_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_backfill_test,
    )
    
    update_backfill_var = PythonOperator(
        task_id="update_backfill_variable",
        python_callable=update_backfill_variable,
    )
    
    get_backfill_range >> prepare_backfill_vars_task >> dbt_backfill_task >> dbt_backfill_test >> update_backfill_var

