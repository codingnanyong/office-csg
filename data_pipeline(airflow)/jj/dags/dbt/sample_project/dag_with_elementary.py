"""
sample_project dbt 프로젝트를 실행하는 Airflow DAG (Elementary 데이터 품질 체크 포함)

이 DAG는 sample_project dbt 프로젝트의 모든 모델을 실행하고,
Elementary를 사용하여 데이터 품질을 체크합니다.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# dbt 프로젝트 경로 (Airflow 컨테이너 내 절대 경로)
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/sample_project"

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,  # 데이터 품질 이슈 시 알림
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Profile 설정 (PostgreSQL 사용 예시)
profile_config = ProfileConfig(
    profile_name="sample_project",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",  # Airflow Connection ID
        profile_args={"schema": "public"}
    ),
)

# Execution 설정
execution_config = ExecutionConfig(
    dbt_executable_path="dbt",
)

# Render 설정 (dbt_deps는 operator_args의 install_deps와 일치해야 함)
render_config = RenderConfig(
    dbt_deps=False,  # 별도 태스크로 설치하므로 False
)

# DAG 정의
with DAG(
    dag_id="dbt_sample_project_with_quality",
    default_args=default_args,
    description="sample_project dbt 프로젝트 실행 및 Elementary 데이터 품질 체크",
    schedule_interval=None,  # 수동 실행 (테스트용)
    catchup=False,
    tags=["dbt", "sample_project", "cosmos", "data-quality", "elementary"],
    max_active_runs=1,
) as dag:
    
    # 1. dbt 의존성 설치 (패키지 포함)
    install_deps = BashOperator(
        task_id="install_dbt_dependencies",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt deps --profiles-dir .
        """,
    )
    
    # 2. Elementary 테이블 초기화 및 업데이트
    # Elementary 모델을 실행하여 메타데이터 테이블 생성/업데이트
    # 이미 테이블이 존재하면 업데이트, 없으면 생성됨
    elementary_setup = BashOperator(
        task_id="elementary_setup",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --select elementary --profiles-dir .
        """,
    )
    
    # 3. dbt 모델 실행 (Elementary 모델 제외)
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,  # RenderConfig 명시적 설정
        operator_args={
            "install_deps": False,  # 이미 설치했으므로 False (render_config.dbt_deps와 일치)
            "select": "path:models",  # elementary 모델 제외
        },
    )
    
    # 4. Elementary 데이터 품질 체크 실행
    # 모델 실행 후 최신 데이터를 반영하여 Elementary 메타데이터를 업데이트하고
    # 데이터 품질 테스트를 실행합니다
    elementary_quality_checks = BashOperator(
        task_id="elementary_quality_checks",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --select elementary --profiles-dir . && \
        dbt test --select elementary --profiles-dir .
        """,
    )
    
    # 5. 일반 dbt 테스트 실행
    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt test --select test_type:data --profiles-dir .
        """,
    )
    
    # 태스크 의존성 설정
    install_deps >> elementary_setup >> dbt_transform >> elementary_quality_checks >> dbt_tests
