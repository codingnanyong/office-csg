"""
sample_project dbt 프로젝트를 실행하는 Airflow DAG

이 DAG는 sample_project dbt 프로젝트의 모든 모델을 실행합니다.
프로젝트 단위로 관리되므로 이 DAG는 sample_project 디렉토리 내에 위치합니다.
"""

from datetime import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os

# dbt 프로젝트 경로 (Airflow 컨테이너 내 절대 경로)
# 현재 파일이 /opt/airflow/dags/dbt/sample_project/dag.py에 위치하므로
# 프로젝트 루트는 /opt/airflow/dags/dbt/sample_project
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/sample_project"

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 300,  # 5분
}

# Profile 설정 (PostgreSQL 사용 예시)
# Airflow Connection ID를 사용하여 데이터베이스 연결 정보를 가져옵니다
profile_config = ProfileConfig(
    profile_name="sample_project",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",  # Airflow Connection ID (Admin → Connections에서 설정 필요)
        profile_args={"schema": "public"}  # 기본 public 스키마 사용
    ),
)

# Execution 설정
execution_config = ExecutionConfig(
    dbt_executable_path="dbt",  # dbt가 PATH에 있는 경우
    # install_deps=True,  # dbt 의존성 자동 설치 (필요 시 주석 해제)
)

# DAG 정의
with DAG(
    dag_id="dbt_sample_project",  # 프로젝트 이름 기반 DAG ID
    default_args=default_args,
    description="sample_project dbt 프로젝트 실행 파이프라인",
    schedule_interval=None,  # 수동 실행 (테스트용)
    catchup=False,  # 과거 작업 실행 안 함
    tags=["dbt", "sample_project", "cosmos"],
    max_active_runs=1,  # 동시 실행 제한
) as dag:
    
    # dbt TaskGroup 생성
    # 현재 디렉토리를 dbt 프로젝트 경로로 사용
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,  # 현재 디렉토리 (자동 감지)
            # dbt_project_name="sample_project",  # 프로젝트 이름 (선택사항)
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "install_deps": True,  # dbt 의존성 자동 설치
        },
    )

