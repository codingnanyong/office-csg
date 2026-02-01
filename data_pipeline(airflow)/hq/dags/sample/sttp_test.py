from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
import os
from pathlib import Path
import subprocess
import tempfile

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sftp_test_dag',
    default_args=default_args,
    description='SFTP 연결 및 파일 다운로드 테스트',
    schedule_interval=None,
    catchup=False,
    tags=['sftp', 'test'],
)

def test_sftp_connection():
    """SFTP 연결을 테스트하는 함수"""
    try:
        # Airflow의 SFTP Hook 사용
        sftp_hook = SFTPHook(ftp_conn_id='test_dev_local')
        
        print("SFTP 서버에 연결 중...")
        with sftp_hook.get_conn() as sftp:
            print("SFTP 연결 성공!")
            
            # 현재 디렉토리 내용 나열 (pwd 대신 listdir 사용)
            print("현재 디렉토리 내용:")
            try:
                for item in sftp.listdir():
                    print(f"  - {item}")
            except Exception as e:
                print(f"  디렉토리 읽기 오류: {e}")
            
            return "SFTP 연결 테스트 성공"
            
    except Exception as e:
        print(f"SFTP 연결 실패: {e}")
        raise

def download_sftp_files():
    """SFTP에서 파일을 다운로드하는 함수"""
    try:
        sftp_hook = SFTPHook(ftp_conn_id='test_dev_local')
        
        print("SFTP 연결 성공, 파일 다운로드 시작...")
        
        # 다운로드할 파일 목록 - Downloads/config_mqtt2prometheus.yaml 파일
        remote_files = [
            'Downloads/config_mqtt2prometheus.yaml',
        ]
        
        # 로컬 다운로드 디렉토리 - DAG 파일과 같은 디렉토리 사용 (호스트와 공유됨)
        download_dir = Path("/opt/airflow/dags/sample")
        download_dir.mkdir(exist_ok=True)
        
        downloaded_files = []
        
        for remote_file in remote_files:
            try:
                # 파일명 추출
                filename = os.path.basename(remote_file)
                local_path = download_dir / filename
                
                print(f"다운로드 중: {remote_file} -> {local_path}")
                
                # 파일 다운로드
                sftp_hook.retrieve_file(remote_file, str(local_path))
                
                # 파일 정보 확인
                if sftp_hook.path_exists(remote_file):
                    print(f"  - 파일 다운로드 성공: {local_path}")
                    
                    # 로컬 파일 크기 확인
                    if local_path.exists():
                        file_size = local_path.stat().st_size
                        print(f"  - 다운로드된 파일 크기: {file_size} bytes")
                        
                        # 파일 내용 일부 확인 (디버깅용)
                        try:
                            with open(local_path, 'r', encoding='utf-8') as f:
                                first_line = f.readline().strip()
                                print(f"  - 파일 첫 줄: {first_line}")
                        except Exception as e:
                            print(f"  - 파일 읽기 오류: {e}")
                    else:
                        print(f"  - 경고: 파일이 존재하지 않습니다: {local_path}")
                
                downloaded_files.append(str(local_path))
                
            except Exception as e:
                print(f"파일 다운로드 실패 {remote_file}: {e}")
        
        if not remote_files:
            print("다운로드할 파일이 지정되지 않았습니다.")
            print("remote_files 리스트에 실제 파일 경로를 추가하세요.")
        
        return f"다운로드 완료: {len(downloaded_files)} 개 파일"
        
    except Exception as e:
        print(f"파일 다운로드 중 오류: {e}")
        raise

def list_sftp_directory(remote_path='.'):
    """SFTP 디렉토리의 내용을 나열하는 함수"""
    try:
        sftp_hook = SFTPHook(ftp_conn_id='test_dev_local')
        
        print(f"디렉토리 내용 나열: {remote_path}")
        
        # 디렉토리 내용 가져오기
        files = sftp_hook.list_directory(remote_path)
        
        print(f"\n디렉토리: {remote_path}")
        print("-" * 80)
        print(f"{'이름':<50} {'타입'}")
        print("-" * 80)
        
        # 파일 정보 디버깅
        print(f"파일 목록 타입: {type(files)}")
        print(f"파일 목록: {files}")
        
        # 파일 목록이 문자열 리스트인 경우
        if isinstance(files, list) and len(files) > 0 and isinstance(files[0], str):
            for filename in files:
                print(f"{filename:<50} FILE")
        else:
            # 딕셔너리 형태인 경우
            for file_info in files:
                if isinstance(file_info, dict):
                    file_type = 'DIR' if file_info.get('type') == 'dir' else 'FILE'
                    print(f"{file_info.get('name', 'Unknown'):<50} {file_type}")
                else:
                    print(f"{str(file_info):<50} UNKNOWN")
        
        return f"디렉토리 내용 나열 완료: {len(files)} 개 항목"
        
    except Exception as e:
        print(f"디렉토리 나열 중 오류: {e}")
        raise

def upload_test_file():
    """테스트용 파일을 SFTP 서버에 업로드하는 함수"""
    try:
        sftp_hook = SFTPHook(ftp_conn_id='test_dev_local')
        
        # 테스트 파일 생성
        test_content = f"SFTP 테스트 파일 - 생성 시간: {datetime.now()}"
        test_file_path = "/tmp/sftp_test_upload.txt"
        
        print(f"테스트 파일 생성 중: {test_file_path}")
        with open(test_file_path, 'w') as f:
            f.write(test_content)
        
        # 파일이 실제로 생성되었는지 확인
        if os.path.exists(test_file_path):
            file_size = os.path.getsize(test_file_path)
            print(f"테스트 파일 생성 성공: {test_file_path} (크기: {file_size} bytes)")
        else:
            print(f"테스트 파일 생성 실패: {test_file_path}")
            raise FileNotFoundError(f"테스트 파일을 생성할 수 없습니다: {test_file_path}")
        
        # SFTP 서버에 업로드 (Downloads 폴더에 업로드)
        remote_path = "Downloads/sftp_test_upload.txt"
        print(f"파일 업로드 중: {test_file_path} -> {remote_path}")
        
        # SFTP 연결을 직접 사용하여 파일 업로드
        with sftp_hook.get_conn() as sftp:
            sftp.put(test_file_path, remote_path)
        
        print("파일 업로드 성공!")
        
        # 업로드된 파일 확인
        if sftp_hook.path_exists(remote_path):
            print("업로드된 파일이 서버에 존재합니다.")
        else:
            print("업로드된 파일이 서버에 존재하지 않습니다.")
        
        return "파일 업로드 테스트 성공"
        
    except Exception as e:
        print(f"파일 업로드 중 오류: {e}")
        raise

def verify_downloaded_file():
    """다운로드된 파일을 확인하는 함수"""
    try:
        file_path = Path("/opt/airflow/dags/sample/config_mqtt2prometheus.yaml")
        
        if file_path.exists():
            print(f"파일 확인 성공: {file_path}")
            
            # 파일 정보 출력
            file_stat = file_path.stat()
            print(f"  - 파일 크기: {file_stat.st_size} bytes")
            print(f"  - 수정 시간: {datetime.fromtimestamp(file_stat.st_mtime)}")
            
            # 파일 내용 일부 읽기 (처음 10줄)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()[:10]
                    print(f"  - 파일 내용 (처음 10줄):")
                    for i, line in enumerate(lines, 1):
                        print(f"    {i:2d}: {line.rstrip()}")
                    
                    if len(lines) == 10:
                        print("    ... (더 많은 내용이 있습니다)")
                        
            except Exception as e:
                print(f"  - 파일 읽기 오류: {e}")
            
            return "파일 확인 완료"
        else:
            print(f"파일이 존재하지 않습니다: {file_path}")
            return "파일 확인 실패 - 파일 없음"
            
    except Exception as e:
        print(f"파일 확인 중 오류: {e}")
        raise

def copy_to_host():
    """파일이 이미 호스트에 저장되었는지 확인하는 함수"""
    try:
        print("호스트 파일 확인 중...")
        
        file_path = Path("/opt/airflow/dags/sample/config_mqtt2prometheus.yaml")
        
        if file_path.exists():
            file_size = file_path.stat().st_size
            print(f"호스트 파일 확인 성공: {file_path}")
            print(f"파일 크기: {file_size} bytes")
            
            # 파일 내용 일부 확인
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    print(f"파일 첫 줄: {first_line}")
            except Exception as e:
                print(f"파일 읽기 오류: {e}")
            
            return "호스트 파일 확인 완료"
        else:
            print("호스트에 파일이 존재하지 않습니다.")
            return "호스트 파일 없음"
            
    except Exception as e:
        print(f"호스트 파일 확인 중 오류: {e}")
        raise

def transfer_to_external_server():
    """SFTP를 사용하여 외부 Linux 서버로 파일을 전송하는 함수"""
    try:
        print("SFTP를 사용한 외부 서버 파일 전송 시작...")
        
        # 외부 서버 SFTP 연결 (Airflow connection 사용)
        external_sftp_hook = SFTPHook(ftp_conn_id='external_server_sftp')
        
        # 로컬 파일 경로 (다운로드된 파일)
        local_file_path = "/opt/airflow/dags/sample/config_mqtt2prometheus.yaml"
        
        # 외부 서버의 대상 경로
        remote_file_path = "/home/remote_user/apps/config_mqtt2prometheus.yaml"
        
        print(f"파일 전송 중: {local_file_path} -> {remote_file_path}")
        
        # 파일 업로드
        external_sftp_hook.store_file(local_file_path, remote_file_path)
        
        # 전송 확인
        if external_sftp_hook.path_exists(remote_file_path):
            print("외부 서버 전송 성공!")
            return "외부 서버 전송 완료"
        else:
            print("외부 서버 전송 실패 - 파일이 존재하지 않습니다.")
            return "외부 서버 전송 실패"
            
    except Exception as e:
        print(f"외부 서버 전송 중 오류: {e}")
        raise

# DAG 태스크 정의
test_connection_task = PythonOperator(
    task_id='test_sftp_connection',
    python_callable=test_sftp_connection,
    dag=dag,
)

list_directory_task = PythonOperator(
    task_id='list_sftp_directory',
    python_callable=list_sftp_directory,
    dag=dag,
)

upload_test_task = PythonOperator(
    task_id='upload_test_file',
    python_callable=upload_test_file,
    dag=dag,
)

download_files_task = PythonOperator(
    task_id='download_sftp_files',
    python_callable=download_sftp_files,
    dag=dag,
)

verify_file_task = PythonOperator(
    task_id='verify_downloaded_file',
    python_callable=verify_downloaded_file,
    dag=dag,
)

# 새로운 태스크 추가
copy_to_host_task = PythonOperator(
    task_id='copy_to_host',
    python_callable=copy_to_host,
    dag=dag,
)

# 외부 서버 전송 태스크 추가
transfer_to_external_task = PythonOperator(
    task_id='transfer_to_external_server',
    python_callable=transfer_to_external_server,
    dag=dag,
)

# 태스크 의존성 수정 - 모든 태스크 실행
test_connection_task >> list_directory_task >> upload_test_task >> download_files_task >> verify_file_task >> copy_to_host_task >> transfer_to_external_task
