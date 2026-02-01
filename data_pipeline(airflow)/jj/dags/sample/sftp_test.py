from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
import os
from pathlib import Path
import subprocess
import tempfile
import shutil

# 상수 정의
DOWNLOAD_DIR = "/opt/airflow/dags/downloads"
TARGET_PATH = "/media/btx/process/os/osr/banbury/unit03/raws"
SFTP_DOWNLOAD_CONN = 'sftp_jj_banbury_03'
SFTP_UPLOAD_CONN = 'sftp_jj_fdw'

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

def create_completion_report(report_type, timestamp, source, target, files, status="SUCCESS", **kwargs):
    """완료 보고서를 생성하는 공통 함수"""
    report_content = f"""# {report_type} Completion Report
# Generated at: {timestamp}
# Source: {source}
# Target: {target}

"""
    
    if files:
        report_content += f"# {report_type} Files:\n"
        for file_path in files:
            if report_type == "Download":
                file_name = os.path.basename(file_path)
                try:
                    file_size = os.path.getsize(file_path)
                    report_content += f"- {file_name} ({file_size} bytes, {file_size / 1024 / 1024:.2f} MB)\n"
                except:
                    report_content += f"- {file_name}\n"
            else:
                file_name = os.path.basename(file_path)
                report_content += f"- {file_name}\n"
    
    report_content += f"""
# Status: {status}
# Total files {report_type.lower()}ed: {len(files) if files else 0}
"""
    
    # 추가 정보가 있다면 추가
    for key, value in kwargs.items():
        if value:
            report_content += f"# {key}: {value}\n"
    
    return report_content

def upload_completion_report(sftp_hook, report_content, report_type, timestamp):
    """완료 보고서를 SFTP에 업로드하는 공통 함수"""
    try:
        # 로컬에 임시 파일 생성
        temp_file_path = f"/tmp/{report_type.lower()}_completion_{timestamp}.txt"
        with open(temp_file_path, 'w') as f:
            f.write(report_content)
        
        # SFTP를 통해 /mi/ 경로에 업로드
        remote_file_path = f"/mi/{report_type.lower()}_completion_{timestamp}.txt"
        
        with sftp_hook.get_conn() as sftp:
            sftp.put(temp_file_path, remote_file_path)
            print(f"{report_type} 완료 보고서 생성: {remote_file_path}")
        
        # 임시 파일 삭제
        os.remove(temp_file_path)
        return True
        
    except Exception as e:
        print(f"{report_type} 완료 보고서 생성 실패: {e}")
        return False

def test_sftp_connections():
    """두 SFTP 연결을 모두 테스트하는 함수"""
    try:
        # sftp_jj_banbury_03 연결 테스트 (다운로드용)
        print("=== sftp_jj_banbury_03 연결 테스트 (다운로드용) ===")
        sftp_dev_hook = SFTPHook(ftp_conn_id=SFTP_DOWNLOAD_CONN)
        
        with sftp_dev_hook.get_conn() as sftp:
            print("sft_dev SFTP 연결 성공!")
            print("현재 디렉토리 내용:")
            try:
                for item in sftp.listdir():
                    print(f"  - {item}")
            except Exception as e:
                print(f"  디렉토리 읽기 오류: {e}")
        
        # sftp_jj_fdw 연결 테스트 (업로드용)
        print("\n=== sftp_jj_fdw 연결 테스트 (업로드용) ===")
        sftp_jj_hook = SFTPHook(ftp_conn_id=SFTP_UPLOAD_CONN)
        
        with sftp_jj_hook.get_conn() as sftp:
            print("sftp_jj_fdw SFTP 연결 성공!")
            
            # 타겟 디렉토리 확인
            print(f"타겟 디렉토리 확인: {TARGET_PATH}")
            
            try:
                if sftp_jj_hook.path_exists(TARGET_PATH):
                    print(f"타겟 디렉토리 존재: {TARGET_PATH}")
                    files = sftp.listdir(TARGET_PATH)
                    print(f"현재 파일 수: {len(files)}")
                    for item in files[:5]:  # 처음 5개만 표시
                        print(f"  - {item}")
                    if len(files) > 5:
                        print(f"  ... (총 {len(files)}개 파일)")
                else:
                    print(f"타겟 디렉토리가 존재하지 않습니다: {TARGET_PATH}")
                    print("타겟 디렉토리 생성 시도 중...")
                    
                    # 디렉토리 경로를 분할하여 단계별로 생성
                    path_parts = TARGET_PATH.strip('/').split('/')
                    current_path = ""
                    
                    for part in path_parts:
                        if current_path:
                            current_path += f"/{part}"
                        else:
                            current_path = f"/{part}"
                        
                        try:
                            if not sftp_jj_hook.path_exists(current_path):
                                print(f"디렉토리 생성: {current_path}")
                                sftp.mkdir(current_path)
                            else:
                                print(f"디렉토리 이미 존재: {current_path}")
                        except Exception as e:
                            print(f"디렉토리 생성 실패 {current_path}: {e}")
                    
                    print(f"타겟 디렉토리 생성 완료: {TARGET_PATH}")
            except Exception as e:
                print(f"타겟 디렉토리 확인 오류: {e}")
            
            return "두 SFTP 연결 테스트 성공"
            
    except Exception as e:
        print(f"SFTP 연결 실패: {e}")
        raise

def download_from_sft_dev():
    """sftp_jj_banbury_03에서 파일을 다운로드하는 함수"""
    try:
        sftp_hook = SFTPHook(ftp_conn_id=SFTP_DOWNLOAD_CONN)
        
        print("sftp_jj_banbury_03에서 파일 다운로드 시작...")
        
        # 다운로드할 파일 목록
        remote_files = [
            '../../mi/CBM_192.168.8.51_2025082610_T1_ST1.zip',
            # 여기에 실제 다운로드할 파일들을 추가하세요
        ]
        
        # 로컬 다운로드 디렉토리
        download_dir = Path(DOWNLOAD_DIR)
        download_dir.mkdir(exist_ok=True)
        
        downloaded_files = []
        
        # 하나의 SFTP 연결로 모든 작업 처리
        with sftp_hook.get_conn() as sftp:
            # 현재 작업 디렉토리 확인
            current_dir = sftp.getcwd()
            print(f"SFTP 현재 작업 디렉토리: {current_dir}")
            
            # 현재 디렉토리 내용 확인
            print("현재 디렉토리 내용:")
            try:
                current_files = sftp.listdir('.')
                for item in current_files[:10]:  # 처음 10개만
                    print(f"  - {item}")
                if len(current_files) > 10:
                    print(f"  ... (총 {len(current_files)}개 항목)")
            except Exception as e:
                print(f"  디렉토리 읽기 실패: {e}")
            
            for remote_file in remote_files:
                try:
                    filename = os.path.basename(remote_file)
                    local_path = download_dir / filename
                    
                    print(f"다운로드 중: {remote_file} -> {local_path}")
                    print(f"로컬 절대 경로: {local_path.absolute()}")
                    
                    # 파일 존재 여부 확인
                    if sftp_hook.path_exists(remote_file):
                        print(f"원격 파일 존재 확인: {remote_file}")
                        
                        # 파일 크기 확인
                        try:
                            stat = sftp.stat(remote_file)
                            print(f"원격 파일 크기: {stat.st_size} bytes ({stat.st_size / 1024 / 1024:.2f} MB)")
                        except Exception as e:
                            print(f"파일 크기 확인 실패: {e}")
                        
                        # 파일 다운로드
                        try:
                            with open(str(local_path), 'wb') as local_file:
                                sftp.getfo(remote_file, local_file)
                            print(f"  - 파일 다운로드 성공: {local_path}")
                            
                            # 다운로드 후 로컬 파일 확인
                            if local_path.exists():
                                file_size = local_path.stat().st_size
                                print(f"  - 다운로드된 파일 크기: {file_size} bytes ({file_size / 1024 / 1024:.2f} MB)")
                                downloaded_files.append(str(local_path))
                            else:
                                print(f"  - 경고: 로컬 파일이 존재하지 않습니다: {local_path}")
                        except Exception as e:
                            print(f"  - 파일 다운로드 실패: {e}")
                    else:
                        print(f"원격 파일이 존재하지 않습니다: {remote_file}")
                        
                except Exception as e:
                    print(f"파일 다운로드 실패 {remote_file}: {e}")
        
        print(f"다운로드 완료된 파일 목록: {downloaded_files}")
        
        # 다운로드 완료 후 완료 보고서 생성 및 업로드
        if downloaded_files:
            try:
                print("다운로드 완료 보고서 생성 중...")
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
                report_content = create_completion_report(
                    "Download", 
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    SFTP_DOWNLOAD_CONN,
                    SFTP_UPLOAD_CONN,
                    downloaded_files,
                    next_step="Files will be uploaded to target directory"
                )
                
                sftp_hook = SFTPHook(ftp_conn_id=SFTP_DOWNLOAD_CONN)
                upload_completion_report(sftp_hook, report_content, "Download", timestamp)
                
            except Exception as e:
                print(f"다운로드 완료 보고서 생성 실패: {e}")
        
        return f"sftp_jj_banbury_03에서 다운로드 완료: {len(downloaded_files)} 개 파일"
        
    except Exception as e:
        print(f"파일 다운로드 중 오류: {e}")
        raise

def upload_to_sftp_jj_fdw():
    """sftp_jj_fdw의 타겟 경로에 파일을 업로드하는 함수"""
    try:
        sftp_hook = SFTPHook(ftp_conn_id=SFTP_UPLOAD_CONN)
        
        print("sftp_jj_fdw에 파일 업로드 시작...")
        
        # 업로드할 로컬 파일들 (다운로드된 파일들)
        local_files = [
            f"{DOWNLOAD_DIR}/CBM_192.168.8.51_2025082610_T1_ST1.zip",
            # 여기에 실제 업로드할 파일들을 추가하세요
        ]
        
        uploaded_files = []
        
        for local_file in local_files:
            try:
                local_path = Path(local_file)
                print(f"파일 존재 확인: {local_file}")
                print(f"절대 경로: {local_path.absolute()}")
                
                if not local_path.exists():
                    print(f"로컬 파일이 존재하지 않습니다: {local_file}")
                    # 다운로드 디렉토리 내용 확인
                    download_dir = Path(DOWNLOAD_DIR)
                    if download_dir.exists():
                        print(f"다운로드 디렉토리 내용:")
                        for item in download_dir.iterdir():
                            print(f"  - {item.name} ({item.stat().st_size} bytes)")
                    else:
                        print(f"다운로드 디렉토리가 존재하지 않습니다: {download_dir}")
                    continue
                
                print(f"파일 존재 확인됨: {local_file}")
                print(f"파일 크기: {local_path.stat().st_size} bytes")
                
                # 원격 파일명 (로컬 파일명 사용)
                remote_filename = local_path.name
                remote_file_path = f"{TARGET_PATH}/{remote_filename}"
                
                print(f"업로드 중: {local_file} -> {remote_file_path}")
                
                # SFTP 연결을 직접 사용하여 파일 업로드 및 확인
                with sftp_hook.get_conn() as sftp:
                    sftp.put(str(local_path), remote_file_path)
                    
                    # 같은 연결 내에서 업로드 확인
                    try:
                        stat = sftp.stat(remote_file_path)
                        print(f"  - 파일 업로드 성공: {remote_file_path}")
                        print(f"  - 업로드된 파일 크기: {stat.st_size} bytes ({stat.st_size / 1024 / 1024:.2f} MB)")
                        uploaded_files.append(remote_file_path)
                    except Exception as e:
                        print(f"  - 업로드 실패: {remote_file_path}")
                        print(f"  - 파일 확인 오류: {e}")
                
            except Exception as e:
                print(f"파일 업로드 실패 {local_file}: {e}")
        
        # 업로드 완료 후 완료 보고서 생성 및 업로드
        if uploaded_files:
            try:
                print("업로드 완료 보고서 생성 중...")
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
                report_content = create_completion_report(
                    "Upload",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Local downloaded files",
                    TARGET_PATH,
                    uploaded_files,
                    target_directory=TARGET_PATH,
                    process_status="Process completed successfully"
                )
                
                upload_completion_report(sftp_hook, report_content, "Upload", timestamp)
                
            except Exception as e:
                print(f"업로드 완료 보고서 생성 실패: {e}")
        
        # 업로드 완료 후 다운로드 디렉토리 정리
        cleanup_downloads_directory()
        
        return f"sftp_jj_fdw 업로드 완료: {len(uploaded_files)} 개 파일"
        
    except Exception as e:
        print(f"파일 업로드 중 오류: {e}")
        raise

def create_test_data_and_upload():
    """테스트 데이터를 생성하고 sftp_jj_fdw에 업로드하는 함수"""
    try:
        sftp_hook = SFTPHook(ftp_conn_id=SFTP_UPLOAD_CONN)
        
        print("테스트 데이터 생성 및 업로드 시작...")
        
        # 테스트 데이터 생성
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        test_data = f"""# JJ Factory Data Warehouse Test Data
                        # Generated at: {datetime.now()}
                        # Target Path: {TARGET_PATH}

                        # Production Process Data
                        process_type: OS (Outsole)
                        unit_id: unit03
                        equipment: Banbury
                        timestamp: {timestamp}

                        # Raw Data Sample
                        temperature: 185.5
                        pressure: 2.3
                        speed: 45.2
                        status: running

                        # Log Data
                        log_level: INFO
                        message: Test data upload successful

                        # File Transfer Test
                        test_file_type: ZIP
                        original_file: CBM_192.168.8.51_2025082610_T1_ST1.zip
                        transfer_status: completed
                        """
        
        # 로컬 테스트 파일 생성
        test_file_path = f"/tmp/jj_factory_test_{timestamp}.yaml"
        with open(test_file_path, 'w') as f:
            f.write(test_data)
        
        print(f"테스트 파일 생성: {test_file_path}")
        
        # 파일이 실제로 생성되었는지 확인
        if not os.path.exists(test_file_path):
            raise FileNotFoundError(f"테스트 파일이 생성되지 않았습니다: {test_file_path}")
        
        file_size = os.path.getsize(test_file_path)
        print(f"생성된 파일 크기: {file_size} bytes")
        
        # 원격 파일 경로
        remote_filename = f"jj_factory_test_{timestamp}.yaml"
        remote_file_path = f"{TARGET_PATH}/{remote_filename}"
        
        print(f"업로드 중: {test_file_path} -> {remote_file_path}")
        
        # SFTP 연결을 직접 사용하여 파일 업로드 및 확인
        with sftp_hook.get_conn() as sftp:
            sftp.put(test_file_path, remote_file_path)
            
            # 같은 연결 내에서 업로드 확인
            try:
                stat = sftp.stat(remote_file_path)
                print(f"테스트 데이터 업로드 성공: {remote_file_path}")
                print(f"업로드된 파일 크기: {stat.st_size} bytes ({stat.st_size / 1024:.2f} KB)")
                
                # 업로드 성공 후 로컬 임시 파일 삭제
                try:
                    os.remove(test_file_path)
                    print(f"로컬 임시 파일 삭제 완료: {test_file_path}")
                except Exception as e:
                    print(f"로컬 임시 파일 삭제 실패: {e}")
                
                return f"테스트 데이터 업로드 완료: {remote_file_path}"
            except Exception as e:
                print(f"테스트 데이터 업로드 실패: {remote_file_path}")
                print(f"파일 확인 오류: {e}")
                
                # 업로드 실패 시에도 로컬 임시 파일 삭제
                try:
                    os.remove(test_file_path)
                    print(f"로컬 임시 파일 삭제 완료: {test_file_path}")
                except Exception as del_e:
                    print(f"로컬 임시 파일 삭제 실패: {del_e}")
                
                return "테스트 데이터 업로드 실패"
        
    except Exception as e:
        print(f"테스트 데이터 생성 및 업로드 중 오류: {e}")
        raise

def cleanup_downloads_directory():
    """마운트된 로컬 downloads 디렉토리의 모든 파일을 정리하는 함수"""
    try:
        # 로컬 호스트에 마운트된 경로 사용 (일반적인 Airflow 볼륨 마운트 경로)
        # docker-compose나 docker run에서 -v 옵션으로 마운트된 경로
        local_mount_paths = [
            "./downloads",  # 상대 경로 (현재 작업 디렉토리 기준)
            "/tmp/airflow_downloads",  # 임시 디렉토리
            "/home/airflow/downloads",  # 사용자 홈 디렉토리
            DOWNLOAD_DIR  # 컨테이너 내부 경로 (fallback)
        ]
        
        downloads_dir = None
        
        # 사용 가능한 마운트 경로 찾기
        for path in local_mount_paths:
            if os.path.exists(path):
                downloads_dir = path
                print(f"사용 가능한 마운트 경로 발견: {path}")
                break
        
        if downloads_dir is None:
            print("사용 가능한 마운트 경로를 찾을 수 없습니다.")
            print("사용 가능한 경로들:")
            for path in local_mount_paths:
                print(f"  - {path}")
            return "사용 가능한 마운트 경로 없음"
        
        print(f"다운로드 디렉토리 정리 시작: {downloads_dir}")
        
        deleted_count = 0
        failed_count = 0
        
        # 디렉토리 내용을 먼저 확인
        try:
            files = os.listdir(downloads_dir)
            print(f"정리할 파일 수: {len(files)}")
            for file_name in files:
                print(f"  - {file_name}")
        except Exception as e:
            print(f"디렉토리 읽기 실패: {e}")
            return f"디렉토리 읽기 실패: {e}"
        
        # 각 파일 삭제
        for file_name in files:
            file_path = os.path.join(downloads_dir, file_name)
            if os.path.isfile(file_path):
                try:
                    file_size = os.path.getsize(file_path)
                    os.remove(file_path)
                    print(f"삭제된 파일: {file_name} ({file_size} bytes)")
                    deleted_count += 1
                except Exception as e:
                    print(f"파일 삭제 실패 {file_name}: {str(e)}")
                    failed_count += 1
            elif os.path.isdir(file_path):
                try:
                    # 하위 디렉토리가 있다면 재귀적으로 삭제
                    shutil.rmtree(file_path)
                    print(f"삭제된 디렉토리: {file_name}")
                    deleted_count += 1
                except Exception as e:
                    print(f"디렉토리 삭제 실패 {file_name}: {str(e)}")
                    failed_count += 1
        
        print(f"다운로드 디렉토리 정리 완료: {deleted_count}개 항목 삭제, {failed_count}개 실패")
        
        # 정리 후 디렉토리가 비어있는지 확인
        try:
            remaining_files = os.listdir(downloads_dir)
            if not remaining_files:
                print("다운로드 디렉토리가 완전히 비워졌습니다.")
            else:
                print(f"경고: {len(remaining_files)}개 항목이 남아있습니다.")
                for item in remaining_files:
                    print(f"  - {item}")
        except Exception as e:
            print(f"정리 후 디렉토리 확인 실패: {e}")
        
        return f"정리 완료: {deleted_count}개 항목 삭제"
        
    except Exception as e:
        print(f"다운로드 디렉토리 정리 중 오류: {e}")
        raise

def list_target_directory():
    """타겟 디렉토리의 내용을 나열하는 함수"""
    try:
        sftp_hook = SFTPHook(ftp_conn_id=SFTP_UPLOAD_CONN)
        
        print(f"타겟 디렉토리 내용 나열: {TARGET_PATH}")
        
        if sftp_hook.path_exists(TARGET_PATH):
            files = sftp_hook.list_directory(TARGET_PATH)
            
            print(f"\n디렉토리: {TARGET_PATH}")
            print("-" * 80)
            print(f"{'이름':<50} {'타입'}")
            print("-" * 80)
            
            if isinstance(files, list) and len(files) > 0:
                for filename in files:
                    print(f"{filename:<50} FILE")
            else:
                for file_info in files:
                    if isinstance(file_info, dict):
                        file_type = 'DIR' if file_info.get('type') == 'dir' else 'FILE'
                        print(f"{file_info.get('name', 'Unknown'):<50} {file_type}")
                    else:
                        print(f"{str(file_info):<50} UNKNOWN")
            
            return f"타겟 디렉토리 내용 나열 완료: {len(files)} 개 항목"
        else:
            print(f"타겟 디렉토리가 존재하지 않습니다: {TARGET_PATH}")
            return "타겟 디렉토리 없음"
        
    except Exception as e:
        print(f"타겟 디렉토리 나열 중 오류: {e}")
        raise

with DAG(
    'sftp_test_dag',
    default_args=default_args,
    description='SFTP 연결 및 파일 다운로드 테스트',
    schedule_interval=None,
    catchup=False,
    tags=['sftp', 'test'],
) as dag:
    # DAG 태스크 정의
    test_connections_task = PythonOperator(
        task_id='test_sftp_connections',
        python_callable=test_sftp_connections,
        dag=dag,
    )

    download_task = PythonOperator(
        task_id='download_from_sft_dev',
        python_callable=download_from_sft_dev,
        dag=dag,
    )

    upload_task = PythonOperator(
        task_id='upload_to_sftp_jj_fdw',
        python_callable=upload_to_sftp_jj_fdw,
        dag=dag,
    )

    create_test_data_task = PythonOperator(
        task_id='create_test_data_and_upload',
        python_callable=create_test_data_and_upload,
        dag=dag,
    )

    list_target_task = PythonOperator(
        task_id='list_target_directory',
        python_callable=list_target_directory,
        dag=dag,
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_downloads_directory',
        python_callable=cleanup_downloads_directory,
        dag=dag,
    )

    # 태스크 의존성 설정
    test_connections_task >> download_task >> upload_task >> create_test_data_task >> list_target_task >> cleanup_task
