"""
PLC Test DAG for Banbury #3 PLC Connection
사용자의 테스트 코드에 맞춰 pyModbusTCP 사용
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyModbusTCP.client import ModbusClient
import logging
import pandas as pd
import os

# DAG 기본 설정
default_args = {
    'owner': 'flet-montrg',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'plc_test_banbury3',
    default_args=default_args,
    description='PLC Test DAG for Banbury #3 PLC Connection using pyModbusTCP',
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=['plc', 'test', 'banbury3'],
)

def test_plc_connection(**context):
    """
    PLC 연결 테스트 함수 (pyModbusTCP 사용)
    """
    try:
        # Airflow Connection에서 PLC 연결 정보 가져오기
        connection = BaseHook.get_connection('plc_jj_banbury3')
        
        # 연결 정보 출력
        logging.info(f"PLC Connection Info:")
        logging.info(f"  Host: {connection.host}")
        logging.info(f"  Port: {connection.port}")
        logging.info(f"  Connection Type: {connection.conn_type}")
        
        # pyModbusTCP 클라이언트 생성
        client = ModbusClient(host=connection.host, port=connection.port)
        
        # 연결 시도
        if client.open():
            logging.info("✅ PLC 연결 성공!")
            logging.info(f"  연결된 주소: {connection.host}:{connection.port}")
            
            # 간단한 읽기 테스트 (Holding Register 0부터 5개 읽기)
            try:
                result = client.read_holding_registers(0, 5)
                if result:
                    logging.info(f"✅ PLC 데이터 읽기 성공: {result}")
                else:
                    logging.error("❌ PLC 읽기 실패")
            except Exception as e:
                logging.error(f"❌ PLC 읽기 중 오류: {str(e)}")
            
            # 연결 종료
            client.close()
            logging.info("PLC 연결 종료")
            
        else:
            logging.error("❌ PLC 연결 실패!")
            raise Exception("PLC 연결에 실패했습니다.")
            
    except Exception as e:
        logging.error(f"❌ PLC 테스트 중 오류 발생: {str(e)}")
        raise

def get_plc_data(**context):
    """
    CSV 파일의 주소 리스트를 기반으로 PLC 데이터 읽기
    """
    try:
        connection = BaseHook.get_connection('plc_jj_banbury3')
        
        logging.info("PLC 데이터 수집 시작...")
        
        # CSV 파일 읽기
        csv_file_path = "/opt/airflow/dags/sample/all_list.csv"
        logging.info(f"CSV 파일 읽기: {csv_file_path}")
        
        try:
            # CSV 파일 읽기 (여러 인코딩 시도)
            encodings = ['utf-8', 'cp949', 'euc-kr', 'utf-8-sig']
            data = None
            
            for encoding in encodings:
                try:
                    data = pd.read_csv(csv_file_path, encoding=encoding, dtype=str)
                    # 컬럼명 정규화: BOM/공백 제거
                    data.columns = [str(c).replace('\ufeff', '').strip() for c in data.columns]
                    logging.info(f"✓ 인코딩 '{encoding}' 성공!")
                    logging.info(f"컬럼명: {list(data.columns)}")
                    logging.info(f"첫 번째 행: {data.iloc[0].to_dict()}")
                    break
                except Exception as e:
                    logging.warning(f"인코딩 '{encoding}' 실패: {e}")
                    continue
            
            if data is None:
                raise Exception("CSV 파일을 읽을 수 없습니다.")
                
            logging.info(f"CSV 파일 읽기 완료: {len(data)} 행")
            
        except Exception as e:
            logging.error(f"CSV 파일 읽기 오류: {e}")
            raise
        
        # pyModbusTCP 클라이언트 생성
        client = ModbusClient(host=connection.host, port=connection.port)
        
        try:
            if client.open():
                logging.info("✅ PLC 연결 성공!")
                
                plc_data = {}
                failed_addresses = []
                
                # CSV의 각 주소에 대해 데이터 읽기
                for index, row in data.iterrows():
                    try:
                        key = str(row.get('Key', '')).strip()
                        address = int(row.get('Address', 0)) if pd.notna(row.get('Address', 0)) else 0
                        data_type = str(row.get('Type', '')).strip()
                        description = str(row.get('Description', '')).strip()
                        modbus_address = int(row.get('Modbus_Address', 0)) if pd.notna(row.get('Modbus_Address', 0)) else 0
                        
                        # 디버깅: 처음 5개 주소만 상세 로그
                        if index < 5:
                            logging.info(f"처리 중: {key}, Address={address}, Modbus_Address={modbus_address}, Type={data_type}")
                        
                        if not key or address == 0:
                            if index < 5:
                                logging.warning(f"건너뛰기: {key} (address={address})")
                            continue
                        
                        # Key의 첫 번째 문자를 기준으로 Modbus 함수 결정
                        address_prefix = key[0].upper()
                        
                        if address_prefix == 'D':
                            # D 주소: Holding Register 읽기 (Int16 타입)
                            value = client.read_holding_registers(modbus_address, 1)
                            if value and len(value) >= 1:
                                int_value = value[0]
                                # 부호 있는 16비트 정수로 변환
                                if int_value >= 32768:
                                    int_value = int_value - 65536
                                plc_data[key] = {
                                    'value': int_value,
                                    'type': 'Int16',
                                    'description': description,
                                    'address': address,
                                    'modbus_address': modbus_address
                                }
                            else:
                                failed_addresses.append(key)
                                
                        elif address_prefix in ['M', 'Y']:
                            # M, Y 주소: Coil 읽기 (BOOL 타입)
                            value = client.read_coils(modbus_address, 1)
                            if value is not None:
                                plc_data[key] = {
                                    'value': value[0],
                                    'type': 'BOOL',
                                    'description': description,
                                    'address': address,
                                    'modbus_address': modbus_address
                                }
                            else:
                                failed_addresses.append(key)
                                
                        elif address_prefix == 'X':
                            # X 주소: Discrete Input 읽기 (BOOL 타입)
                            if index < 5:
                                logging.info(f"X 주소 읽기 시도: {key} -> Modbus Address {modbus_address}")
                            value = client.read_discrete_inputs(modbus_address, 1)
                            if index < 5:
                                logging.info(f"X 주소 읽기 결과: {value}")
                            if value is not None:
                                plc_data[key] = {
                                    'value': value[0],
                                    'type': 'BOOL',
                                    'description': description,
                                    'address': address,
                                    'modbus_address': modbus_address
                                }
                                if index < 5:
                                    logging.info(f"X 주소 성공: {key} = {value[0]}")
                            else:
                                failed_addresses.append(key)
                                if index < 5:
                                    logging.warning(f"X 주소 실패: {key}")
                                
                        elif address_prefix == 'T':
                            # T 주소: Holding Register 읽기 (타이머/REAL 타입)
                            value = client.read_holding_registers(modbus_address, 1)
                            if value and len(value) >= 1:
                                int_value = value[0]
                                # 부호 있는 16비트 정수로 변환
                                if int_value >= 32768:
                                    int_value = int_value - 65536
                                plc_data[key] = {
                                    'value': int_value,
                                    'type': 'REAL',
                                    'description': description,
                                    'address': address,
                                    'modbus_address': modbus_address
                                }
                            else:
                                failed_addresses.append(key)
                        
                        # 진행 상황 로깅 (100개마다)
                        if (index + 1) % 100 == 0:
                            logging.info(f"진행 상황: {index + 1}/{len(data)} 주소 처리 완료")
                            
                    except Exception as e:
                        logging.error(f"주소 {key} 읽기 오류: {str(e)}")
                        failed_addresses.append(key)
                        continue
                
                # 결과 요약
                logging.info(f"✓ {len(plc_data)} 개 주소 읽기 성공")
                if failed_addresses:
                    logging.warning(f"⚠️ {len(failed_addresses)} 개 주소 읽기 실패")
                
                # XCom에 데이터 저장
                context['task_instance'].xcom_push(
                    key='plc_data',
                    value={
                        'successful_reads': len(plc_data),
                        'failed_reads': len(failed_addresses),
                        'failed_addresses': failed_addresses,
                        'timestamp': datetime.now().isoformat(),
                        'host': connection.host,
                        'port': connection.port
                    }
                )
                
                # 샘플 데이터도 저장
                sample_data = dict(list(plc_data.items())[:10])  # 처음 10개만
                context['task_instance'].xcom_push(
                    key='plc_sample_data',
                    value=sample_data
                )
                
                logging.info("PLC 데이터가 XCom에 저장되었습니다.")
                
            else:
                raise Exception("PLC 연결 실패")
                
        finally:
            # 항상 연결 종료
            if client.is_open:
                client.close()
                logging.info("PLC 연결 종료")

    except Exception as e:
        logging.error(f"PLC 데이터 수집 중 오류: {str(e)}")
        raise

def save_plc_data_to_db(**context):
    """
    PLC 데이터를 데이터베이스에 저장
    """
    try:
        # XCom에서 PLC 데이터 가져오기
        plc_data = context['task_instance'].xcom_pull(task_ids='get_plc_data', key='plc_data')
        plc_sample_data = context['task_instance'].xcom_pull(task_ids='get_plc_data', key='plc_sample_data')
        
        if not plc_sample_data:
            logging.warning("저장할 PLC 데이터가 없습니다.")
            return
        
        # PostgreSQL 연결
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = BaseHook.get_connection('plc_jj_banbury3')
        
        logging.info(f"데이터베이스에 {len(plc_sample_data)} 개 레코드 저장 시작...")
        
        # 배치 삽입을 위한 데이터 준비
        insert_data = []
        current_time = datetime.now()
        
        for key, data in plc_sample_data.items():
            # 데이터 타입에 따라 값 설정
            bool_value = None
            int_value = None
            real_value = None
            raw_value = str(data['value'])
            
            if data['type'] == 'BOOL':
                bool_value = data['value']
            elif data['type'] == 'Int16':
                int_value = data['value']
            elif data['type'] == 'REAL':
                real_value = float(data['value'])
            
            insert_data.append((
                current_time,                       # collection_timestamp
                key,                               # plc_key
                bool_value,                        # bool_value
                int_value,                         # int_value
                real_value,                        # real_value
                raw_value                          # raw_value
            ))
        
        # SQL 삽입 쿼리 (시계열 최적화)
        insert_sql = """
        INSERT INTO bronze.plc_raw_data (
            collection_timestamp, plc_key, bool_value, int_value, real_value, raw_value
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (collection_timestamp, plc_key) 
        DO UPDATE SET
            bool_value = EXCLUDED.bool_value,
            int_value = EXCLUDED.int_value,
            real_value = EXCLUDED.real_value,
            raw_value = EXCLUDED.raw_value,
            created_at = NOW()
        """
        
        # 배치 삽입 실행
        postgres_hook.run(insert_sql, parameters=insert_data)
        
        logging.info(f"✅ {len(insert_data)} 개 레코드가 데이터베이스에 저장되었습니다.")
        
        # 저장 통계 로그
        bool_count = len([d for d in insert_data if d[9] is not None])
        int_count = len([d for d in insert_data if d[10] is not None])
        real_count = len([d for d in insert_data if d[11] is not None])
        
        logging.info(f"저장 통계: BOOL={bool_count}, Int16={int_count}, REAL={real_count}")
        
    except Exception as e:
        logging.error(f"데이터베이스 저장 중 오류: {str(e)}")
        raise

def init_plc_address_master(**context):
    """
    PLC 주소 마스터 테이블 초기화 (CSV 파일 기반)
    """
    try:
        # CSV 파일 읽기
        csv_file_path = "/opt/airflow/dags/sample/all_list.csv"
        logging.info(f"마스터 테이블 초기화: {csv_file_path}")
        
        # CSV 파일 읽기
        encodings = ['utf-8', 'cp949', 'euc-kr', 'utf-8-sig']
        data = None
        
        for encoding in encodings:
            try:
                data = pd.read_csv(csv_file_path, encoding=encoding, dtype=str)
                data.columns = [str(c).replace('\ufeff', '').strip() for c in data.columns]
                logging.info(f"✓ 인코딩 '{encoding}' 성공!")
                break
            except Exception as e:
                logging.warning(f"인코딩 '{encoding}' 실패: {e}")
                continue
        
        if data is None:
            raise Exception("CSV 파일을 읽을 수 없습니다.")
        
        # PostgreSQL 연결
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 기존 데이터 삭제 (전체 재초기화)
        delete_sql = "DELETE FROM bronze.plc_address_master"
        postgres_hook.run(delete_sql)
        logging.info("기존 마스터 데이터 삭제 완료")
        
        # 마스터 데이터 삽입
        insert_data = []
        for index, row in data.iterrows():
            key = str(row.get('Key', '')).strip()
            if not key:
                continue
                
            address = int(row.get('Address', 0)) if pd.notna(row.get('Address', 0)) else 0
            modbus_address = int(row.get('Modbus_Address', 0)) if pd.notna(row.get('Modbus_Address', 0)) else 0
            data_type = str(row.get('Type', '')).strip()
            description = str(row.get('Description', '')).strip()
            
            # 주소 타입 결정
            address_type = key[0].upper() if key else 'X'
            
            insert_data.append((
                key,                    # plc_key
                address,               # plc_address
                modbus_address,        # modbus_address
                address_type,          # address_type
                data_type,             # data_type
                description            # description
            ))
        
        # 배치 삽입
        insert_sql = """
        INSERT INTO bronze.plc_address_master (
            plc_key, plc_address, modbus_address, address_type, data_type, description
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (plc_key) 
        DO UPDATE SET
            plc_address = EXCLUDED.plc_address,
            modbus_address = EXCLUDED.modbus_address,
            address_type = EXCLUDED.address_type,
            data_type = EXCLUDED.data_type,
            description = EXCLUDED.description,
            updated_at = NOW()
        """
        
        postgres_hook.run(insert_sql, parameters=insert_data)
        logging.info(f"✅ {len(insert_data)} 개 마스터 레코드가 저장되었습니다.")
        
    except Exception as e:
        logging.error(f"마스터 테이블 초기화 중 오류: {str(e)}")
        raise

# Task 정의
test_connection_task = PythonOperator(
    task_id='test_plc_connection',
    python_callable=test_plc_connection,
    dag=dag,
)

get_data_task = PythonOperator(
    task_id='get_plc_data',
    python_callable=get_plc_data,
    dag=dag,
)

init_master_task = PythonOperator(
    task_id='init_plc_address_master',
    python_callable=init_plc_address_master,
    dag=dag,
)

save_data_task = PythonOperator(
    task_id='save_plc_data_to_db',
    python_callable=save_plc_data_to_db,
    dag=dag,
)

# Task 의존성 설정
init_master_task >> test_connection_task >> get_data_task >> save_data_task