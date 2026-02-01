#!/usr/bin/env python3
"""
PLC 연결 및 데이터 읽기 테스트 스크립트
DB 저장 없이 PLC에서 데이터를 읽어서 콘솔에 출력
"""
import asyncio
import sys
import os
import csv
import pandas as pd
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from app.core.plc_connector import PLCConnector
from app.config.settings import settings
from app.utils.logger import setup_logging

def save_to_csv(data: dict, filename: str = None):
    """
    추출된 PLC 데이터를 CSV 파일로 저장 (일별 파일 관리)
    
    Args:
        data: 추출된 PLC 데이터 (주소 타입별로 분류된 딕셔너리)
        filename: 저장할 파일명 (기본값: 일별 파일)
    """
    if not filename:
        # 일별 CSV 파일명 생성
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"plc_data_{date_str}.csv"
    
    # CSV 파일 경로 설정
    csv_path = os.path.join(os.path.dirname(__file__), "..", "data", filename)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    # 모든 데이터를 하나의 리스트로 통합
    all_data = []
    for address_type, values in data.items():
        for item in values:
            all_data.append({
                'address_type': address_type,
                'plc_address': item['address'],
                'value': item['value'],
                'timestamp': item['timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            })
    
    # CSV 파일로 저장 (기존 파일에 추가)
    if all_data:
        df = pd.DataFrame(all_data)
        
        # 기존 파일이 있으면 추가, 없으면 새로 생성
        if os.path.exists(csv_path):
            df.to_csv(csv_path, mode='a', header=False, index=False, encoding='utf-8')
        else:
            df.to_csv(csv_path, index=False, encoding='utf-8')
        
        print(f"📁 CSV 파일 저장 완료: {csv_path}")
        print(f"   - 총 {len(all_data)}개 데이터 저장")
        
        # 주소 타입별 통계
        type_counts = df['address_type'].value_counts()
        print("   - 주소 타입별 데이터 개수:")
        for addr_type, count in type_counts.items():
            print(f"     {addr_type}: {count}개")
    else:
        print("❌ 저장할 데이터가 없습니다")
    
    # 오래된 CSV 파일 정리
    cleanup_old_csv_files(os.path.dirname(csv_path), days=7)
    
    return csv_path

def cleanup_old_csv_files(data_dir: str, days: int = 7):
    """오래된 CSV 파일 정리"""
    try:
        import glob
        from datetime import timedelta
        
        cutoff_date = datetime.now() - timedelta(days=days)
        csv_files = glob.glob(os.path.join(data_dir, "plc_data_*.csv"))
        
        for csv_file in csv_files:
            file_time = datetime.fromtimestamp(os.path.getmtime(csv_file))
            if file_time < cutoff_date:
                os.remove(csv_file)
                print(f"오래된 CSV 파일 삭제: {csv_file}")
    except Exception as e:
        print(f"CSV 파일 정리 중 오류: {e}")

async def test_plc_connection():
    """PLC 연결 및 데이터 읽기 테스트"""
    print("🔧 PLC 연결 테스트 시작")
    print(f"PLC 주소: {settings.plc.host}:{settings.plc.port}")
    print(f"읽기 간격: {settings.plc.read_interval}초")
    print("-" * 50)
    
    # PLC 연결
    plc = PLCConnector()
    
    try:
        # 연결 시도
        print("📡 PLC 연결 중...")
        if not await plc.connect():
            print("❌ PLC 연결 실패")
            return False
        
        print("✅ PLC 연결 성공!")
        print()
        
        # 전체 주소 읽기 및 시간 측정
        print("📊 전체 주소 읽기 테스트...")
        print("⏱️  시간 측정 시작...")
        
        import time
        start_total_time = time.time()
        
        data = await plc.read_continuous_addresses()
        
        total_time = time.time() - start_total_time
        
        if not data:
            print("❌ 데이터를 읽지 못했습니다")
            return False
        
        # 결과 출력
        print("✅ 전체 주소 읽기 성공!")
        print()
        
        # 각 주소 타입별 통계
        total_count = 0
        for address_type, values in data.items():
            print(f"📋 {address_type} 주소: {len(values)}개 값 추출")
            total_count += len(values)
        
        print()
        print("⏱️  시간 측정 결과:")
        print(f"  - 총 소요시간: {total_time:.3f}초")
        print(f"  - 총 추출 데이터: {total_count}개")
        print(f"  - 평균 처리 속도: {total_count/total_time:.1f} 데이터/초")
        print()
        
        # 설정된 전체 주소 범위 정보
        address_config = settings.get_plc_addresses()
        total_read_count = 0
        for address_type, config in address_config.items():
            total_read_count += config['count']
            print(f"📊 {address_type}: {config['count']}개 주소 읽기 → {len(data.get(address_type, []))}개 추출")
        
        print()
        print(f"📈 전체 통계:")
        print(f"  - 읽은 주소: {total_read_count}개")
        print(f"  - 추출된 데이터: {total_count}개")
        print(f"  - 추출 비율: {total_count/total_read_count*100:.1f}%")
        print(f"  - 읽기 속도: {total_read_count/total_time:.1f} 주소/초")
        
        # CSV 파일로 저장
        print()
        print("💾 CSV 파일 저장 중...")
        csv_path = save_to_csv(data)
        
        return True
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return False
        
    finally:
        # 연결 해제
        await plc.disconnect()
        print("🔌 PLC 연결 해제")

async def main():
    """메인 함수"""
    # 로깅 설정
    setup_logging()
    
    print("🚀 PLC 데이터 추출 테스트")
    print("=" * 50)
    
    # 테스트 실행
    success = await test_plc_connection()
    
    if success:
        print("\n🎉 테스트 성공!")
    else:
        print("\n💥 테스트 실패!")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
