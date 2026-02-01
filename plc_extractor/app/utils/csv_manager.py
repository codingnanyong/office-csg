"""
CSV 파일 관리 유틸리티
"""
import os
import glob
import pandas as pd
import gzip
import shutil
from datetime import datetime, timedelta
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class CSVManager:
    """CSV 파일 관리 클래스"""
    
    def __init__(self, data_dir: str = "/media/btx/plc_extractor/data"):
        self.data_dir = data_dir
        self.last_compression_date = None
        self.ensure_data_dir()
    
    def ensure_data_dir(self):
        """데이터 디렉토리 생성"""
        os.makedirs(self.data_dir, exist_ok=True)
    
    async def save_plc_data(self, data: Dict[str, Any]) -> str:
        """PLC 데이터를 CSV 파일로 저장"""
        try:
            # 일별 CSV 파일명
            date_str = datetime.now().strftime("%Y%m%d")
            filename = f"plc_data_{date_str}.csv"
            csv_path = os.path.join(self.data_dir, filename)
            
            # 데이터 변환
            all_data = []
            for address_type, values in data.items():
                for item in values:
                    all_data.append({
                        'address_type': address_type,
                        'plc_address': item['address'],
                        'value': item['value'],
                        'timestamp': item['timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    })
            
            if all_data:
                df = pd.DataFrame(all_data)
                
                # 기존 파일에 추가
                if os.path.exists(csv_path):
                    df.to_csv(csv_path, mode='a', header=False, index=False, encoding='utf-8')
                    logger.info(f"📁 CSV 파일 추가: {len(all_data)}개 데이터")
                else:
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    logger.info(f"📁 CSV 파일 생성: {len(all_data)}개 데이터")
                
                # 날짜가 바뀔 때만 전일 파일 압축 및 오래된 파일 정리
                self.check_and_compress_if_date_changed()
                self.cleanup_old_files()
                
                return csv_path
            else:
                logger.warning("저장할 데이터가 없습니다")
                return None
                
        except Exception as e:
            logger.error(f"CSV 저장 중 오류: {e}")
            return None
    
    def check_and_compress_if_date_changed(self):
        """날짜가 바뀔 때만 전일 파일 압축"""
        try:
            current_date = datetime.now().strftime("%Y%m%d")
            
            # 마지막 압축 날짜와 현재 날짜가 다를 때만 압축 수행
            if self.last_compression_date != current_date:
                self.compress_previous_day_files()
                self.last_compression_date = current_date
                logger.info(f"📅 날짜 변경 감지: {current_date} - 전일 파일 압축 수행")
            else:
                logger.debug(f"📅 날짜 변경 없음: {current_date} - 압축 건너뜀")
                
        except Exception as e:
            logger.error(f"날짜 변경 체크 중 오류: {e}")
    
    def compress_previous_day_files(self):
        """전일 CSV 파일을 압축"""
        try:
            yesterday = datetime.now() - timedelta(days=1)
            yesterday_str = yesterday.strftime("%Y%m%d")
            yesterday_file = os.path.join(self.data_dir, f"plc_data_{yesterday_str}.csv")
            compressed_file = os.path.join(self.data_dir, f"plc_data_{yesterday_str}.csv.gz")
            
            # 전일 파일이 존재하고 아직 압축되지 않은 경우
            if os.path.exists(yesterday_file) and not os.path.exists(compressed_file):
                with open(yesterday_file, 'rb') as f_in:
                    with gzip.open(compressed_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # 원본 파일 삭제
                os.remove(yesterday_file)
                logger.info(f"🗜️ 전일 CSV 파일 압축 완료: {yesterday_file} → {compressed_file}")
            else:
                logger.debug(f"📁 전일 파일 압축 불필요: {yesterday_file} (존재: {os.path.exists(yesterday_file)}, 압축됨: {os.path.exists(compressed_file)})")
                
        except Exception as e:
            logger.error(f"CSV 파일 압축 중 오류: {e}")
    
    def cleanup_old_files(self, days: int = 1):
        """오래된 CSV 파일 정리 (압축 파일 포함) - 전일 파일 삭제"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            # CSV 파일과 압축 파일 모두 검색
            csv_files = glob.glob(os.path.join(self.data_dir, "plc_data_*.csv")) + glob.glob(os.path.join(self.data_dir, "plc_data_*.csv.gz"))
            
            for csv_file in csv_files:
                file_time = datetime.fromtimestamp(os.path.getmtime(csv_file))
                if file_time < cutoff_date:
                    os.remove(csv_file)
                    logger.info(f"🗑️ 오래된 CSV 파일 삭제: {csv_file}")
                    
        except Exception as e:
            logger.error(f"CSV 파일 정리 중 오류: {e}")
    
    def get_csv_files(self) -> list:
        """CSV 파일 목록 반환 (압축 파일 포함)"""
        try:
            csv_files = glob.glob(os.path.join(self.data_dir, "plc_data_*.csv")) + glob.glob(os.path.join(self.data_dir, "plc_data_*.csv.gz"))
            return sorted(csv_files, reverse=True)
        except Exception as e:
            logger.error(f"CSV 파일 목록 조회 중 오류: {e}")
            return []
    
    def get_file_info(self, file_path: str) -> dict:
        """CSV 파일 정보 반환"""
        try:
            if not os.path.exists(file_path):
                return None
            
            stat = os.stat(file_path)
            return {
                'file_path': file_path,
                'file_size': stat.st_size,
                'created_time': datetime.fromtimestamp(stat.st_ctime),
                'modified_time': datetime.fromtimestamp(stat.st_mtime)
            }
        except Exception as e:
            logger.error(f"CSV 파일 정보 조회 중 오류: {e}")
            return None


# 전역 CSV 매니저 인스턴스
csv_manager = CSVManager()
