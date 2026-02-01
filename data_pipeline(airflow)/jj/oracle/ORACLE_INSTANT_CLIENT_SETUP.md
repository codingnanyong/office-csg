# Oracle Instant Client 설치 가이드

이 디렉토리는 Airflow에서 Oracle 데이터베이스 연결을 위한 Oracle Instant Client를 포함합니다.

## 필요한 파일

Oracle Instant Client Basic 패키지가 필요합니다.

## 다운로드 방법

1. Oracle 공식 웹사이트 방문:
   https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html

2. **Instant Client Basic** 패키지 다운로드
   - 버전: 23.7.0.25.01 (또는 최신 버전)
   - 파일명: `instantclient-basic-linux.x64-23.7.0.25.01.zip`

3. 다운로드한 파일을 이 디렉토리(`/oracle/`)에 저장

## 설치

```bash
# oracle 디렉토리로 이동
cd /home/de/apps/airflow/oracle

# zip 파일 압축 해제
unzip instantclient-basic-linux.x64-23.7.0.25.01.zip

# 환경 변수 설정 (필요시)
export LD_LIBRARY_PATH=/home/de/apps/airflow/oracle/instantclient_23_7:$LD_LIBRARY_PATH
```

## 참고 사항

- `.gitignore`에 `*.zip` 파일이 제외 처리되어 있어 Git에 커밋되지 않습니다
- 각 개발자는 위 다운로드 방법을 따라 직접 Instant Client를 다운로드해야 합니다
- Oracle 계정이 필요할 수 있습니다 (무료 등록 가능)

## 문제 해결

Oracle Instant Client 관련 오류가 발생하면:
1. LD_LIBRARY_PATH가 올바르게 설정되었는지 확인
2. Instant Client 버전이 호환되는지 확인
3. 필요한 의존성 패키지가 설치되어 있는지 확인

