FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    ORACLE_CLIENT_LIB=/opt/oracle/instantclient_23_7 \
    LD_LIBRARY_PATH=/opt/oracle/instantclient_23_7

WORKDIR /app

# Oracle Instant Client 설치를 위한 의존성 (Debian trixie 기준)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        unzip \
        libaio1t64 \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    # oracledb가 요구하는 libaio.so.1 심볼릭 링크 보장 (Debian trixie: libaio1t64 → libaio.so.1t64)
    && if [ -f /lib/x86_64-linux-gnu/libaio.so.1t64 ]; then \
         ln -sf /lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/libaio.so.1; \
       elif [ -f /usr/lib/x86_64-linux-gnu/libaio.so.1t64 ]; then \
         ln -sf /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/libaio.so.1; \
       fi

# Oracle Instant Client 설치
# 빌드 시 instantclient-basic.zip 파일을 docker/ 디렉토리에 두고 COPY하거나
# 빌드 인자로 전달해야 합니다.
# 
# 사용 방법:
# 1. Oracle Instant Client Basic ZIP 파일을 docker/ 디렉토리에 복사
# 2. docker build --build-arg INSTANTCLIENT_ZIP=instantclient-basic-linux.x64-23.7.0.25.01.zip -t unified-montrg .
#
# 또는 COPY 방법 (ZIP 파일이 docker/ 디렉토리에 있는 경우):
ARG INSTANTCLIENT_ZIP
COPY docker/${INSTANTCLIENT_ZIP:-instantclient-basic.zip} /tmp/instantclient-basic.zip

RUN if [ -f /tmp/instantclient-basic.zip ]; then \
        mkdir -p /opt/oracle \
        && cd /opt/oracle \
        && unzip -q /tmp/instantclient-basic.zip \
        && rm -f /tmp/instantclient-basic.zip \
        && mv instantclient_* instantclient_23_7 \
        && cd /opt/oracle/instantclient_23_7 \
        && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci \
        && echo /opt/oracle/instantclient_23_7 > /etc/ld.so.conf.d/oracle-instantclient.conf \
        && ldconfig \
        && echo "Oracle Instant Client installed successfully"; \
    else \
        echo "Warning: Oracle Instant Client ZIP file not found. Thin mode will be used if supported."; \
    fi

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY app ./app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]

