#!/bin/bash

set -e  # 스크립트 실행 중 오류 발생 시 즉시 종료

# 1️⃣ `jq` 명령어가 있는지 확인
if ! command -v jq &> /dev/null; then
    echo "❌ jq is not installed! Please install jq before running the script."
    exit 1
fi

# 2️⃣ 현재 실행 중인 컨테이너 확인
CURRENT_VERSION=$(curl -s http://localhost/deploy | jq -r '.current_deploy' 2>/dev/null)

# 3️⃣ 초기 배포 시 기본 컨테이너 설정
if [ -z "$CURRENT_VERSION" ] || [ "$CURRENT_VERSION" == "null" ]; then
    echo "⚠️ No active deployment found! Starting fresh deployment with 'blue'"
    docker compose up -d blue
    docker compose stop green
    exit 0
fi

# 4️⃣ 현재 실행 중인 컨테이너 확인 후 반대 컨테이너 선택
if [ "$CURRENT_VERSION" == "blue" ]; then
    STOP_VERSION="blue"
    START_VERSION="green"
    START_PORT=8002
else
    STOP_VERSION="green"
    START_VERSION="blue"
    START_PORT=8001
fi

echo "🔵 Current active version: $CURRENT_VERSION"

# 5️⃣ 🔬 배포 전 테스트 실행
echo "🧪 Running tests before deployment..."

# ✅ pytest 설치 확인
if ! docker exec fastapi_$CURRENT_VERSION pytest --version &> /dev/null; then
    echo "❌ pytest is not installed in the container! Please install it before running tests."
    exit 1
fi

# ✅ ENV=test를 설정하고 pytest 실행
docker exec -it fastapi_$CURRENT_VERSION pytest --disable-warnings --maxfail=1 app/tests/

if [ $? -ne 0 ]; then
    echo "❌ Tests failed! Deployment aborted."
    exit 1
fi
echo "✅ All tests passed! Proceeding with deployment."

echo "🛑 Stopping $STOP_VERSION container..."
docker compose stop $STOP_VERSION

echo "🟢 Deploying new version: $START_VERSION"
docker compose up -d $START_VERSION
sleep 3 

# 6️⃣ 새로운 컨테이너 헬스 체크 (최대 30초까지 재시도)
echo "🩺 Checking new version health..."
for i in {1..6}; do
    sleep 5
    HEALTH_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$START_PORT/deploy)
    echo "🔍 Health Check Attempt #$i: HTTP $HEALTH_RESPONSE"

    if [ "$HEALTH_RESPONSE" -eq 200 ]; then
        echo "✅ Health check passed!"
        break
    fi

    if [ "$i" -eq 6 ]; then
        echo "❌ Deployment failed! Rolling back to previous version ($STOP_VERSION)."
        docker compose up -d $STOP_VERSION
        docker compose stop $START_VERSION  
        exit 1
    fi
done

# 7️⃣ 새로운 컨테이너 환경 변수 체크
NEW_DEPLOY_ENV=$(docker exec fastapi_$START_VERSION env | grep DEPLOY_ENV | cut -d '=' -f2 | tr -d '\r\n')

if [ "$NEW_DEPLOY_ENV" != "$START_VERSION" ]; then
    echo "❌ Deployment failed due to incorrect environment variable! Rolling back..."
    docker compose up -d $STOP_VERSION
    docker compose stop $START_VERSION  
    exit 1
fi

# 8️⃣ 🔄 새로운 `upstream.conf` 생성
echo "🔄 Updating Nginx upstream configuration..."
echo "upstream backend { server fastapi_$START_VERSION:8000 max_fails=5 fail_timeout=10s; }" > nginx/conf.d/upstream_temp.conf

# 9️⃣ ✅ 기존 `upstream.conf`를 직접 덮어쓰지 않고 새로운 파일을 생성 후 적용
docker cp nginx/conf.d/upstream_temp.conf nginx:/etc/nginx/conf.d/upstream_temp.conf || {
    echo "❌ Failed to copy Nginx configuration! Rolling back..."
    docker compose up -d $STOP_VERSION
    docker compose stop $START_VERSION  
    exit 1
}

docker exec nginx bash -c "
    cp /etc/nginx/conf.d/upstream_temp.conf /etc/nginx/conf.d/upstream.conf
    nginx -s reload
" || {
    echo "❌ Nginx reload failed! Rolling back..."
    docker compose up -d $STOP_VERSION
    docker compose stop $START_VERSION  
    exit 1
}

echo "✅ Deployment successful! Now running version: $START_VERSION"

if ! docker network inspect prometheus-network --format '{{range .Containers}}{{.Name}} {{end}}' | grep -q nginx; then
    echo "🔄 Connecting nginx container to prometheus-network..."
    docker network connect prometheus-network nginx
    echo "✅ nginx container connected to prometheus-network."
else
    echo "🌐 nginx container is already connected to prometheus-network."
fi