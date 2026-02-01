pipeline {
    agent any

    environment {
        DOCKER_IMAGE = 'csg/openapi'
        DOCKER_TAG = 'latest'
        NETWORK_NAME = 'openapi_network'
        NGINX_CONTAINER = 'nginx'
        NGINX_CONTAINER_CONFIG_PATH = '/etc/nginx/nginx.conf' 
    }

    stages {
        stage('Checkout') {
            steps {
                echo '🔍 Checking out source code...'
                checkout scm 
            }
        }

        stage('Check Docker Permissions') {
            steps {
                echo '🔐 Checking Docker permissions...'
                script {
                    sh '''
                        echo "Current user: $(whoami)"
                        echo "Current groups: $(groups)"
                        docker version || { echo "Docker access failed"; exit 1; }
                    '''
                }
            }
        }

        stage('Run Tests') {
            when {
                anyOf {
                    branch 'master'
                }
            }
            steps {
                echo '🧪 Running tests...'
                script {
                    sh """
                        docker build -f docker/Dockerfile -t test-image .
                        docker run --rm test-image python -m pytest app/tests/ -v --disable-warnings || { echo "Tests completed with warnings or failed."; exit 1; }
                    """
                }
            }
        }

        stage('Build Docker Image') {
            when {
                anyOf {
                    branch 'master'
                }
            }
            steps {
                echo '🐳 Building Docker image...'
                script {
                    sh """
                        docker build -f docker/Dockerfile -t ${DOCKER_IMAGE}:${DOCKER_TAG} .
                        echo "✅ Image built successfully"
                        docker images | grep csg
                    """
                }
            }
        }

        stage('Prepare Deployment') {
            when {
                branch 'master'
            }
            steps {
                echo '🔧 Preparing deployment environment...'
                script {
                    sh """
                        # 네트워크 생성 (이미 존재하면 무시)
                        docker network create ${NETWORK_NAME} 2>/dev/null || echo "Network ${NETWORK_NAME} already exists"

                        # nginx 컨테이너가 실행 중인지 확인
                        if ! docker ps | grep -q "${NGINX_CONTAINER}"; then
                            echo "❌ Nginx container '${NGINX_CONTAINER}' is not running. Please start nginx first."
                            exit 1
                        fi
                        
                        # Blue/Green 컨테이너가 모두 실행 중인지 확인
                        if ! docker ps | grep -q "openapi_blue"; then
                            echo "❌ openapi_blue container is not running. Please start both blue and green containers first."
                            exit 1
                        fi
                        
                        if ! docker ps | grep -q "openapi_green"; then
                            echo "❌ openapi_green container is not running. Please start both blue and green containers first."
                            exit 1
                        fi
                    """
                    
                    def currentActiveNginxBackend = sh(
                        script: "docker cp ${NGINX_CONTAINER}:${NGINX_CONTAINER_CONFIG_PATH} /tmp/nginx_current.conf_for_jenkins && grep 'proxy_pass http://openapi_' /tmp/nginx_current.conf_for_jenkins | head -n 1 | sed -n 's/.*proxy_pass http:\\/\\/openapi_\\([^_]*\\)_backend.*/\\1/p' || echo 'blue'",
                        returnStdout: true
                    ).trim()
                    
                    if (currentActiveNginxBackend == null || currentActiveNginxBackend.isEmpty()) {
                        currentActiveNginxBackend = 'blue'
                    }
                    
                    env.CURRENT_ACTIVE_VERSION = currentActiveNginxBackend
                    env.TARGET_DEPLOY_VERSION = (env.CURRENT_ACTIVE_VERSION == 'blue') ? 'green' : 'blue'
                    
                    echo "Nginx currently points to: openapi_${env.CURRENT_ACTIVE_VERSION}_backend"
                    echo "Target version for new deployment: openapi_${env.TARGET_DEPLOY_VERSION}"
                }
            }
        }

        stage('Deploy New Version') {
            when {
                branch 'master'
            }
            steps {
                echo '🚀 Deploying new version...'
                script {
                    sh """
                        echo "Updating openapi_${env.TARGET_DEPLOY_VERSION} container with new image..."
                        
                        # 기존 컨테이너 중지 및 제거 (새 이미지로 교체하기 위해)
                        docker stop openapi_${env.TARGET_DEPLOY_VERSION} 2>/dev/null || true
                        docker rm openapi_${env.TARGET_DEPLOY_VERSION} 2>/dev/null || true
                        
                        # 새 이미지로 컨테이너 재시작
                        docker run -d \\
                            --name openapi_${env.TARGET_DEPLOY_VERSION} \\
                            --network ${NETWORK_NAME} \\
                            -p ${env.TARGET_DEPLOY_VERSION == 'blue' ? '8001' : '8002'}:8000 \\
                            -e DEPLOY_ENV=${env.TARGET_DEPLOY_VERSION} \\
                            -e PYTHONUNBUFFERED=1 \\
                            --restart unless-stopped \\
                            --health-cmd="curl -f http://localhost:8000/deploy || exit 1" \\
                            --health-interval=30s \\
                            --health-timeout=10s \\
                            --health-retries=3 \\
                            ${DOCKER_IMAGE}:${DOCKER_TAG}
                        
                        echo "⏳ Giving the ${env.TARGET_DEPLOY_VERSION} container some time to initialize..."
                        sleep 15
                    """
                }
            }
        }
        
        stage('Health Check New Container') {
            when {
                branch 'master'
            }
            steps {
                echo '🏥 Performing health check on new container...'
                script {
                    sh """
                        echo "🔍 Health checking openapi_${env.TARGET_DEPLOY_VERSION} container directly (Docker health check)..."
                        
                        timeout 120s bash -c '
                            while true; do
                                STATUS=\$(docker inspect --format="{{.State.Health.Status}}" openapi_${env.TARGET_DEPLOY_VERSION} 2>/dev/null || echo "not_found")
                                if [ "\$STATUS" == "healthy" ]; then
                                    echo "✅ openapi_${env.TARGET_DEPLOY_VERSION} is healthy."
                                    break
                                elif [ "\$STATUS" == "unhealthy" ]; then
                                    echo "❌ openapi_${env.TARGET_DEPLOY_VERSION} is unhealthy. Aborting deployment."
                                    exit 1
                                elif [ "\$STATUS" == "not_found" ]; then
                                    echo "❌ openapi_${env.TARGET_DEPLOY_VERSION} container not found. Aborting deployment."
                                    exit 1
                                fi
                                echo "⏳ Waiting for openapi_${env.TARGET_DEPLOY_VERSION} to become healthy (current: \$STATUS)..."
                                sleep 5
                            done
                        ' || { echo "❌ Health check for openapi_${env.TARGET_DEPLOY_VERSION} timed out or failed."; exit 1; }
                    """
                }
            }
        }

        stage('Update Nginx Configuration') {
            when {
                branch 'master'
            }
            steps {
                echo '🌐 Nginx 설정을 업데이트 중...'
                script {
                    sh """
                        echo "===== 디버깅 정보 ====="
                        echo "현재 컨테이너 내부 사용자: \$(whoami)"
                        echo "현재 작업 디렉토리: \$(pwd)"
                        echo "ls -la /home/de/apps/openapi/nginx/nginx.conf 결과:"
                        ls -la /home/de/apps/openapi/nginx/nginx.conf || echo "❌ /home/de/apps/openapi/nginx/nginx.conf 파일 없음"
                        echo "ls -la ./nginx/nginx.conf 결과 (현재 디렉토리 기준):"
                        ls -la ./nginx/nginx.conf || echo "❌ ./nginx/nginx.conf 파일 없음"
                        echo "===== 디버깅 정보 끝 ====="

                        echo "Updating Nginx configuration to use openapi_${env.TARGET_DEPLOY_VERSION}_backend..."

                        # nginx를 중지하고 호스트 파일 수정 후 재시작
                        docker stop ${NGINX_CONTAINER}
                        # 수정하려는 파일 경로를 명확히 지정합니다.
                        sed -i 's|proxy_pass http://openapi_[^_]*_backend|proxy_pass http://openapi_${env.TARGET_DEPLOY_VERSION}_backend|g' /home/de/apps/openapi/nginx/nginx.conf || { echo "❌ sed 명령 실패: nginx.conf 파일 수정 불가. 경로 또는 내용 확인."; exit 1; }
                        docker start ${NGINX_CONTAINER}

                        echo "Testing Nginx configuration syntax..."
                        docker exec ${NGINX_CONTAINER} nginx -t || { echo "❌ Nginx config test failed. Check syntax or content."; exit 1; }

                        echo "Restarting Nginx to apply changes and switch traffic..."
                        docker restart ${NGINX_CONTAINER}

                        echo "Waiting for nginx to fully restart..."
                        sleep 5

                        echo "✅ Nginx configuration updated to use ${env.TARGET_DEPLOY_VERSION}."
                    """
                }
            }
        }
        
        stage('Synchronize Previous Version Image') {
            when {
                branch 'master'
            }
            steps {
                echo '🔄 Synchronizing previous version with new image...'
                script {
                    sh """
                        echo "Updating openapi_${env.CURRENT_ACTIVE_VERSION} container with new image..."
                        
                        # 이전 활성 버전 컨테이너를 신규 이미지로 업데이트
                        docker stop openapi_${env.CURRENT_ACTIVE_VERSION} 2>/dev/null || true
                        docker rm openapi_${env.CURRENT_ACTIVE_VERSION} 2>/dev/null || true
                        
                        docker run -d \\
                            --name openapi_${env.CURRENT_ACTIVE_VERSION} \\
                            --network ${NETWORK_NAME} \\
                            -p ${env.CURRENT_ACTIVE_VERSION == 'blue' ? '8001' : '8002'}:8000 \\
                            -e DEPLOY_ENV=${env.CURRENT_ACTIVE_VERSION} \\
                            -e PYTHONUNBUFFERED=1 \\
                            --restart unless-stopped \\
                            --health-cmd="curl -f http://localhost:8000/deploy || exit 1" \\
                            --health-interval=30s \\
                            --health-timeout=10s \\
                            --health-retries=3 \\
                            ${DOCKER_IMAGE}:${DOCKER_TAG}
                        
                        echo "⏳ Giving the ${env.CURRENT_ACTIVE_VERSION} container some time to initialize..."
                        sleep 10
                        
                        echo "✅ Previous version container updated with new image."
                    """
                }
            }
        }
        
        stage('Final Verification') {
            when {
                branch 'master'
            }
            steps {
                echo '✅ Final deployment verification...'
                script {
                    sh '''
                        sleep 5
                        echo "📋 Running application containers:"
                        docker ps | grep "openapi_" || echo "No openapi containers found"
                        
                        echo "🌐 Nginx container status:"
                        docker ps | grep "${NGINX_CONTAINER}" || echo "Nginx container not running"
                        
                        echo "🎉 Deployment successful and verified!"
                        echo "💡 Both blue and green containers are now running."
                        echo "🔄 You can manually switch traffic by updating nginx.conf and reloading nginx."
                    '''
                }
            }
        }
    }

    post {
        always {
            echo '🧹 Cleaning up workspace and temporary files...'
            script {
                sh '''
                    # 테스트 이미지 정리
                    docker rmi test-image 2>/dev/null || true
                    # dangling 이미지 정리
                    docker images -f "dangling=true" -q | xargs -r docker rmi -f 2>/dev/null || true
                '''
            }
        }
        success {
            echo '🎉 Pipeline completed successfully!'
            echo '💡 Blue-Green deployment completed. Both containers are running.'
        }
        failure {
            echo '❌ Pipeline failed! Attempting rollback...'
            script {
                sh """
                    echo "🔄 Initiating rollback to previous active version: ${env.CURRENT_ACTIVE_VERSION}..."
                    
                    # nginx 컨테이너 내부에서 직접 롤백 설정 적용
                    docker exec ${NGINX_CONTAINER} sed -i 's|proxy_pass http://openapi_[^_]*_backend|proxy_pass http://openapi_${env.CURRENT_ACTIVE_VERSION}_backend|g' /etc/nginx/nginx.conf
                    
                    echo "Testing Nginx configuration syntax for rollback..."
                    docker exec ${NGINX_CONTAINER} nginx -t || { echo "❌ Nginx rollback config test failed. Manual intervention may be needed."; }
                    
                    echo "Restarting Nginx to roll back traffic to ${env.CURRENT_ACTIVE_VERSION}..."
                    docker restart ${NGINX_CONTAINER}
                    
                    echo "Waiting for nginx to fully restart after rollback..."
                    sleep 5
                    echo "✅ Nginx configuration rolled back to ${env.CURRENT_ACTIVE_VERSION}."
        
                    # 이전 버전 컨테이너 재시작 (만약 중지되었다면)
                    echo "Restarting previous version container: openapi_${env.CURRENT_ACTIVE_VERSION}..."
                    docker start openapi_${env.CURRENT_ACTIVE_VERSION} 2>/dev/null || true
                    echo "✅ Previous version container openapi_${env.CURRENT_ACTIVE_VERSION} restarted."
                """
            }
        }
    }
}