#!/bin/bash

# Blue-Green 배포 스크립트
set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 로그 함수
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 현재 활성 버전 확인
get_current_version() {
    local response=$(curl -s http://${DEPLOY_HOST:-localhost}/deploy 2>/dev/null || echo '{"current_deploy":"blue"}')
    if echo "$response" | grep -q '"current_deploy"'; then
        echo "$response" | sed 's/.*"current_deploy":"\([^"]*\)".*/\1/'
    else
        echo "blue"
    fi
}

# 배포할 버전 결정
get_target_version() {
    local current=$1
    if [ "$current" = "blue" ]; then
        echo "green"
    else
        echo "blue"
    fi
}

# 컨테이너 상태 확인
check_container_health() {
    local container_name=$1
    local max_attempts=30
    local attempt=1
    
    log_info "Checking health of $container_name..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker ps | grep -q "$container_name.*Up"; then
            if docker exec "$container_name" curl -f http://localhost:8000/deploy >/dev/null 2>&1; then
                log_success "$container_name is healthy"
                return 0
            fi
        fi
        
        log_warning "Attempt $attempt/$max_attempts: $container_name not ready yet..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    log_error "$container_name failed to become healthy"
    return 1
}

# nginx 설정 업데이트
update_nginx_config() {
    local target_version=$1
    local nginx_conf="nginx/nginx.conf"
    
    log_info "Updating nginx configuration for $target_version..."
    
    # 백업 생성
    cp "$nginx_conf" "${nginx_conf}.backup.$(date +%Y%m%d_%H%M%S)"
    
    # 설정 업데이트
    sed -i "s/server openapi_[^:]*:8000/server openapi_${target_version}:8000/g" "$nginx_conf"
    
    # 설정 테스트
    if docker exec nginx nginx -t; then
        # nginx 리로드
        docker exec nginx nginx -s reload
        log_success "Nginx configuration updated successfully"
    else
        log_error "Nginx configuration test failed, restoring backup..."
        cp "${nginx_conf}.backup.$(date +%Y%m%d_%H%M%S)" "$nginx_conf"
        docker exec nginx nginx -s reload
        exit 1
    fi
}

# 이전 컨테이너 정리
cleanup_old_container() {
    local old_version=$1
    
    log_info "Cleaning up old container: openapi_$old_version"
    
    docker stop "openapi_$old_version" 2>/dev/null || log_warning "Container openapi_$old_version was not running"
    docker rm "openapi_$old_version" 2>/dev/null || log_warning "Container openapi_$old_version was not found"
    
    log_success "Old container cleanup completed"
}

# 메인 배포 로직
main() {
    log_info "Starting Blue-Green deployment..."
    
    # 현재 작업 디렉토리 확인
    log_info "Current working directory: $(pwd)"
    log_info "Directory contents:"
    ls -la
    
    # 현재 버전 확인
    local current_version=$(get_current_version)
    local target_version=$(get_target_version "$current_version")
    
    log_info "Current version: $current_version"
    log_info "Target version: $target_version"
    
    # Docker Compose 파일 경로
    local compose_file="docker/docker-compose.ops.yml"
    
    # 1. 기존 컨테이너 중지 및 제거
    log_info "Stopping and removing existing $target_version container..."
    docker-compose -f "$compose_file" stop "$target_version" 2>/dev/null || log_warning "Container not running"
    docker-compose -f "$compose_file" rm -f "$target_version" 2>/dev/null || log_warning "Container not found"
    
    # 2. 새 컨테이너 빌드 및 시작
    log_info "Building and starting $target_version container..."
    docker-compose -f "$compose_file" up -d --build "$target_version"
    
    # 3. 새 컨테이너 헬스체크
    if ! check_container_health "openapi_$target_version"; then
        log_error "New container failed health check"
        exit 1
    fi
    
    # 4. nginx 설정 업데이트
    update_nginx_config "$target_version"
    
    # 5. 배포 검증
    log_info "Verifying deployment..."
    sleep 5
    
    local verification_response=$(curl -s http://${DEPLOY_HOST:-localhost}/deploy 2>/dev/null || echo "{}")
    log_info "Deployment verification response: $verification_response"
    
    if curl -f http://${DEPLOY_HOST:-localhost}/deploy >/dev/null 2>&1; then
        log_success "Deployment verification successful"
    else
        log_error "Deployment verification failed"
        exit 1
    fi
    
    # 6. 이전 컨테이너 정리
    cleanup_old_container "$current_version"
    
    # 7. 최종 상태 확인
    log_info "Final deployment status:"
    docker ps | grep openapi_ || log_warning "No openapi containers found"
    
    log_success "Blue-Green deployment completed successfully!"
    log_info "Active version: $target_version"
}

# 스크립트 실행
main "$@" 