/**
 * 서비스 관리 - 서비스 목록 로드 및 관리
 */

async function loadServices() {
    try {
        const response = await fetch('/api/v1/swagger/services');
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const data = await response.json();
        AppState.availableServices = data.services || {};
        
        updateServiceSelector();
        updateServiceCount();
        updateDefaultSwaggerHeader();
        
    } catch (error) {
        console.error('Failed to load services:', error);
        showError('Failed to load service list: ' + error.message);
    }
}

function updateServiceSelector() {
    const select = document.getElementById('service-select');
    
    // service-select 요소가 없으면 스킵 (조용히 처리)
    if (!select) {
        // 경고 제거: 선택적 기능이므로 경고가 필요 없음
        return;
    }
    
    // 기존 옵션 제거 (첫 번째 통합 옵션 제외)
    while (select.children.length > 1) {
        select.removeChild(select.lastChild);
    }
    
    // 서비스별 옵션 추가
    Object.entries(AppState.availableServices).forEach(([serviceName, spec]) => {
        const option = document.createElement('option');
        option.value = serviceName;
        
        if (spec.is_available) {
            option.textContent = `📋 ${spec.title} (v${spec.version})`;
        } else {
            option.textContent = `❌ ${serviceName} (unavailable)`;
            option.disabled = true;
        }
        
        select.appendChild(option);
    });
}

function updateServiceCount() {
    const serviceCount = document.getElementById('service-count');
    if (!serviceCount) {
        // service-count 요소가 없으면 스킵 (조용히 처리)
        return;
    }
    
    const available = Object.values(AppState.availableServices).filter(s => s.is_available).length;
    const total = Object.keys(AppState.availableServices).length;
    serviceCount.textContent = `${available}/${total} available microservices`;
}

async function loadSelectedService() {
    try {
        const select = document.getElementById('service-select');
        if (!select) {
            // 선택적 기능이므로 조용히 처리
            return;
        }
        
        const selectedService = select.value;
        
        clearError();
        clearServiceInfo();
        showLoading();
        
        let specUrl;
        let title;
        
        if (selectedService === 'integrated') {
            specUrl = '/openapi.json';
            title = 'Felt Montrg API Documentation';
            showServiceInfo({
                title: 'Felt Montrg API Documentation',
                description: `Total ${Object.values(AppState.availableServices).filter(s => s.is_available).length} microservices integrated API`,
                version: '1.0.0',
                services: Object.keys(AppState.availableServices).filter(name => AppState.availableServices[name].is_available)
            });
        } else {
            specUrl = `/api/v1/swagger/services/${selectedService}/spec`;
            const serviceSpec = AppState.availableServices[selectedService];
            title = serviceSpec ? serviceSpec.title : selectedService;
            
            if (serviceSpec) {
                showServiceInfo(serviceSpec);
            }
            
            // 디버깅을 위한 로그
            console.log(`Loading service spec: ${selectedService}`);
            console.log(`Spec URL: ${specUrl}`);
        }
        
        // Swagger UI 다시 초기화 (에러 처리 강화)
        try {
            AppState.swaggerUI = initSwaggerUI(specUrl, title);
        } catch (initError) {
            console.error('Failed to initialize Swagger UI:', initError);
            showError('Failed to initialize API documentation: ' + initError.message);
            hideLoading();
        }
        
    } catch (error) {
        console.error('Failed to load service spec:', error);
        // 브라우저 확장 프로그램과의 충돌을 방지하기 위해 에러를 조용히 처리
        try {
            showError('Failed to load API specification: ' + error.message);
        } catch (displayError) {
            console.error('Failed to display error:', displayError);
        }
        hideLoading();
    }
}

async function refreshSpecs() {
    showLoading();
    clearError();
    
    try {
        // 서버에서 스펙 새로고침 요청
        const response = await fetch('/api/v1/swagger/refresh', { method: 'POST' });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        // 서비스 목록 다시 로드 (헤더 정보도 업데이트됨)
        await loadServices();
        
        // 현재 선택된 서비스 다시 로드
        const serviceSelect = document.getElementById('service-select');
        if (serviceSelect) {
            await loadSelectedService();
        } else {
            // 서비스 선택이 없으면 Swagger UI만 다시 초기화
            AppState.swaggerUI = initSwaggerUI('/openapi.json', 'Felt Montrg API Documentation');
        }
        
        console.log('API specs refreshed successfully');
        
    } catch (error) {
        console.error('Failed to refresh specs:', error);
        showError('Failed to refresh API specification: ' + error.message);
    } finally {
        hideLoading();
    }
}