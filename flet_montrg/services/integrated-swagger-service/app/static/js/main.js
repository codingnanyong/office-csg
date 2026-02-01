/**
 * 메인 초기화 로직
 */

// 중복 초기화 방지
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeApp);
} else {
    // DOM이 이미 로드된 경우 즉시 실행
    initializeApp();
}

async function initializeApp() {
    // 중복 실행 방지
    if (window._appInitialized) {
        console.warn('⚠️ App already initialized, skipping...');
        return;
    }
    window._appInitialized = true;
    
    console.log('🚀 Initializing Swagger UI...');
    
    try {
        // 서비스 선택 요소가 있으면 이벤트 리스너 추가
        const serviceSelect = document.getElementById('service-select');
        if (serviceSelect) {
            // 기존 리스너 제거 후 새로 추가 (중복 방지)
            const newSelect = serviceSelect.cloneNode(true);
            serviceSelect.parentNode.replaceChild(newSelect, serviceSelect);
            newSelect.addEventListener('change', loadSelectedService);
            console.log('✅ Service selector found and event listener attached');
        }
        // service-select가 없어도 정상 동작하므로 경고 제거
        
        // 서비스 목록 먼저 로드 (헤더 정보 업데이트를 위해)
        try {
            await loadServices();
        } catch (error) {
            console.warn('⚠️ Failed to load services list:', error);
        }
        
        // 통합 API 스펙 로드 (기본값)
        try {
            showLoading();
            console.log('📡 Loading integrated API spec from /openapi.json...');
            AppState.swaggerUI = initSwaggerUI('/openapi.json', 'Felt Montrg API Documentation');
            console.log('✅ Swagger UI initialized');
        } catch (error) {
            console.error('❌ Failed to initialize Swagger UI:', error);
            showError('Failed to load API specification: ' + error.message);
            hideLoading();
        }
    } catch (error) {
        console.error('❌ App initialization error:', error);
        // 에러를 조용히 처리하여 브라우저 확장 프로그램과의 충돌 방지
    }
}