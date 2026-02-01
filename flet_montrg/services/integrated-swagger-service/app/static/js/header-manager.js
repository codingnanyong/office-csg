/**
 * 헤더 관리 - 동적 헤더 정보 업데이트
 */

function updateDefaultSwaggerHeader() {
    try {
        const infoContainer = document.querySelector('.swagger-ui .information-container .info');
        if (!infoContainer) {
            // Swagger UI가 아직 로드되지 않았을 수 있으므로 조용히 처리
            // startHeaderUpdateInterval에서 재시도하므로 경고 불필요
            return;
        }
        
        // 기존 서비스 정보 요소 제거 (중복 방지)
        const existingExtra = infoContainer.querySelector('.service-info-extra');
        if (existingExtra) {
            existingExtra.remove();
        }
        
        // 중복되는 contact, link, servers 정보 숨기기
        const contact = infoContainer.querySelector('.info__contact');
        if (contact) contact.style.display = 'none';
        
        const link = infoContainer.querySelector('.link');
        if (link) link.style.display = 'none';
        
        const baseUrl = infoContainer.querySelector('.base-url');
        if (baseUrl) baseUrl.style.display = 'none';
        
        const serviceNames = Object.keys(AppState.availableServices).filter(name => {
            const service = AppState.availableServices[name];
            return service && service.is_available;
        });
        
        const count = serviceNames.length;
        
        // description 내용 업데이트 (서비스 개수 표시) - 중앙 정렬
        const description = infoContainer.querySelector('.description');
        if (description) {
            description.textContent = `Total ${count} microservices integrated API documentation`;
            description.style.textAlign = 'center';
            description.style.width = '100%';
        }
        
        // 서비스 목록만 추가 (중앙 정렬)
        const serviceInfoDiv = document.createElement('div');
        serviceInfoDiv.className = 'service-info-extra';
        
        let serviceListHtml = '<strong>Included services :</strong> ';
        if (serviceNames.length > 0) {
            const serviceTags = serviceNames.map(name => 
                `<span class="service-tag">${name}</span>`
            ).join('');
            serviceListHtml += `<span id="service-list" class="service-tags-container">${serviceTags}</span>`;
        } else {
            serviceListHtml += '<span id="service-list">Loading...</span>';
        }
        
        serviceInfoDiv.innerHTML = `
            <p style="margin: 0; text-align: center; width: 100%;">
                ${serviceListHtml}
            </p>
        `;
        
        // description 뒤에 추가
        if (description) {
            description.insertAdjacentElement('afterend', serviceInfoDiv);
        } else {
            // description이 없으면 title 뒤에 추가
            const title = infoContainer.querySelector('.title');
            if (title) {
                title.insertAdjacentElement('afterend', serviceInfoDiv);
            }
        }
        
        console.log(`✅ Default Swagger header updated: ${count} microservices`);
    } catch (error) {
        console.error('Failed to update default Swagger header:', error);
    }
}

function startHeaderUpdateInterval() {
    const updateInterval = setInterval(() => {
        const infoContainer = document.querySelector('.swagger-ui .information-container .info');
        if (infoContainer && !infoContainer.querySelector('.service-info-extra')) {
            updateDefaultSwaggerHeader();
        }
    }, UI_CONFIG.headerUpdateInterval);
    
    setTimeout(() => clearInterval(updateInterval), UI_CONFIG.headerUpdateDuration);
}

// 레거시 호환성
function updateHeaderServiceInfo() {
    updateDefaultSwaggerHeader();
}