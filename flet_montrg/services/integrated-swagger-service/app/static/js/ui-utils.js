/**
 * UI 유틸리티 함수
 */

function showLoading() {
    const loadingEl = document.getElementById('loading');
    if (loadingEl) {
        loadingEl.style.display = 'block';
    }
}

function hideLoading() {
    const loadingEl = document.getElementById('loading');
    if (loadingEl) {
        loadingEl.style.display = 'none';
    }
}

function showError(message) {
    const container = document.getElementById('error-container');
    if (container) {
        container.innerHTML = `<div class="error">${message}</div>`;
    }
}

function clearError() {
    const container = document.getElementById('error-container');
    if (container) {
        container.innerHTML = '';
    }
}

function showServiceInfo(info) {
    const container = document.getElementById('service-info-container');
    if (!container) return;
    
    let servicesText = '';
    if (info.services && info.services.length > 0) {
        servicesText = `<p><strong>Included services:</strong> ${info.services.join(', ')}</p>`;
    }
    
    container.innerHTML = `
        <div class="service-info">
            <h3>${info.title}</h3>
            <p><strong>Version:</strong> ${info.version}</p>
            <p><strong>Description:</strong> ${info.description || 'API documentation'}</p>
            ${servicesText}
        </div>
    `;
}

function clearServiceInfo() {
    const container = document.getElementById('service-info-container');
    if (container) {
        container.innerHTML = '';
    }
}