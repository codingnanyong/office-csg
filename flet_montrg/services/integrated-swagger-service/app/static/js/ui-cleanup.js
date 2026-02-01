/**
 * UI 클린업 - 불필요한 요소 제거
 */

function removeInformationContainerPadding() {
    const infoContainers = document.querySelectorAll(
        '.information-container, .information-container.wrapper, .wrapper.information-container'
    );
    infoContainers.forEach(container => {
        container.style.setProperty('padding', '0', 'important');
        container.style.setProperty('padding-top', '0', 'important');
        container.style.setProperty('padding-bottom', '0', 'important');
        container.style.setProperty('padding-left', '0', 'important');
        container.style.setProperty('padding-right', '0', 'important');
        container.style.setProperty('margin', '0', 'important');
    });
}

function hideUnwantedElements() {
    UNWANTED_SELECTORS.forEach(selector => {
        const elements = document.querySelectorAll(selector);
        elements.forEach(el => {
            el.style.display = 'none';
            console.log("🧹 Hidden schema/model element:", selector);
        });
    });
}

function ensureAPISectionsVisible() {
    // 태그 헤더만 표시 (opblock은 접힌 상태 유지)
    const tagHeaders = document.querySelectorAll('.swagger-ui .opblock-tag, #swagger-ui .opblock-tag');
    tagHeaders.forEach(el => {
        el.style.display = 'block';
        el.style.visibility = 'visible';
    });
    
    // opblock은 접힌 상태로 유지 (사용자가 클릭한 경우만 표시)
    // ensureAPISectionsVisible에서는 opblock을 표시하지 않음
}

function cleanupDynamicElements() {
    let found = false;
    
    // 동적 선택자로 요소 제거
    DYNAMIC_UNWANTED_SELECTORS.forEach(selector => {
        const elements = document.querySelectorAll(selector);
        if (elements.length > 0) {
            elements.forEach(el => {
                el.style.display = 'none';
                el.style.visibility = 'hidden';
                el.remove();
                found = true;
            });
        }
    });
    
    // INVALID 텍스트가 포함된 모든 버튼 제거
    const allButtons = document.querySelectorAll('button, .btn, [class*="btn"], [class*="button"]');
    allButtons.forEach(btn => {
        const text = (btn.textContent || btn.innerText || '').trim();
        const ariaLabel = (btn.getAttribute('aria-label') || '').toUpperCase();
        const title = (btn.getAttribute('title') || '').toUpperCase();
        
        if (text.toUpperCase().includes('INVALID') || 
            ariaLabel.includes('INVALID') ||
            title.includes('INVALID') ||
            btn.classList.contains('invalid') ||
            btn.classList.toString().toUpperCase().includes('INVALID')) {
            btn.style.display = 'none';
            btn.style.visibility = 'hidden';
            btn.style.opacity = '0';
            btn.style.position = 'absolute';
            btn.style.left = '-9999px';
            btn.remove();
            found = true;
        }
    });
    
    // {} 아이콘이 있는 버튼도 제거
    const allElements = document.querySelectorAll('*');
    allElements.forEach(el => {
        const text = (el.textContent || el.innerText || '').trim();
        if ((text === '{}' || text === '{ }' || text.includes('INVALID')) && 
            (el.tagName === 'BUTTON' || el.classList.contains('btn') || el.getAttribute('role') === 'button')) {
            el.style.display = 'none';
            el.style.visibility = 'hidden';
            el.remove();
            found = true;
        }
    });
    
    if (found) {
        console.log("🧹 Cleaned up dynamically added unwanted elements");
    }
    
    return found;
}

function startCleanupInterval() {
    const cleanUpInterval = setInterval(() => {
        cleanupDynamicElements();
    }, UI_CONFIG.cleanupInterval);
    
    setTimeout(() => {
        clearInterval(cleanUpInterval);
        console.log("✅ Cleanup interval stopped");
    }, UI_CONFIG.cleanupDuration);
}