/**
 * 전역 설정 및 변수
 */

// 전역 상태
const AppState = {
    swaggerUI: null,
    availableServices: {}
};

// 서비스명 매핑
const SERVICE_MAPPING = {
    'aggregation': 'aggregation-service',
    'location': 'location-service',
    'realtime': 'realtime-service',
    'thresholds': 'thresholds-service',
    'alerts': 'alert-service',
    'subscriptions': 'alert-subscription-service',
    'notifications': 'alert-notification-service',
    'mappings': 'sensor-threshold-mapping-service'
};

// UI 설정
const UI_CONFIG = {
    cleanupInterval: 500,
    cleanupDuration: 10000,
    headerUpdateInterval: 1000,
    headerUpdateDuration: 5000,
    initDelay: 200
};

// 불필요한 요소 선택자
const UNWANTED_SELECTORS = [
    '.swagger-ui .topbar',
    '.swagger-ui .authorization__btn',
    '.swagger-ui .auth-wrapper',
    '.swagger-ui .scheme-container',
    '.swagger-ui .models',
    '.swagger-ui .model-container',
    '.swagger-ui .model',
    '.swagger-ui .model-toggle',
    '.swagger-ui .model-box',
    '.swagger-ui .model-box-control',
    '.swagger-ui .models-control',
    '.swagger-ui .invalid',
    '.swagger-ui .validation-errors',
    '.swagger-ui .response-schema',
    '.swagger-ui table.model',
    '.swagger-ui .property-row',
    '.swagger-ui .download-contents',
    '.swagger-ui .copy-to-clipboard'
];

const DYNAMIC_UNWANTED_SELECTORS = [
    '.swagger-ui .models',
    '.swagger-ui .model',
    '.swagger-ui .invalid',
    '.swagger-ui .btn.invalid',
    '.swagger-ui table.model',
    '.swagger-ui .model-container',
    '.swagger-ui .response-schema',
    '.swagger-ui .model-toggle',
    '.swagger-ui .model-box',
    '.swagger-ui .model-box-control',
    '.swagger-ui .scheme-container .model-toggle',
    'button[title*="invalid" i]',
    'button[title*="model" i]'
];