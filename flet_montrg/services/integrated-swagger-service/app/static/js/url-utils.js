/**
 * URL 유틸리티 함수
 * SERVICE_MAPPING은 config.js에서 import 필요
 */

function getServiceFromUrl(url) {
    try {
        const urlObj = new URL(url);
        const hostname = urlObj.hostname;
        // k8s 서비스명 형태인지 확인 (service-name.namespace.svc.cluster.local)
        if (hostname.includes('.')) {
            return hostname.split('.')[0];
        }
        return hostname;
    } catch (e) {
        return 'unknown';
    }
}

function getPathFromUrl(url) {
    try {
        const urlObj = new URL(url);
        return urlObj.pathname + urlObj.search;
    } catch (e) {
        return '/';
    }
}

function extractServiceFromPath(path) {
    // /api/v1/aggregation/ -> aggregation-service
    // /api/v1/location/ -> location-service  
    // /api/v1/realtime/ -> realtime-service
    // /api/v1/thresholds/ -> thresholds-service
    // /api/v1/alerts/ -> alert-service
    // /api/v1/subscriptions/ -> alert-subscription-service
    // /api/v1/notifications/ -> alert-notification-service
    // /api/v1/mappings/ -> sensor-threshold-mapping-service
    
    const pathSegments = path.split('/').filter(seg => seg);
    if (pathSegments.length >= 3 && pathSegments[0] === 'api' && pathSegments[1] === 'v1') {
        const servicePath = pathSegments[2];
        return SERVICE_MAPPING[servicePath] || servicePath;
    }
    
    return null;
}