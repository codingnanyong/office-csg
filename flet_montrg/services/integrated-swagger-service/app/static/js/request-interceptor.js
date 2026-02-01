/**
 * 요청 인터셉터 - API 요청을 프록시 경로로 변환
 */

function createRequestInterceptor() {
    return (request) => {
        console.log("🔍 Request Interceptor - Original request:", request.url);
        
        // Swagger 관련 요청은 프록시를 거치지 않음
        if (request.url.includes('/swagger/')) {
            console.log("⏭️ Skipping swagger request:", request.url);
            return request;
        }
        
        // OpenAPI 스펙 요청도 프록시를 거치지 않음  
        if (request.url.includes('openapi.json') || request.url.includes('/spec')) {
            console.log("⏭️ Skipping spec request:", request.url);
            return request;
        }
        
        // API 프록시를 통해 요청 라우팅
        if (request.url.startsWith('/api/proxy/')) {
            console.log("✅ Already proxy URL:", request.url);
            return request;
        }
        
        // API v1 경로인 경우 서비스명을 추출해서 프록시 경로로 변환
        if (request.url.includes('/api/v1/')) {
            let pathToConvert = request.url;
            let baseUrl = '';
            
            // 절대 URL인 경우 경로 부분만 추출
            if (request.url.startsWith('http')) {
                const urlObj = new URL(request.url);
                pathToConvert = urlObj.pathname + urlObj.search;
                baseUrl = urlObj.origin;
            }
            
            const serviceName = extractServiceFromPath(pathToConvert);
            console.log("🔄 Extracting service name from:", pathToConvert, "-> Service:", serviceName);
            
            if (serviceName) {
                // 초단축 URL: /api/{resource}/ 형태로 변환
                let resourcePath = pathToConvert.replace(/^\/api\/v\d+\//, '/');
                const newUrl = baseUrl + `/api${resourcePath}`;
                console.log("🚀 Converting URL:", request.url, "->", newUrl);
                console.log("📏 Ultra-shortened URL - Original:", pathToConvert, "-> Final:", `/api${resourcePath}`);
                request.url = newUrl;
            }
        }
        
        console.log("📤 Final request URL:", request.url);
        return request;
    };
}