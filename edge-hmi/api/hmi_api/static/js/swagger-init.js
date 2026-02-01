/**
 * Edge HMI API - Swagger UI init (Docker-compose, /openapi.json)
 */
function initSwaggerUI(specUrl, title) {
  const ui = SwaggerUIBundle({
    url: specUrl,
    dom_id: '#swagger-ui',
    deepLinking: true,
    presets: [
      SwaggerUIBundle.presets.apis,
      SwaggerUIStandalonePreset
    ],
    plugins: [SwaggerUIBundle.plugins.DownloadUrl],
    layout: 'StandaloneLayout',
    displayOperationId: false,
    showExtensions: false,
    tryItOutEnabled: true,
    docExpansion: 'list',
    requestInterceptor: createRequestInterceptor(),
    onComplete: () => {
      setTimeout(() => {
        try {
          removeInformationContainerPadding();
          updateDefaultSwaggerHeader();
          startHeaderUpdateInterval();
          hideUnwantedElements();
          ensureAPISectionsVisible();
          hideLoading();
        } catch (e) {
          console.debug('swagger-init onComplete:', e);
          hideLoading();
        }
      }, typeof UI_CONFIG !== 'undefined' ? UI_CONFIG.initDelay : 200);
    },
    onFailure: (err) => {
      console.error('Swagger UI load failed:', err);
      try { showError('API 스펙 로드 실패: ' + (err && err.message ? err.message : String(err))); } catch (_) {}
      try { hideLoading(); } catch (_) {}
    }
  });
  return ui;
}
