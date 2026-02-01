/**
 * Edge HMI API - config (Docker/docker-compose, no K8s)
 */
const AppState = {
  swaggerUI: null,
  availableServices: {}
};

const UI_CONFIG = {
  cleanupInterval: 500,
  initDelay: 200
};

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
  '.swagger-ui .models-control',
  '.swagger-ui .invalid',
  '.swagger-ui .validation-errors',
  '.swagger-ui .response-schema',
  '.swagger-ui table.model',
  '.swagger-ui .property-row'
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
  '.swagger-ui .scheme-container .model-toggle'
];
