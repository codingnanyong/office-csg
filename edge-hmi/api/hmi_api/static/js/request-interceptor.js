/**
 * Edge HMI API - request interceptor
 * Docker-compose: paths are /line_mst, /line_mst/{id} etc. Gateway proxies directly.
 * No URL rewriting needed (unlike flet_montrg /api/v1 -> /api/proxy).
 */
function createRequestInterceptor() {
  return (request) => request;
}
