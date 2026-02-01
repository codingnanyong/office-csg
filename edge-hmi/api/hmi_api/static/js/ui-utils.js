/**
 * Edge HMI API - UI utils
 */
function showLoading() {
  const el = document.getElementById('loading');
  if (el) el.style.display = 'block';
}

function hideLoading() {
  const el = document.getElementById('loading');
  if (el) el.style.display = 'none';
}

function showError(message) {
  const container = document.getElementById('error-container');
  if (container) container.innerHTML = `<div class="error">${message}</div>`;
}

function clearError() {
  const container = document.getElementById('error-container');
  if (container) container.innerHTML = '';
}
