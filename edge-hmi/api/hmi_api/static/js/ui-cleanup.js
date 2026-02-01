/**
 * Edge HMI API - UI cleanup
 */
function removeInformationContainerPadding() {
  const els = document.querySelectorAll('.information-container, .information-container.wrapper, .wrapper.information-container');
  els.forEach(el => {
    el.style.setProperty('padding', '0', 'important');
    el.style.setProperty('margin', '0', 'important');
  });
}

function hideUnwantedElements() {
  (typeof UNWANTED_SELECTORS !== 'undefined' ? UNWANTED_SELECTORS : []).forEach(sel => {
    document.querySelectorAll(sel).forEach(el => { el.style.display = 'none'; });
  });
}

function ensureAPISectionsVisible() {
  document.querySelectorAll('.swagger-ui .opblock-tag, #swagger-ui .opblock-tag').forEach(el => {
    el.style.display = 'block';
    el.style.visibility = 'visible';
  });
}

function cleanupDynamicElements() {
  (typeof DYNAMIC_UNWANTED_SELECTORS !== 'undefined' ? DYNAMIC_UNWANTED_SELECTORS : []).forEach(sel => {
    document.querySelectorAll(sel).forEach(el => {
      el.style.display = 'none';
      el.style.visibility = 'hidden';
    });
  });
}

function startCleanupInterval() {
  let n = 0;
  const id = setInterval(() => {
    cleanupDynamicElements();
    if (++n >= 20) clearInterval(id);
  }, UI_CONFIG.cleanupInterval || 500);
}
