/**
 * Edge HMI API - header manager (Included services from /info)
 */
function updateDefaultSwaggerHeader() {
  try {
    const info = document.querySelector('.swagger-ui .information-container .info');
    if (!info) return;

    const extra = info.querySelector('.service-info-extra');
    if (extra) extra.remove();

    const serviceNames = Object.keys(AppState.availableServices || {}).filter(
      k => AppState.availableServices[k] && AppState.availableServices[k].is_available !== false
    );
    const count = serviceNames.length;

    const desc = info.querySelector('.description');
    if (desc) {
      desc.textContent = `Total ${count} tables integrated API documentation`;
      desc.style.textAlign = 'center';
      desc.style.width = '100%';
    }

    const div = document.createElement('div');
    div.className = 'service-info-extra';
    const tags = serviceNames.length
      ? serviceNames.map(s => `<span class="service-tag">${s}</span>`).join('')
      : 'Loading...';
    div.innerHTML = `<p style="margin:0;text-align:center;width:100%"><strong>Included services:</strong> <span class="service-tags-container">${tags}</span></p>`;

    if (desc) desc.insertAdjacentElement('afterend', div);
    else info.querySelector('.title')?.insertAdjacentElement('afterend', div);
  } catch (e) {
    console.debug('header-manager:', e);
  }
}

function startHeaderUpdateInterval() {
  let n = 0;
  const id = setInterval(() => {
    updateDefaultSwaggerHeader();
    if (++n >= 5) clearInterval(id);
  }, 400);
}
