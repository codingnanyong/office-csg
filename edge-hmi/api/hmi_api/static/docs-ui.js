const DESC = {
  line_mst: '라인 마스터 - 라인 기준 정보 관리 API',
  equip_mst: '설비 마스터 - 설비 기준 정보 관리 API',
  sensor_mst: '센서 마스터 - 센서 기준 정보 관리 API',
  worker_mst: '작업자 마스터 - 작업자 기준 정보 관리 API',
  shift_cfg: 'Shift 설정 - 교대 설정 관리 API',
  kpi_cfg: 'KPI 설정 - KPI 설정 관리 API',
  alarm_cfg: '알람 설정 - 알람 정의 관리 API',
  maint_cfg: '보전 설정 - 보전 유형 관리 API',
  measurement: '측정값 - 시계열 측정 데이터 API (hypertable)',
  status_his: '상태 이력 - 설비 상태 이력 API',
  prod_his: '생산 이력 - 생산 실적 이력 API',
  alarm_his: '알람 이력 - 알람 발생 이력 API',
  maint_his: '보전 이력 - 보전 작업 이력 API',
  shift_map: 'Shift 매핑 - 일자별 shift·작업자 매핑 API',
  kpi_sum: 'KPI 집계 - KPI 집계 결과 조회 API (read-only)',
};

const KNOWN_TAGS = new Set(Object.keys(DESC));

function methodClass(m) {
  const u = (m || '').toLowerCase();
  if (['get'].includes(u)) return 'get';
  if (['post'].includes(u)) return 'post';
  if (['patch', 'put'].includes(u)) return 'patch';
  if (['delete'].includes(u)) return 'delete';
  return '';
}

async function load() {
  try {
    const r = await fetch('/openapi.json');
    if (!r.ok) throw new Error('Failed to fetch OpenAPI spec');
    const spec = await r.json();
    document.getElementById('loading').style.display = 'none';
    if (spec.info?.version) document.getElementById('version').textContent = spec.info.version;
    if (spec['x-source'] !== 'gateway-aggregated') {
      const el = document.getElementById('error');
      el.style.display = 'block';
      el.textContent = 'OpenAPI가 게이트웨이 aggregated가 아닙니다. 테이블별 스키마를 사용할 수 없습니다.';
      return;
    }
    render(spec);
  } catch (e) {
    document.getElementById('loading').style.display = 'none';
    const el = document.getElementById('error');
    el.style.display = 'block';
    el.textContent = 'OpenAPI 로드 실패: ' + (e.message || String(e));
  }
}

function render(spec) {
  const byTag = {};
  const paths = spec.paths || {};
  for (const [path, pathItem] of Object.entries(paths)) {
    if (path === '/' || path === '/health' || path === '/openapi.json') continue;
    for (const [method, op] of Object.entries(pathItem)) {
      if (!['get', 'post', 'put', 'patch', 'delete'].includes(method)) continue;
      const tags = op.tags || [];
      for (const tag of tags) {
        if (!KNOWN_TAGS.has(tag) || tag === 'default') continue;
        if (!byTag[tag]) byTag[tag] = [];
        byTag[tag].push({
          method: method.toUpperCase(),
          path,
          summary: op.summary || op.operationId || '',
        });
      }
    }
  }
  const tags = Object.keys(byTag).sort();
  document.getElementById('total-count').textContent = tags.length;

  const tagsEl = document.getElementById('tags');
  tagsEl.innerHTML = tags.map((t) => `<span class="tag">${t}</span>`).join('');

  const cardsEl = document.getElementById('cards');
  if (tags.length === 0) {
    cardsEl.innerHTML = '<p class="empty-msg">테이블 API 컨테이너가 기동되지 않았거나 연결할 수 없습니다.<br><code>docker compose up -d --build</code> 로 전체 서비스를 띄운 뒤 새로고침하세요.</p>';
    return;
  }
  let epId = 0;
  cardsEl.innerHTML = tags
    .map((tag) => {
      const items = byTag[tag];
      const desc = DESC[tag] || `${tag} API`;
      const endpointsHtml = items
        .map((ep) => {
          const id = `ep-${++epId}`;
          return `
          <li class="endpoint" data-method="${ep.method}" data-path="${ep.path}">
            <div class="endpoint-row">
              <span class="method ${methodClass(ep.method)}">${ep.method}</span>
              <span class="path">${ep.path}</span>
              <span class="summary">${ep.summary || ''}</span>
              <button type="button" class="btn-execute" data-ep-id="${id}">Execute</button>
            </div>
            <div id="${id}" class="response-box" style="display:none;"></div>
          </li>`;
        })
        .join('');
      return `
          <div class="card" data-tag="${tag}">
            <div class="card-header">
              <span class="card-arrow">›</span>
              <div>
                <h2 class="card-title">${tag}</h2>
                <p class="card-desc">${desc}</p>
              </div>
            </div>
            <div class="card-body">
              <ul class="endpoints">${endpointsHtml}</ul>
            </div>
          </div>`;
    })
    .join('');

  cardsEl.querySelectorAll('.card-header').forEach((h) => {
    h.addEventListener('click', () => {
      const card = h.closest('.card');
      card.classList.toggle('open');
    });
  });

  cardsEl.querySelectorAll('.btn-execute').forEach((btn) => {
    btn.addEventListener('click', async (e) => {
      const row = btn.closest('.endpoint');
      const method = row.dataset.method;
      const path = row.dataset.path;
      const box = document.getElementById(btn.dataset.epId);
      box.style.display = 'block';
      box.innerHTML = '<div class="response-loading">요청 중…</div>';
      try {
        let url = path;
        if (method === 'GET' && !/\{\w+\}/.test(path)) {
          const sep = path.includes('?') ? '&' : '?';
          url = path + sep + 'skip=0&limit=100';
        }
        const res = await fetch(url, { method });
        const type = res.headers.get('content-type') || '';
        let body = await res.text();
        if (type.includes('application/json')) {
          try {
            body = JSON.stringify(JSON.parse(body), null, 2);
          } catch (_) {}
        }
        const displayUrl = window.location.origin + (path !== url ? url : path);
        box.innerHTML = `
          <div class="response-head">
            <span class="response-status">Server response: ${res.status} ${res.statusText}</span>
            <button type="button" class="btn-clear">Clear</button>
          </div>
          <div class="response-url">Request URL: <code>${escapeHtml(displayUrl)}</code></div>
          <pre class="response-body">${escapeHtml(body)}</pre>`;
        box.querySelector('.btn-clear').addEventListener('click', () => {
          box.style.display = 'none';
          box.innerHTML = '';
        });
      } catch (err) {
        box.innerHTML = `
          <div class="response-head">
            <span class="response-error">${escapeHtml(String(err.message))}</span>
            <button type="button" class="btn-clear">Clear</button>
          </div>`;
        box.querySelector('.btn-clear').addEventListener('click', () => {
          box.style.display = 'none';
          box.innerHTML = '';
        });
      }
    });
  });
}

function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

load();
