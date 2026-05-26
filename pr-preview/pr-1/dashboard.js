const REFRESH_MS = 30_000;
const palette = {
  accent: '#bdff00',
  accentSoft: 'rgba(189, 255, 0, 0.14)',
  accentLine: 'rgba(189, 255, 0, 0.85)',
  red: '#ff4d4d',
  redSoft: 'rgba(255, 77, 77, 0.16)',
  blue: '#4da6ff',
  blueSoft: 'rgba(77, 166, 255, 0.16)',
  amber: '#ffb547',
  text: '#ededed',
  dim: '#707070',
  faint: '#404040',
  grid: 'rgba(255, 255, 255, 0.04)',
  axis: 'rgba(255, 255, 255, 0.06)',
  bg: '#050505',
};

Chart.defaults.color = palette.dim;
Chart.defaults.borderColor = palette.grid;
Chart.defaults.font.family = '"JetBrains Mono", ui-monospace, monospace';
Chart.defaults.font.size = 10.5;

const baseScales = {
  x: {
    ticks: { color: palette.dim, maxRotation: 0, autoSkipPadding: 24, font: { size: 10 } },
    grid: { color: palette.grid, drawTicks: false },
    border: { color: palette.axis },
  },
  y: {
    ticks: { color: palette.dim, font: { size: 10 }, padding: 6 },
    grid: { color: palette.grid, drawTicks: false },
    border: { display: false },
    beginAtZero: true,
  },
};

const baseTooltip = {
  backgroundColor: '#000',
  borderColor: '#262626',
  borderWidth: 1,
  cornerRadius: 0,
  padding: 10,
  titleColor: palette.text,
  titleFont: { family: '"JetBrains Mono", monospace', size: 10, weight: '500' },
  bodyColor: palette.text,
  bodyFont: { family: '"JetBrains Mono", monospace', size: 11 },
  displayColors: true,
  boxPadding: 4,
};

const baseChartDefaults = {
  responsive: true,
  maintainAspectRatio: false,
  animation: { duration: 250 },
  interaction: { mode: 'index', intersect: false },
  plugins: {
    legend: {
      labels: {
        color: palette.dim,
        font: { size: 10, family: '"JetBrains Mono", monospace' },
        usePointStyle: true,
        pointStyle: 'rectRounded',
        boxWidth: 8,
        boxHeight: 8,
      },
    },
    tooltip: baseTooltip,
  },
  scales: baseScales,
};

const buckets = { deploys: 'day', verified: 'week' };
const charts = {};
const recentState = {
  limit: 20,
  page: 0,
  cursors: [null],
  nextCursor: null,
  hasMore: false,
  loading: false,
};

async function fetchJson(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${path} → ${r.status}`);
  return r.json();
}

async function postJson(path, payload) {
  const r = await fetch(path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data.error || `${path} → ${r.status}`);
  return data;
}

function fmtNumber(n) {
  if (n === null || n === undefined) return '—';
  const v = Number(n);
  if (!isFinite(v)) return '—';
  const abs = Math.abs(v);
  if (abs >= 1e9) return (v / 1e9).toFixed(2) + 'B';
  if (abs >= 1e6) return (v / 1e6).toFixed(2) + 'M';
  if (abs >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  return String(v);
}

function fmtFull(n) {
  if (n === null || n === undefined) return '—';
  return Number(n).toLocaleString('en-US');
}

function fmtBytes(n) {
  if (n < 1024) return n + ' B';
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
  return (n / 1024 / 1024).toFixed(1) + ' MB';
}

function fmtTime(iso) {
  if (!iso) return '—';
  return new Date(iso).toISOString().slice(0, 16).replace('T', ' ') + ' UTC';
}

function bucketLabel(row, bucket) {
  if (!row.timestamp) return `${fmtFull(row.block_start)}-${fmtFull(row.block_end)}`;
  const timestamp = Date.parse(row.timestamp);
  if (!Number.isFinite(timestamp)) return `${fmtFull(row.block_start)}-${fmtFull(row.block_end)}`;
  const day = new Date(timestamp).toISOString().slice(0, 10);
  return bucket === 'month' ? day.slice(0, 7) : day;
}

function setStatus(state, text) {
  const dot = document.getElementById('status-dot');
  const txt = document.getElementById('status-text');
  dot.classList.remove('connected', 'sync', 'live', 'error');
  if (state === 'connected') dot.classList.add('connected');
  if (state === 'sync') dot.classList.add('sync');
  if (state === 'live') dot.classList.add('live');
  if (state === 'error') dot.classList.add('error');
  txt.textContent = text;
}

function isoAgeMs(iso) {
  if (!iso) return Infinity;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? Date.now() - t : Infinity;
}

function shortError(message) {
  if (!message) return '';
  return message.length > 72 ? message.slice(0, 69) + '...' : message;
}

function renderRuntimeStatus(runtime) {
  const ts = new Date().toISOString().slice(11, 19) + ' UTC';
  if (!runtime) {
    setStatus('connected', `snapshot · ${ts}`);
    return;
  }

  if (runtime.tail_enabled) {
    const freshMs = Math.max((runtime.tail_interval_secs || 60) * 3 * 1000, 180_000);
    const tailErrorIsLatest =
      runtime.tail_last_error &&
      isoAgeMs(runtime.tail_last_error_at) < isoAgeMs(runtime.tail_last_ok_at);
    if (tailErrorIsLatest) {
      setStatus('error', `tail error · ${shortError(runtime.tail_last_error)}`);
      return;
    }
    if (runtime.tail_last_ok_at && isoAgeMs(runtime.tail_last_ok_at) <= freshMs) {
      const block = runtime.tail_last_block ? ` · block ${fmtFull(runtime.tail_last_block)}` : '';
      setStatus('live', `live${block}`);
      return;
    }
    if (runtime.tail_last_error) {
      setStatus('error', `tail error · ${shortError(runtime.tail_last_error)}`);
      return;
    }
    setStatus('sync', runtime.tail_running ? 'tailing latest contracts' : 'tail starting');
    return;
  }

  if (runtime.metadata_sync_enabled) {
    const errorIsLatest =
      runtime.metadata_last_error &&
      isoAgeMs(runtime.metadata_last_error_at) < isoAgeMs(runtime.metadata_last_ok_at);
    if (errorIsLatest) {
      setStatus('error', `metadata sync error · ${shortError(runtime.metadata_last_error)}`);
      return;
    }
    setStatus('sync', runtime.metadata_sync_running ? 'syncing metadata' : 'metadata sync on');
    return;
  }

  setStatus('connected', runtime.read_only ? `read only · ${ts}` : `snapshot · ${ts}`);
}

async function loadStats() {
  const s = await fetchJson('/api/stats');
  document.getElementById('m-total').textContent = fmtFull(s.total_contracts);
  document.getElementById('m-block-range').textContent =
    s.first_block === 0 && s.last_block === 0
      ? 'no data yet'
      : `block range: ${fmtFull(s.first_block)} - ${fmtFull(s.last_block)}`;

  document.getElementById('m-verified').textContent = fmtNumber(s.verified_count);
  document.getElementById('m-verified-pct').innerHTML =
    s.verified_pct > 0
      ? `<span class="pct">${s.verified_pct.toFixed(2)}%</span> of checked`
      : 'awaiting metadata sync';

  document.getElementById('m-unverified').textContent = fmtNumber(s.unverified_count);
  document.getElementById('m-coverage').textContent = `${s.enrichment_coverage_pct.toFixed(2)}%`;

  document.getElementById('m-last-block').textContent = fmtFull(s.last_block);
  document.getElementById('m-last-block-time').textContent = `updated ${fmtTime(s.last_updated)}`;
}

async function loadDeploys() {
  const data = await fetchJson(`/api/deploys-over-time?bucket=${buckets.deploys}`);
  const points = data.buckets.map(b => ({ x: b.timestamp, y: b.count }));
  const ctx = document.getElementById('chart-deploys');
  if (charts.deploys) charts.deploys.destroy();
  charts.deploys = new Chart(ctx, {
    type: 'line',
    data: {
      datasets: [{
        label: 'contracts',
        data: points,
        borderColor: palette.accentLine,
        backgroundColor: palette.accentSoft,
        fill: true,
        tension: 0.2,
        pointRadius: 0,
        pointHoverRadius: 3,
        pointHoverBackgroundColor: palette.accent,
        pointHoverBorderColor: '#000',
        borderWidth: 1.4,
      }],
    },
    options: {
      ...baseChartDefaults,
      plugins: { ...baseChartDefaults.plugins, legend: { display: false } },
      scales: {
        x: { ...baseScales.x, type: 'time', time: { tooltipFormat: 'yyyy-MM-dd HH:mm' } },
        y: { ...baseScales.y, ticks: { ...baseScales.y.ticks, callback: v => fmtNumber(v) } },
      },
    },
  });
}

async function loadVerified() {
  const data = await fetchJson(`/api/verified-ratio?bucket=${buckets.verified}`);
  const all = data.buckets || [];

  const ctx = document.getElementById('chart-verified');
  if (charts.verified) charts.verified.destroy();

  if (all.length === 0) {
    const c = ctx.getContext('2d');
    c.clearRect(0, 0, ctx.width, ctx.height);
    c.fillStyle = palette.faint;
    c.font = '11px "JetBrains Mono", monospace';
    c.textAlign = 'center';
    c.fillText('no contract data yet', ctx.width / 2, ctx.height / 2);
    return;
  }

  const points = all.map(b => {
    const total = (b.verified || 0) + (b.unverified || 0) + (b.unknown || 0);
    const pct = (n) => total > 0 ? (100 * n / total) : 0;
    return {
      verified: pct(b.verified || 0),
      unverified: pct(b.unverified || 0),
      unknown: pct(b.unknown || 0),
      vAbs: b.verified || 0,
      uAbs: b.unverified || 0,
      kAbs: b.unknown || 0,
      blockStart: b.block_start,
      blockEnd: b.block_end,
    };
  });

  const labels = all.map(b => bucketLabel(b, buckets.verified));

  charts.verified = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'verified',
          data: points.map(p => p.verified),
          borderColor: palette.accent,
          backgroundColor: palette.accentSoft,
          fill: true, stack: 'pct', pointRadius: 0, borderWidth: 1.4, tension: 0.3,
        },
        {
          label: 'unverified',
          data: points.map(p => p.unverified),
          borderColor: palette.red,
          backgroundColor: palette.redSoft,
          fill: true, stack: 'pct', pointRadius: 0, borderWidth: 1.4, tension: 0.3,
        },
        {
          label: 'unknown',
          data: points.map(p => p.unknown),
          borderColor: 'rgba(112,112,112,0.5)',
          backgroundColor: 'rgba(112,112,112,0.15)',
          fill: true, stack: 'pct', pointRadius: 0, borderWidth: 1.0, tension: 0.3,
        },
      ],
    },
    options: {
      ...baseChartDefaults,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        ...baseChartDefaults.plugins,
        tooltip: {
          ...baseChartDefaults.plugins.tooltip,
          callbacks: {
            title: items => labels[items[0].dataIndex],
            label: (item) => {
              const p = points[item.dataIndex];
              const abs = item.datasetIndex === 0 ? p.vAbs
                        : item.datasetIndex === 1 ? p.uAbs
                        : p.kAbs;
              return `${item.dataset.label}: ${item.parsed.y.toFixed(1)}% (${fmtFull(abs)})`;
            },
            afterBody: items => {
              const p = points[items[0].dataIndex];
              return `blocks ${fmtFull(p.blockStart)} - ${fmtFull(p.blockEnd)}`;
            },
          },
        },
      },
      scales: {
        x: {
          ...baseScales.x,
          ticks: { ...baseScales.x.ticks, autoSkipPadding: 20, maxTicksLimit: 6 },
        },
        y: {
          ...baseScales.y,
          stacked: true,
          max: 100,
          ticks: { ...baseScales.y.ticks, callback: v => `${v}%` },
        },
      },
    },
  });
}

async function loadSizes() {
  const data = await fetchJson('/api/bytecode-sizes');
  const labels = data.bins.map(b => b.label || `${fmtBytes(b.size_min)}-${fmtBytes(b.size_max)}`);
  const shortLabels = labels;
  const counts = data.bins.map(b => b.count);
  const grandTotal = counts.reduce((a, b) => a + b, 0);

  const ctx = document.getElementById('chart-sizes');
  if (charts.sizes) charts.sizes.destroy();
  charts.sizes = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: shortLabels,
      datasets: [{
        label: 'contracts',
        data: counts,
        backgroundColor: palette.blueSoft,
        borderColor: palette.blue,
        borderWidth: 1,
        barPercentage: 0.95,
        categoryPercentage: 0.95,
      }],
    },
    options: {
      ...baseChartDefaults,
      plugins: {
        ...baseChartDefaults.plugins,
        legend: { display: false },
        tooltip: {
          ...baseTooltip,
          callbacks: {
            title: items => `size ${labels[items[0].dataIndex]}`,
            label: item => {
              const pct = grandTotal > 0
                ? (item.parsed.y / grandTotal * 100).toFixed(2)
                : '0.00';
              return `${fmtFull(item.parsed.y)} contracts (${pct}%)`;
            },
          },
        },
      },
      scales: {
        x: {
          ...baseScales.x,
          title: { display: true, text: 'bytecode size', color: palette.faint, font: { size: 10 } },
          ticks: { ...baseScales.x.ticks, font: { size: 9 }, maxRotation: 45, minRotation: 45 },
        },
        y: {
          ...baseScales.y,
          title: { display: true, text: 'contracts in range', color: palette.faint, font: { size: 10 } },
          ticks: { ...baseScales.y.ticks, callback: v => fmtNumber(v) },
        },
      },
    },
  });
}

async function loadLanguages() {
  const data = await fetchJson('/api/languages');
  const langs = data.languages || [];
  const total = langs.reduce((a, b) => a + b.count, 0);
  if (total === 0) {
    document.getElementById('m-lang-top').textContent = '—';
    document.getElementById('m-lang-sub').textContent = 'run `blink decode`';
    return;
  }
  const known = langs.filter(l => l.language !== 'unknown');
  const knownCount = known.reduce((a, b) => a + b.count, 0);
  const unknown = total - knownCount;
  const knownPct = total > 0 ? (knownCount / total * 100).toFixed(1) : '0.0';
  document.getElementById('m-lang-top').textContent = `${knownPct}% known`;
  const knownParts = known.slice(0, 2).map(l => `${l.language} ${fmtNumber(l.count)}`);
  const unknownPart = unknown > 0 ? `${fmtNumber(unknown)} unknown` : '';
  document.getElementById('m-lang-sub').textContent =
    [...knownParts, unknownPart].filter(Boolean).join(' · ') || `${fmtNumber(total)} decoded`;
}

async function loadStandards() {
  const data = await fetchJson('/api/standards');
  const total = data.total_decoded || 0;
  const cov = document.getElementById('standards-coverage');
  cov.textContent = total > 0 ? `${fmtNumber(total)} decoded` : 'no data';

  const ctx = document.getElementById('chart-standards');
  if (charts.standards) charts.standards.destroy();

  if (total === 0) {
    const c = ctx.getContext('2d');
    c.clearRect(0, 0, ctx.width, ctx.height);
    c.fillStyle = palette.faint;
    c.font = '11px "JetBrains Mono", monospace';
    c.textAlign = 'center';
    c.fillText('no decoded contracts yet — run `blink decode`', ctx.width / 2, ctx.height / 2);
    return;
  }

  const labels = [
    'ERC-20 selectors',
    'ERC-721 selectors',
    'ERC-1155 selectors',
    'EIP-1167 minimal proxy',
    'EIP-1967 proxy',
    'PUSH0 opcode',
    'source metadata hash',
  ];
  const details = [
    'core ERC-20 function selectors found',
    'core ERC-721 function selectors found',
    'core ERC-1155 function selectors found',
    '45-byte clone shape (factory-deployed proxies)',
    'EIP-1967 implementation/admin slot found',
    'runtime bytecode uses the PUSH0 opcode',
    'compiler metadata includes a source hash',
  ];
  const counts = [
    data.erc20, data.erc721, data.erc1155,
    data.proxy_minimal || 0, data.proxy_eip1967,
    data.uses_push0, data.has_source_hash,
  ];
  const colors = [
    palette.accent, palette.blue, palette.amber,
    '#00d6ff', palette.red,
    '#9d6cff', palette.dim,
  ];

  charts.standards = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'contracts',
        data: counts,
        backgroundColor: colors.map(c => c + '33'),
        borderColor: colors,
        borderWidth: 1,
        barPercentage: 0.75,
        categoryPercentage: 0.75,
      }],
    },
    options: {
      ...baseChartDefaults,
      indexAxis: 'y',
      interaction: { mode: 'nearest', axis: 'y', intersect: false },
      plugins: {
        ...baseChartDefaults.plugins,
        legend: { display: false },
        tooltip: {
          ...baseTooltip,
          callbacks: {
            title: items => labels[items[0].dataIndex],
            label: item => {
              const pct = (item.parsed.x / total * 100).toFixed(2);
              return `${fmtFull(item.parsed.x)} contracts (${pct}% of decoded)`;
            },
            afterLabel: item => details[item.dataIndex],
          },
        },
      },
      scales: {
        x: { ...baseScales.x, ticks: { ...baseScales.x.ticks, callback: v => fmtNumber(v) } },
        y: { ...baseScales.y, ticks: { ...baseScales.y.ticks } },
      },
    },
  });
}

async function loadCompilers() {
  const data = await fetchJson('/api/compilers?limit=12');
  if (!data.compilers.length) {
    const ctx = document.getElementById('chart-compilers');
    if (charts.compilers) charts.compilers.destroy();
    const c = ctx.getContext('2d');
    c.clearRect(0, 0, ctx.width, ctx.height);
    c.fillStyle = palette.faint;
    c.font = '11px "JetBrains Mono", monospace';
    c.textAlign = 'center';
    c.fillText('no compiler data — run `blink decode`', ctx.width / 2, ctx.height / 2);
    return;
  }
  const fullLabels = data.compilers.map(c => c.compiler_version);
  const labels = fullLabels.map(c => c.replace(/^v/, '').split('+')[0]);
  const counts = data.compilers.map(c => c.count);
  const totalKnown = data.total_known || counts.reduce((a, b) => a + b, 0);
  const ctx = document.getElementById('chart-compilers');
  if (charts.compilers) charts.compilers.destroy();
  charts.compilers = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'contracts',
        data: counts,
        backgroundColor: palette.accentSoft,
        borderColor: palette.accent,
        borderWidth: 1,
        barPercentage: 0.85,
        categoryPercentage: 0.85,
      }],
    },
    options: {
      ...baseChartDefaults,
      indexAxis: 'y',
      interaction: { mode: 'nearest', axis: 'y', intersect: false },
      plugins: {
        ...baseChartDefaults.plugins,
        legend: { display: false },
        tooltip: {
          ...baseTooltip,
          callbacks: {
            title: items => fullLabels[items[0].dataIndex],
            label: item => {
              const pct = totalKnown > 0 ? (item.parsed.x / totalKnown * 100).toFixed(2) : '0.00';
              return `${fmtFull(item.parsed.x)} contracts (${pct}% of known compilers)`;
            },
          },
        },
      },
      scales: {
        x: { ...baseScales.x, ticks: { ...baseScales.x.ticks, callback: v => fmtNumber(v) } },
        y: { ...baseScales.y, ticks: { ...baseScales.y.ticks } },
      },
    },
  });
}

function shortAddr(addr) {
  if (!addr) return '—';
  return addr.slice(0, 8) + '…' + addr.slice(-6);
}

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  })[ch]);
}

function verifiedBadge(v) {
  if (v === true)  return '<span class="badge ok">verified</span>';
  if (v === false) return '<span class="badge no">unverified</span>';
  return '<span class="badge dim">unchecked</span>';
}

function renderQueryValue(value) {
  if (value === null || value === undefined) return '<span class="dim-text">NULL</span>';
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') return Number.isInteger(value) ? fmtFull(value) : String(value);
  return escapeHtml(value);
}

function renderQueryResult(data) {
  const table = document.getElementById('query-table');
  const thead = table.querySelector('thead');
  const tbody = table.querySelector('tbody');
  const cols = data.columns || [];
  const rows = data.rows || [];

  thead.innerHTML = cols.length
    ? `<tr>${cols.map(c => `<th title="${escapeHtml(c)}">${escapeHtml(c)}</th>`).join('')}</tr>`
    : '';
  tbody.innerHTML = '';

  if (!rows.length) {
    tbody.innerHTML = '<tr><td class="query-empty">0 rows</td></tr>';
    return;
  }

  for (const row of rows) {
    const tr = document.createElement('tr');
    tr.innerHTML = row.map(value => `<td title="${escapeHtml(value)}">${renderQueryValue(value)}</td>`).join('');
    tbody.appendChild(tr);
  }
}

async function runSqlQuery() {
  const button = document.getElementById('query-run');
  const status = document.getElementById('query-status');
  const editor = document.getElementById('query-editor');
  button.disabled = true;
  status.textContent = 'running';
  try {
    const data = await postJson('/api/query', {
      sql: editor.value,
      limit: 200,
    });
    renderQueryResult(data);
    status.textContent = `${fmtFull(data.row_count)} rows · ${fmtFull(data.elapsed_ms)} ms`;
  } catch (err) {
    console.error(err);
    status.textContent = shortError(err.message);
    const tbody = document.querySelector('#query-table tbody');
    tbody.innerHTML = `<tr><td class="query-empty">${escapeHtml(err.message)}</td></tr>`;
  } finally {
    button.disabled = false;
  }
}

async function loadRecent() {
  if (recentState.loading) return;
  recentState.loading = true;
  updateRecentPager();
  try {
    const cursor = recentState.cursors[recentState.page];
    const params = new URLSearchParams({ limit: String(recentState.limit) });
    if (cursor) {
      params.set('before_block', String(cursor.block_number));
      params.set('before_create_index', String(cursor.create_index));
    }
    const data = await fetchJson(`/api/recent?${params.toString()}`);
    const tbody = document.querySelector('#recent-table tbody');
    tbody.innerHTML = '';
    for (const c of data.contracts) {
      const tr = document.createElement('tr');
      const safeAddress = escapeHtml(c.address);
      const safeCompiler = c.compiler_version
        ? escapeHtml(c.compiler_version.replace(/^v/, '').split('+')[0])
        : '<span class="dim-text">—</span>';
      const name = c.contract_name
        ? `<span title="${escapeHtml(c.contract_name)}">${escapeHtml(c.contract_name)}</span>`
        : '<span class="dim-text">—</span>';
      tr.innerHTML = `
        <td>${fmtFull(c.block_number)}</td>
        <td><a class="address" href="https://etherscan.io/address/${encodeURIComponent(c.address)}" target="_blank" rel="noopener" title="${safeAddress}">${escapeHtml(shortAddr(c.address))}</a></td>
        <td>${name}</td>
        <td>${safeCompiler}</td>
        <td>${fmtBytes(c.n_code_bytes)}</td>
        <td>${verifiedBadge(c.is_verified)}</td>
      `;
      tbody.appendChild(tr);
    }
    recentState.hasMore = Boolean(data.has_more);
    const last = data.contracts[data.contracts.length - 1];
    recentState.nextCursor = last
      ? { block_number: last.block_number, create_index: last.create_index }
      : null;
    const start = recentState.page * recentState.limit + 1;
    const end = recentState.page * recentState.limit + data.contracts.length;
    document.getElementById('recent-range').textContent =
      data.contracts.length === 0 ? '0' : `${fmtFull(start)} – ${fmtFull(end)}`;
  } finally {
    recentState.loading = false;
    updateRecentPager();
  }
}

function updateRecentPager() {
  document.getElementById('recent-prev').disabled = recentState.loading || recentState.page === 0;
  document.getElementById('recent-next').disabled =
    recentState.loading || !recentState.hasMore || !recentState.nextCursor;
}

async function refresh() {
  try {
    const [runtime] = await Promise.all([
      fetchJson('/api/runtime'),
      loadStats(),
      loadDeploys(),
      loadVerified(),
      loadSizes(),
      loadCompilers(),
      loadLanguages(),
      loadStandards(),
      loadRecent(),
    ]);
    const ts = new Date().toISOString().slice(11, 19) + ' UTC';
    renderRuntimeStatus(runtime);
    document.getElementById('footer-refresh').textContent = `last refresh ${ts}`;
  } catch (err) {
    console.error(err);
    setStatus('error', err.message);
  }
}

function attachBucketToggles() {
  document.querySelectorAll('.seg').forEach(group => {
    const target = group.dataset.target;
    group.querySelectorAll('button').forEach(btn => {
      btn.addEventListener('click', () => {
        group.querySelectorAll('button').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        buckets[target] = btn.dataset.bucket;
        if (target === 'deploys') loadDeploys().catch(console.error);
        if (target === 'verified') loadVerified().catch(console.error);
      });
    });
  });
}

function attachRecentPager() {
  document.getElementById('recent-prev').addEventListener('click', () => {
    if (recentState.loading || recentState.page === 0) return;
    recentState.page -= 1;
    loadRecent().catch(console.error);
  });
  document.getElementById('recent-next').addEventListener('click', () => {
    if (recentState.loading || !recentState.hasMore || !recentState.nextCursor) return;
    recentState.page += 1;
    recentState.cursors[recentState.page] = recentState.nextCursor;
    recentState.cursors.length = recentState.page + 1;
    loadRecent().catch(console.error);
  });
}

function attachQueryRunner() {
  const button = document.getElementById('query-run');
  const editor = document.getElementById('query-editor');
  button.addEventListener('click', () => runSqlQuery().catch(console.error));
  editor.addEventListener('keydown', event => {
    if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
      event.preventDefault();
      runSqlQuery().catch(console.error);
    }
  });
}

attachBucketToggles();
attachRecentPager();
attachQueryRunner();
refresh();
setInterval(refresh, REFRESH_MS);
