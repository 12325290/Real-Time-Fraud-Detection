/**
 * Real-Time Fraud Detection Dashboard
 * Handles: WebSocket, Charts, Transactions, Alerts, Simulation
 */

'use strict';

// ─────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────
const API = window.location.origin;
const wsProtocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
const WS_URL = `${wsProtocol}://${window.location.host}/ws/${crypto.randomUUID()}`;

// ─────────────────────────────────────────────
// State
// ─────────────────────────────────────────────
let ws = null;
let wsReconnectTimer = null;
let simulationRunning = false;
let allTransactions = [];
let alerts = [];
let unreadAlerts = 0;
let charts = {};
let riskScoreHistory = [];

const stats = {
  total: 0, fraud: 0, safe: 0,
  avgRisk: 0, totalAmount: 0,
  high: 0, medium: 0, low: 0,
};

// ─────────────────────────────────────────────
// DOM References
// ─────────────────────────────────────────────
const $ = id => document.getElementById(id);
const statusDot  = $('status-dot');
const statusText = $('status-text');

// ─────────────────────────────────────────────
// Navigation
// ─────────────────────────────────────────────
function initNav() {
  document.querySelectorAll('.nav-item').forEach(item => {
    item.addEventListener('click', e => {
      e.preventDefault();
      const section = item.dataset.section;
      document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
      document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
      item.classList.add('active');
      $(`section-${section}`).classList.add('active');
      $('page-title').textContent = item.textContent.trim();
      if (section === 'transactions') loadTransactions();
      if (section === 'alerts') loadAlerts();
      if (section === 'analytics') updateAnalyticsCharts();
    });
  });
}

// ─────────────────────────────────────────────
// WebSocket
// ─────────────────────────────────────────────
function connectWS() {
  try {
    ws = new WebSocket(WS_URL);
  } catch (e) {
    scheduleReconnect();
    return;
  }

  ws.onopen = () => {
    setStatus('connected', 'Connected');
    clearTimeout(wsReconnectTimer);
    // keep-alive ping every 20s
    ws._ping = setInterval(() => ws.readyState === 1 && ws.send('ping'), 20000);
  };

  ws.onmessage = ({ data }) => {
    try {
      const msg = JSON.parse(data);
      handleWSMessage(msg);
    } catch (_) {}
  };

  ws.onclose = () => {
    setStatus('error', 'Disconnected');
    clearInterval(ws._ping);
    scheduleReconnect();
  };

  ws.onerror = () => {
    setStatus('error', 'Connection error');
  };
}

function scheduleReconnect() {
  wsReconnectTimer = setTimeout(() => {
    setStatus('', 'Reconnecting…');
    connectWS();
  }, 4000);
}

function setStatus(cls, text) {
  statusDot.className = 'status-dot ' + cls;
  statusText.textContent = text;
}

// ─────────────────────────────────────────────
// WS Message Router
// ─────────────────────────────────────────────
function handleWSMessage(msg) {
  switch (msg.type) {
    case 'transaction': onTransaction(msg.data); break;
    case 'alert':       onAlert(msg.data);       break;
    case 'stats':       applyStats(msg.data);    break;
    case 'connected':   fetchStats();            break;
    case 'pong':        break;
  }
}

// ─────────────────────────────────────────────
// Transaction Handler
// ─────────────────────────────────────────────
function onTransaction(txn) {
  allTransactions.unshift(txn);
  if (allTransactions.length > 500) allTransactions.pop();

  // Update local stats
  stats.total++;
  stats.totalAmount += txn.amount;
  if (txn.is_fraud) stats.fraud++; else stats.safe++;
  stats.avgRisk = ((stats.avgRisk * (stats.total - 1)) + txn.risk_score) / stats.total;
  if (txn.risk_level === 'high')   stats.high++;
  else if (txn.risk_level === 'medium') stats.medium++;
  else stats.low++;

  riskScoreHistory.push(txn.risk_score);
  if (riskScoreHistory.length > 100) riskScoreHistory.shift();

  renderKPIs();
  addFeedItem(txn);
  prependTableRow(txn);
  updateVolumeChart(txn);
  updateFraudRatioChart();
  updateRiskBars();
}

// ─────────────────────────────────────────────
// KPIs
// ─────────────────────────────────────────────
function renderKPIs() {
  animateValue('stat-total', stats.total);
  animateValue('stat-fraud', stats.fraud);
  $('stat-fraud-rate').textContent = stats.total
    ? `${((stats.fraud / stats.total) * 100).toFixed(1)}% rate` : '0% rate';
  $('stat-amount').textContent = formatAmount(stats.totalAmount);
  $('stat-avg-risk').textContent = stats.avgRisk.toFixed(1);
  $('stat-risk-level').textContent =
    stats.avgRisk >= 70 ? '⚠ High Risk' :
    stats.avgRisk >= 40 ? '⚡ Medium Risk' : '✅ Low Risk';
  $('stat-fraud-rate').className = 'kpi-delta' + (stats.fraud > 0 ? ' danger' : '');
}

function animateValue(id, val) {
  const el = $(id);
  el.textContent = val.toLocaleString();
  const card = el.closest('.kpi-card');
  if (card) { card.classList.remove('updated'); void card.offsetWidth; card.classList.add('updated'); }
}

function applyStats(s) {
  stats.total = s.total_transactions;
  stats.fraud = s.total_fraud;
  stats.safe  = s.total_safe;
  stats.avgRisk = s.avg_risk_score;
  stats.totalAmount = s.total_amount_processed;
  stats.high   = s.high_risk_count;
  stats.medium = s.medium_risk_count;
  stats.low    = s.low_risk_count;
  renderKPIs();
  updateRiskBars();
  updateFraudRatioChart();
}

// ─────────────────────────────────────────────
// Live Feed
// ─────────────────────────────────────────────
function addFeedItem(txn) {
  const feed = $('live-feed');
  const empty = feed.querySelector('.feed-empty');
  if (empty) empty.remove();

  // Keep max 5 items
  const items = feed.querySelectorAll('.feed-item');
  if (items.length >= 5) items[items.length - 1].remove();

  const div = document.createElement('div');
  div.className = 'feed-item';
  div.innerHTML = `
    <div class="feed-risk-badge badge-${txn.risk_level}">${txn.risk_score.toFixed(0)}</div>
    <div class="feed-meta">
      <div class="feed-merchant">${esc(txn.merchant)}</div>
      <div class="feed-sub">${esc(txn.user_id)} · ${esc(txn.payment_method)} · ${esc(txn.user_city || '')}</div>
    </div>
    <div>
      <div class="feed-amount">${formatAmount(txn.amount)}</div>
      <div class="feed-score score-${txn.risk_level}">${txn.risk_level.toUpperCase()}</div>
    </div>`;
  div.addEventListener('click', () => openModal(txn));
  feed.prepend(div);
}

// ─────────────────────────────────────────────
// Transaction Table (section view)
// ─────────────────────────────────────────────
function prependTableRow(txn) {
  const tbody = $('txn-tbody');
  const empty = tbody.querySelector('.table-empty');
  if (empty) tbody.innerHTML = '';

  // only update if on transactions section (avoid DOM bloat)
  const rows = tbody.querySelectorAll('tr');
  if (rows.length > 200) rows[rows.length - 1].remove();

  const tr = buildRow(txn);
  tbody.prepend(tr);
}

function buildRow(txn) {
  const tr = document.createElement('tr');
  tr.innerHTML = `
    <td><span style="font-family:monospace;font-size:11px;color:var(--text-muted)">${txn.transaction_id.slice(0,12)}…</span></td>
    <td style="font-weight:600;color:var(--text-primary)">${formatAmount(txn.amount)}</td>
    <td>${esc(txn.merchant)}</td>
    <td>${esc(txn.payment_method)}</td>
    <td style="font-family:monospace;font-size:11px">${esc(txn.user_id)}</td>
    <td>${riskScoreCell(txn.risk_score, txn.risk_level)}</td>
    <td>${statusChip(txn.status)}</td>
    <td style="font-size:11px;color:var(--text-muted)">${timeAgo(txn.created_at)}</td>`;
  tr.addEventListener('click', () => openModal(txn));
  return tr;
}

function riskScoreCell(score, level) {
  const color = level === 'high' ? 'var(--red)' : level === 'medium' ? 'var(--yellow)' : 'var(--green)';
  return `<div class="risk-score-bar">
    <div class="score-bar-track"><div class="score-bar-fill" style="width:${score}%;background:${color}"></div></div>
    <span class="score-val" style="color:${color}">${score.toFixed(0)}</span>
  </div>`;
}

function statusChip(status) {
  const map = { approved:'chip-approved', flagged:'chip-flagged', blocked:'chip-blocked', pending:'chip-pending' };
  return `<span class="status-chip ${map[status] || 'chip-pending'}">${status}</span>`;
}

async function loadTransactions() {
  const risk  = $('filter-risk').value;
  const fraud = $('filter-fraud').value;
  let url = `${API}/api/transactions?limit=100`;
  if (risk)  url += `&risk_level=${risk}`;
  if (fraud !== '') url += `&is_fraud=${fraud}`;
  try {
    const res = await fetch(url);
    const data = await res.json();
    const tbody = $('txn-tbody');
    tbody.innerHTML = '';
    if (!data.length) {
      tbody.innerHTML = '<tr><td colspan="8" class="table-empty">No transactions found.</td></tr>';
      return;
    }
    data.forEach(txn => tbody.appendChild(buildRow(txn)));
  } catch (e) {
    console.error('Failed to load transactions', e);
  }
}

// ─────────────────────────────────────────────
// Alerts Handler
// ─────────────────────────────────────────────
function onAlert(alert) {
  alerts.unshift(alert);
  unreadAlerts++;
  updateAlertBadge();
  showToast(alert);
  if ($('section-alerts').classList.contains('active')) renderAlertsList();
}

function updateAlertBadge() {
  const badge = $('alert-badge');
  badge.textContent = unreadAlerts;
  badge.classList.toggle('visible', unreadAlerts > 0);
}

function showToast(alert) {
  const container = $('toast-container');
  const toast = document.createElement('div');
  toast.className = 'toast';
  toast.innerHTML = `
    <div class="toast-icon">🚨</div>
    <div>
      <div class="toast-title">Fraud Alert Detected!</div>
      <div class="toast-body">${esc(alert.alert_message || 'High-risk transaction flagged')}</div>
    </div>
    <button class="toast-close" onclick="this.parentElement.remove()">✕</button>`;
  container.appendChild(toast);
  setTimeout(() => {
    toast.style.animation = 'toast-out 0.3s forwards';
    setTimeout(() => toast.remove(), 300);
  }, 6000);
}

function showErrorToast(message) {
  const container = $('toast-container');
  const toast = document.createElement('div');
  toast.className = 'toast';
  toast.innerHTML = `
    <div class="toast-icon">⚠️</div>
    <div>
      <div class="toast-title">Request Failed</div>
      <div class="toast-body">${esc(message || 'Something went wrong. Please try again.')}</div>
    </div>
    <button class="toast-close" onclick="this.parentElement.remove()">✕</button>`;
  container.appendChild(toast);
  setTimeout(() => {
    toast.style.animation = 'toast-out 0.3s forwards';
    setTimeout(() => toast.remove(), 300);
  }, 5000);
}

async function loadAlerts() {
  try {
    const res = await fetch(`${API}/api/alerts?limit=50`);
    const data = await res.json();
    alerts = data;
    unreadAlerts = data.filter(a => !a.acknowledged).length;
    updateAlertBadge();
    renderAlertsList();
  } catch (e) { console.error('Failed to load alerts', e); }
}

function renderAlertsList() {
  const list = $('alerts-list');
  if (!alerts.length) {
    list.innerHTML = '<div class="alert-empty">No alerts yet. System is monitoring…</div>';
    return;
  }
  list.innerHTML = '';
  alerts.forEach(a => {
    const div = document.createElement('div');
    div.className = 'alert-card' + (a.acknowledged ? ' acknowledged' : '');
    div.innerHTML = `
      <div class="alert-icon">🚨</div>
      <div class="alert-content">
        <div class="alert-title">${a.risk_level.toUpperCase()} RISK — Score: ${a.risk_score.toFixed(1)}</div>
        <div class="alert-sub">${esc(a.alert_message || '')}</div>
        <div class="alert-time">${timeAgo(a.created_at)}</div>
      </div>
      ${!a.acknowledged ? `<button class="alert-ack-btn" data-id="${a.id}">Acknowledge</button>` : '<span style="font-size:11px;color:var(--green)">✓ Acknowledged</span>'}`;
    list.appendChild(div);
  });
  list.querySelectorAll('.alert-ack-btn').forEach(btn => {
    btn.addEventListener('click', () => acknowledgeAlert(+btn.dataset.id));
  });
}

async function acknowledgeAlert(id) {
  try {
    await fetch(`${API}/api/alerts/${id}/acknowledge`, { method: 'PATCH' });
    const a = alerts.find(x => x.id === id);
    if (a) { a.acknowledged = true; unreadAlerts = Math.max(0, unreadAlerts - 1); }
    updateAlertBadge();
    renderAlertsList();
  } catch (e) { console.error(e); }
}

// ─────────────────────────────────────────────
// Risk Bars
// ─────────────────────────────────────────────
function updateRiskBars() {
  const total = stats.total || 1;
  $('risk-low-count').textContent  = stats.low;
  $('risk-med-count').textContent  = stats.medium;
  $('risk-high-count').textContent = stats.high;
  $('risk-low-bar').style.width  = `${(stats.low / total * 100).toFixed(1)}%`;
  $('risk-med-bar').style.width  = `${(stats.medium / total * 100).toFixed(1)}%`;
  $('risk-high-bar').style.width = `${(stats.high / total * 100).toFixed(1)}%`;
}

// ─────────────────────────────────────────────
// Charts
// ─────────────────────────────────────────────
const chartDefaults = {
  color: '#94a3b8',
  borderColor: 'rgba(255,255,255,0.07)',
};

function initCharts() {
  Chart.defaults.color = chartDefaults.color;
  Chart.defaults.borderColor = chartDefaults.borderColor;

  // Volume line chart
  charts.volume = new Chart($('chart-volume'), {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        { label: 'Normal', data: [], borderColor: '#22d3a5', backgroundColor: 'rgba(34,211,165,0.08)', fill: true, tension: 0.4, borderWidth: 2, pointRadius: 3 },
        { label: 'Fraud',  data: [], borderColor: '#ef4444', backgroundColor: 'rgba(239,68,68,0.08)',  fill: true, tension: 0.4, borderWidth: 2, pointRadius: 3 },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'top', labels: { usePointStyle: true, boxWidth: 6, padding: 16 } } },
      scales: {
        x: { grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { grid: { color: 'rgba(255,255,255,0.04)' }, beginAtZero: true },
      },
    },
  });

  // Fraud ratio doughnut
  charts.fraudRatio = new Chart($('chart-fraud-ratio'), {
    type: 'doughnut',
    data: {
      labels: ['Fraud', 'Safe'],
      datasets: [{ data: [0, 1], backgroundColor: ['#ef4444', '#22d3a5'], borderColor: '#131928', borderWidth: 3 }],
    },
    options: {
      responsive: true, maintainAspectRatio: false, cutout: '72%',
      plugins: { legend: { display: false } },
    },
  });

  // Analytics: Risk score histogram (bar)
  charts.riskDist = new Chart($('chart-risk-scores'), {
    type: 'bar',
    data: {
      labels: ['0-10','10-20','20-30','30-40','40-50','50-60','60-70','70-80','80-90','90-100'],
      datasets: [{ label: 'Transactions', data: Array(10).fill(0), backgroundColor: generateGradientColors(), borderRadius: 6 }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { display: false } },
        y: { grid: { color: 'rgba(255,255,255,0.04)' }, beginAtZero: true },
      },
    },
  });

  // Analytics: Payment methods bar
  charts.paymentMethods = new Chart($('chart-payment-methods'), {
    type: 'bar',
    data: {
      labels: ['Credit Card','Debit Card','UPI','Net Banking','Wallet'],
      datasets: [{ label: 'Count', data: [0,0,0,0,0], backgroundColor: '#6366f1', borderRadius: 6 }],
    },
    options: {
      responsive: true, maintainAspectRatio: false, indexAxis: 'y',
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { color: 'rgba(255,255,255,0.04)' }, beginAtZero: true },
        y: { grid: { display: false } },
      },
    },
  });
}

function generateGradientColors() {
  return [
    '#22d3a5','#34d399','#6ee7b7','#fbbf24','#f59e0b',
    '#fb923c','#f87171','#ef4444','#dc2626','#b91c1c',
  ];
}

// Rolling time buckets (last 10 transactions, grouped by minute)
const volumeBuckets = {};
function updateVolumeChart(txn) {
  const key = new Date(txn.created_at).toLocaleTimeString('en-IN', { hour:'2-digit', minute:'2-digit' });
  if (!volumeBuckets[key]) volumeBuckets[key] = { normal: 0, fraud: 0 };
  if (txn.is_fraud) volumeBuckets[key].fraud++;
  else volumeBuckets[key].normal++;

  const keys = Object.keys(volumeBuckets).slice(-12);
  charts.volume.data.labels = keys;
  charts.volume.data.datasets[0].data = keys.map(k => volumeBuckets[k].normal);
  charts.volume.data.datasets[1].data = keys.map(k => volumeBuckets[k].fraud);
  charts.volume.update('none');
}

function updateFraudRatioChart() {
  const f = stats.fraud, s = stats.safe || 1;
  charts.fraudRatio.data.datasets[0].data = [f, s];
  charts.fraudRatio.update('none');
  const pct = stats.total ? ((f / stats.total) * 100).toFixed(1) : '0.0';
  $('doughnut-label').textContent = `${pct}%`;
}

function updateAnalyticsCharts() {
  // Risk score histogram
  const buckets = Array(10).fill(0);
  riskScoreHistory.forEach(s => {
    const idx = Math.min(Math.floor(s / 10), 9);
    buckets[idx]++;
  });
  charts.riskDist.data.datasets[0].data = buckets;
  charts.riskDist.update('none');

  // Payment method distribution
  const pmCounts = [0, 0, 0, 0, 0];
  const pmMap = { credit_card:0, debit_card:1, upi:2, net_banking:3, wallet:4 };
  allTransactions.forEach(t => { if (pmMap[t.payment_method] !== undefined) pmCounts[pmMap[t.payment_method]]++; });
  charts.paymentMethods.data.datasets[0].data = pmCounts;
  charts.paymentMethods.update('none');
}

// ─────────────────────────────────────────────
// Transaction Modal
// ─────────────────────────────────────────────
function openModal(txn) {
  const scoreColor = txn.risk_level === 'high' ? 'var(--red)' : txn.risk_level === 'medium' ? 'var(--yellow)' : 'var(--green)';
  $('modal-body').innerHTML = `
    <div class="modal-grid">
      ${field('Transaction ID', txn.transaction_id)}
      ${field('Amount', formatAmount(txn.amount))}
      ${field('Merchant', txn.merchant)}
      ${field('Category', txn.merchant_category || '—')}
      ${field('Payment Method', txn.payment_method)}
      ${field('User ID', txn.user_id)}
      ${field('City', txn.user_city || '—')}
      ${field('Device', txn.device_type || '—')}
      ${field('IP Address', txn.ip_address || '—')}
      ${field('Transactions/Hour', txn.transactions_last_hour)}
      ${field('Status', txn.status)}
      ${field('Time', new Date(txn.created_at).toLocaleString())}
    </div>
    <div class="modal-risk-gauge">
      <div class="gauge-label">Fraud Risk Assessment</div>
      <div class="gauge-bar">
        <div class="gauge-fill" style="width:${txn.risk_score}%;background:${scoreColor}"></div>
      </div>
      <div class="gauge-row">
        <span class="gauge-score" style="color:${scoreColor}">${txn.risk_score.toFixed(1)}<small style="font-size:14px;color:var(--text-muted)">/100</small></span>
        <span class="gauge-verdict" style="background:rgba(from ${scoreColor} r g b / 0.12);color:${scoreColor}">
          ${txn.risk_level.toUpperCase()} RISK${txn.is_fraud ? ' — BLOCKED' : ''}
        </span>
      </div>
    </div>`;
  $('modal-overlay').classList.add('open');
}

function field(label, val) {
  return `<div class="modal-field"><label>${label}</label><span>${esc(String(val))}</span></div>`;
}

// ─────────────────────────────────────────────
// Simulation Controls
// ─────────────────────────────────────────────
async function apiPost(path, body) {
  const res = await fetch(`${API}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const message = data?.detail || data?.message || `HTTP ${res.status}`;
    throw new Error(message);
  }
  return data;
}

async function toggleSimulation() {
  const btn = $('btn-sim-toggle');
  const label = $('sim-label');
  btn.disabled = true;
  try {
    if (!simulationRunning) {
      await apiPost('/api/simulation', { action: 'start', interval: 2.0 });
      simulationRunning = true;
      label.textContent = '⏹ Stop Simulation';
      btn.style.background = 'linear-gradient(135deg,#ef4444,#dc2626)';
    } else {
      await apiPost('/api/simulation', { action: 'stop' });
      simulationRunning = false;
      label.textContent = '▶ Start Simulation';
      btn.style.background = '';
    }
  } catch (e) {
    console.error(e);
    showErrorToast(e.message || 'Unable to update simulation state.');
  }
  btn.disabled = false;
}

async function singleTransaction() {
  try {
    await apiPost('/api/simulation', { action: 'single', force_fraud: false });
  } catch (e) {
    console.error(e);
    showErrorToast(e.message || 'Unable to generate transaction.');
  }
}

async function forceFraud() {
  try {
    await apiPost('/api/simulation', { action: 'single', force_fraud: true });
  } catch (e) {
    console.error(e);
    showErrorToast(e.message || 'Unable to generate fraud transaction.');
  }
}

// ─────────────────────────────────────────────
// Fetch initial stats from REST
// ─────────────────────────────────────────────
async function fetchStats() {
  try {
    const res = await fetch(`${API}/api/stats`);
    const data = await res.json();
    applyStats(data);
  } catch (_) {}

  try {
    const res = await fetch(`${API}/api/stats/timeseries?hours=24`);
    const data = await res.json();
    data.forEach(bucket => {
      const key = new Date(bucket.time).toLocaleTimeString('en-IN', { hour:'2-digit', minute:'2-digit' });
      volumeBuckets[key] = { normal: bucket.safe, fraud: bucket.fraud };
    });
    const keys = Object.keys(volumeBuckets).slice(-12);
    charts.volume.data.labels = keys;
    charts.volume.data.datasets[0].data = keys.map(k => volumeBuckets[k].normal);
    charts.volume.data.datasets[1].data = keys.map(k => volumeBuckets[k].fraud);
    charts.volume.update('none');
  } catch (_) {}
}

// ─────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────
function esc(str) {
  const d = document.createElement('div');
  d.textContent = str;
  return d.innerHTML;
}

function formatAmount(n) {
  if (n >= 1_00_000) return `₹${(n / 1_00_000).toFixed(1)}L`;
  if (n >= 1_000) return `₹${(n / 1_000).toFixed(1)}K`;
  return `₹${n.toFixed(0)}`;
}

function timeAgo(iso) {
  if (!iso) return '—';
  const diff = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (diff < 60)   return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff/60)}m ago`;
  return new Date(iso).toLocaleTimeString('en-IN', { hour:'2-digit', minute:'2-digit' });
}

// ─────────────────────────────────────────────
// Bootstrap
// ─────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initNav();
  initCharts();
  connectWS();

  // Simulation buttons
  $('btn-sim-toggle').addEventListener('click', toggleSimulation);
  $('btn-single').addEventListener('click', singleTransaction);
  $('btn-fraud').addEventListener('click', forceFraud);

  // Filters
  $('btn-refresh-txns').addEventListener('click', loadTransactions);
  $('filter-risk').addEventListener('change', loadTransactions);
  $('filter-fraud').addEventListener('change', loadTransactions);

  // Alerts
  $('btn-clear-alerts').addEventListener('click', async () => {
    for (const a of alerts.filter(x => !x.acknowledged)) await acknowledgeAlert(a.id);
  });

  // Modal
  $('modal-close').addEventListener('click', () => $('modal-overlay').classList.remove('open'));
  $('modal-overlay').addEventListener('click', e => { if (e.target === $('modal-overlay')) $('modal-overlay').classList.remove('open'); });
});
