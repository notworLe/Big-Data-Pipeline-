/* app.js — Steam Analytics Dashboard Logic
 * Tab 1: Fetch REST API (Hive/HDFS) → Chart.js static charts
 * Tab 2: SSE EventSource (MongoDB) → Chart.js live update
 */

// ── Chart.js Defaults — Neubrutalism style ───────────────────────────────
Chart.defaults.color = '#333';
Chart.defaults.borderColor = 'rgba(0,0,0,0.1)';
Chart.defaults.font.family = "'Space Grotesk', sans-serif";

// Flat, bold neubrutalist palette (no gradients!)
const COLORS = {
  yellow:  '#FFE500',
  lime:    '#B8FF4F',
  cyan:    '#00D4FF',
  pink:    '#FF6BB5',
  orange:  '#FF7A1A',
  purple:  '#7C3AED',
  green:   '#00C47A',
  red:     '#FF2D55',
  blue:    '#1A6FFF',
  black:   '#0D0D0D',
};

// Neubrutalism chart palette — 10 flat colors
const PALETTE = [
  '#FFE500', '#B8FF4F', '#00D4FF', '#FF6BB5', '#FF7A1A',
  '#7C3AED', '#00C47A', '#FF2D55', '#1A6FFF', '#F97316',
];

// ── Tab Switching ─────────────────────────────────────────────────────────
function switchTab(tab) {
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById(`tab-${tab}`).classList.add('active');
  document.getElementById(`tab-${tab}-btn`).classList.add('active');
}

// ── Formatting helpers ────────────────────────────────────────────────────
const fmt = {
  pct:   (v) => `${(v * 100).toFixed(1)}%`,
  num:   (v) => v >= 1e6 ? `${(v/1e6).toFixed(1)}M` : v >= 1e3 ? `${(v/1e3).toFixed(0)}K` : String(v),
  time:  ()  => new Date().toLocaleTimeString('vi-VN'),
};

// ── Chart instances ───────────────────────────────────────────────────────
let charts = {};

function makeChart(id, config) {
  const canvas = document.getElementById(id);
  if (!canvas) return null;
  if (charts[id]) { charts[id].destroy(); }
  charts[id] = new Chart(canvas, config);
  return charts[id];
}

// ─────────────────────────────────────────────────────────────────────────
// TAB 1: LỊCH SỬ
// ─────────────────────────────────────────────────────────────────────────

async function loadHistory() {
  try {
    const [topGames, genre, year, month] = await Promise.all([
      fetch('/api/top-games').then(r => r.json()),
      fetch('/api/sentiment-by-genre').then(r => r.json()),
      fetch('/api/trend-by-year').then(r => r.json()),
      fetch('/api/trend-by-month').then(r => r.json()),
    ]);

    renderTopGames(topGames);
    renderGenre(genre);
    renderTrendYear(year);
    renderTrendMonth(month);
    updateHistoryStats(topGames, year);
  } catch (err) {
    console.error('History load error:', err);
  }
}

function updateHistoryStats(topGames, year) {
  document.getElementById('stat-games').textContent = topGames.length;
  const totalReviews = topGames.reduce((s, d) => s + (d.total_reviews || 0), 0);
  document.getElementById('stat-reviews').textContent = fmt.num(totalReviews);
  const avgRate = topGames.reduce((s, d) => s + (d.recommend_rate || 0), 0) / (topGames.length || 1);
  document.getElementById('stat-avg-rate').textContent = fmt.pct(avgRate);
  const years = year.map(d => d.review_year).filter(Boolean);
  document.getElementById('stat-years').textContent = years.length ? `${Math.min(...years)}–${Math.max(...years)}` : '—';
}

function renderTopGames(data) {
  const labels = data.map(d => d.name);
  const values = data.map(d => +(d.recommend_rate * 100).toFixed(1));

  makeChart('chartTopGames', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Recommend Rate (%)',
        data: values,
        backgroundColor: labels.map((_, i) => PALETTE[i % PALETTE.length]),
        borderColor: '#000',
        borderWidth: 2,
        borderSkipped: false,
      }],
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: (c) => ` ${c.raw}%` } },
      },
      scales: {
        x: { min: 0, max: 100, ticks: { callback: (v) => `${v}%`, font: { family: "'Space Mono'" } }, grid: { color: 'rgba(0,0,0,0.06)' } },
        y: { ticks: { font: { size: 11, family: "'Space Grotesk'" } } },
      },
    },
  });
}

function renderGenre(data) {
  const sorted = [...data].sort((a, b) => b.recommend_rate - a.recommend_rate).slice(0, 12);
  const labels = sorted.map(d => d.genre);
  const values = sorted.map(d => +(d.recommend_rate * 100).toFixed(1));

  makeChart('chartGenre', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Recommend Rate (%)',
        data: values,
        backgroundColor: labels.map((_, i) => PALETTE[i % PALETTE.length]),
        borderColor: '#000',
        borderWidth: 2,
        borderSkipped: false,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: (c) => ` ${c.raw}%` } },
      },
      scales: {
        y: { min: 0, max: 100, ticks: { callback: (v) => `${v}%`, font: { family: "'Space Mono'" } }, grid: { color: 'rgba(0,0,0,0.06)' } },
        x: { ticks: { font: { size: 10, family: "'Space Grotesk'" } } },
      },
    },
  });
}

function renderTrendYear(data) {
  const labels = data.map(d => String(d.review_year));
  const values = data.map(d => +(d.recommend_rate * 100).toFixed(2));
  const counts = data.map(d => d.total_reviews);

  makeChart('chartTrendYear', {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Recommend Rate (%)',
          data: values,
          borderColor: COLORS.purple,
          backgroundColor: COLORS.purple + '22',
          fill: true, tension: 0.3,
          pointBackgroundColor: COLORS.purple,
          pointBorderColor: '#000', pointBorderWidth: 2,
          pointRadius: 6,
          borderWidth: 3,
          yAxisID: 'y',
        },
        {
          label: 'Tổng Reviews',
          data: counts,
          borderColor: COLORS.cyan,
          backgroundColor: 'transparent',
          borderDash: [5, 3], tension: 0.3,
          pointRadius: 4,
          borderWidth: 2,
          yAxisID: 'y2',
        },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 14, font: { size: 11 } } } },
      scales: {
        y:  { position: 'left',  ticks: { callback: (v) => `${v}%` }, grid: { color: 'rgba(0,0,0,0.06)' } },
        y2: { position: 'right', ticks: { callback: (v) => fmt.num(v) }, grid: { drawOnChartArea: false } },
        x:  { grid: { color: 'rgba(0,0,0,0.06)' } },
      },
    },
  });
}

function renderTrendMonth(data) {
  const sorted = [...data].sort((a, b) => a.review_month > b.review_month ? 1 : -1);
  const labels = sorted.map(d => d.review_month);
  const values = sorted.map(d => +(d.recommend_rate * 100).toFixed(2));

  makeChart('chartTrendMonth', {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Recommend Rate (%)',
        data: values,
        borderColor: COLORS.orange,
        backgroundColor: COLORS.orange + '22',
        fill: true, tension: 0.3, borderWidth: 3,
        pointRadius: 2, pointHoverRadius: 6,
        pointBackgroundColor: COLORS.orange,
        pointBorderColor: '#000', pointBorderWidth: 2,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: { ticks: { callback: (v) => `${v}%` }, grid: { color: 'rgba(0,0,0,0.06)' } },
        x: { ticks: { maxTicksLimit: 12, font: { size: 10 } } },
      },
    },
  });
}

// ─────────────────────────────────────────────────────────────────────────
// TAB 2: REAL-TIME (SSE)
// ─────────────────────────────────────────────────────────────────────────

const RT_HISTORY_MAX = 20; // điểm lịch sử lưu trên chart
let rtHistory = [];        // [{time, totalNeg}]

function initRealtimeCharts() {
  // Chart: Positive vs Negative grouped bar
  makeChart('chartRTSentiment', {
    type: 'bar',
    data: { labels: [], datasets: [
      { label: 'Positive', data: [], backgroundColor: COLORS.lime,   borderColor: '#000', borderWidth: 2, borderSkipped: false },
      { label: 'Negative', data: [], backgroundColor: COLORS.red,    borderColor: '#000', borderWidth: 2, borderSkipped: false },
    ]},
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 14, font: { size: 11 } } } },
      scales: { x: { ticks: { font: { size: 10 } } }, y: { grid: { color: 'rgba(0,0,0,0.06)' } } },
    },
  });

  // Chart: Negative ratio doughnut
  makeChart('chartRTRatio', {
    type: 'doughnut',
    data: { labels: [], datasets: [{ data: [], backgroundColor: [], borderWidth: 3, borderColor: '#000' }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { boxWidth: 14, font: { size: 11 } } } },
      cutout: '60%',
    },
  });

  // Chart: History line (total negative over time)
  makeChart('chartRTHistory', {
    type: 'line',
    data: {
      labels: [],
      datasets: [{
        label: 'Total Negative / batch',
        data: [],
        borderColor: COLORS.red,
        backgroundColor: COLORS.red + '22',
        fill: true, tension: 0.3, borderWidth: 3,
        pointRadius: 4, pointBackgroundColor: COLORS.red,
        pointBorderColor: '#000', pointBorderWidth: 2,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { font: { size: 10 } } },
        y: { grid: { color: 'rgba(0,0,0,0.06)' } },
      },
    },
  });
}

function updateRealtimeCharts(data) {
  if (!data || !Array.isArray(data) || data.length === 0) return;

  const labels   = data.map(d => d.game_name || d.game_id || '?');
  const positive = data.map(d => d.positive || 0);
  const negative = data.map(d => d.negative || 0);
  const alerts   = data.filter(d => d.alert);

  // ── Stats ────────────────────────────────────────────────────────────
  document.getElementById('rt-total-positive').textContent = fmt.num(positive.reduce((a, b) => a + b, 0));
  document.getElementById('rt-total-negative').textContent = fmt.num(negative.reduce((a, b) => a + b, 0));
  document.getElementById('rt-alert-count').textContent    = alerts.length;

  // ── Alert Banner ──────────────────────────────────────────────────────
  const banner = document.getElementById('alert-banner');
  if (alerts.length > 0) {
    banner.classList.remove('hidden');
    document.getElementById('alert-games-list').textContent =
      alerts.map(a => `${a.game_name || a.game_id}: ${a.negative} negative / 30s`).join(' · ');
  } else {
    banner.classList.add('hidden');
  }

  // ── Chart: Sentiment Bar ──────────────────────────────────────────────
  const sc = charts['chartRTSentiment'];
  if (sc) {
    sc.data.labels = labels;
    sc.data.datasets[0].data = positive;
    sc.data.datasets[1].data = negative;
    sc.update('none');
  }

  // ── Chart: Ratio Doughnut ─────────────────────────────────────────────
  const rc = charts['chartRTRatio'];
  if (rc) {
    const ratioLabels = labels;
    const ratioData   = data.map(d => {
      const total = (d.positive || 0) + (d.negative || 0);
      return total > 0 ? +((d.negative / total) * 100).toFixed(1) : 0;
    });
    const ratioColors = ratioData.map(v => v > 40 ? COLORS.red : v > 20 ? COLORS.orange : COLORS.lime);
    rc.data.labels             = ratioLabels;
    rc.data.datasets[0].data   = ratioData;
    rc.data.datasets[0].backgroundColor = ratioColors;
    rc.update('none');
  }

  // ── Chart: History Line ───────────────────────────────────────────────
  const totalNeg = negative.reduce((a, b) => a + b, 0);
  rtHistory.push({ time: fmt.time(), totalNeg });
  if (rtHistory.length > RT_HISTORY_MAX) rtHistory.shift();

  const hc = charts['chartRTHistory'];
  if (hc) {
    hc.data.labels           = rtHistory.map(p => p.time);
    hc.data.datasets[0].data = rtHistory.map(p => p.totalNeg);
    hc.update('none');
  }

  // ── Last update ───────────────────────────────────────────────────────
  document.getElementById('last-update').textContent = `Last update: ${fmt.time()}`;
}

// ── SSE Connection ────────────────────────────────────────────────────────
function connectSSE() {
  const statusEl = document.getElementById('sse-status');
  statusEl.textContent = '●';
  statusEl.className = 'stat-value sse-status';

  const es = new EventSource('/api/stream/realtime');

  es.onopen = () => {
    console.log('[SSE] Connected');
    statusEl.textContent = '●';
    statusEl.classList.add('connected');
    document.getElementById('last-update').textContent = `Kết nối lúc ${fmt.time()}`;
  };

  es.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      if (!data.error) updateRealtimeCharts(data);
    } catch (e) {
      console.warn('[SSE] Parse error:', e);
    }
  };

  es.onerror = () => {
    console.warn('[SSE] Connection error — reconnecting...');
    statusEl.textContent = '●';
    statusEl.classList.add('error');
    document.getElementById('last-update').textContent = 'Mất kết nối — đang reconnect...';
    // EventSource tự reconnect sau ~3s
  };
}

// ── Init ──────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  loadHistory();
  initRealtimeCharts();
  connectSSE();
});
