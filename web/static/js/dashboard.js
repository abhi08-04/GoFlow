/* Dashboard: throughput chart + status breakdown donut, polled every 5s. */
(function () {
  'use strict';
  if (!window.Chart) return;

  // Read CSS vars so chart colors follow the theme automatically.
  function cssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }
  function themeTokens() {
    return {
      accent:   cssVar('--c-accent'),
      success:  cssVar('--c-success'),
      danger:   cssVar('--c-danger'),
      warning:  cssVar('--c-warning'),
      muted:    cssVar('--c-text-muted'),
      border:   cssVar('--c-border'),
      text:     cssVar('--c-text'),
    };
  }

  let throughput;
  let donut;

  async function loadSeries() {
    const res = await fetch('/api/metrics/timeseries');
    return res.json();
  }
  async function loadSnapshot() {
    const res = await fetch('/api/metrics/snapshot');
    return res.json();
  }

  function toSeries(samples) {
    const labels = [];
    const completed = [];
    const failed = [];
    const queue = [];
    for (let i = 1; i < samples.length; i++) {
      const prev = samples[i - 1];
      const cur = samples[i];
      const dSec = Math.max(1, (new Date(cur.timestamp) - new Date(prev.timestamp)) / 1000);
      const compRate = (cur.jobs_completed - prev.jobs_completed) / dSec;
      const failRate = (cur.jobs_failed - prev.jobs_failed) / dSec;
      const d = new Date(cur.timestamp);
      labels.push(d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }));
      completed.push(+compRate.toFixed(3));
      failed.push(+failRate.toFixed(3));
      queue.push(cur.queue_depth);
    }
    return { labels, completed, failed, queue };
  }

  function makeThroughput(ctx, data) {
    const t = themeTokens();
    const common = {
      tension: 0.3,
      borderWidth: 2,
      pointRadius: 0,
      pointHoverRadius: 4,
      fill: true,
    };
    return new Chart(ctx, {
      type: 'line',
      data: {
        labels: data.labels,
        datasets: [
          {
            label: 'completed/s',
            data: data.completed,
            borderColor: t.success,
            backgroundColor: t.success + '18',
            ...common,
          },
          {
            label: 'failed/s',
            data: data.failed,
            borderColor: t.danger,
            backgroundColor: t.danger + '18',
            ...common,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 300 },
        interaction: { mode: 'index', intersect: false },
        scales: {
          x: {
            grid: { display: false },
            ticks: { color: t.muted, font: { size: 11 }, maxRotation: 0, maxTicksLimit: 8 },
          },
          y: {
            beginAtZero: true,
            grid: { color: t.border, drawBorder: false },
            ticks: { color: t.muted, font: { size: 11 }, maxTicksLimit: 5 },
          },
        },
        plugins: {
          legend: {
            position: 'top',
            align: 'end',
            labels: { color: t.muted, font: { size: 12 }, boxWidth: 10, boxHeight: 10, usePointStyle: true, pointStyle: 'circle' },
          },
          tooltip: {
            backgroundColor: t.text,
            titleColor: cssVar('--c-bg'),
            bodyColor: cssVar('--c-bg'),
            borderWidth: 0,
            padding: 10,
            displayColors: true,
          },
        },
      },
    });
  }

  function makeDonut(ctx, snap) {
    const t = themeTokens();
    const pending  = Math.max(0, snap.queue_depth);
    const running  = Math.max(0, snap.in_flight);
    const done     = snap.jobs_completed;
    const failed   = snap.jobs_failed;
    const dlq      = snap.dlq_size;
    return new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['Completed', 'Failed', 'Running', 'Pending', 'DLQ'],
        datasets: [{
          data: [done, failed, running, pending, dlq],
          backgroundColor: [t.success, t.danger, t.accent, t.warning, cssVar('--c-text-faint')],
          borderWidth: 0,
          hoverOffset: 4,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '70%',
        plugins: {
          legend: {
            position: 'right',
            labels: {
              color: t.muted, font: { size: 12 }, boxWidth: 10, boxHeight: 10,
              usePointStyle: true, pointStyle: 'circle', padding: 12,
            },
          },
          tooltip: {
            backgroundColor: t.text,
            titleColor: cssVar('--c-bg'),
            bodyColor: cssVar('--c-bg'),
            borderWidth: 0, padding: 10,
          },
        },
      },
    });
  }

  async function refresh() {
    const [series, snap] = await Promise.all([loadSeries(), loadSnapshot()]);
    const data = toSeries(series);
    if (throughput) {
      throughput.data.labels = data.labels;
      throughput.data.datasets[0].data = data.completed;
      throughput.data.datasets[1].data = data.failed;
      throughput.update('none');
    }
    if (donut) {
      donut.data.datasets[0].data = [
        snap.jobs_completed, snap.jobs_failed, snap.in_flight, snap.queue_depth, snap.dlq_size,
      ];
      donut.update('none');
    }
  }

  async function init() {
    const tCanvas = document.getElementById('chart-throughput');
    const dCanvas = document.getElementById('chart-donut');
    if (!tCanvas) return;
    const [series, snap] = await Promise.all([loadSeries(), loadSnapshot()]);
    const data = toSeries(series);
    throughput = makeThroughput(tCanvas.getContext('2d'), data);
    if (dCanvas) donut = makeDonut(dCanvas.getContext('2d'), snap);
    setInterval(refresh, 5000);
  }

  function rethemeCharts() {
    if (throughput) { throughput.destroy(); throughput = null; }
    if (donut) { donut.destroy(); donut = null; }
    init();
  }

  window.GoFlow = Object.assign(window.GoFlow || {}, { rethemeCharts });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
