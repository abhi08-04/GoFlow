/* GoFlow client — tiny glue, no framework. */
(function () {
  'use strict';

  // ---------- theme ----------
  const THEME_KEY = 'goflow-theme';
  function applyTheme(t) {
    document.documentElement.setAttribute('data-theme', t);
  }
  function initTheme() {
    const saved = localStorage.getItem(THEME_KEY);
    if (saved) {
      applyTheme(saved);
      return;
    }
    const dark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    applyTheme(dark ? 'dark' : 'light');
  }
  function toggleTheme() {
    const cur = document.documentElement.getAttribute('data-theme') || 'light';
    const next = cur === 'dark' ? 'light' : 'dark';
    applyTheme(next);
    localStorage.setItem(THEME_KEY, next);
    // Chart.js needs explicit re-theming when CSS vars change.
    if (window.GoFlow && window.GoFlow.rethemeCharts) window.GoFlow.rethemeCharts();
  }
  initTheme();

  // ---------- toast ----------
  function toast(kind, title, body, ttl) {
    const tray = document.getElementById('toast-tray');
    if (!tray) return;
    const el = document.createElement('div');
    el.className = 'toast ' + (kind || '');
    el.innerHTML =
      '<div><strong></strong><p></p></div>';
    el.querySelector('strong').textContent = title || '';
    el.querySelector('p').textContent = body || '';
    tray.appendChild(el);
    setTimeout(() => {
      el.style.transition = 'opacity .2s, transform .2s';
      el.style.opacity = '0';
      el.style.transform = 'translateX(12px)';
      setTimeout(() => el.remove(), 220);
    }, ttl || 3800);
  }

  // ---------- modal ----------
  function openModal(id) {
    const el = document.getElementById(id);
    if (el) {
      el.setAttribute('data-open', 'true');
      const firstInput = el.querySelector('input, select, textarea, button');
      if (firstInput) firstInput.focus();
    }
  }
  function closeModal(id) {
    const el = document.getElementById(id);
    if (el) el.setAttribute('data-open', 'false');
  }
  function closeAllModals() {
    document.querySelectorAll('.modal-root[data-open="true"]').forEach(m => m.setAttribute('data-open', 'false'));
  }
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeAllModals();
  });
  document.addEventListener('click', (e) => {
    const t = e.target;
    if (!(t instanceof Element)) return;
    const opener = t.closest('[data-modal-open]');
    if (opener) { openModal(opener.getAttribute('data-modal-open')); e.preventDefault(); return; }
    const closer = t.closest('[data-modal-close]');
    if (closer) { closeModal(closer.getAttribute('data-modal-close')); return; }
    if (t.classList && t.classList.contains('modal-root') && t.getAttribute('data-open') === 'true') {
      t.setAttribute('data-open', 'false');
    }
  });

  // ---------- select-all helper for DLQ tables ----------
  function wireSelectAll() {
    document.addEventListener('change', (e) => {
      const t = e.target;
      if (!(t instanceof HTMLInputElement)) return;
      if (t.matches('[data-select-all]')) {
        const scope = t.getAttribute('data-select-all');
        document.querySelectorAll('[data-select-row="' + scope + '"]').forEach(cb => {
          if (cb instanceof HTMLInputElement) cb.checked = t.checked;
        });
        updateSelectionBar(scope);
      }
      if (t.matches('[data-select-row]')) {
        updateSelectionBar(t.getAttribute('data-select-row'));
      }
    });
  }
  function selectedIds(scope) {
    return Array.from(document.querySelectorAll('[data-select-row="' + scope + '"]:checked'))
      .map(cb => cb.getAttribute('data-id'))
      .filter(Boolean);
  }
  function updateSelectionBar(scope) {
    const bar = document.querySelector('[data-selection-bar="' + scope + '"]');
    if (!bar) return;
    const n = selectedIds(scope).length;
    bar.querySelector('[data-count]').textContent = n;
    bar.setAttribute('data-active', n > 0 ? 'true' : 'false');
  }

  // ---------- bulk DLQ actions (uses JSON API, shows toast) ----------
  async function postJSON(url) {
    const res = await fetch(url, { method: 'POST' });
    const body = await res.json().catch(() => ({}));
    return { ok: res.ok, status: res.status, body };
  }
  async function doBulkRetry() {
    if (!confirm('Requeue every job currently in the DLQ?')) return;
    const r = await postJSON('/dlq/retry-all');
    if (r.ok) {
      toast('success', 'DLQ requeued', `${r.body.requeued} jobs requeued, ${r.body.rejected} rejected (queue full)`);
      refreshPage();
    } else {
      toast('error', 'Requeue failed', r.body.error || 'Server error');
    }
  }
  async function doPurge() {
    if (!confirm('Permanently delete every job in the DLQ? This cannot be undone.')) return;
    const r = await postJSON('/dlq/purge');
    if (r.ok) {
      toast('warn', 'DLQ purged', `${r.body.purged} jobs removed`);
      refreshPage();
    } else {
      toast('error', 'Purge failed', r.body.error || 'Server error');
    }
  }
  async function doRetryOne(id) {
    const r = await postJSON('/dlq/' + id + '/retry');
    if (r.ok) {
      toast('success', 'Requeued', id.slice(0, 8));
      refreshPage();
    } else {
      toast('error', 'Requeue failed', r.body.error || 'Server error');
    }
  }
  async function doDeleteOne(id) {
    if (!confirm('Delete this job from the DLQ?')) return;
    const res = await fetch('/dlq/' + id, { method: 'DELETE' });
    const body = await res.json().catch(() => ({}));
    if (res.ok) {
      toast('warn', 'Deleted', id.slice(0, 8));
      refreshPage();
    } else {
      toast('error', 'Delete failed', body.error || 'Server error');
    }
  }
  function refreshPage() {
    // Trigger htmx reloads of marked containers; fall back to location.reload if none.
    const live = document.querySelectorAll('[hx-get][data-refresh-on-action]');
    if (live.length && window.htmx) {
      live.forEach(el => window.htmx.trigger(el, 'refresh'));
    } else {
      setTimeout(() => location.reload(), 250);
    }
  }

  // ---------- create-job form ----------
  async function submitCreateJob(ev) {
    ev.preventDefault();
    const f = ev.target;
    const idemKey = f.elements['idem_key'].value || ('ui-' + Date.now());
    const payload = {
      type: f.elements['type'].value,
      payload: f.elements['payload'].value,
    };
    const res = await fetch('/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idemKey },
      body: JSON.stringify(payload),
    });
    const body = await res.json().catch(() => ({}));
    if (res.ok) {
      toast('success', res.status === 201 ? 'Job created' : 'Job returned (idempotent)', body.id ? body.id.slice(0, 8) : '');
      closeAllModals();
      f.reset();
      refreshPage();
    } else {
      toast('error', 'Create failed', body.error || 'Server error');
    }
    return false;
  }

  // ---------- queue filter ----------
  // Reloads the current page with ?queue=<value> (or removes it when
  // "All queues" is picked). Also rewrites every HTMX polling URL on
  // the page so live refreshes stay scoped to the selected queue.
  function setQueueFilter(value) {
    const url = new URL(window.location.href);
    if (value) url.searchParams.set('queue', value);
    else url.searchParams.delete('queue');
    window.location.href = url.toString();
  }
  // Patch HTMX GET URLs so poll partials include the current ?queue=.
  function applyQueueToHtmx() {
    const current = new URL(window.location.href).searchParams.get('queue') || '';
    document.querySelectorAll('[hx-get]').forEach(el => {
      const raw = el.getAttribute('hx-get');
      if (!raw) return;
      const u = new URL(raw, window.location.origin);
      if (current) u.searchParams.set('queue', current);
      else u.searchParams.delete('queue');
      el.setAttribute('hx-get', u.pathname + (u.search || ''));
    });
  }
  // Run on load and after any HTMX swap (so swapped partials carry the filter too).
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', applyQueueToHtmx);
  } else {
    applyQueueToHtmx();
  }
  document.body && document.body.addEventListener('htmx:afterSwap', applyQueueToHtmx);

  // ---------- exports ----------
  window.GoFlow = Object.assign(window.GoFlow || {}, {
    toggleTheme, toast, openModal, closeModal, closeAllModals,
    doBulkRetry, doPurge, doRetryOne, doDeleteOne, submitCreateJob,
    selectedIds, refreshPage, setQueueFilter,
  });

  wireSelectAll();
})();
