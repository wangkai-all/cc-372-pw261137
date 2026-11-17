// Pinned-version export console.
// Plain ES modules, no framework, no chart library. All text from the API is
// inserted with textContent - never as HTML.

const $ = (sel) => document.querySelector(sel);

const state = {
  overview: null,
  jobs: [],
  probe: null,
  selectedJob: null,
  detail: null,
  pollTimer: null,
  detailTimer: null,
  busy: false,
};

// ---- formatting ---------------------------------------------------------

const nf = new Intl.NumberFormat();

function fmtInt(n) {
  if (n == null || Number.isNaN(n)) return '—';
  return nf.format(Math.round(n));
}

function fmtBytes(n) {
  if (n == null || !Number.isFinite(n)) return '—';
  if (n < 1024) return `${n} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v < 10 ? v.toFixed(2) : v.toFixed(1)} ${units[i]}`;
}

function fmtMs(ms) {
  if (ms == null || !Number.isFinite(ms)) return '—';
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)} s`;
  return `${(ms / 60_000).toFixed(1)} min`;
}

function fmtWhen(ts) {
  if (!ts) return '—';
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function fmtClock(ts) {
  if (!ts) return '—';
  return new Date(ts).toLocaleString([], {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
}

function fmtDuration(sec) {
  if (sec == null) return '—';
  if (sec < 60) return `${Math.round(sec)} s`;
  if (sec < 3600) return `${(sec / 60).toFixed(sec < 600 ? 1 : 0)} min`;
  return `${(sec / 3600).toFixed(1)} h`;
}

// ---- tiny DOM helpers ---------------------------------------------------

function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null) continue;
    if (k === 'class') node.className = v;
    else if (k === 'text') node.textContent = v;
    else if (k === 'title') node.title = v;
    else if (k.startsWith('on') && typeof v === 'function') node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const child of [].concat(children)) {
    if (child == null) continue;
    node.append(child.nodeType ? child : document.createTextNode(String(child)));
  }
  return node;
}

function toast(message, isError = false) {
  const t = $('#toast');
  t.textContent = message;
  t.className = `toast show${isError ? ' error' : ''}`;
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => {
    t.className = 'toast';
  }, isError ? 5200 : 3200);
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    method: opts.method || 'GET',
    headers: opts.body ? { 'content-type': 'application/json' } : undefined,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* non-JSON response */
  }
  if (!res.ok) {
    const message = (json && json.error) || `${res.status} ${res.statusText}`;
    const err = new Error(message);
    err.status = res.status;
    err.payload = json;
    throw err;
  }
  return json;
}

// ---- theme --------------------------------------------------------------

function applyTheme(mode) {
  if (mode === 'dark' || mode === 'light') document.documentElement.dataset.theme = mode;
  else delete document.documentElement.dataset.theme;
  $('#theme-toggle').textContent =
    document.documentElement.dataset.theme === 'dark' ||
    (!document.documentElement.dataset.theme &&
      window.matchMedia('(prefers-color-scheme: dark)').matches)
      ? 'Light'
      : 'Dark';
}

// ---- overview poll ------------------------------------------------------

async function refresh() {
  try {
    const data = await api('/api/overview');
    state.overview = data;
    state.jobs = data.jobs || [];
    if (data.probe) state.probe = data.probe.detail || state.probe;
    renderKpis();
    renderJobs();
    renderEvents();
    renderCostFromProbe();
    renderGcHint();
    const running = state.jobs.some((j) => j.status === 'running' || j.status === 'pending');
    schedulePoll(running ? 900 : 3000);
  } catch (err) {
    schedulePoll(4000);
    toast(`Could not refresh: ${err.message}`, true);
  }
}

function schedulePoll(ms) {
  clearTimeout(state.pollTimer);
  state.pollTimer = setTimeout(refresh, ms);
}

function renderKpis() {
  const o = state.overview;
  if (!o) return;
  const s = o.source;
  $('#kpi-rows').textContent = fmtInt(s.liveRows);
  $('#kpi-rows-foot').textContent =
    `${fmtInt(s.deletedRows)} deleted · max id ${fmtInt(s.maxId)} · v${fmtInt(s.changeSeq)}`;
  $('#kpi-changes').textContent = fmtInt(s.changeCount);
  $('#kpi-changes-foot').textContent =
    `${fmtBytes(s.changeBytes)} history` +
    (o.oldestActivePin != null ? ` · replay pins hold ≥ v${fmtInt(o.oldestActivePin)}` : ' · no replay pin holds history');
  $('#kpi-snapshot').textContent = fmtBytes(o.storage.snapshotBytes);
  $('#kpi-snapshot-foot').textContent = o.storage.snapshotBytes
    ? 'held until each pin expires'
    : 'no materialised pins right now';
  $('#kpi-parts').textContent = fmtBytes(o.storage.partsBytes);
  $('#kpi-parts-foot').textContent = `${state.jobs.length} export${state.jobs.length === 1 ? '' : 's'} in the registry`;
}

function renderGcHint() {
  const plan = state.overview?.gcPlan || [];
  const due = plan.filter((p) => !p.protected).length;
  const held = plan.filter((p) => p.protected).length;
  $('#gc-hint').textContent = plan.length
    ? `${due} due for cleanup · ${held} protected`
    : 'nothing past retention';
}

// ---- cost ---------------------------------------------------------------

function renderCostFromProbe() {
  const probe = state.overview?.probe;
  if (!probe) return;
  const detail = probe.detail || {};
  if (detail.strategies) {
    state.probe = detail;
    renderCost(detail);
  }
}

function renderCost(probe) {
  if (!probe || !probe.strategies) return;
  const { snapshot, replay } = probe.strategies;
  const maxHeld = Math.max(snapshot.heldBytes, replay.heldBytes, 1);

  $('#cost-snap-disk').textContent = fmtBytes(snapshot.heldBytes);
  $('#cost-snap-pin').textContent = fmtMs(snapshot.pinMs);
  $('#cost-snap-rebuild').textContent = '0 rows';
  $('#cost-snap-resume').textContent = `${fmtMs(snapshot.resumeMsTotal)} / full resume`;
  $('#cost-snap-tag').textContent =
    `v${fmtInt(probe.measured.version)} · ${fmtInt(probe.measured.windowCount)} × ${fmtInt(probe.measured.windowWidth)} ids`;
  $('#meter-snap-disk').style.width = `${Math.max(1, (snapshot.heldBytes / maxHeld) * 100)}%`;

  $('#cost-replay-disk').textContent = fmtBytes(replay.heldBytes);
  $('#cost-replay-pin').textContent = 'none';
  $('#cost-replay-rebuild').textContent = `${fmtInt(replay.rebuildRowsOnResume)} rows / resume`;
  $('#cost-replay-resume').textContent = `${fmtMs(replay.resumeMsTotal)} / full resume`;
  $('#cost-replay-tag').textContent = `v${fmtInt(probe.measured.version)} · ${fmtInt(probe.measured.changeCount)} changes`;
  $('#meter-replay-disk').style.width = `${Math.max(1, (replay.heldBytes / maxHeld) * 100)}%`;

  $('#cost-snap-meter-label').textContent =
    `Held bytes · ${fmtBytes(snapshot.heldBytes)} of ${fmtBytes(maxHeld)} across strategies`;
  $('#cost-replay-meter-label').textContent =
    `Retained bytes · ${fmtBytes(replay.heldBytes)} of ${fmtBytes(maxHeld)} across strategies`;

  const rows = [
    ['Held for the job', fmtBytes(snapshot.heldBytes), fmtBytes(replay.heldBytes), 'measured'],
    ['One-time pin compute', fmtMs(snapshot.pinMs), 'none', 'measured'],
    ['Rows recomputed per resume', '0', fmtInt(replay.rebuildRowsOnResume), 'measured'],
    ['Resume read cost', fmtMs(snapshot.resumeMsTotal), fmtMs(replay.resumeMsTotal), 'measured'],
    [
      'Sample',
      `${fmtInt(probe.measured.sample)} windows`,
      `${fmtInt(probe.measured.sample)} windows`,
      `${fmtInt(probe.measured.rowsSampled)} rows probed of ${fmtInt(probe.measured.rowsTotal)}`,
    ],
  ];
  const body = $('#cost-table-body');
  body.replaceChildren();
  for (const [label, a, b, basis] of rows) {
    body.append(
      el('tr', {}, [
        el('th', { scope: 'row', text: label }),
        el('td', { class: 'num', text: a }),
        el('td', { class: 'num', text: b }),
        el('td', { class: 'mono', text: basis }),
      ])
    );
  }
  $('#probe-when').textContent = `measured ${fmtWhen(probe.measured.at)} · ${fmtMs(probe.measured.elapsedMs)}`;
}

// ---- jobs ---------------------------------------------------------------

const STATUS_LABEL = {
  running: 'exporting',
  pending: 'queued',
  pinned: 'pinned',
  done: 'complete',
  paused: 'paused',
  paused_flagged: 'pausing',
  failed: 'failed',
  canceled: 'canceled',
  expired: 'expired',
};

function statusLabel(job) {
  if (job.pauseRequested && job.status === 'running') return 'pausing';
  return STATUS_LABEL[job.status] || job.status;
}

function jobActions(job, compact = true) {
  const wrap = el('div', { class: 'job-actions' });
  const btn = (label, kind, handler, title) =>
    el('button', {
      class: `btn ${kind} small`,
      type: 'button',
      text: label,
      title: title || label,
      onclick: async (e) => {
        e.stopPropagation();
        await handler();
      },
    });

  if (['running', 'pending'].includes(job.status) && !job.pauseRequested) {
    wrap.append(btn('Pause', 'ghost', () => act(`/api/jobs/${job.id}/pause`, 'Paused export')));
  }
  if (['paused', 'pending'].includes(job.status) || job.pauseRequested) {
    wrap.append(
      btn(
        job.status === 'pending' ? 'Start' : 'Resume',
        'ghost',
        () => act(`/api/jobs/${job.id}/resume`, job.status === 'pending' ? 'Export started' : 'Resumed export')
      )
    );
  }
  if (!['done', 'expired', 'canceled'].includes(job.status) && job.progress.committed > 0) {
    wrap.append(btn('Download', 'ghost', () => download(job)));
  }
  if (['paused', 'done', 'failed'].includes(job.status)) {
    wrap.append(btn('Verify', 'ghost', () => verify(job, 'quick')));
  }
  wrap.append(btn('Details', 'ghost', () => openJob(job.id)));
  return wrap;
}

function progressBar(job) {
  const p = job.progress;
  const total = Math.max(1, p.total);
  const committed = el('div', {
    class: 'progress-seg committed',
    style: `width:${(p.committed / total) * 100}%`,
    title: `${p.committed} committed`,
  });
  const failed = p.failed
    ? el('div', {
        class: 'progress-seg failed',
        style: `width:${(p.failed / total) * 100}%`,
        title: `${p.failed} failed`,
      })
    : null;
  return el('div', { class: 'progress' }, [
    el('div', { class: 'progress-head' }, [
      el('span', {}, [el('strong', { text: `${p.committed} / ${p.total}` }), ' parts committed']),
      el('span', { text: p.pending ? `${p.pending} pending` : p.failed ? `${p.failed} failed` : 'all committed' }),
    ]),
    el('div', { class: 'progress-track', role: 'img', 'aria-label': `${p.committed} of ${p.total} parts committed` }, [committed, failed]),
    el('div', { class: 'progress-legend' }, [
      el('span', {}, [el('span', { class: 'dot', style: 'background:var(--series-1)' }), 'committed']),
      p.failed ? el('span', {}, [el('span', { class: 'dot', style: 'background:var(--critical)' }), 'failed']) : null,
      el('span', { text: `${fmtInt(p.rowsExported)} rows · ${fmtBytes(p.bytes)}` }),
    ]),
  ]);
}

function renderJobs() {
  const list = $('#jobs-list');
  if (!state.jobs.length) {
    list.replaceChildren(el('p', { class: 'empty', text: 'No exports yet. Create one above.' }));
    return;
  }
  list.replaceChildren();
  for (const job of state.jobs) {
    const card = el('article', { class: 'job' }, [
      el('div', { class: 'job-top' }, [
        el('div', {}, [
          el('div', { class: 'job-title' }, [
            el('span', { class: 'job-name', text: job.name }),
            el('span', { class: `status ${job.status}`, text: statusLabel(job) }),
            el('span', { class: 'tag', text: job.strategy === 'snapshot' ? 'snapshot' : 'replay' }),
          ]),
          el('div', {
            class: 'job-meta',
            text:
              `pinned v${fmtInt(job.pinnedVersion)} · ${job.windowCount} × ${job.windowWidth} ids · ` +
              `created ${fmtClock(job.createdAt)}` +
              (job.expiresAt ? ` · retention until ${fmtClock(job.expiresAt)}` : ''),
          }),
        ]),
        jobActions(job),
      ]),
      progressBar(job),
      el('div', { class: 'job-facts' }, [
        el('span', {}, [
          job.strategy === 'snapshot' ? 'pin work ' : 'pin cost ',
          el('b', { text: job.strategy === 'snapshot' ? fmtMs(job.pinMs) : 'version only' }),
        ]),
        el('span', {}, [
          'copy ',
          el('b', {
            text: job.strategy === 'snapshot' ? fmtBytes(job.snapshotBytes) : 'none (re-reads the log)',
          }),
        ]),
        el('span', {}, ['rebuild ', el('b', { text: `${fmtInt(job.rebuildRows)} rows · ${fmtMs(job.rebuildMs)}` }) ]),
        el('span', {}, ['write time ', el('b', { text: fmtMs(job.deriveMs) }) ]),
        job.expiresAt && Date.now() > job.expiresAt
          ? el('span', { class: 'status paused', text: 'retention passed · kept while resumable' })
          : null,
        job.error ? el('span', { class: 'status failed', text: job.error }) : null,
      ]),
    ]);
    list.append(card);
  }
}

function renderEvents() {
  const list = $('#events-list');
  const events = state.overview?.events || [];
  list.replaceChildren();
  if (!events.length) {
    list.append(el('li', { class: 'event-msg', text: 'No activity yet.' }));
    return;
  }
  for (const e of events) {
    list.append(
      el('li', {}, [
        el('span', { class: 'event-time', text: fmtWhen(e.at) }),
        el('span', { class: 'event-kind', text: e.kind }),
        el('span', { class: 'event-msg', text: e.message }),
      ])
    );
  }
}

// ---- actions ------------------------------------------------------------

async function act(path, okMessage, body = null) {
  if (state.busy) return null;
  state.busy = true;
  try {
    const out = await api(path, { method: 'POST', body: body || {} });
    if (okMessage) toast(okMessage);
    await refresh();
    if (state.selectedJob) await loadDetail(state.selectedJob, { silent: true });
    return out;
  } catch (err) {
    toast(err.message, true);
    return null;
  } finally {
    state.busy = false;
  }
}

function download(job) {
  const a = el('a', { href: `/api/jobs/${job.id}/download` });
  document.body.append(a);
  a.click();
  a.remove();
  toast('Download started — the job is protected while bytes are in flight.');
}

async function verify(job, mode) {
  const out = $('#dlg-verify-out');
  try {
    toast(`${mode === 'deep' ? 'Deep' : 'Quick'} verification running…`);
    const result = await api(`/api/jobs/${job.id}/verify`, { method: 'POST', body: { mode } });
    if (out) renderVerifyResult(out, result);
    toast(
      result.ok
        ? `Verified ${result.parts} parts, ${fmtInt(result.checkedRows)} rows`
        : `Verification failed: ${result.failedParts} part(s)`,
      !result.ok
    );
    if (state.selectedJob) await loadDetail(state.selectedJob, { silent: true });
    await refresh();
  } catch (err) {
    toast(err.message, true);
  }
}

function renderVerifyResult(container, result) {
  container.hidden = false;
  container.className = `verify-out ${result.ok ? 'ok' : 'bad'}`;
  const head = el('div', {}, [
    el('strong', { text: result.ok ? 'Verification passed' : 'Verification failed' }),
    ` · ${result.mode} · ${result.parts} part(s) · ${fmtInt(result.checkedRows)} rows · ${fmtMs(result.elapsedMs)}`,
  ]);
  const list = el('ul');
  const bad = result.results.filter((r) => !r.ok);
  const shown = (bad.length ? bad : result.results).slice(0, 8);
  for (const r of shown) {
    list.append(
      el('li', { class: r.ok ? '' : 'failures' }, [
        el('span', { class: 'mono', text: `${r.file} [${r.range[0]}–${r.range[1]}]` }),
        r.ok
          ? ` · ${fmtInt(r.checked)} rows · checksums match · ${fmtMs(r.ms)}`
          : ` · ${r.failureCount} problem(s): ${r.failures.map((f) => f.message).join('; ')}`,
      ])
    );
  }
  container.replaceChildren(head, list);
}

// ---- job detail ---------------------------------------------------------

async function openJob(jobId) {
  state.selectedJob = jobId;
  const dlg = $('#job-dialog');
  if (!dlg.open) dlg.showModal();
  await loadDetail(jobId);
  clearInterval(state.detailTimer);
  state.detailTimer = setInterval(() => {
    if (!dlg.open) {
      clearInterval(state.detailTimer);
      return;
    }
    const job = state.detail?.job;
    if (job && ['running', 'pending'].includes(job.status)) loadDetail(jobId, { silent: true });
  }, 1200);
}

async function loadDetail(jobId, { silent = false } = {}) {
  try {
    const data = await api(`/api/jobs/${jobId}`);
    state.detail = data;
    renderDetail(data);
  } catch (err) {
    if (!silent) toast(err.message, true);
  }
}

function renderDetail(data) {
  const job = data.job;
  $('#dlg-title').textContent = job.name;
  $('#dlg-sub').textContent =
    `pinned v${fmtInt(job.pinnedVersion)} · ${job.strategy === 'snapshot' ? 'materialised snapshot' : 'replay recipe'} · ` +
    `${job.windowCount} × ${job.windowWidth} ids · ${statusLabel(job)}`;

  const p = job.progress;
  $('#dlg-summary').replaceChildren(
    el('span', {}, ['committed ', el('b', { text: `${p.committed}/${p.total}` })]),
    el('span', {}, ['rows ', el('b', { text: fmtInt(p.rowsExported) })]),
    el('span', {}, ['bytes ', el('b', { text: fmtBytes(p.bytes) })]),
    el('span', {}, ['pin ', el('b', { text: fmtMs(job.pinMs) })]),
    el('span', {}, ['rebuild ', el('b', { text: `${fmtInt(job.rebuildRows)} rows` })]),
    el('span', {}, ['retention ', el('b', { text: job.expiresAt ? fmtDuration((job.expiresAt - Date.now()) / 1000) : '—' })]),
    el('span', {}, ['leases ', el('b', { text: data.leases.length ? data.leases.map((l) => l.purpose).join(', ') : 'none' })])
  );

  $('#dlg-download').disabled = p.committed === 0 || job.status === 'expired';
  $('#dlg-verify-quick').disabled = p.committed === 0;
  $('#dlg-verify-deep').disabled = p.committed === 0;
  $('#dlg-pause').disabled = !['running', 'pending'].includes(job.status) || job.pauseRequested;
  $('#dlg-resume').disabled = !(
    job.status === 'paused' ||
    job.status === 'pending' ||
    job.pauseRequested ||
    (job.status === 'done' && p.pending > 0)
  );
  $('#dlg-cancel').disabled = ['done', 'expired', 'canceled'].includes(job.status);

  $('#dlg-download').onclick = () => download(job);
  $('#dlg-verify-quick').onclick = () => verify(job, 'quick');
  $('#dlg-verify-deep').onclick = () => verify(job, 'deep');
  $('#dlg-pause').onclick = () => act(`/api/jobs/${job.id}/pause`, 'Pause requested');
  $('#dlg-resume').onclick = () => act(`/api/jobs/${job.id}/resume`, 'Resumed');
  $('#dlg-cancel').onclick = () => act(`/api/jobs/${job.id}/cancel`, 'Export canceled');

  const body = $('#windows-body');
  body.replaceChildren();
  for (const w of job.windows) {
    body.append(renderWindowRow(job, w));
  }
}

function renderWindowRow(job, w) {
  const actions = el('div', { class: 'row-actions' });
  const small = (label, fn, title) =>
    el('button', { class: 'btn ghost small', type: 'button', text: label, title: title || label, onclick: fn });

  if (w.status === 'pending') {
    actions.append(
      small('Split', () => revise(job, { action: 'split', idx: w.idx, parts: 2 }), 'Split into two windows (no re-read)'),
      small('Merge', () => revise(job, { action: 'merge', idx: w.idx, count: 2 }), 'Merge with the next window (no re-read)')
    );
  }
  if (w.status === 'committed') {
    actions.append(
      small('Re-issue', () => revise(job, { action: 'reissue', from: w.idx, to: w.idx }), 'Delete the part and recompute it from the pin'),
      small('Verify', () => verifySingle(job, w.idx), 'Verify just this part')
    );
  }
  if (w.status !== 'committed') {
    actions.append(
      small('Retarget', () => {
        const from = Number(prompt(`New start id (1–${job.maxId})`, String(w.id_from)));
        if (!from) return;
        const to = Number(prompt('New end id', String(w.id_to)));
        if (!to) return;
        revise(job, { action: 'retarget', idx: w.idx, idFrom: from, idTo: to });
      }, 'Change this window’s id range')
    );
  }

  let check;
  if (w.verify_ok === 1) check = el('span', { class: 'check-ok', text: 'verified' });
  else if (w.verify_ok === 0) check = el('span', { class: 'check-bad', text: 'failed' });
  else check = el('span', { class: 'check-none', text: '—' });

  return el('tr', {}, [
    el('td', { class: 'num mono', text: String(w.idx) }),
    el('td', { class: 'mono', text: `${fmtInt(w.id_from)}–${fmtInt(w.id_to)}` }),
    el('td', {}, [el('span', { class: `state ${w.status}`, text: w.status })]),
    el('td', { class: 'num', text: w.status === 'committed' ? fmtInt(w.rows) : '—' }),
    el('td', { class: 'num', text: w.status === 'committed' ? fmtBytes(w.bytes) : '—' }),
    el('td', {}, [check, w.verify_note ? el('div', { class: 'hint', text: w.verify_note }) : null]),
    el('td', {}, [actions]),
  ]);
}

async function verifySingle(job, idx) {
  try {
    const result = await api(`/api/jobs/${job.id}/verify`, {
      method: 'POST',
      body: { mode: 'deep', indices: [idx] },
    });
    const out = $('#dlg-verify-out');
    if (out) renderVerifyResult(out, result);
    toast(result.ok ? `Part ${idx} verified` : `Part ${idx} failed verification`, !result.ok);
    await loadDetail(job.id, { silent: true });
  } catch (err) {
    toast(err.message, true);
  }
}

async function revise(job, body) {
  try {
    const out = await api(`/api/jobs/${job.id}/revise`, { method: 'POST', body });
    toast(
      out.removedFiles
        ? `Re-issued: ${out.removedFiles} part(s), ${fmtBytes(out.removedBytes)} will be recomputed from the pin`
        : 'Partition updated — no rows re-read'
    );
    await loadDetail(job.id, { silent: true });
    await refresh();
  } catch (err) {
    toast(err.message, true);
  }
}

// ---- control wiring -----------------------------------------------------

async function createJob(strategy) {
  const body = {
    strategy,
    windowWidth: Number($('#job-width').value) || 1000,
    workers: Number($('#job-workers').value) || 2,
    retentionSec: Number($('#job-retention').value) || 900,
  };
  try {
    const out = await api('/api/jobs', { method: 'POST', body });
    toast(`${out.job.name} pinned at v${fmtInt(out.job.pinnedVersion)} (${strategy})`);
    await refresh();
  } catch (err) {
    toast(err.message, true);
  }
}

function wire() {
  const stored = localStorage.getItem('export-theme');
  applyTheme(stored);
  $('#theme-toggle').addEventListener('click', () => {
    const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    applyTheme(next);
    localStorage.setItem('export-theme', next);
  });

  $('#gen-run').addEventListener('click', async () => {
    try {
      const rows = Number($('#gen-rows').value) || 12000;
      const payloadBytes = Number($('#gen-bytes').value) || 420;
      toast(`Generating ${fmtInt(rows)} rows…`);
      await api('/api/source/generate', { method: 'POST', body: { rows, payloadBytes } });
      toast('Source regenerated');
      await refresh();
    } catch (err) {
      toast(err.message, true);
    }
  });

  $('#mut-run').addEventListener('click', async () => {
    try {
      const body = {
        updates: Number($('#mut-updates').value) || 0,
        deletes: Number($('#mut-deletes').value) || 0,
        inserts: Number($('#mut-inserts').value) || 0,
      };
      const out = await api('/api/source/mutate', { method: 'POST', body });
      toast(`Source at v${fmtInt(out.topVersion.version)} — running exports keep their pin`);
      await refresh();
    } catch (err) {
      toast(err.message, true);
    }
  });

  $('#job-snapshot').addEventListener('click', () => createJob('snapshot'));
  $('#job-replay').addEventListener('click', () => createJob('replay'));

  $('#probe-run').addEventListener('click', async () => {
    try {
      $('#probe-when').textContent = 'measuring…';
      const probe = await api('/api/cost/probe', {
        method: 'POST',
        body: {
          windowWidth: Number($('#job-width').value) || 1000,
          sampleWindows: 3,
        },
      });
      if (probe.ok) {
        state.probe = probe;
        renderCost(probe);
        toast('Costs measured on a sample of windows');
      } else {
        toast(probe.reason || 'Could not measure', true);
      }
    } catch (err) {
      toast(err.message, true);
    }
  });

  $('#gc-sweep').addEventListener('click', async () => {
    try {
      const out = await api('/api/gc/sweep', { method: 'POST', body: {} });
      const freed = out.results.reduce((s, r) => s + (r.freedBytes || 0), 0);
      const refused = out.results.filter((r) => r.collected === false).length;
      toast(
        `Sweep: ${fmtBytes(freed)} freed` + (refused ? ` · ${refused} protected job(s) kept` : ''),
        false
      );
      await refresh();
    } catch (err) {
      toast(err.message, true);
    }
  });

  $('#clock').textContent = new Date().toLocaleDateString([], { weekday: 'short', month: 'short', day: 'numeric' });
}

wire();
refresh();
