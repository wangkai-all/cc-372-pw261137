'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { URL } = require('node:url');

const { Workspace } = require('./workspace');
const { Collector } = require('./gc');
const { Reviser } = require('./revise');
const { verifyWindow } = require('./verify');
const { measureCosts } = require('./cost');
const { ZipStream } = require('./zip');
const { SourceStore } = require('./source');
const { SnapshotReader, ReplayReader } = require('./export');
const { snapshotPath } = require('./pin');
const {
  nowMs,
  fileSize,
  readJsonBody,
  humanBytes,
} = require('./util');

const ROOT = path.resolve(__dirname, '..');
const PUBLIC_DIR = path.join(ROOT, 'public');
const DATA_DIR = process.env.DATA_DIR || path.join(ROOT, 'data');
const PORT = Number(process.env.PORT || 7317);
const HOST = process.env.HOST || '127.0.0.1';

const ws = new Workspace(DATA_DIR);
const collector = new Collector(ws);
const reviser = new Reviser(ws);

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};

function sendJson(res, status, body) {
  const data = Buffer.from(`${JSON.stringify(body)}\n`);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': data.length,
    'cache-control': 'no-store',
  });
  res.end(data);
}

function sendError(res, err) {
  const status = err.statusCode || 500;
  if (status >= 500) console.error(err);
  sendJson(res, status, { error: err.message || 'internal error' });
}

function intParam(value, name) {
  const n = Number.parseInt(value, 10);
  if (!Number.isFinite(n) || n < 0) {
    throw Object.assign(new Error(`invalid ${name}`), { statusCode: 400 });
  }
  return n;
}

function partPath(jobId, win) {
  if (!win?.file) return null;
  return path.join(ws.partsDir, jobId, win.file);
}

// ---- manifests -----------------------------------------------------------

function buildManifest(job, windows) {
  const committed = windows.filter((w) => w.status === 'committed');
  return {
    jobId: job.id,
    name: job.name,
    pinnedVersion: job.pinned_version,
    strategy: job.strategy,
    createdAt: new Date(job.created_at).toISOString(),
    windowWidth: job.window_width,
    windowCount: job.window_count,
    committedParts: committed.length,
    totalRows: committed.reduce((s, w) => s + w.rows, 0),
    totalBytes: committed.reduce((s, w) => s + w.bytes, 0),
    note: 'Every part is derived from pinnedVersion. Rows are ordered by id; parts partition the id space.',
    parts: committed
      .slice()
      .sort((a, b) => a.idx - b.idx)
      .map((w) => ({
        file: w.file,
        index: w.idx,
        idFrom: w.id_from,
        idTo: w.id_to,
        rows: w.rows,
        bytes: w.bytes,
        firstId: w.first_id,
        lastId: w.last_id,
        sha256: w.sha256,
        crc32: w.crc32,
      })),
  };
}

// ---- verification --------------------------------------------------------

function makePinnedReader(job) {
  if (job.strategy === 'snapshot') {
    const p = snapshotPath(ws, job.id);
    if (!fs.existsSync(p)) return null;
    return new SnapshotReader(p);
  }
  const src = new SourceStore(ws.sourcePath, { readonly: true });
  const reader = new ReplayReader(src.db, job.pinned_version);
  reader.close = () => {
    try {
      src.close();
    } catch {
      /* ignore */
    }
  };
  return reader;
}

async function runVerification(job, { mode = 'quick', indices = null } = {}) {
  const startedAt = nowMs();
  const deep = mode === 'deep';
  const windows = ws.state.getWindows(job.id);
  const selected = indices
    ? windows.filter((w) => indices.includes(w.idx))
    : windows.filter((w) => w.status === 'committed');
  if (!selected.length) {
    throw Object.assign(new Error('no exported parts to verify'), { statusCode: 400 });
  }
  const owner = `verify-${process.pid}-${crypto.randomBytes(3).toString('hex')}`;
  // A verification reads the parts and the pin: hold a download lease so the
  // collector cannot delete the files mid-check.
  const lease = ws.state.acquireLease(job.id, owner, 'download', 120_000);
  if (!lease.ok) {
    throw Object.assign(new Error(`verification blocked: ${lease.blocker.purpose} lease`), {
      statusCode: 409,
    });
  }
  let reader = null;
  try {
    reader = makePinnedReader(job);
    const results = [];
    for (const win of selected) {
      const p = partPath(job.id, win);
      let expectedRows = null;
      if (reader) {
        expectedRows =
          job.strategy === 'snapshot'
            ? Number(
                reader.db
                  .prepare('SELECT COUNT(*) AS n FROM rows WHERE id >= ? AND id <= ?')
                  .get(win.id_from, win.id_to).n
              )
            : ws.source.replayCount(job.pinned_version, win.id_from, win.id_to);
      }
      const result = await verifyWindow({
        job,
        win,
        reader,
        partPath: p,
        deep: deep && !!reader,
        expectedRows,
      });
      ws.state.setWindowVerification(
        job.id,
        win.idx,
        result.ok,
        result.failures.map((f) => f.message).join('; ') || null
      );
      results.push({
        idx: win.idx,
        file: win.file,
        range: [win.id_from, win.id_to],
        ok: result.ok,
        checked: result.checked,
        bytes: result.bytes,
        sha256: result.sha256,
        failures: result.failures,
        failureCount: result.failureCount ?? result.failures.length,
        ms: result.ms,
      });
    }
    const checked = results.reduce((s, r) => s + r.checked, 0);
    const failed = results.filter((r) => !r.ok);
    ws.state.recordVerification({
      jobId: job.id,
      mode: deep ? 'deep' : 'quick',
      ok: failed.length === 0,
      checked,
      failed: failed.length,
      startedAt,
      detail: results,
    });
    ws.state.event(
      'verify',
      `${deep ? 'deep' : 'quick'} verification ${failed.length ? 'FAILED' : 'passed'}: ` +
        `${results.length} part(s), ${checked.toLocaleString()} rows`,
      job.id
    );
    return {
      jobId: job.id,
      mode: deep ? 'deep' : 'quick',
      ok: failed.length === 0,
      parts: results.length,
      checkedRows: checked,
      failedParts: failed.length,
      elapsedMs: nowMs() - startedAt,
      deep,
      pinAvailable: !!reader,
      results,
    };
  } finally {
    if (reader) reader.close?.();
    ws.state.releaseLease(job.id, owner);
  }
}

// ---- downloads -----------------------------------------------------------

function acquireDownloadLease(jobId) {
  const owner = `dl-${process.pid}-${crypto.randomBytes(3).toString('hex')}`;
  const lease = ws.state.acquireLease(jobId, owner, 'download', 120_000);
  if (!lease.ok) {
    throw Object.assign(
      new Error(`download blocked: ${lease.blocker.purpose} holds the job`),
      { statusCode: 409 }
    );
  }
  // Bytes on the wire keep the job alive: extend retention while this stream runs.
  ws.touchRetention(jobId, 120_000);
  return owner;
}

async function downloadJob(req, res, jobId) {
  const job = ws.state.getJob(jobId);
  if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
  if (job.status === 'expired') {
    throw Object.assign(
      new Error('export expired: its retention window passed and files were removed'),
      { statusCode: 410 }
    );
  }
  const windows = ws.state.getWindows(jobId);
  const committed = windows
    .filter((w) => w.status === 'committed')
    .sort((a, b) => a.idx - b.idx);
  if (!committed.length) {
    throw Object.assign(new Error('no committed parts yet'), { statusCode: 409 });
  }
  const owner = acquireDownloadLease(jobId);
  const renew = setInterval(() => ws.state.renewLease(jobId, owner, 120_000), 20_000);
  renew.unref?.();

  const manifest = buildManifest(job, windows);
  const manifestBuf = Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`);
  try {
    res.writeHead(200, {
      'content-type': 'application/zip',
      'content-disposition': `attachment; filename="${jobId}.zip"`,
      'cache-control': 'no-store',
      'x-export-version': String(job.pinned_version),
      'x-export-parts': String(committed.length),
      'x-export-strategy': job.strategy,
    });
    const zip = new ZipStream(res, { mtime: new Date(job.created_at) });
    await zip.addBuffer('manifest.json', manifestBuf);
    for (const w of committed) {
      const p = partPath(jobId, w);
      if (!p || !fs.existsSync(p)) {
        throw Object.assign(new Error(`part ${w.file} vanished during download`), {
          statusCode: 410,
        });
      }
      const size = fs.statSync(p).size;
      if (size !== w.bytes) throw new Error(`part ${w.file} no longer matches its commit record`);
      await zip.addStream(w.file, fs.createReadStream(p), { crc32: w.crc32, size });
    }
    await zip.finish();
    res.end();
    ws.state.event(
      'download',
      `${job.name}: streamed ${committed.length} part(s) + manifest as ZIP`,
      jobId
    );
  } catch (err) {
    if (!res.headersSent) throw err;
    res.destroy(err);
  } finally {
    clearInterval(renew);
    ws.state.releaseLease(jobId, owner);
  }
}

async function downloadPart(req, res, jobId, idx) {
  const job = ws.state.getJob(jobId);
  if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
  if (job.status === 'expired') {
    throw Object.assign(new Error('export expired'), { statusCode: 410 });
  }
  const win = ws.state.getWindow(jobId, idx);
  if (!win || win.status !== 'committed' || !win.file) {
    throw Object.assign(new Error('part is not committed'), { statusCode: 404 });
  }
  const p = partPath(jobId, win);
  if (!fs.existsSync(p)) throw Object.assign(new Error('part file missing'), { statusCode: 410 });

  const owner = acquireDownloadLease(jobId);
  const size = fs.statSync(p).size;
  res.writeHead(200, {
    'content-type': 'application/x-ndjson; charset=utf-8',
    'content-length': size,
    'content-disposition': `attachment; filename="${win.file}"`,
    'x-part-sha256': win.sha256 || '',
    'x-part-crc32': win.crc32 != null ? String(win.crc32) : '',
    'x-part-rows': String(win.rows),
    'x-part-range': `${win.id_from}-${win.id_to}`,
    'x-export-version': String(job.pinned_version),
    'cache-control': 'no-store',
  });
  const stream = fs.createReadStream(p);
  const done = () => ws.state.releaseLease(jobId, owner);
  stream.on('end', () => {
    done();
    ws.state.event(
      'download',
      `part ${win.file} (ids ${win.id_from}-${win.id_to}, ${win.rows} rows) streamed`,
      jobId
    );
  });
  stream.on('error', () => {
    done();
    res.destroy();
  });
  res.on('close', done);
  stream.pipe(res);
}

// ---- static --------------------------------------------------------------

function serveStatic(pathname, res) {
  const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const target = path.resolve(PUBLIC_DIR, rel);
  if (!target.startsWith(PUBLIC_DIR + path.sep) && target !== PUBLIC_DIR) {
    res.writeHead(403).end('forbidden');
    return;
  }
  fs.stat(target, (err, stat) => {
    if (err || !stat.isFile()) {
      res.writeHead(404, { 'content-type': 'text/plain' }).end('not found');
      return;
    }
    const type = MIME[path.extname(target)] || 'application/octet-stream';
    res.writeHead(200, { 'content-type': type, 'cache-control': 'no-cache' });
    fs.createReadStream(target).pipe(res);
  });
}

// ---- router --------------------------------------------------------------

async function route(req, res) {
  const url = new URL(req.url, `http://${req.headers.host || HOST}`);
  const { pathname } = url;
  const method = req.method;

  if (!pathname.startsWith('/api/')) return serveStatic(url.pathname, res);

  if (pathname === '/api/overview' && method === 'GET') {
    const jobs = ws.state.listJobs(15).map((j) => ws.serializeJob(j));
    return sendJson(res, 200, {
      source: ws.source.stats(),
      storage: ws.storage(),
      jobs,
      oldestActivePin: ws.oldestActivePin(),
      pinFloors: ws.pinFloors(),
      events: ws.state.listEvents(40),
      probe: ws.state.latestProbe(),
      gcPlan: collector.plan(),
      leases: ws.state.allLiveLeases(),
      recovery: [...ws._running.keys()],
      now: nowMs(),
    });
  }

  if (pathname === '/api/source/generate' && method === 'POST') {
    const body = await readJsonBody(req);
    const out = await ws.generateSource({
      rows: Math.max(0, Math.min(500_000, Number(body.rows) || 12000)),
      payloadBytes: Math.max(16, Math.min(8192, Number(body.payloadBytes) || 420)),
      seed: Number(body.seed) || 7,
      chunk: 500,
    });
    return sendJson(res, 200, { ok: true, ...out });
  }

  if (pathname === '/api/source/mutate' && method === 'POST') {
    const body = await readJsonBody(req);
    const stats = ws.source.stats();
    const out = ws.source.mutateBatch({
      updates: Math.max(0, Math.min(20_000, Number(body.updates) || 0)),
      deletes: Math.max(0, Math.min(20_000, Number(body.deletes) || 0)),
      inserts: Math.max(0, Math.min(20_000, Number(body.inserts) || 0)),
      payloadBytes: Number(body.payloadBytes) || stats.payloadBytes || 420,
    });
    ws.state.event(
      'source',
      `source edited: +${out.inserts} inserted, ${out.updates} corrected, ${out.deletes} deleted ` +
        `(now v${ws.source.topVersion().version})`
    );
    return sendJson(res, 200, { ok: true, ...out, topVersion: ws.source.topVersion() });
  }

  if (pathname === '/api/source/prune' && method === 'POST') {
    const body = await readJsonBody(req);
    const top = ws.source.topVersion();
    const oldest = ws.oldestActivePin();
    const requested = Number(body.boundary) || top.version;
    // History a live replay pin still needs may never be pruned.
    const boundary = Math.min(requested, oldest == null ? top.version + 1 : oldest);
    if (boundary <= 1) {
      return sendJson(res, 409, {
        ok: false,
        reason:
          oldest == null
            ? 'change log is empty'
            : `the oldest live replay pin is v${oldest}; pruning would break its resume`,
        oldestActivePin: oldest,
      });
    }
    const removed = ws.source.pruneChanges(boundary);
    ws.state.event(
      'prune',
      `pruned ${removed.toLocaleString()} superseded change-log entries below v${boundary}` +
        (oldest != null ? ` (oldest live replay pin v${oldest})` : '')
    );
    return sendJson(res, 200, {
      ok: true,
      boundary,
      requested,
      removed,
      oldestActivePin: oldest,
      note:
        removed === 0
          ? 'nothing was older than the oldest live replay pin'
          : `kept the newest change per row below v${boundary}, so every version >= v${boundary} still replays exactly`,
    });
  }

  if (pathname === '/api/cost/probe' && method === 'POST') {
    const body = await readJsonBody(req);
    const probe = measureCosts(ws.source, {
      windowWidth: Math.max(10, Math.min(50_000, Number(body.windowWidth) || 1000)),
      sampleWindows: Math.max(1, Math.min(8, Number(body.sampleWindows) || 3)),
      probeDir: path.join(ws.dataDir, 'probe'),
    });
    if (probe.ok) {
      ws.state.recordProbe({
        pinnedVersion: probe.measured.version,
        rowsProbed: probe.measured.rowsSampled,
        exact: false,
        snapshotBytes: probe.strategies.snapshot.heldBytes,
        snapshotMs: probe.strategies.snapshot.pinMs,
        replayMs: probe.strategies.replay.resumeMsTotal,
        windowCount: probe.measured.windowCount,
        changeBytes: probe.strategies.replay.heldBytes,
        detail: probe,
      });
      ws.state.event(
        'probe',
        `measured pin cost at v${probe.measured.version}: snapshot holds ` +
          `${humanBytes(probe.strategies.snapshot.heldBytes)} vs replay retains ` +
          `${humanBytes(probe.strategies.replay.heldBytes)} of history`
      );
    }
    return sendJson(res, 200, probe);
  }

  if (pathname === '/api/jobs' && method === 'GET') {
    return sendJson(res, 200, {
      jobs: ws.state.listJobs(25).map((j) => ws.serializeJob(j)),
    });
  }

  if (pathname === '/api/jobs' && method === 'POST') {
    const body = await readJsonBody(req);
    const job = await ws.createJob({
      strategy: body.strategy === 'replay' ? 'replay' : 'snapshot',
      windowWidth: Number(body.windowWidth) || 1000,
      workers: Number(body.workers) || 2,
      retentionSec: Number(body.retentionSec) || 900,
      name: body.name || null,
    });
    if (body.autoStart !== false) ws.start(job.id);
    return sendJson(res, 201, { job: ws.serializeJob(ws.state.getJob(job.id), { full: true }) });
  }

  let m;

  if ((m = pathname.match(/^\/api\/jobs\/([^/]+)$/)) && method === 'GET') {
    const job = ws.state.getJob(m[1]);
    if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
    return sendJson(res, 200, {
      job: ws.serializeJob(job, { full: true }),
      verifications: ws.state.listVerifications(job.id),
      leases: ws.state.liveLeases(job.id),
      isRunning: ws.isRunning(job.id),
    });
  }

  if ((m = pathname.match(/^\/api\/jobs\/([^/]+)\/(start|pause|resume|cancel)$/))) {
    const [, id, action] = m;
    if (method !== 'POST') throw Object.assign(new Error('method not allowed'), { statusCode: 405 });
    let job;
    if (action === 'pause') job = ws.pause(id);
    else if (action === 'resume') job = ws.resume(id);
    else if (action === 'cancel') job = ws.cancel(id);
    else {
      ws.start(id);
      job = ws.state.getJob(id);
    }
    return sendJson(res, 200, { job: ws.serializeJob(job, { full: true }) });
  }

  if ((m = pathname.match(/^\/api\/jobs\/([^/]+)\/revise$/)) && method === 'POST') {
    const [, id] = m;
    const body = await readJsonBody(req);
    let out;
    if (body.action === 'split') {
      out = reviser.split(id, intParam(body.idx, 'idx'), Number(body.parts) || 2);
    } else if (body.action === 'merge') {
      out = reviser.merge(id, intParam(body.idx, 'idx'), Number(body.count) || 2);
    } else if (body.action === 'reissue') {
      out = reviser.reissue(
        id,
        intParam(body.from, 'from'),
        body.to == null ? null : intParam(body.to, 'to')
      );
    } else if (body.action === 'retarget') {
      out = reviser.retarget(
        id,
        intParam(body.idx, 'idx'),
        intParam(body.idFrom, 'idFrom'),
        intParam(body.idTo, 'idTo')
      );
    } else {
      throw Object.assign(new Error('unknown revision action'), { statusCode: 400 });
    }
    return sendJson(res, 200, {
      job: ws.serializeJob(out.job, { full: true }),
      removedFiles: out.removedFiles ?? 0,
      removedBytes: out.removedBytes ?? 0,
    });
  }

  if ((m = pathname.match(/^\/api\/jobs\/([^/]+)\/verify$/)) && method === 'POST') {
    const job = ws.state.getJob(m[1]);
    if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
    const body = await readJsonBody(req);
    const indices = Array.isArray(body.indices) ? body.indices.map(Number) : null;
    const result = await runVerification(job, {
      mode: body.mode === 'deep' ? 'deep' : 'quick',
      indices,
    });
    return sendJson(res, 200, result);
  }

  if ((m = pathname.match(/^\/api\/jobs\/([^/]+)\/download$/)) && method === 'GET') {
    return downloadJob(req, res, m[1]);
  }

  if ((m = pathname.match(/^\/api\/jobs\/([^/]+)\/parts\/(\d+)$/)) && method === 'GET') {
    return downloadPart(req, res, m[1], intParam(m[2], 'idx'));
  }

  if ((m = pathname.match(/^\/api\/jobs\/([^/]+)\/manifest$/)) && method === 'GET') {
    const job = ws.state.getJob(m[1]);
    if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
    return sendJson(res, 200, buildManifest(job, ws.state.getWindows(job.id)));
  }

  if (pathname === '/api/gc/plan' && method === 'GET') {
    return sendJson(res, 200, { plan: collector.plan(), now: nowMs() });
  }

  if (pathname === '/api/gc/sweep' && method === 'POST') {
    return sendJson(res, 200, { results: collector.sweep() });
  }

  if ((m = pathname.match(/^\/api\/jobs\/([^/]+)\/collect$/)) && method === 'POST') {
    const body = await readJsonBody(req);
    const result = collector.collect(m[1], {
      force: !!body.force,
      reason: body.reason || 'manual',
    });
    if (!result.ok) {
      return sendJson(res, result.code === 'not-found' ? 404 : 409, result);
    }
    return sendJson(res, 200, result);
  }

  if ((m = pathname.match(/^\/api\/jobs\/([^/]+)\/retention$/)) && method === 'POST') {
    const job = ws.state.getJob(m[1]);
    if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
    const body = await readJsonBody(req);
    const sec = Math.max(5, Math.min(86_400, Number(body.seconds) || job.retention_sec));
    ws.state.updateJob(job.id, {
      retention_sec: sec,
      expires_at: nowMs() + sec * 1000,
    });
    ws.state.event('retention', `${job.name}: retention set to ${sec}s from now`, job.id);
    return sendJson(res, 200, { job: ws.serializeJob(ws.state.getJob(job.id)) });
  }

  throw Object.assign(new Error('not found'), { statusCode: 404 });
}

// ---- startup -------------------------------------------------------------

const server = http.createServer((req, res) => {
  route(req, res).catch((err) => {
    if (res.headersSent) res.destroy(err);
    else sendError(res, err);
  });
});

async function ensureSource() {
  const stats = ws.source.stats();
  if (!stats.seeded) {
    await ws.generateSource({ rows: 12000, payloadBytes: 420, seed: 7, chunk: 500 });
  }
}

if (require.main === module) {
  server.listen(PORT, HOST, async () => {
    await ensureSource();
    const s = ws.source.stats();
    console.log(`export console: http://${HOST}:${PORT}`);
    console.log(
      `source: ${s.liveRows.toLocaleString()} live rows, ` +
        `${s.changeCount.toLocaleString()} logged changes (v${s.changeSeq})`
    );
  });

  const shutdown = () => {
    server.close(() => {
      ws.close();
      process.exit(0);
    });
    setTimeout(() => process.exit(0), 1500).unref();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

module.exports = { server, ws, route, ensureSource };
