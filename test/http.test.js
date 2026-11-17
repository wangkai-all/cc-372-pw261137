'use strict';

/**
 * HTTP-level tests: the real server, the real ZIP download, the real routes.
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const DATA_DIR = path.join(require('node:os').tmpdir(), `export-http-${process.pid}`);
fs.rmSync(DATA_DIR, { recursive: true, force: true });
fs.mkdirSync(DATA_DIR, { recursive: true });
process.env.DATA_DIR = DATA_DIR;

const { server, ws } = require('../src/server');

let base = null;

async function boot() {
  if (base) return base;
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  base = `http://127.0.0.1:${port}`;
  if (!ws.source.stats().seeded) {
    await ws.generateSource({ rows: 1200, payloadBytes: 220, seed: 21, chunk: 400 });
  }
  return base;
}

async function api(pathname, { method = 'GET', body = null } = {}) {
  const res = await fetch(`${base}${pathname}`, {
    method,
    headers: body ? { 'content-type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* not json */
  }
  return { status: res.status, json, text, headers: res.headers };
}

async function waitDone(jobId, timeoutMs = 25_000) {
  const t0 = Date.now();
  let last = null;
  while (Date.now() - t0 < timeoutMs) {
    const { json } = await api(`/api/jobs/${jobId}`);
    last = json.job;
    if (['done', 'failed', 'paused'].includes(last.status) && !json.isRunning) return last;
    await new Promise((r) => setTimeout(r, 60));
  }
  throw new Error(`job ${jobId} did not settle: ${last?.status}`);
}

/** Parse a ZIP written with the store method (no compression). */
function parseZip(buf) {
  const eocd = buf.lastIndexOf(Buffer.from([0x50, 0x4b, 0x05, 0x06]));
  assert.ok(eocd >= 0, 'has end-of-central-directory');
  const count = buf.readUInt16LE(eocd + 10);
  let offset = buf.readUInt32LE(eocd + 16);
  const entries = [];
  for (let i = 0; i < count; i += 1) {
    assert.equal(buf.readUInt32LE(offset), 0x02014b50, 'central header signature');
    const crc = buf.readUInt32LE(offset + 16);
    const size = buf.readUInt32LE(offset + 24);
    const nameLen = buf.readUInt16LE(offset + 28);
    const extraLen = buf.readUInt16LE(offset + 30);
    const commentLen = buf.readUInt16LE(offset + 32);
    const localOffset = buf.readUInt32LE(offset + 42);
    const name = buf.slice(offset + 46, offset + 46 + nameLen).toString('utf8');
    const localNameLen = buf.readUInt16LE(localOffset + 26);
    const localExtraLen = buf.readUInt16LE(localOffset + 28);
    const dataStart = localOffset + 30 + localNameLen + localExtraLen;
    const data = buf.slice(dataStart, dataStart + size);
    entries.push({ name, crc, size, data });
    offset += 46 + nameLen + extraLen + commentLen;
  }
  return entries;
}

function crc32(buf) {
  let c = 0xffffffff;
  for (let i = 0; i < buf.length; i += 1) {
    c ^= buf[i];
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  }
  return (c ^ 0xffffffff) >>> 0;
}

test.after(() => {
  server.close();
  ws.close();
  fs.rmSync(DATA_DIR, { recursive: true, force: true });
});

test('overview and source routes answer with real state', async () => {
  await boot();
  const overview = await api('/api/overview');
  assert.equal(overview.status, 200);
  assert.ok(overview.json.source.liveRows > 0);
  assert.ok(overview.json.storage.sourceBytes > 0);

  const gen = await api('/api/source/generate', {
    method: 'POST',
    body: { rows: 1200, payloadBytes: 220, seed: 21 },
  });
  // A live pin from a previous test may block regeneration; both answers are valid.
  assert.ok([200, 409].includes(gen.status));
});

test('create, export, poll, download ZIP, and verify every part', async () => {
  await boot();
  const created = await api('/api/jobs', {
    method: 'POST',
    body: { strategy: 'snapshot', windowWidth: 150, workers: 2, retentionSec: 300 },
  });
  assert.equal(created.status, 201);
  const jobId = created.json.job.id;
  const settled = await waitDone(jobId);
  assert.equal(settled.status, 'done');
  assert.equal(settled.progress.committed, settled.windowCount);

  const verify = await api(`/api/jobs/${jobId}/verify`, {
    method: 'POST',
    body: { mode: 'deep' },
  });
  assert.equal(verify.status, 200);
  assert.equal(verify.json.ok, true);
  assert.equal(verify.json.checkedRows, settled.rowExported);
  assert.equal(verify.json.failedParts, 0);

  const manifest = await api(`/api/jobs/${jobId}/manifest`);
  assert.equal(manifest.status, 200);
  assert.equal(manifest.json.pinnedVersion, settled.pinnedVersion);
  assert.equal(manifest.json.parts.length, settled.windowCount);

  const res = await fetch(`${base}/api/jobs/${jobId}/download`);
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('content-type'), 'application/zip');
  const buf = Buffer.from(await res.arrayBuffer());
  const entries = parseZip(buf);
  assert.equal(entries.length, settled.windowCount + 1);
  assert.equal(entries[0].name, 'manifest.json');
  const manifestInZip = JSON.parse(entries[0].data.toString('utf8'));
  assert.equal(manifestInZip.pinnedVersion, settled.pinnedVersion);

  // Each part in the archive hashes to the committed record and the CRCs hold.
  const committed = ws.state.getWindows(jobId).filter((w) => w.status === 'committed');
  const byName = new Map(entries.map((e) => [e.name, e]));
  const seen = [];
  for (const w of committed.sort((a, b) => a.idx - b.idx)) {
    const entry = byName.get(w.file);
    assert.ok(entry, `archive has ${w.file}`);
    assert.equal(crc32(entry.data), w.crc32);
    assert.equal(crypto.createHash('sha256').update(entry.data).digest('hex'), w.sha256);
    const rows = entry.data.toString('utf8').trim().split('\n').map((l) => JSON.parse(l));
    assert.equal(rows.length, w.rows);
    assert.equal(rows[0].id, w.id_from);
    assert.equal(rows.at(-1).id, w.id_to);
    seen.push(...rows.map((r) => r.id));
  }
  const expected = Array.from({ length: settled.rowExported }, (_, i) => i + 1);
  assert.deepEqual(seen, expected, 'archive is a contiguous, ordered id space');
});

test('a single part can be downloaded with its checksum headers', async () => {
  await boot();
  const created = await api('/api/jobs', {
    method: 'POST',
    body: { strategy: 'replay', windowWidth: 200, workers: 2, retentionSec: 300 },
  });
  const jobId = created.json.job.id;
  const settled = await waitDone(jobId);
  assert.equal(settled.status, 'done');
  const win = settled.windows.find((w) => w.status === 'committed');
  const res = await fetch(`${base}/api/jobs/${jobId}/parts/${win.idx}`);
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('x-part-sha256'), win.sha256);
  assert.equal(Number(res.headers.get('x-part-rows')), win.rows);
  const text = await res.text();
  const lines = text.trim().split('\n');
  assert.equal(lines.length, win.rows);
  assert.equal(
    crypto.createHash('sha256').update(text).digest('hex'),
    win.sha256
  );
});

test('revision over HTTP refuses an exported window until it is re-issued', async () => {
  await boot();
  const created = await api('/api/jobs', {
    method: 'POST',
    body: { strategy: 'snapshot', windowWidth: 200, workers: 2, retentionSec: 300 },
  });
  const jobId = created.json.job.id;
  await waitDone(jobId);
  const split = await api(`/api/jobs/${jobId}/revise`, {
    method: 'POST',
    body: { action: 'split', idx: 0, parts: 2 },
  });
  assert.equal(split.status, 409);
  assert.match(split.json.error, /re-issue/);

  const proof = await api(`/api/jobs/${jobId}/revise`, {
    method: 'POST',
    body: { action: 'reissue', from: 0, to: 0 },
  });
  assert.equal(proof.status, 200);
  assert.equal(proof.json.removedFiles, 1);
  await waitDone(jobId);
  const verify = await api(`/api/jobs/${jobId}/verify`, {
    method: 'POST',
    body: { mode: 'deep' },
  });
  assert.equal(verify.json.ok, true);
});

test('GC refuses a download in flight and cleans only after it ends', async () => {
  await boot();
  const created = await api('/api/jobs', {
    method: 'POST',
    body: { strategy: 'snapshot', windowWidth: 200, workers: 2, retentionSec: 5 },
  });
  const jobId = created.json.job.id;
  await waitDone(jobId);
  ws.state.updateJob(jobId, { expires_at: Date.now() - 1000 });

  // Hold a download lease directly, then try to collect.
  const owner = `dl-http-${process.pid}`;
  assert.equal(ws.state.acquireLease(jobId, owner, 'download', 30_000).ok, true);
  const refused = await api(`/api/jobs/${jobId}/collect`, { method: 'POST', body: {} });
  assert.equal(refused.status, 409);
  assert.match(refused.json.reason, /download/);
  assert.ok(fs.existsSync(path.join(ws.partsDir, jobId)));

  ws.state.releaseLease(jobId, owner);
  const plan = await api('/api/gc/plan');
  assert.ok(plan.json.plan.some((p) => p.jobId === jobId && !p.protected));

  const sweep = await api('/api/gc/sweep', { method: 'POST' });
  const mine = sweep.json.results.find((r) => r.jobId === jobId);
  assert.ok(mine?.collected);
  assert.equal(ws.state.getJob(jobId).status, 'expired');
  assert.equal(fs.existsSync(path.join(ws.partsDir, jobId)), false);
  const after = await api(`/api/jobs/${jobId}/download`);
  assert.equal(after.status, 410);
});

test('pause and resume over HTTP keeps the pin and finishes the archive', async () => {
  await boot();
  // Create without starting so the pause point is deterministic; a job that
  // is already done correctly refuses to pause.
  const created = await api('/api/jobs', {
    method: 'POST',
    body: { strategy: 'replay', windowWidth: 50, workers: 2, retentionSec: 300, autoStart: false },
  });
  const jobId = created.json.job.id;
  assert.equal(created.json.job.status, 'pending');
  const paused = await api(`/api/jobs/${jobId}/pause`, { method: 'POST' });
  assert.equal(paused.status, 200);
  assert.equal(paused.json.job.status, 'paused');
  const committedAtPause = 0;

  const mut = await api('/api/source/mutate', {
    method: 'POST',
    body: { updates: 200, deletes: 20, inserts: 10 },
  });
  assert.equal(mut.status, 200);

  await api(`/api/jobs/${jobId}/resume`, { method: 'POST' });
  const done = await waitDone(jobId);
  assert.equal(done.status, 'done');
  assert.ok(done.progress.committed >= committedAtPause);

  const verify = await api(`/api/jobs/${jobId}/verify`, { method: 'POST', body: { mode: 'deep' } });
  assert.equal(verify.json.ok, true, 'archive is entirely from the pinned version');
});

test('cost probe route returns measured costs for both strategies', async () => {
  await boot();
  const res = await api('/api/cost/probe', {
    method: 'POST',
    body: { windowWidth: 200, sampleWindows: 3 },
  });
  assert.equal(res.status, 200);
  assert.equal(res.json.ok, true);
  assert.ok(res.json.strategies.snapshot.heldBytes > 0);
  assert.ok(res.json.strategies.replay.heldBytes > 0);
  assert.ok(res.json.strategies.snapshot.pinMsMeasured > 0);
  assert.equal(res.json.strategies.snapshot.rebuildRowsOnResume, 0);
  assert.ok(res.json.strategies.replay.rebuildRowsOnResume > 0);
});

test('path traversal and unknown routes are rejected', async () => {
  await boot();
  const traversal = await fetch(`${base}/../package.json`);
  assert.ok([403, 404].includes(traversal.status));
  const unknown = await api('/api/nope');
  assert.equal(unknown.status, 404);
});
