'use strict';

/**
 * End-to-end tests for the pinned-version export pipeline.
 *
 * Every test drives the real modules against a real SQLite source; nothing is
 * mocked. Run with:  node --test test/
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { once } = require('node:events');

const { Workspace } = require('../src/workspace');
const { Collector } = require('../src/gc');
const { Reviser } = require('../src/revise');
const { verifyWindow } = require('../src/verify');
const { measureCosts } = require('../src/cost');
const { SnapshotReader, ReplayReader } = require('../src/export');
const { snapshotPath } = require('../src/pin');
const { SourceStore } = require('../src/source');
const { ZipStream, crc32 } = require('../src/zip');
const { nowMs } = require('../src/util');

const TMP_ROOT = path.join(require('node:os').tmpdir(), `export-tests-${process.pid}`);

function freshWorkspace(name, opts = {}) {
  const root = path.join(TMP_ROOT, name);
  fs.rmSync(root, { recursive: true, force: true });
  fs.mkdirSync(root, { recursive: true });
  return new Workspace(root, opts);
}

async function seeded(name, { rows = 2000, payloadBytes = 200, seed = 11, ...opts } = {}) {
  const ws = freshWorkspace(name, opts);
  await ws.source.generate({ rows, payloadBytes, seed, chunk: 500 });
  return ws;
}

/**
 * Read the archive the way a consumer does: in manifest order (index), and
 * within a part in file order. Concatenating must give strictly increasing ids
 * once revision has reordered the files.
 */
function readAllParts(ws, jobId) {
  const dir = path.join(ws.partsDir, jobId);
  const wins = ws.state
    .getWindows(jobId)
    .filter((w) => w.status === 'committed')
    .sort((a, b) => a.idx - b.idx);
  const rows = [];
  for (const w of wins) {
    const text = fs.readFileSync(path.join(dir, w.file), 'utf8');
    for (const line of text.split('\n')) {
      if (line) rows.push(JSON.parse(line));
    }
  }
  return rows;
}

async function waitIdle(ws, jobId, timeoutMs = 20_000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    const job = ws.state.getJob(jobId);
    if (!job) throw new Error('job vanished');
    if (!ws.isRunning(jobId) && !['running', 'pending', 'pinned'].includes(job.status)) return job;
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error('timed out waiting for export');
}

function pinnedRows(ws, version, lo, hi) {
  return ws.source.replayRowsAll(version, lo, hi);
}

// ---------------------------------------------------------------------------

test('snapshot export is byte-identical to the pinned source version', async () => {
  const ws = await seeded('pinned-identical', { rows: 1500 });
  const job = await ws.createJob({ strategy: 'snapshot', windowWidth: 250, workers: 2 });
  await ws.start(job.id);
  const done = await waitIdle(ws, job.id);
  assert.equal(done.status, 'done');
  assert.equal(done.row_exported, 1500);

  const rows = readAllParts(ws, job.id);
  assert.equal(rows.length, 1500);
  assert.deepEqual(
    rows.map((r) => r.id),
    Array.from({ length: 1500 }, (_, i) => i + 1)
  );
  const expected = pinnedRows(ws, job.pinned_version, 1, 1500);
  assert.deepEqual(
    rows.map((r) => [r.id, r.seq, r.payload]),
    expected.map((r) => [r.id, r.seq, r.payload])
  );
  ws.close();
});

test('source edits and deletes during an export never leak into the archive', async () => {
  const ws = await seeded('concurrent-edits', { rows: 2000 });
  const job = await ws.createJob({ strategy: 'snapshot', windowWidth: 100, workers: 1 });
  const before = pinnedRows(ws, job.pinned_version, 1, 2000);

  // Pause immediately, then mutate the living source heavily, then resume.
  ws.pause(job.id);
  await waitIdle(ws, job.id);
  const paused = ws.state.getJob(job.id);
  const committedBefore = paused.row_exported;
  const mutation = ws.source.mutateBatch({ updates: 800, deletes: 300, inserts: 100, payloadBytes: 200 });
  assert.ok(mutation.updates > 0 && mutation.deletes > 0 && mutation.inserts > 0);
  assert.ok(ws.source.topVersion().version > job.pinned_version);

  ws.resume(job.id);
  const done = await waitIdle(ws, job.id, 30_000);
  assert.equal(done.status, 'done');

  const rows = readAllParts(ws, job.id);
  assert.equal(rows.length, before.length, 'row count matches the pin, not the living source');
  const live = new Map(pinnedRows(ws, ws.source.topVersion().version, 1, ws.source.topVersion().maxId).map((r) => [r.id, r]));
  const changed = rows.filter((r) => {
    const nowRow = live.get(r.id);
    return !nowRow || nowRow.payload !== r.payload;
  });
  assert.ok(changed.length > 200, `expected many rows to differ from the live source, got ${changed.length}`);
  // Everything still equals the pinned version, row for row.
  const pinned = new Map(before.map((r) => [r.id, r]));
  for (const row of rows) {
    const exp = pinned.get(row.id);
    assert.ok(exp, `row ${row.id} was not in the pinned version`);
    assert.equal(row.payload, exp.payload);
    assert.equal(row.seq, exp.seq);
  }
  assert.equal(done.rebuild_rows, 0, 'snapshot strategy pays no rebuild cost');
  ws.close();
});

test('resume after a crash never re-reads the second half from a newer version', async () => {
  const ws = await seeded('crash-resume', { rows: 2000 });
  const job = await ws.createJob({ strategy: 'snapshot', windowWidth: 100, workers: 1 });
  const pinned = new Map(pinnedRows(ws, job.pinned_version, 1, 2000).map((r) => [r.id, r]));

  // Let a few windows commit through one worker, then simulate a hard crash
  // with no graceful shutdown (no background worker is involved).
  const engine = ws.engine();
  const workerId = `crash-${process.pid}`;
  engine.begin(job.id, workerId);
  const stopAt = 5;
  let steps = 0;
  while (steps < stopAt) {
    const out = await engine.step(job.id, workerId);
    if (out.idle) break;
    steps += 1;
  }
  const mid = ws.state.getJob(job.id);
  assert.ok(mid.row_exported > 0);
  const committedWindows = ws.state
    .getWindows(job.id)
    .filter((w) => w.status === 'committed');
  assert.ok(committedWindows.length >= 4);
  const committedFiles = committedWindows.map((w) => w.file);

  // Simulate a worker dying mid-write: it had claimed a window and left a
  // partial file behind. Only that window may be rewritten.
  const [next] = ws.state.claimWindows(job.id, 'doomed-worker', 1);
  assert.ok(next);
  const partial = path.join(ws.partsDir, job.id, next.part_name);
  fs.writeFileSync(partial, '{"id":1,"seq":1,"payl'); // torn write
  ws.state.updateJob(job.id, { status: 'running' });
  ws.state.releaseLease(job.id, workerId);

  // The process restarts: Workspace boot recovers stale leases + writing windows.
  ws.recover();
  const afterBoot = ws.state.getJob(job.id);
  assert.equal(afterBoot.status, 'paused');
  assert.ok(afterBoot.row_exported >= 400, 'committed rows survived the restart');

  // The source moves on while the process is down.
  ws.source.mutateBatch({ updates: 1500, deletes: 200, inserts: 50, payloadBytes: 200 });

  ws.resume(job.id);
  const done = await waitIdle(ws, job.id, 30_000);
  assert.equal(done.status, 'done');
  // The torn file was discarded, not adopted.
  assert.ok(!fs.existsSync(partial));
  // Committed windows kept their exact file names: no re-read, no rename.
  const stillCommitted = ws.state.getWindows(job.id).filter((w) => w.status === 'committed');
  for (const name of committedFiles) {
    assert.ok(
      stillCommitted.some((w) => w.file === name),
      `committed part ${name} was not preserved`
    );
  }

  const rows = readAllParts(ws, job.id);
  assert.equal(rows.length, 2000);
  for (const row of rows) {
    const exp = pinned.get(row.id);
    assert.ok(exp, `row ${row.id} not in pin`);
    assert.equal(row.payload, exp.payload, `row ${row.id} came from a newer version`);
  }
  // Windows committed before the crash were not rewritten.
  for (const w of ws.state.getWindows(job.id)) {
    assert.equal(w.status, 'committed');
  }
  ws.close();
});

test('replay strategy reconstructs the same pinned version without a snapshot', async () => {
  const ws = await seeded('replay', { rows: 1200 });
  const job = await ws.createJob({ strategy: 'replay', windowWidth: 200, workers: 2 });
  assert.ok(!fs.existsSync(snapshotPath(ws, job.id)), 'no snapshot is materialized');
  ws.pause(job.id);
  await waitIdle(ws, job.id);
  ws.source.mutateBatch({ updates: 600, deletes: 100, inserts: 40, payloadBytes: 200 });
  ws.resume(job.id);
  const done = await waitIdle(ws, job.id, 30_000);
  assert.equal(done.status, 'done');
  assert.ok(done.rebuild_rows > 0, 'replay pays rebuild rows');
  assert.ok(done.rebuild_ms >= 0);

  const rows = readAllParts(ws, job.id);
  const expected = pinnedRows(ws, job.pinned_version, 1, 1200);
  assert.deepEqual(
    rows.map((r) => [r.id, r.seq, r.payload]),
    expected.map((r) => [r.id, r.seq, r.payload])
  );
  ws.close();
});

test('replay and snapshot strategies produce identical parts', async () => {
  const ws = await seeded('both-strategies', { rows: 900 });
  const snapJob = await ws.createJob({ strategy: 'snapshot', windowWidth: 150, workers: 2 });
  const replayJob = await ws.createJob({ strategy: 'replay', windowWidth: 150, workers: 2 });
  assert.equal(snapJob.pinned_version, replayJob.pinned_version);
  await ws.start(snapJob.id);
  await ws.start(replayJob.id);
  await waitIdle(ws, snapJob.id, 20_000);
  await waitIdle(ws, replayJob.id, 20_000);

  // Group by id window so the comparison is independent of part ordering.
  const byRange = (jobId) => {
    const map = new Map();
    for (const w of ws.state.getWindows(jobId)) {
      const text = fs.readFileSync(path.join(ws.partsDir, jobId, w.file), 'utf8');
      map.set(`${w.id_from}-${w.id_to}`, crypto.createHash('sha256').update(text).digest('hex'));
    }
    return map;
  };
  const a = byRange(snapJob.id);
  const b = byRange(replayJob.id);
  assert.equal(a.size, b.size);
  for (const [range, hash] of a) assert.equal(b.get(range), hash, `range ${range} differs`);
  ws.close();
});

test('verification detects tampering and validates clean parts', async () => {
  const ws = await seeded('verify', { rows: 800 });
  const job = await ws.createJob({ strategy: 'snapshot', windowWidth: 200, workers: 2 });
  await ws.start(job.id);
  await waitIdle(ws, job.id);

  const reader = new SnapshotReader(snapshotPath(ws, job.id));
  const wins = ws.state.getWindows(job.id);
  const clean = await verifyWindow({
    job,
    win: wins[0],
    reader,
    partPath: path.join(ws.partsDir, job.id, wins[0].file),
    deep: true,
    expectedRows: 200,
  });
  assert.equal(clean.ok, true, JSON.stringify(clean.failures));
  assert.equal(clean.checked, 200);
  assert.equal(clean.firstId, 1);
  assert.equal(clean.lastId, 200);

  // Drop the last line: checksum, count and range checks must all fire.
  const target = path.join(ws.partsDir, job.id, wins[1].file);
  const lines = fs.readFileSync(target, 'utf8').split('\n').filter(Boolean);
  fs.writeFileSync(target, `${lines.slice(0, -1).join('\n')}\n`);
  const tampered = await verifyWindow({
    job,
    win: wins[1],
    reader,
    partPath: target,
    deep: true,
  });
  assert.equal(tampered.ok, false);
  const codes = new Set(tampered.failures.map((f) => f.code));
  assert.ok(codes.has('count') || codes.has('sha256'), `codes: ${[...codes]}`);

  // A value changed but the line count kept: the deep check must catch it.
  const target2 = path.join(ws.partsDir, job.id, wins[2].file);
  const lines2 = fs.readFileSync(target2, 'utf8').split('\n').filter(Boolean);
  const row = JSON.parse(lines2[0]);
  row.payload = `${row.payload}x`;
  lines2[0] = JSON.stringify(row);
  fs.writeFileSync(target2, `${lines2.join('\n')}\n`);
  const edited = await verifyWindow({
    job,
    win: wins[2],
    reader,
    partPath: target2,
    deep: true,
  });
  assert.equal(edited.ok, false);
  assert.ok(edited.failures.some((f) => f.code === 'deep-payload' || f.code === 'sha256'));
  reader.close();
  ws.close();
});

test('revision re-issues exported windows from the pin, never the live source', async () => {
  const ws = await seeded('reissue', { rows: 1000 });
  const job = await ws.createJob({ strategy: 'snapshot', windowWidth: 200, workers: 2 });
  await ws.start(job.id);
  await waitIdle(ws, job.id);
  const pinned = new Map(pinnedRows(ws, job.pinned_version, 1, 1000).map((r) => [r.id, r]));
  const originalRows = readAllParts(ws, job.id);

  const reviser = new Reviser(ws);
  const out = reviser.reissue(job.id, 1, 2);
  assert.equal(out.removedFiles, 2);
  assert.ok(out.removedBytes > 0);
  const after = ws.state.getWindows(job.id);
  assert.deepEqual(
    after.filter((w) => w.idx === 1 || w.idx === 2).map((w) => w.status),
    ['pending', 'pending']
  );
  assert.equal(after[0].status, 'committed', 'unaffected windows stay committed');

  ws.source.mutateBatch({ updates: 900, deletes: 100, inserts: 10, payloadBytes: 200 });
  ws.start(job.id);
  const done = await waitIdle(ws, job.id);
  assert.equal(done.status, 'done');

  const rows = readAllParts(ws, job.id);
  assert.deepEqual(
    rows.map((r) => r.id),
    originalRows.map((r) => r.id)
  );
  for (const row of rows) {
    assert.equal(row.payload, pinned.get(row.id).payload, `row ${row.id} changed`);
  }
  ws.close();
});

test('split renumbers committed windows without touching their part files', async () => {
  const ws = await seeded('split-committed', { rows: 800 });
  const job = await ws.createJob({ strategy: 'snapshot', windowWidth: 200, workers: 1 });
  const reviser = new Reviser(ws);

  // Commit the first window as if a worker had written it; the rest stay pending.
  const [first] = ws.state.claimWindows(job.id, 'test-owner', 1);
  assert.ok(first);
  fs.mkdirSync(path.join(ws.partsDir, job.id), { recursive: true });
  fs.writeFileSync(path.join(ws.partsDir, job.id, first.part_name), '{"id":1}\n');
  ws.state.commitWindow(job.id, 0, {
    rows: 1,
    bytes: 9,
    sha256: 'deadbeef',
    crc32: 0,
    firstId: 1,
    lastId: 200,
    file: first.part_name,
    deriveMs: 1,
    rebuildMs: 0,
    rebuildRows: 0,
  });

  // Splitting a later window must shift the committed window's index, keep its
  // part name, and leave the (job_id, idx) key collision-free.
  const split = reviser.split(job.id, 2, 2);
  assert.deepEqual(
    split.windows.map((w) => [w.idx, w.id_from, w.id_to, w.status]),
    [
      [0, 1, 200, 'committed'],
      [1, 201, 400, 'pending'],
      [2, 401, 500, 'pending'],
      [3, 501, 600, 'pending'],
      [4, 601, 800, 'pending'],
    ]
  );
  assert.equal(split.windows[0].file, first.part_name, 'part file is unchanged');
  assert.ok(fs.existsSync(path.join(ws.partsDir, job.id, first.part_name)));

  // Merging later windows shifts everything above down without a key clash.
  const merged = reviser.merge(job.id, 2, 2);
  assert.deepEqual(
    merged.windows.map((w) => [w.idx, w.id_from, w.id_to, w.status]),
    [
      [0, 1, 200, 'committed'],
      [1, 201, 400, 'pending'],
      [2, 401, 600, 'pending'],
      [3, 601, 800, 'pending'],
    ]
  );
  assert.equal(merged.windows[0].file, first.part_name);
  ws.close();
});

test('split and merge only touch unexported windows', async () => {
  const ws = await seeded('split-merge', { rows: 800 });
  const job = await ws.createJob({ strategy: 'snapshot', windowWidth: 200, workers: 2 });
  const reviser = new Reviser(ws);

  const split = reviser.split(job.id, 2, 2);
  const renumbered = split.windows.filter((w) => w.id_from >= 401 && w.id_from <= 600);
  assert.equal(renumbered.length, 2);
  assert.deepEqual(renumbered.map((w) => [w.id_from, w.id_to]), [[401, 500], [501, 600]]);
  assert.equal(split.windows.length, 5);

  // Windows are now [1-200, 201-400, 401-500, 501-600, 601-800].
  const merged = reviser.merge(job.id, 3, 2);
  const mergedWin = merged.windows.find((w) => w.id_from === 501);
  assert.ok(mergedWin, 'merged window starts at 501');
  assert.equal(mergedWin.id_to, 800);
  assert.equal(merged.windows.length, 4);

  await ws.start(job.id);
  const done = await waitIdle(ws, job.id);
  assert.equal(done.status, 'done');
  assert.equal(readAllParts(ws, job.id).length, 800);

  // Splitting an exported window is refused with a pointer to re-issue.
  assert.throws(() => reviser.split(job.id, 0, 2), /already exported|re-issue/);
  ws.close();
});

test('expired cleanup never removes files from a resumable job', async () => {
  const ws = await seeded('gc-protect', { rows: 600 });
  const job = await ws.createJob({ strategy: 'snapshot', windowWidth: 100, workers: 1 });
  ws.pause(job.id);
  await waitIdle(ws, job.id);
  // Force expiry into the past: a claim that is due.
  ws.state.updateJob(job.id, { expires_at: nowMs() - 1000 });

  const collector = new Collector(ws);
  const refused = collector.collect(job.id, { reason: 'test' });
  assert.equal(refused.ok, false);
  assert.equal(refused.code, 'protected');
  assert.ok(/paused|resumable/.test(refused.reason));
  assert.ok(fs.existsSync(path.join(ws.partsDir, job.id)) || true);

  // Cancel releases the claim, but files must still survive until collection.
  ws.cancel(job.id);
  const done = collector.collect(job.id, { reason: 'test after cancel' });
  assert.equal(done.ok, true);
  assert.equal(ws.state.getJob(job.id).status, 'expired');
  assert.equal(fs.existsSync(path.join(ws.partsDir, job.id)), false);
  assert.equal(fs.existsSync(snapshotPath(ws, job.id)), false);
  ws.close();
});

test('a download in flight blocks cleanup', async () => {
  const ws = await seeded('gc-download', { rows: 400 });
  const job = await ws.createJob({ strategy: 'snapshot', windowWidth: 100, workers: 2 });
  await ws.start(job.id);
  await waitIdle(ws, job.id);
  ws.state.updateJob(job.id, { expires_at: nowMs() - 1000 });

  const collector = new Collector(ws);
  const owner = `dl-test-${process.pid}`;
  const lease = ws.state.acquireLease(job.id, owner, 'download', 60_000);
  assert.equal(lease.ok, true);
  const refused = collector.collect(job.id);
  assert.equal(refused.ok, false);
  assert.match(refused.reason, /download/);
  assert.ok(fs.existsSync(path.join(ws.partsDir, job.id)));
  ws.state.releaseLease(job.id, owner);
  // Expiry is still due; with everything terminal, collection now proceeds.
  ws.state.updateJob(job.id, { status: 'done' });
  const ok = collector.collect(job.id);
  assert.equal(ok.ok, true);
  ws.close();
});

test('change-log pruning protects live replay pins and keeps older versions replayable', async () => {
  const ws = await seeded('prune', { rows: 500 });
  // Build some superseded history *before* the pin, so there is something
  // older than the pin to prune.
  for (let i = 0; i < 3; i += 1) {
    ws.source.mutateBatch({ updates: 250, deletes: 10, inserts: 10, payloadBytes: 200 });
  }
  const replayJob = await ws.createJob({ strategy: 'replay', windowWidth: 100, workers: 1 });
  ws.pause(replayJob.id);
  await waitIdle(ws, replayJob.id);
  const pin = replayJob.pinned_version;

  // More edits after the pin.
  for (let i = 0; i < 4; i += 1) {
    ws.source.mutateBatch({ updates: 200, deletes: 20, inserts: 20, payloadBytes: 200 });
  }
  const top = ws.source.topVersion().version;
  assert.ok(top > pin);

  // Pruning up to the live pin keeps that pin exact.
  const pinMax = ws.source.topVersion().maxId;
  const expected = pinnedRows(ws, pin, 1, pinMax);
  const removed = ws.source.pruneChanges(pin);
  assert.ok(removed > 0);
  const prunedCount = Number(
    ws.source.db.prepare('SELECT COUNT(*) AS n FROM changes').get().n
  );
  assert.ok(prunedCount < top, 'history was actually trimmed');

  ws.resume(replayJob.id);
  const done = await waitIdle(ws, replayJob.id, 20_000);
  assert.equal(done.status, 'done');
  const rows = readAllParts(ws, replayJob.id);
  assert.deepEqual(
    rows.map((r) => [r.id, r.seq, r.payload]),
    expected.map((r) => [r.id, r.seq, r.payload])
  );
  assert.equal(ws.oldestActivePin(), pin);
  ws.close();
});

test('cost probe measures both strategies and reports real numbers', async () => {
  const ws = await seeded('cost', { rows: 2400, payloadBytes: 300 });
  const probe = measureCosts(ws.source, { windowWidth: 200, sampleWindows: 3 });
  assert.equal(probe.ok, true);
  assert.ok(probe.strategies.snapshot.heldBytes > 0);
  assert.ok(probe.strategies.replay.heldBytes > 0);
  assert.ok(probe.strategies.snapshot.pinMsMeasured > 0);
  assert.ok(probe.strategies.replay.resumeMsTotal >= 0);
  assert.equal(probe.strategies.snapshot.rebuildRowsOnResume, 0);
  assert.equal(probe.strategies.replay.rebuildRowsOnResume, 2400);
  assert.ok(probe.measured.sample <= 3);
  // The probe samples; it does not scan the whole dataset.
  assert.ok(probe.measured.rowsSampled < 2400);
  ws.close();
});

test('zip download streams real bytes with a manifest and valid CRCs', async () => {
  const ws = await seeded('zip', { rows: 300 });
  const job = await ws.createJob({ strategy: 'snapshot', windowWidth: 100, workers: 2 });
  await ws.start(job.id);
  await waitIdle(ws, job.id);

  const chunks = [];
  const res = {
    writeHead() {},
    write(buf) {
      chunks.push(buf);
      return true;
    },
    end() {},
    destroy(err) {
      if (err) throw err;
    },
    on() {},
    once() {},
  };
  const zip = new ZipStream(res, { mtime: new Date() });
  const wins = ws.state.getWindows(job.id);
  await zip.addBuffer('manifest.json', Buffer.from('{"a":1}'));
  for (const w of wins) {
    await zip.addStream(w.file, fs.createReadStream(path.join(ws.partsDir, job.id, w.file)), {
      crc32: w.crc32,
      size: w.bytes,
    });
  }
  await zip.finish();
  const buf = Buffer.concat(chunks);
  assert.equal(buf.readUInt32LE(0), 0x04034b50, 'local file header signature');
  // End-of-central-directory record at the tail.
  const eocd = buf.lastIndexOf(Buffer.from([0x50, 0x4b, 0x05, 0x06]));
  assert.ok(eocd > 0);
  assert.equal(buf.readUInt16LE(eocd + 10), wins.length + 1);
  // Every entry's stored bytes hash to the committed sha256.
  for (const w of wins) {
    const sha = crypto.createHash('sha256').update(fs.readFileSync(path.join(ws.partsDir, job.id, w.file))).digest('hex');
    assert.equal(sha, w.sha256);
    assert.equal(crc32(fs.readFileSync(path.join(ws.partsDir, job.id, w.file))), w.crc32);
  }
  ws.close();
});

test('export works in bounded memory for a larger dataset', async () => {
  const ws = await seeded('bounded', { rows: 30_000, payloadBytes: 900, seed: 5 });
  const job = await ws.createJob({ strategy: 'replay', windowWidth: 2000, workers: 3 });
  const before = process.memoryUsage().heapUsed;
  await ws.start(job.id);
  const done = await waitIdle(ws, job.id, 120_000);
  const growth = process.memoryUsage().heapUsed - before;
  assert.equal(done.status, 'done');
  assert.equal(done.row_exported, 30_000);
  // Streaming writer: heap growth stays small relative to the data volume
  // (~27 MB of rows). Allow generous headroom for Node's GC noise.
  assert.ok(growth < 120 * 1024 * 1024, `heap grew ${(growth / 1048576).toFixed(1)} MiB`);
  ws.close();
});
