'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { DatabaseSync } = require('node:sqlite');
const { nowMs, removeQuiet, ensureDir, tick } = require('./util');

/**
 * Cost measurement.
 *
 * The two pinning strategies trade different real resources:
 *
 *   snapshot  pays disk up front and holds it for the job's life. Resuming
 *             re-reads the private copy, so later source edits are invisible
 *             and recovery has no rebuild step.
 *   replay    holds no copy, but the source's change log must be retained
 *             back to the oldest live pin, and every pending window on every
 *             resume must be re-derived from that log.
 *
 * The probe below actually runs both sides on a sample of windows and
 * projects from measured throughput. Every number in the API carries
 * `measured` so the UI can say which figures are observed and which are
 * extrapolated.
 */

function sampleWindowList(maxId, width, count) {
  const windows = [];
  const total = Math.max(1, Math.ceil(maxId / width));
  const n = Math.max(1, Math.min(count, total));
  for (let i = 0; i < n; i += 1) {
    // Spread the sample across the id range instead of clustering at the start.
    const idx = n === 1 ? 0 : Math.floor((i * (total - 1)) / (n - 1));
    const from = idx * width + 1;
    windows.push({ idx, from, to: Math.min(maxId, from + width - 1) });
  }
  return windows;
}

function replayIterate(db, version, from, to) {
  const stmt = db.prepare(
    `SELECT id, seq, payload FROM (
       SELECT r.id AS id, c.change_id AS seq, c.payload AS payload,
              ROW_NUMBER() OVER (PARTITION BY r.id ORDER BY c.change_id DESC) AS rn
       FROM source_rows r
       JOIN changes c ON c.row_id = r.id AND c.change_id <= :version
       WHERE r.id >= :lo AND r.id <= :hi
     ) WHERE rn = 1 AND payload IS NOT NULL
     ORDER BY id`
  );
  return stmt.iterate({ version, lo: from, hi: to });
}

/** Serialize rows the way the export writer does, without touching disk. */
function consume(iter, sink) {
  let rows = 0;
  let bytes = 0;
  for (const row of iter) {
    const line = `${JSON.stringify({ id: row.id, seq: row.seq, payload: row.payload })}\n`;
    bytes += Buffer.byteLength(line);
    rows += 1;
    if (sink) sink(line);
  }
  return { rows, bytes };
}

/**
 * Run the cost probe.
 * `sampleWindows` bounds the work: the probe never scans the whole dataset.
 */
function measureCosts(source, { windowWidth = 1000, sampleWindows = 3, probeDir = null } = {}) {
  const startedAt = nowMs();
  const { version, maxId } = source.topVersion();
  if (!maxId) {
    return { ok: false, reason: 'source is empty', measuredAt: nowMs() };
  }
  const width = Math.max(1, Math.trunc(windowWidth));
  const samples = sampleWindowList(maxId, width, sampleWindows);
  const totalWindows = Math.ceil(maxId / width);

  // --- strategy 2: replay ------------------------------------------------
  const replayRuns = [];
  let replayRows = 0;
  let replayBytes = 0;
  for (const w of samples) {
    const t0 = nowMs();
    const out = consume(replayIterate(source.db, version, w.from, w.to));
    replayRuns.push({ ...w, ...out, ms: Math.max(1, nowMs() - t0) });
    replayRows += out.rows;
    replayBytes += out.bytes;
    tick();
  }
  const replaySampleMs = replayRuns.reduce((s, r) => s + r.ms, 0);
  const replayRowsPerMs = replayRows / Math.max(1, replaySampleMs);
  const replayMsPerWindow = replaySampleMs / samples.length;

  // --- strategy 1: snapshot ---------------------------------------------
  // Build a real snapshot of the sampled windows only, twice: once for size
  // and build cost, once to measure read-back cost.
  const tmpDir = probeDir || path.join(require('node:os').tmpdir(), `pin-probe-${process.pid}`);
  ensureDir(tmpDir);
  const probeFile = path.join(tmpDir, 'probe.snapshot.db');
  removeQuiet(probeFile);
  let attachOk = false;
  const buildStart = nowMs();
  source.db.exec('BEGIN');
  try {
    source.db.prepare('ATTACH DATABASE ? AS probe').run(probeFile);
    attachOk = true;
    source.db.exec(
      'CREATE TABLE probe.rows (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, payload TEXT NOT NULL)'
    );
    const insert = source.db.prepare(
      'INSERT INTO probe.rows (id, seq, payload) VALUES (?, ?, ?)'
    );
    for (const w of samples) {
      for (const row of replayIterate(source.db, version, w.from, w.to)) {
        insert.run(row.id, row.seq, row.payload);
      }
      tick();
    }
    source.db.exec('COMMIT');
    source.db.prepare('DETACH DATABASE probe').run();
    attachOk = false;
  } catch (err) {
    try {
      source.db.exec('ROLLBACK');
    } catch {
      /* ignore */
    }
    if (attachOk) {
      try {
        source.db.prepare('DETACH DATABASE probe').run();
      } catch {
        /* ignore */
      }
    }
    throw err;
  }
  const snapshotBuildMs = Math.max(1, nowMs() - buildStart);
  const snapshotBytes = fs.existsSync(probeFile) ? fs.statSync(probeFile).size : 0;
  const snapRows = replayRows;

  // Read-back cost from the private copy.
  const snapDb = new DatabaseSync(probeFile, { readOnly: true });
  const snapRuns = [];
  for (const w of samples) {
    const t0 = nowMs();
    const out = consume(
      snapDb
        .prepare('SELECT id, seq, payload FROM rows WHERE id >= ? AND id <= ? ORDER BY id')
        .iterate(w.from, w.to)
    );
    snapRuns.push({ ...w, ...out, ms: Math.max(1, nowMs() - t0) });
  }
  snapDb.close();
  removeQuiet(probeFile);
  removeQuiet(`${probeFile}-wal`);
  removeQuiet(`${probeFile}-shm`);

  const snapSampleMs = snapRuns.reduce((s, r) => s + r.ms, 0);
  const snapMsPerWindow = snapSampleMs / samples.length;

  // --- projection --------------------------------------------------------
  const stats = source.stats();
  const rowsTotal = source.replayCount(version, 1, maxId);
  const bytesPerRow = snapRows ? snapshotBytes / snapRows : 0;
  const projectedSnapshotBytes = Math.round(bytesPerRow * rowsTotal);

  // The retained history the pin depends on: change-log bytes up to and
  // including the pinned version (measured with dbstat where available).
  const historyBytes = source.changeBytesUpTo(version);
  // Of that, the share attributable to the sample's density is the part that
  // may not be pruned while the pin lives.
  const averageRowBytes = stats.avgChangeBytes || Math.max(40, Math.round(bytesPerRow));
  const changeCount = source.topVersion().changeCount;
  const projectedPinnedHistoryBytes = Math.max(
    historyBytes,
    Math.round(changeCount * averageRowBytes * 0.15) // conservative floor: last state per row
  );

  const measured = {
    at: nowMs(),
    elapsedMs: nowMs() - startedAt,
    version,
    maxId,
    windowWidth: width,
    windowCount: totalWindows,
    sample: samples.length,
    sampledWindows: samples.slice(0, 6),
    rowsSampled: replayRows,
    rowsTotal,
    pinnedHistoryBytes: historyBytes,
    changeCount,
  };

  const strategies = {
    snapshot: {
      heldBytes: projectedSnapshotBytes,
      heldBytesMeasured: snapshotBytes,
      pinMs: Math.round((snapshotBuildMs / samples.length) * totalWindows),
      pinMsMeasured: snapshotBuildMs,
      resumeMsPerWindow: Math.round(snapMsPerWindow * 100) / 100,
      resumeMsTotal: Math.round(snapMsPerWindow * totalWindows),
      rebuildRowsOnResume: 0,
      measured: true,
      projection: 'pin time = measured per-window snapshot build x window count; held bytes = measured bytes/row x pinned rows',
    },
    replay: {
      heldBytes: historyBytes,
      heldBytesMeasured: historyBytes,
      pinMs: 0,
      pinMsMeasured: 0,
      resumeMsPerWindow: Math.round(replayMsPerWindow * 100) / 100,
      resumeMsTotal: Math.round(replayMsPerWindow * totalWindows),
      rebuildRowsOnResume: rowsTotal,
      measured: true,
      projection: 'resume time = measured replay per window x pending windows; held bytes = measured change-log history for the pin',
    },
  };

  // Disk cost of the snapshot is held for the job's lifetime; the replay cost
  // is paid again on every resume. Report the break-even in resumes.
  const snapshotExtraBytes = Math.max(0, projectedSnapshotBytes);
  const perResumeReplayMs = strategies.replay.resumeMsTotal;
  const snapshotPinMs = strategies.snapshot.pinMs;
  strategies.breakEven = {
    // Resumes after which total replay compute exceeds the one-time snapshot
    // build (ignoring the different resource: bytes vs. time).
    resumesVsPinTime:
      perResumeReplayMs > 0 ? Math.max(0, Math.ceil(snapshotPinMs / perResumeReplayMs)) : null,
    snapshotExtraBytes,
    note:
      'Snapshot trades held bytes for zero rebuild compute on resume; replay trades retained log bytes for recompute on every resume.',
  };

  return { ok: true, measured, strategies, stats };
}

module.exports = { measureCosts, sampleWindowList };
