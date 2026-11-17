'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');
const { nowMs, ensureDir, fileSize, removeQuiet } = require('./util');

/**
 * Pin registration.
 *
 * An export must read one consistent data version from first chunk to last,
 * even while the source keeps receiving corrections and deletes. Two ways to
 * buy that guarantee, at different costs (measured by cost.js):
 *
 *  - `snapshot`: copy the pinned version into a private database up front.
 *    Disk is paid immediately and held for the job's life; resume reads the
 *    copy, so later source edits are invisible and no recomputation happens.
 *  - `replay`: reconstruct the pinned version from the append-only change log
 *    at run time. No held copy, but every window - including every resumed
 *    one - replays changes, and the log must be retained back to the oldest
 *    live pin.
 *
 * Both are true pins: the choice is cost, not correctness. The registered
 * pin is (version, window partitioning); both strategies derive every part
 * from exactly that pair.
 */

function snapshotPath(store, jobId) {
  return path.join(store.jobsDir, `${jobId}.snapshot.db`);
}

const COPY_SELECT = `
  INSERT INTO snap.rows (id, seq, payload)
  SELECT id, seq, payload FROM (
    SELECT r.id AS id, c.change_id AS seq, c.payload AS payload,
           ROW_NUMBER() OVER (PARTITION BY r.id ORDER BY c.change_id DESC) AS rn
    FROM source_rows r
    JOIN changes c ON c.row_id = r.id AND c.change_id <= :version
  ) WHERE rn = 1 AND payload IS NOT NULL
  ORDER BY id`;

/**
 * Materialize the pinned version into a private database.
 * The whole copy runs inside one source read transaction, so concurrent
 * writes cannot leak a newer state into the copy.
 */
function buildSnapshot(workspace, source, jobId, version, maxId) {
  const file = snapshotPath(workspace, jobId);
  for (const suffix of ['', '-wal', '-shm', '-journal']) removeQuiet(`${file}${suffix}`);
  ensureDir(path.dirname(file));

  const startedAt = nowMs();
  let attached = false;
  source.db.exec('BEGIN');
  try {
    source.db.prepare('ATTACH DATABASE ? AS snap').run(file);
    attached = true;
    source.db.exec(
      'CREATE TABLE snap.rows (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, payload TEXT NOT NULL)'
    );
    source.db.prepare(COPY_SELECT).run({ version });
    const copied = Number(source.db.prepare('SELECT COUNT(*) AS n FROM snap.rows').get().n);
    source.db.exec('COMMIT');
    source.db.prepare('DETACH DATABASE snap').run();
    attached = false;

    const bytes = fileSize(file) ?? 0;
    let integrity = 'unknown';
    try {
      const check = new DatabaseSync(file, { readOnly: true });
      integrity = check.prepare('PRAGMA integrity_check').get()?.integrity_check ?? 'unknown';
      check.close();
    } catch {
      /* integrity is reported, never fatal here */
    }
    return {
      file,
      rows: copied,
      bytes,
      elapsedMs: nowMs() - startedAt,
      integrity,
    };
  } catch (err) {
    try {
      source.db.exec('ROLLBACK');
    } catch {
      /* ignore */
    }
    if (attached) {
      try {
        source.db.prepare('DETACH DATABASE snap').run();
      } catch {
        /* ignore */
      }
    }
    for (const suffix of ['', '-wal', '-shm', '-journal']) removeQuiet(`${file}${suffix}`);
    throw err;
  }
}

/** Remove a job's held snapshot (garbage collection). */
function removeSnapshot(workspace, jobId) {
  const file = snapshotPath(workspace, jobId);
  let freed = 0;
  for (const suffix of ['', '-wal', '-shm', '-journal']) {
    const p = `${file}${suffix}`;
    freed += fileSize(p) ?? 0;
    removeQuiet(p);
  }
  return freed;
}

module.exports = { buildSnapshot, removeSnapshot, snapshotPath };
