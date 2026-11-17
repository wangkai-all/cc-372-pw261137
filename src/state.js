'use strict';

const crypto = require('node:crypto');
const path = require('node:path');
const { openDb, tx } = require('./db');
const { ensureDir, nowMs } = require('./util');

const SCHEMA = `
CREATE TABLE IF NOT EXISTS jobs (
  id              TEXT PRIMARY KEY,
  name            TEXT NOT NULL,
  status          TEXT NOT NULL,
  strategy        TEXT NOT NULL,
  pinned_version  INTEGER NOT NULL,
  max_id          INTEGER NOT NULL,
  window_width    INTEGER NOT NULL,
  window_count    INTEGER NOT NULL,
  row_total       INTEGER NOT NULL DEFAULT 0,
  row_exported    INTEGER NOT NULL DEFAULT 0,
  parts_bytes     INTEGER NOT NULL DEFAULT 0,
  parts_rows      INTEGER NOT NULL DEFAULT 0,
  created_at      INTEGER NOT NULL,
  last_activity   INTEGER,
  started_at      INTEGER,
  finished_at     INTEGER,
  paused_at       INTEGER,
  pause_requested INTEGER NOT NULL DEFAULT 0,
  error           TEXT,
  expires_at      INTEGER,
  snapshot_bytes  INTEGER,
  snapshot_ms     INTEGER,
  pin_ms          INTEGER,
  pin_state       TEXT NOT NULL DEFAULT 'pending',
  rebuild_ms      INTEGER NOT NULL DEFAULT 0,
  rebuild_rows    INTEGER NOT NULL DEFAULT 0,
  derive_ms       INTEGER NOT NULL DEFAULT 0,
  workers         INTEGER NOT NULL DEFAULT 2,
  retention_sec   INTEGER NOT NULL DEFAULT 900,
  source_witness  INTEGER NOT NULL DEFAULT 0,
  pin_floor       INTEGER NOT NULL DEFAULT 0,
  part_seq        INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS windows (
  job_id       TEXT NOT NULL,
  idx          INTEGER NOT NULL,
  id_from      INTEGER NOT NULL,
  id_to        INTEGER NOT NULL,
  status       TEXT NOT NULL DEFAULT 'pending',
  rows         INTEGER NOT NULL DEFAULT 0,
  bytes        INTEGER NOT NULL DEFAULT 0,
  sha256       TEXT,
  crc32        INTEGER,
  file         TEXT,
  part_name    TEXT,
  first_id     INTEGER,
  last_id      INTEGER,
  claimed_by   TEXT,
  attempts     INTEGER NOT NULL DEFAULT 0,
  derive_ms    INTEGER NOT NULL DEFAULT 0,
  committed_at INTEGER,
  verified_at  INTEGER,
  verify_ok    INTEGER,
  verify_note  TEXT,
  PRIMARY KEY (job_id, idx)
);

CREATE TABLE IF NOT EXISTS leases (
  job_id     TEXT NOT NULL,
  owner      TEXT NOT NULL,
  purpose    TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (job_id, owner)
);

CREATE TABLE IF NOT EXISTS verifications (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id      TEXT NOT NULL,
  mode        TEXT NOT NULL,
  ok          INTEGER NOT NULL,
  checked     INTEGER NOT NULL,
  failed      INTEGER NOT NULL,
  started_at  INTEGER NOT NULL,
  finished_at INTEGER NOT NULL,
  detail      TEXT
);

CREATE TABLE IF NOT EXISTS events (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id     TEXT,
  at         INTEGER NOT NULL,
  kind       TEXT NOT NULL,
  message    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cost_probes (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at     INTEGER NOT NULL,
  pinned_version INTEGER NOT NULL,
  rows_probed    INTEGER NOT NULL,
  exact          INTEGER NOT NULL,
  snapshot_bytes INTEGER NOT NULL,
  snapshot_ms    INTEGER NOT NULL,
  replay_ms      INTEGER NOT NULL,
  window_count   INTEGER NOT NULL,
  change_bytes   INTEGER NOT NULL,
  detail         TEXT
);
`;

function newJobId() {
  return `j${nowMs().toString(36)}${crypto.randomBytes(2).toString('hex')}`;
}

function newOwner(purpose) {
  return `${purpose}-${process.pid}-${crypto.randomBytes(3).toString('hex')}`;
}

const TERMINAL = new Set(['done', 'failed', 'canceled', 'expired']);

class StateStore {
  constructor(statePath) {
    this.path = statePath;
    ensureDir(path.dirname(statePath));
    this.db = openDb(statePath);
    this.db.exec(SCHEMA);
    this.migrate();
  }

  /**
   * Additive migrations. The registry is authoritative for in-flight work, so
   * a schema change may never require recreating it.
   */
  migrate() {
    const columns = (table) =>
      new Set(this.db.prepare(`PRAGMA table_info(${table})`).all().map((r) => r.name));
    const add = (table, name, ddl) => {
      if (!columns(table).has(name)) {
        this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${name} ${ddl}`);
      }
    };
    add('jobs', 'part_seq', 'INTEGER NOT NULL DEFAULT 0');
    add('jobs', 'paused_at', 'INTEGER');
    add('jobs', 'pin_state', "TEXT NOT NULL DEFAULT 'pending'");
    add('jobs', 'pin_ms', 'INTEGER');
    add('jobs', 'rebuild_ms', 'INTEGER NOT NULL DEFAULT 0');
    add('jobs', 'rebuild_rows', 'INTEGER NOT NULL DEFAULT 0');
    add('jobs', 'derive_ms', 'INTEGER NOT NULL DEFAULT 0');
    add('windows', 'part_name', 'TEXT');
    add('windows', 'crc32', 'INTEGER');
    add('windows', 'derive_ms', 'INTEGER NOT NULL DEFAULT 0');
  }

  /**
   * Single-process ownership: on boot no other process is writing, so every
   * lease left in the registry is stale. Clearing them makes recovery
   * immediate instead of waiting out the lease TTL.
   */
  clearStaleLeases() {
    this.db.prepare('DELETE FROM leases').run();
  }

  close() {
    try {
      this.db.close();
    } catch {
      /* ignore */
    }
  }

  tx(fn) {
    return tx(this.db, fn);
  }

  event(kind, message, jobId = null) {
    this.db
      .prepare('INSERT INTO events (job_id, at, kind, message) VALUES (?, ?, ?, ?)')
      .run(jobId, nowMs(), kind, message);
    // Keep the feed bounded.
    this.db
      .prepare(
        `DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT 200)`
      )
      .run();
  }

  listEvents(limit = 40) {
    return this.db.prepare('SELECT * FROM events ORDER BY id DESC LIMIT ?').all(limit);
  }

  createJob(job) {
    tx(this.db, () => {
      this.db
        .prepare(
          `INSERT INTO jobs (id, name, status, strategy, pinned_version, max_id, window_width,
             window_count, created_at, last_activity, workers, retention_sec, source_witness,
             pin_floor, expires_at, snapshot_bytes, snapshot_ms, pin_ms)
           VALUES (:id, :name, :status, :strategy, :pinned_version, :max_id, :window_width,
             :window_count, :created_at, :last_activity, :workers, :retention_sec,
             :source_witness, :pin_floor, :expires_at, :snapshot_bytes, :snapshot_ms, :pin_ms)`
        )
        .run({
          id: job.id,
          name: job.name,
          status: job.status,
          strategy: job.strategy,
          pinned_version: job.pinnedVersion,
          max_id: job.maxId,
          window_width: job.windowWidth,
          window_count: job.windowCount,
          created_at: job.createdAt,
          last_activity: job.createdAt,
          workers: job.workers,
          retention_sec: job.retentionSec,
          source_witness: job.sourceWitness,
          pin_floor: job.pinFloor,
          expires_at: null,
          snapshot_bytes: job.snapshotBytes ?? null,
          snapshot_ms: job.snapshotMs ?? null,
          pin_ms: job.pinMs ?? null,
        });
      const ins = this.db.prepare(
        `INSERT INTO windows (job_id, idx, id_from, id_to) VALUES (?, ?, ?, ?)`
      );
      for (let i = 0; i < job.windowCount; i += 1) {
        ins.run(job.id, i, i * job.windowWidth + 1, Math.min(job.maxId, (i + 1) * job.windowWidth));
      }
    });
  }

  getJob(id) {
    return this.db.prepare('SELECT * FROM jobs WHERE id = ?').get(id) ?? null;
  }

  listJobs(limit = 20) {
    return this.db.prepare('SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?').all(limit);
  }

  updateJob(id, fields) {
    const keys = Object.keys(fields);
    if (!keys.length) return;
    const params = { job_id: id };
    const sets = keys.map((key) => {
      params[key] = fields[key];
      return `${key} = :${key}`;
    });
    this.db.prepare(`UPDATE jobs SET ${sets.join(', ')} WHERE id = :job_id`).run(params);
  }

  incrJob(id, fields) {
    const keys = Object.keys(fields);
    if (!keys.length) return;
    const params = { job_id: id };
    const sets = keys.map((key) => {
      params[key] = fields[key];
      return `${key} = ${key} + :${key}`;
    });
    this.db.prepare(`UPDATE jobs SET ${sets.join(', ')} WHERE id = :job_id`).run(params);
  }

  getWindows(jobId) {
    return this.db.prepare('SELECT * FROM windows WHERE job_id = ? ORDER BY idx').all(jobId);
  }

  getWindow(jobId, idx) {
    return (
      this.db.prepare('SELECT * FROM windows WHERE job_id = ? AND idx = ?').get(jobId, idx) ?? null
    );
  }

  windowCounts(jobId) {
    return this.db
      .prepare('SELECT status, COUNT(*) AS n FROM windows WHERE job_id = ? GROUP BY status')
      .all(jobId);
  }

  /**
   * Claim pending windows and give each a fresh part name. The counter lives
   * in the jobs row so a name is never handed out twice, even across crashes.
   */
  claimWindows(jobId, owner, n) {
    return tx(this.db, () => {
      const rows = this.db
        .prepare(
          `SELECT * FROM windows WHERE job_id = ? AND status = 'pending'
           ORDER BY idx LIMIT ?`
        )
        .all(jobId, n);
      if (!rows.length) return [];
      let next = Number(
        this.db.prepare('SELECT part_seq FROM jobs WHERE id = ?').get(jobId)?.part_seq ?? 0
      );
      const upd = this.db.prepare(
        `UPDATE windows SET status = 'writing', claimed_by = ?, attempts = attempts + 1,
           part_name = ? WHERE job_id = ? AND idx = ?`
      );
      for (const row of rows) {
        next += 1;
        upd.run(owner, `part-${String(next).padStart(6, '0')}.jsonl`, jobId, row.idx);
      }
      this.db.prepare('UPDATE jobs SET part_seq = ? WHERE id = ?').run(next, jobId);
      return rows.map((row, i) => ({
        ...row,
        part_name: `part-${String(next - rows.length + 1 + i).padStart(6, '0')}.jsonl`,
      }));
    });
  }

  commitWindow(jobId, idx, meta) {
    return tx(this.db, () => {
      const res = this.db
        .prepare(
          `UPDATE windows SET status = 'committed', rows = :rows, bytes = :bytes,
             sha256 = :sha256, crc32 = :crc32, file = :file, first_id = :first_id,
             last_id = :last_id, derive_ms = :derive_ms, claimed_by = NULL,
             committed_at = :committed_at, verify_note = NULL
           WHERE job_id = :job_id AND idx = :idx AND status != 'committed'`
        )
        .run({
          job_id: jobId,
          idx,
          rows: meta.rows,
          bytes: meta.bytes,
          sha256: meta.sha256,
          crc32: meta.crc32,
          file: meta.file,
          first_id: meta.firstId,
          last_id: meta.lastId,
          derive_ms: meta.deriveMs,
          committed_at: nowMs(),
        });
      if (res.changes > 0) {
        this.db
          .prepare(
            `UPDATE jobs SET row_exported = row_exported + :rows,
               parts_bytes = parts_bytes + :bytes, parts_rows = parts_rows + :rows,
               derive_ms = derive_ms + :derive_ms, rebuild_ms = rebuild_ms + :rebuild_ms,
               rebuild_rows = rebuild_rows + :rebuild_rows, last_activity = :now
             WHERE id = :job_id`
          )
          .run({
            job_id: jobId,
            rows: meta.rows,
            bytes: meta.bytes,
            derive_ms: meta.deriveMs,
            rebuild_ms: meta.rebuildMs,
            rebuild_rows: meta.rebuildRows,
            now: nowMs(),
          });
      }
      return res.changes > 0;
    });
  }

  /** Return windows left in 'writing' (worker died) to the pending pool. */
  resetWriting(jobId) {
    const rows = this.db
      .prepare(
        `SELECT idx, part_name FROM windows WHERE job_id = ? AND status = 'writing'`
      )
      .all(jobId);
    this.db
      .prepare(
        `UPDATE windows SET status = 'pending', claimed_by = NULL, part_name = NULL
         WHERE job_id = ? AND status = 'writing'`
      )
      .run(jobId);
    return rows;
  }

  setWindowStatus(jobId, idx, status, note = null) {
    this.db
      .prepare('UPDATE windows SET status = ?, verify_note = ? WHERE job_id = ? AND idx = ?')
      .run(status, note, jobId, idx);
  }

  setWindowVerification(jobId, idx, ok, note) {
    this.db
      .prepare(
        `UPDATE windows SET verified_at = ?, verify_ok = ?, verify_note = ? WHERE job_id = ? AND idx = ?`
      )
      .run(nowMs(), ok ? 1 : 0, note, jobId, idx);
  }

  /** Undo a committed window; the caller deletes the file. */
  uncommitWindow(jobId, idx) {
    return tx(this.db, () => {
      const w = this.getWindow(jobId, idx);
      if (!w) return null;
      if (w.status === 'committed') {
        this.db
          .prepare(
            `UPDATE jobs SET row_exported = MAX(0, row_exported - :rows),
               parts_bytes = MAX(0, parts_bytes - :bytes),
               parts_rows = MAX(0, parts_rows - :rows),
               derive_ms = MAX(0, derive_ms - :derive_ms)
             WHERE id = :job_id`
          )
          .run({
            job_id: jobId,
            rows: w.rows,
            bytes: w.bytes,
            derive_ms: w.derive_ms,
          });
      }
      this.db
        .prepare(
          `UPDATE windows SET status = 'pending', rows = 0, bytes = 0, sha256 = NULL,
             crc32 = NULL, file = NULL, part_name = NULL, first_id = NULL, last_id = NULL,
             claimed_by = NULL, committed_at = NULL, verified_at = NULL, verify_ok = NULL
           WHERE job_id = ? AND idx = ?`
        )
        .run(jobId, idx);
      return w;
    });
  }

  /** Replace the whole window partitioning (used by split/merge). */
  replaceWindows(jobId, list) {
    tx(this.db, () => {
      this.db.prepare('DELETE FROM windows WHERE job_id = ?').run(jobId);
      const ins = this.db.prepare(
        `INSERT INTO windows (job_id, idx, id_from, id_to, status, rows, bytes, sha256, crc32,
           file, part_name, first_id, last_id, committed_at, verified_at, verify_ok, verify_note)
         VALUES (:job_id, :idx, :id_from, :id_to, :status, :rows, :bytes, :sha256, :crc32,
           :file, :part_name, :first_id, :last_id, :committed_at, :verified_at, :verify_ok, :verify_note)`
      );
      for (const w of list) {
        ins.run({
          job_id: jobId,
          idx: w.idx,
          id_from: w.id_from,
          id_to: w.id_to,
          status: w.status,
          rows: w.rows,
          bytes: w.bytes,
          sha256: w.sha256,
          crc32: w.crc32,
          file: w.file,
          part_name: w.part_name,
          first_id: w.first_id,
          last_id: w.last_id,
          committed_at: w.committed_at,
          verified_at: w.verified_at,
          verify_ok: w.verify_ok,
          verify_note: w.verify_note,
        });
      }
      this.db
        .prepare('UPDATE jobs SET window_count = ? WHERE id = ?')
        .run(list.length, jobId);
    });
  }

  /** Recompute job aggregates from the windows table (authoritative). */
  recomputeJobTotals(jobId) {
    tx(this.db, () => {
      const agg = this.db
        .prepare(
          `SELECT COUNT(*) AS windows,
                  COALESCE(SUM(CASE WHEN status = 'committed' THEN rows ELSE 0 END), 0) AS rows,
                  COALESCE(SUM(CASE WHEN status = 'committed' THEN bytes ELSE 0 END), 0) AS bytes,
                  COALESCE(SUM(CASE WHEN status = 'committed' THEN derive_ms ELSE 0 END), 0) AS derive_ms
           FROM windows WHERE job_id = ?`
        )
        .get(jobId);
      this.db
        .prepare(
          `UPDATE jobs SET window_count = :windows, row_exported = :rows, parts_bytes = :bytes,
             parts_rows = :rows, derive_ms = :derive_ms
           WHERE id = :job_id`
        )
        .run({ ...agg, job_id: jobId });
    });
  }

  // ---- leases -----------------------------------------------------------
  liveLeases(jobId) {
    return this.db
      .prepare('SELECT * FROM leases WHERE job_id = ? AND expires_at > ? ORDER BY created_at')
      .all(jobId, nowMs());
  }

  allLiveLeases() {
    return this.db.prepare('SELECT * FROM leases WHERE expires_at > ?').all(nowMs());
  }

  /**
   * Leases are what the three parties coordinate through:
   *  - `export` workers share a job; any number may coexist.
   *  - `download` is passive: it never blocks export or revision, but garbage
   *    collection must wait for bytes that are in flight.
   *  - `revise` (splitting/merging/resetting ranges) requires quiescence.
   *  - `gc` requires quiescence and excludes everything else.
   */
  leasesConflict(a, b) {
    if (a === 'export' && b === 'export') return false;
    if (a === 'download' && b === 'download') return false;
    if (a === 'download' || b === 'download') return a === 'gc' || b === 'gc' || a === 'revise' || b === 'revise';
    return true;
  }

  acquireLease(jobId, owner, purpose, ttlMs) {
    return tx(this.db, () => {
      const live = this.liveLeases(jobId);
      const blocker = live.find((l) => this.leasesConflict(purpose, l.purpose));
      if (blocker) return { ok: false, blocker };
      this.db
        .prepare(
          `INSERT INTO leases (job_id, owner, purpose, expires_at, created_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(job_id, owner) DO UPDATE SET purpose = excluded.purpose,
             expires_at = excluded.expires_at, created_at = excluded.created_at`
        )
        .run(jobId, owner, purpose, nowMs() + ttlMs, nowMs());
      return { ok: true, owner };
    });
  }

  renewLease(jobId, owner, ttlMs) {
    const res = this.db
      .prepare('UPDATE leases SET expires_at = ? WHERE job_id = ? AND owner = ?')
      .run(nowMs() + ttlMs, jobId, owner);
    return res.changes > 0;
  }

  releaseLease(jobId, owner) {
    this.db.prepare('DELETE FROM leases WHERE job_id = ? AND owner = ?').run(jobId, owner);
  }

  // ---- misc -------------------------------------------------------------
  recordVerification(v) {
    this.db
      .prepare(
        `INSERT INTO verifications (job_id, mode, ok, checked, failed, started_at, finished_at, detail)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(
        v.jobId,
        v.mode,
        v.ok ? 1 : 0,
        v.checked,
        v.failed,
        v.startedAt,
        nowMs(),
        JSON.stringify(v.detail ?? [])
      );
  }

  listVerifications(jobId) {
    return this.db
      .prepare('SELECT * FROM verifications WHERE job_id = ? ORDER BY id DESC LIMIT 8')
      .all(jobId);
  }

  recordProbe(p) {
    const res = this.db
      .prepare(
        `INSERT INTO cost_probes (created_at, pinned_version, rows_probed, exact, snapshot_bytes,
           snapshot_ms, replay_ms, window_count, change_bytes, detail)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(
        nowMs(),
        p.pinnedVersion,
        p.rowsProbed,
        p.exact ? 1 : 0,
        p.snapshotBytes,
        p.snapshotMs,
        p.replayMs,
        p.windowCount,
        p.changeBytes,
        JSON.stringify(p.detail ?? {})
      );
    return Number(res.lastInsertRowid);
  }

  latestProbe() {
    const row = this.db.prepare('SELECT * FROM cost_probes ORDER BY id DESC LIMIT 1').get();
    if (!row) return null;
    return { ...row, detail: JSON.parse(row.detail || '{}') };
  }

  deleteJobRecord(jobId) {
    tx(this.db, () => {
      this.db.prepare('DELETE FROM windows WHERE job_id = ?').run(jobId);
      this.db.prepare('DELETE FROM verifications WHERE job_id = ?').run(jobId);
      this.db.prepare('DELETE FROM leases WHERE job_id = ?').run(jobId);
      this.db.prepare('DELETE FROM jobs WHERE id = ?').run(jobId);
    });
  }
}

module.exports = { StateStore, newJobId, newOwner, TERMINAL };
