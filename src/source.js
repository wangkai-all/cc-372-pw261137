'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');
const { openDb, tx } = require('./db');
const {
  ensureDir,
  nowMs,
  makeRng,
  makePayload,
  tick,
  sha256File,
  removeQuiet,
  fileSize,
} = require('./util');

const SOURCE_SCHEMA = `
CREATE TABLE IF NOT EXISTS source_rows (
  id         INTEGER PRIMARY KEY,
  seq        INTEGER NOT NULL,
  payload    TEXT NOT NULL,
  deleted    INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL
);

-- Append-only change log. Every row state is reconstructable as of a version
-- by taking, per row, the newest change with change_id <= version.
CREATE TABLE IF NOT EXISTS changes (
  change_id INTEGER PRIMARY KEY,
  row_id    INTEGER NOT NULL,
  op        TEXT NOT NULL,
  payload   TEXT,
  at        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_changes_row ON changes (row_id, change_id DESC);

CREATE TABLE IF NOT EXISTS source_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`;

/**
 * Replay query: rows as they existed at :version, inside [lo, hi].
 * A row's state is its newest change at or before the version; rows created
 * after the version have no such change and drop out.
 */
const REPLAY_SQL = `
SELECT id, seq, payload FROM (
  SELECT r.id AS id, c.change_id AS seq, c.payload AS payload,
         ROW_NUMBER() OVER (PARTITION BY r.id ORDER BY c.change_id DESC) AS rn
  FROM source_rows r
  JOIN changes c ON c.row_id = r.id AND c.change_id <= :version
  WHERE r.id >= :lo AND r.id <= :hi
) WHERE rn = 1 AND payload IS NOT NULL
ORDER BY id
`;

const REPLAY_COUNT_SQL = `
SELECT COUNT(*) AS n FROM (
  SELECT c.change_id AS payload_flag,
         ROW_NUMBER() OVER (PARTITION BY r.id ORDER BY c.change_id DESC) AS rn,
         c.payload AS payload
  FROM source_rows r
  JOIN changes c ON c.row_id = r.id AND c.change_id <= :version
  WHERE r.id >= :lo AND r.id <= :hi
) WHERE rn = 1 AND payload IS NOT NULL
`;

class SourceStore {
  constructor(dbPath, { readonly = false } = {}) {
    ensureDir(path.dirname(dbPath));
    this.path = dbPath;
    this.db = openDb(dbPath, { readOnly: readonly });
    if (!readonly) this.db.exec(SOURCE_SCHEMA);
  }

  close() {
    try {
      this.db.close();
    } catch {
      /* ignore */
    }
  }

  metaMap() {
    const out = {};
    for (const row of this.db.prepare('SELECT key, value FROM source_meta').all()) {
      out[row.key] = row.value;
    }
    return out;
  }

  meta(key, fallback = null) {
    const row = this.db.prepare('SELECT value FROM source_meta WHERE key = ?').get(key);
    return row ? row.value : fallback;
  }

  setMeta(key, value) {
    this.db
      .prepare(
        `INSERT INTO source_meta (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`
      )
      .run(key, String(value));
  }

  /** Point-in-time read of the registration markers. */
  topVersion() {
    this.db.exec('BEGIN');
    try {
      const version = Number(this.meta('change_seq', '0'));
      const maxId = Number(this.meta('max_id', '0'));
      const liveRows = Number(this.meta('live_rows', '0'));
      const deletedRows = Number(this.meta('deleted_rows', '0'));
      const changeCount = Number(this.meta('change_count', '0'));
      return { version, maxId, liveRows, deletedRows, changeCount };
    } finally {
      try {
        this.db.exec('COMMIT');
      } catch {
        /* ignore */
      }
    }
  }

  stats() {
    const meta = this.metaMap();
    const out = {
      rows: Number(meta.total_rows ?? 0),
      liveRows: Number(meta.live_rows ?? 0),
      deletedRows: Number(meta.deleted_rows ?? 0),
      maxId: Number(meta.max_id ?? 0),
      changeSeq: Number(meta.change_seq ?? 0),
      changeCount: Number(meta.change_count ?? 0),
      payloadBytes: Number(meta.payload_bytes ?? 0),
      generatedAt: Number(meta.generated_at ?? 0),
      seeded: Number(meta.total_rows ?? 0) > 0,
      dbBytes: fileSize(this.path) ?? 0,
    };
    try {
      const row = this.db
        .prepare(
          `SELECT SUM(pgsize) AS bytes FROM dbstat
           WHERE name IN ('source_rows', 'changes', 'ix_changes_row')`
        )
        .get();
      out.dataBytes = Number(row?.bytes ?? 0);
      const hist = this.db
        .prepare(
          `SELECT SUM(pgsize) AS bytes FROM dbstat WHERE name IN ('changes', 'ix_changes_row')`
        )
        .get();
      out.changeBytes = Number(hist?.bytes ?? 0);
    } catch {
      out.dataBytes = null;
      out.changeBytes = null;
    }
    try {
      const row = this.db
        .prepare(
          `SELECT AVG(LENGTH(CAST(payload AS BLOB)) + 32) AS avg
           FROM (SELECT payload FROM changes ORDER BY change_id DESC LIMIT 500)`
        )
        .get();
      out.avgChangeBytes = Math.round(Number(row?.avg ?? 0));
    } catch {
      out.avgChangeBytes = 96;
    }
    return out;
  }

  /**
   * Rows at a registered version within an id window. Deterministic and
   * independent of how many later changes landed.
   */
  replayStmt(version, lo, hi) {
    return this.db.prepare(REPLAY_SQL).iterate({ version, lo, hi });
  }

  replayCount(version, lo, hi) {
    return Number(this.db.prepare(REPLAY_COUNT_SQL).get({ version, lo, hi }).n);
  }

  replayRowsAll(version, lo, hi) {
    return this.db.prepare(REPLAY_SQL).all({ version, lo, hi });
  }

  /** Approximate current byte size of rows in a window, used by cost probes. */
  liveCountInRange(lo, hi) {
    return Number(
      this.db
        .prepare('SELECT COUNT(*) AS n FROM source_rows WHERE id >= ? AND id <= ? AND deleted = 0')
        .get(lo, hi).n
    );
  }

  /**
   * Generate a fresh synthetic dataset in bounded chunks.
   * Wipes the source; callers must ensure no live pin depends on the old data.
   */
  async generate({ rows = 20000, payloadBytes = 400, seed = 7, chunk = 1000, onProgress } = {}) {
    const total = Math.max(0, Math.floor(rows));
    const rng = makeRng(seed + 1013904223);
    const startedAt = nowMs();
    this.db.exec('DELETE FROM changes');
    this.db.exec('DELETE FROM source_rows');
    const insRow = this.db.prepare(
      'INSERT INTO source_rows (id, seq, payload, deleted, updated_at) VALUES (?, ?, ?, 0, ?)'
    );
    const insChange = this.db.prepare(
      'INSERT INTO changes (change_id, row_id, op, payload, at) VALUES (?, ?, ?, ?, ?)'
    );
    let changeId = 0;
    let cursor = 0;
    this.setMeta('change_seq', '0');
    while (cursor < total) {
      const end = Math.min(total, cursor + chunk);
      this.db.exec('BEGIN IMMEDIATE');
      try {
        for (let id = cursor + 1; id <= end; id += 1) {
          changeId += 1;
          const size = Math.max(
            24,
            Math.round(payloadBytes * (0.6 + rng() * 0.8))
          );
          const payload = makePayload(rng, size);
          insRow.run(id, changeId, payload, startedAt);
          insChange.run(changeId, id, 'insert', payload, startedAt);
        }
        this.db.exec('COMMIT');
      } catch (err) {
        this.db.exec('ROLLBACK');
        throw err;
      }
      cursor = end;
      if (onProgress) onProgress({ done: cursor, total });
      await tick();
    }
    tx(this.db, () => {
      this.setMeta('dataset_id', this.db.prepare('SELECT lower(hex(randomblob(8))) AS id').get().id);
      this.setMeta('total_rows', total);
      this.setMeta('live_rows', total);
      this.setMeta('deleted_rows', 0);
      this.setMeta('max_id', total);
      this.setMeta('change_seq', changeId);
      this.setMeta('change_count', changeId);
      this.setMeta('prune_floor', 0);
      this.setMeta('payload_bytes', payloadBytes);
      this.setMeta('generated_at', startedAt);
    });
    return { rows: total, payloadBytes, changes: changeId, elapsedMs: nowMs() - startedAt };
  }

  /** Apply one batch of source edits, appending to the change log atomically. */
  mutateBatch({ updates = 0, deletes = 0, inserts = 0, payloadBytes = 400, seed = 1 } = {}) {
    const rng = makeRng((seed ^ (nowMs() & 0xffffffff)) >>> 0);
    const at = nowMs();
    let applied = { updates: 0, deletes: 0, inserts: 0 };
    tx(this.db, () => {
      let changeSeq = Number(this.meta('change_seq', '0'));
      let maxId = Number(this.meta('max_id', '0'));
      let live = Number(this.meta('live_rows', '0'));
      let deleted = Number(this.meta('deleted_rows', '0'));
      let changes = Number(this.meta('change_count', '0'));
      const getRow = this.db.prepare('SELECT id FROM source_rows WHERE id = ? AND deleted = 0');
      const updRow = this.db.prepare(
        'UPDATE source_rows SET seq = ?, payload = ?, updated_at = ? WHERE id = ?'
      );
      const delRow = this.db.prepare(
        'UPDATE source_rows SET seq = ?, deleted = 1, updated_at = ? WHERE id = ?'
      );
      const insRow = this.db.prepare(
        'INSERT INTO source_rows (id, seq, payload, deleted, updated_at) VALUES (?, ?, ?, 0, ?)'
      );
      const insChange = this.db.prepare(
        'INSERT INTO changes (change_id, row_id, op, payload, at) VALUES (?, ?, ?, ?, ?)'
      );

      const pickLiveId = () => {
        if (maxId <= 0 || live <= 0) return null;
        for (let attempt = 0; attempt < 12; attempt += 1) {
          const id = 1 + Math.floor(rng() * maxId);
          if (getRow.get(id)) return id;
        }
        return null;
      };

      for (let i = 0; i < updates; i += 1) {
        const id = pickLiveId();
        if (id == null) break;
        changeSeq += 1;
        changes += 1;
        const size = Math.max(24, Math.round(payloadBytes * (0.6 + rng() * 0.8)));
        const payload = makePayload(rng, size, `rev${changeSeq}`);
        updRow.run(changeSeq, payload, at, id);
        insChange.run(changeSeq, id, 'update', payload, at);
        applied.updates += 1;
      }
      for (let i = 0; i < deletes; i += 1) {
        const id = pickLiveId();
        if (id == null) break;
        changeSeq += 1;
        changes += 1;
        delRow.run(changeSeq, at, id);
        insChange.run(changeSeq, id, 'delete', null, at);
        live -= 1;
        deleted += 1;
        applied.deletes += 1;
      }
      for (let i = 0; i < inserts; i += 1) {
        maxId += 1;
        changeSeq += 1;
        changes += 1;
        const size = Math.max(24, Math.round(payloadBytes * (0.6 + rng() * 0.8)));
        const payload = makePayload(rng, size, `new${maxId}`);
        insRow.run(maxId, changeSeq, payload, at);
        insChange.run(changeSeq, maxId, 'insert', payload, at);
        live += 1;
        applied.inserts += 1;
      }
      this.setMeta('change_seq', changeSeq);
      this.setMeta('change_count', changes);
      this.setMeta('max_id', maxId);
      this.setMeta('live_rows', live);
      this.setMeta('deleted_rows', deleted);
    });
    return { ...applied, at };
  }

  /**
   * Remove old change-log entries while preserving replay for every version
   * >= boundary: per row, the newest change below the boundary is kept (that
   * is the row's state until its first later change).
   */
  pruneChanges(boundary) {
    if (!Number.isFinite(boundary) || boundary <= 1) return 0;
    const res = this.db
      .prepare(
        `DELETE FROM changes
         WHERE change_id < :boundary
           AND change_id NOT IN (
             SELECT MAX(change_id) FROM changes
             WHERE change_id < :boundary GROUP BY row_id
           )`
      )
      .run({ boundary });
    return Number(res.changes ?? 0);
  }

  changeBytesUpTo(version) {
    try {
      return Number(
        this.db
          .prepare(
            `SELECT SUM(pgsize) AS b FROM dbstat
             WHERE name IN ('changes', 'ix_changes_row')`
          )
          .get()?.b ?? 0
      );
    } catch {
      return 0;
    }
  }

  /** Live rows in [lo, hi] at a given version (used by verification). */
  expectedCount(version, lo, hi) {
    return this.replayCount(version, lo, hi);
  }
}

/** Snapshot database written by the materialize strategy. */
class SnapshotStore {
  static open(file) {
    return new DatabaseSync(file, { readOnly: true });
  }

  static count(db, lo, hi) {
    return Number(
      db.prepare('SELECT COUNT(*) AS n FROM rows WHERE id >= ? AND id <= ?').get(lo, hi).n
    );
  }

  static iterate(db, lo, hi) {
    return db
      .prepare('SELECT id, seq, payload FROM rows WHERE id >= ? AND id <= ? ORDER BY id')
      .iterate(lo, hi);
  }
}

module.exports = { SourceStore, SnapshotStore, REPLAY_SQL, removeQuiet, sha256File };
