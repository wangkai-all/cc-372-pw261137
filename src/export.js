'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { DatabaseSync } = require('node:sqlite');
const { Crc32 } = require('./crc32');
const { SourceStore } = require('./source');
const { nowMs, ensureDir, removeQuiet, tick, sleep } = require('./util');

/**
 * A pinned reader exposes one interface - rows in [lo, hi] as of the pinned
 * version - over either backing strategy. The writer never knows which one it
 * has, so both strategies produce byte-identical parts.
 */
class SnapshotReader {
  constructor(file) {
    this.kind = 'snapshot';
    this.db = new DatabaseSync(file, { readOnly: true });
  }

  rows(lo, hi) {
    return this.db
      .prepare('SELECT id, seq, payload FROM rows WHERE id >= ? AND id <= ? ORDER BY id')
      .iterate(lo, hi);
  }

  close() {
    try {
      this.db.close();
    } catch {
      /* ignore */
    }
  }
}

class ReplayReader {
  constructor(db, version) {
    this.kind = 'replay';
    this.db = db;
    this.version = version;
    this.stmt = db.prepare(
      `SELECT id, seq, payload FROM (
         SELECT r.id AS id, c.change_id AS seq, c.payload AS payload,
                ROW_NUMBER() OVER (PARTITION BY r.id ORDER BY c.change_id DESC) AS rn
         FROM source_rows r
         JOIN changes c ON c.row_id = r.id AND c.change_id <= :version
         WHERE r.id >= :lo AND r.id <= :hi
       ) WHERE rn = 1 AND payload IS NOT NULL
       ORDER BY id`
    );
  }

  rows(lo, hi) {
    return this.stmt.iterate({ version: this.version, lo, hi });
  }

  close() {}
}

/**
 * Part names are never reused: each window gets a fresh name from a job-wide
 * counter, so splitting a window that was already written can never alias an
 * existing file, and committed files stay immutable after revision.
 */


function snapshotPath(store, jobId) {
  return path.join(store.jobsDir, `${jobId}.snapshot.db`);
}

/**
 * Write one window's part file.
 *
 * Streaming: one row is in memory at a time. The data is written to a temp
 * file, fsynced, and only then renamed into place; the caller records the
 * commit afterwards. A crash therefore leaves either no file or a complete
 * one, never a half-written part that a resume would trust.
 */
async function writeWindow(reader, dirPath, win) {
  ensureDir(dirPath);
  const finalPath = path.join(dirPath, win.part_name);
  const tmpPath = `${finalPath}.tmp`;
  removeQuiet(tmpPath);

  const hash = crypto.createHash('sha256');
  const crc = new Crc32();
  const fd = fs.openSync(tmpPath, 'w');
  let rows = 0;
  let bytes = 0;
  let firstId = null;
  let lastId = null;
  const startedAt = nowMs();

  try {
    let pending = [];
    let pendingBytes = 0;
    const flush = () => {
      if (!pending.length) return;
      const buf = Buffer.from(pending.join(''), 'utf8');
      fs.writeSync(fd, buf);
      hash.update(buf);
      crc.update(buf);
      bytes += buf.length;
      pending = [];
      pendingBytes = 0;
    };
    for (const row of reader.rows(win.id_from, win.id_to)) {
      const line = `${JSON.stringify({ id: row.id, seq: row.seq, payload: row.payload })}\n`;
      pending.push(line);
      pendingBytes += line.length;
      rows += 1;
      if (firstId == null) firstId = row.id;
      lastId = row.id;
      if (pendingBytes >= 256 * 1024) {
        flush();
        await tick(); // let HTTP and other jobs breathe
      }
    }
    flush();
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }

  fs.renameSync(tmpPath, finalPath);
  try {
    const dirFd = fs.openSync(dirPath, 'r');
    try {
      fs.fsyncSync(dirFd);
    } finally {
      fs.closeSync(dirFd);
    }
  } catch {
    /* directory fsync is best-effort on some platforms */
  }

  return {
    rows,
    bytes,
    firstId,
    lastId,
    sha256: hash.digest('hex'),
    crc32: crc.digest(),
    file: win.part_name,
    deriveMs: nowMs() - startedAt,
  };
}

/**
 * The export engine. A job is a queue of id windows; workers claim windows,
 * write parts and commit them. The state store is the authority and a part is
 * durable before its commit record, so a reload resumes exactly once per
 * window and never re-reads a window from a newer data version.
 */
class ExportEngine {
  constructor(store, opts = {}) {
    this.store = store;
    this.jobsDir = opts.jobsDir ?? path.join(path.dirname(store.path), 'jobs');
    this.partsDir = opts.partsDir ?? path.join(path.dirname(store.path), 'parts');
    this.sourcePath = opts.sourcePath ?? path.join(path.dirname(store.path), 'source.db');
    this.maxRetries = opts.maxRetries ?? 3;
    this.leaseTtlMs = opts.leaseTtlMs ?? 30_000;
  }

  jobDir(jobId) {
    return path.join(this.partsDir, jobId);
  }

  snapshotFile(jobId) {
    return path.join(this.jobsDir, `${jobId}.snapshot.db`);
  }

  readerFor(job) {
    if (job.strategy === 'snapshot') {
      const p = this.snapshotFile(job.id);
      if (!fs.existsSync(p)) throw new Error('pinned snapshot file is missing');
      return new SnapshotReader(p);
    }
    // Replay reads the source through a read-only connection per worker.
    const src = new SourceStore(this.sourcePath, { readonly: true });
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

  /**
   * A window left in `writing` was never committed, so nothing may adopt its
   * bytes. Remove both the temp file and any final-named file it may have been
   * renamed to just before the crash.
   */
  discardPartial(jobId, partName) {
    if (!partName) return;
    removeQuiet(path.join(this.jobDir(jobId), `${partName}.tmp`));
    removeQuiet(path.join(this.jobDir(jobId), partName));
  }

  begin(jobId, workerId) {
    const job = this.store.getJob(jobId);
    if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
    if (['done', 'expired', 'canceled'].includes(job.status)) return { job, done: true };
    if (job.status === 'failed') return { job, done: true };
    const lease = this.store.acquireLease(jobId, workerId, 'export', this.leaseTtlMs);
    if (!lease.ok) {
      throw Object.assign(
        new Error(`cannot export: ${lease.blocker.purpose} holds the job`),
        { statusCode: 409 }
      );
    }
    const reset = this.store.resetWriting(jobId);
    for (const row of reset) this.discardPartial(jobId, row.part_name);
    if (reset.length) {
      this.store.event(
        'resume',
        `recovered ${reset.length} window(s) left mid-write; partial files discarded and the windows will be rewritten from the pin`,
        jobId
      );
    }
    this.store.updateJob(jobId, {
      status: 'running',
      pause_requested: 0,
      started_at: job.started_at ?? nowMs(),
      last_activity: nowMs(),
    });
    return { job: this.store.getJob(jobId), done: false };
  }

  /** One unit of work: claim a pending window, write it, commit it. */
  async step(jobId, workerId) {
    const job = this.store.getJob(jobId);
    if (!job) throw new Error('job not found');
    if (job.pause_requested) return { idle: true, paused: true };
    const claimed = this.store.claimWindows(jobId, workerId, 1);
    if (!claimed.length) return { idle: true };
    const win = claimed[0];
    let reader = null;
    try {
      reader = this.readerFor(job);
      if (job.strategy === 'snapshot') {
        const snap = this.store.getJob(jobId);
        if (snap?.pin_state === 'pending') this.store.updateJob(jobId, { pin_state: 'active' });
      }
      const meta = await writeWindow(reader, this.jobDir(jobId), win);
      const committed = this.store.commitWindow(jobId, win.idx, {
        rows: meta.rows,
        bytes: meta.bytes,
        firstId: meta.firstId,
        lastId: meta.lastId,
        sha256: meta.sha256,
        crc32: meta.crc32,
        file: meta.file,
        deriveMs: meta.deriveMs,
        rebuildMs: job.strategy === 'replay' ? meta.deriveMs : 0,
        rebuildRows: job.strategy === 'replay' ? meta.rows : 0,
      });
      this.store.renewLease(jobId, workerId, this.leaseTtlMs);
      if (!committed) {
        // Another worker committed this window first; drop our duplicate.
        removeQuiet(path.join(this.jobDir(jobId), meta.file));
        return { skipped: true, idx: win.idx };
      }
      return { idx: win.idx, rows: meta.rows, bytes: meta.bytes, ms: meta.deriveMs };
    } catch (err) {
      const attempts = win.attempts;
      if (attempts >= this.maxRetries) {
        this.store.setWindowStatus(jobId, win.idx, 'failed', String(err.message));
        this.store.event(
          'window-failed',
          `window ${win.idx} failed after ${attempts} attempt(s): ${err.message}`,
          jobId
        );
        return { error: err, idx: win.idx, exhausted: true };
      }
      this.store.setWindowStatus(jobId, win.idx, 'pending', `retry ${attempts + 1}: ${err.message}`);
      this.store.event(
        'window-retry',
        `window ${win.idx} requeued (attempt ${attempts + 1}): ${err.message}`,
        jobId
      );
      return { error: err, idx: win.idx, retry: true };
    } finally {
      if (reader) reader.close();
    }
  }

  /**
   * Drive a job to completion with `workers` concurrent loops.
   * Resumable: call again after a crash, pause or restart; committed windows
   * are never rewritten and pending ones are recomputed from the pin.
   */
  async run(jobId, { workers = 2, workerId = `exp-${process.pid}` } = {}) {
    const started = this.begin(jobId, workerId);
    if (started.done) return started.job;
    let stop = false;
    let fatal = null;

    const loop = async (n) => {
      const id = `${workerId}-${n}`;
      let idleSince = 0;
      while (!stop) {
        const current = this.store.getJob(jobId);
        if (!current) return;
        if (current.pause_requested) return;
        if (['failed', 'canceled', 'expired', 'done'].includes(current.status)) return;
        const result = await this.step(jobId, id);
        if (result.error) {
          if (result.exhausted) {
            fatal = result.error;
            stop = true;
            return;
          }
          continue;
        }
        if (result.paused) return;
        if (result.idle) {
          const counts = Object.fromEntries(
            this.store.windowCounts(jobId).map((r) => [r.status, r.n])
          );
          if ((counts.pending ?? 0) === 0 && (counts.writing ?? 0) === 0) return;
          if (!idleSince) idleSince = nowMs();
          if (nowMs() - idleSince > 5000) return;
          await sleep(10);
          continue;
        }
        idleSince = 0;
        this.store.updateJob(jobId, { last_activity: nowMs() });
      }
    };

    const workerCount = Math.max(1, Math.min(8, workers));
    await Promise.all(Array.from({ length: workerCount }, (_, i) => loop(i)));

    const after = this.store.getJob(jobId);
    if (!after) return after;
    if (fatal) {
      this.store.updateJob(jobId, {
        status: 'failed',
        error: String(fatal.message),
        finished_at: nowMs(),
        last_activity: nowMs(),
        pin_state: 'released',
      });
      this.store.event('failed', `export failed: ${fatal.message}`, jobId);
      this.store.releaseLease(jobId, workerId);
      return this.store.getJob(jobId);
    }
    if (after.pause_requested) {
      this.store.updateJob(jobId, {
        status: 'paused',
        paused_at: nowMs(),
        pause_requested: 0,
      });
      this.store.event('paused', 'export paused; committed parts retained', jobId);
      this.store.releaseLease(jobId, workerId);
      return this.store.getJob(jobId);
    }
    const counts = Object.fromEntries(this.store.windowCounts(jobId).map((r) => [r.status, r.n]));
    const pending = (counts.pending ?? 0) + (counts.writing ?? 0);
    if (pending === 0) {
      const failed = counts.failed ?? 0;
      const wasDone = after.status === 'done' && after.finished_at != null;
      // Re-issuing windows from a finished job clears finished_at on purpose:
      // the job is no longer complete and must transition again.
      if (!wasDone || failed || !after.finished_at) {
        this.store.updateJob(jobId, {
          status: failed ? 'failed' : 'done',
          error: failed ? `${failed} window(s) could not be written` : null,
          finished_at: nowMs(),
          last_activity: nowMs(),
        });
        if (!failed) {
          this.store.event('done', `export complete: ${after.window_count} parts`, jobId);
        } else {
          this.store.event('failed', `${failed} window(s) failed`, jobId);
        }
      }
    }
    this.store.releaseLease(jobId, workerId);
    return this.store.getJob(jobId);
  }
}

module.exports = { ExportEngine, SnapshotReader, ReplayReader, writeWindow };
