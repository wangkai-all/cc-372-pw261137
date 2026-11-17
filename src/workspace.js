'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { StateStore, newJobId } = require('./state');
const { SourceStore } = require('./source');
const { ExportEngine } = require('./export');
const { buildSnapshot, snapshotPath, removeSnapshot } = require('./pin');
const { ensureDir, nowMs, fileSize, removeQuiet } = require('./util');

/**
 * Workspace owns on-disk layout:
 *   data/source.db      versioned source data + append-only change log
 *   data/state.db       job registry (windows, commits, leases, verifications)
 *   data/jobs/          pinned snapshot databases
 *   data/parts/<job>/   exported part files
 */
class Workspace {
  constructor(root, opts = {}) {
    this.root = root;
    this.dataDir = path.join(root, 'data');
    this.sourcePath = path.join(this.dataDir, 'source.db');
    this.statePath = path.join(this.dataDir, 'state.db');
    this.jobsDir = path.join(this.dataDir, 'jobs');
    this.partsDir = path.join(this.dataDir, 'parts');
    this.leaseTtlMs = opts.leaseTtlMs ?? 20_000;
    for (const dir of [this.dataDir, this.jobsDir, this.partsDir]) ensureDir(dir);
    this.state = new StateStore(this.statePath);
    this.source = new SourceStore(this.sourcePath);
    this._running = new Map();
    this._genLock = false;
    this.recover();
  }

  close() {
    this.source.close();
    this.state.close();
  }

  discardPartial(jobId, partName) {
    if (!partName) return;
    const dir = path.join(this.partsDir, jobId);
    for (const suffix of ['', '.tmp']) removeQuiet(path.join(dir, `${partName}${suffix}`));
  }

  engine() {
    return new ExportEngine(this.state, {
      leaseTtlMs: this.leaseTtlMs,
      jobsDir: this.jobsDir,
      partsDir: this.partsDir,
      sourcePath: this.sourcePath,
    });
  }

  /**
   * Startup recovery.
   *
   * A job is only "running" because some process held a lease. After a crash
   * those leases are stale; windows stuck in `writing` were never committed,
   * so their partial files are garbage and they must be recomputed from the
   * pin. Committed windows are untouched: resume never re-reads exported data.
   */
  recover() {
    // Single-process ownership: leases left in the registry are from a prior
    // process that no longer exists.
    this.state.clearStaleLeases();
    for (const job of this.state.listJobs(500)) {
      if (!['running', 'pinned', 'pinning'].includes(job.status)) continue;
      // Never recover a job this process is actively working: its in-flight
      // window is legitimate, not a leftover.
      if (this.isRunning(job.id)) continue;
      const reset = this.state.resetWriting(job.id);
      for (const row of reset) this.discardPartial(job.id, row.part_name);
      const missingPin =
        job.strategy === 'snapshot' && !fs.existsSync(snapshotPath(this, job.id));
      this.state.updateJob(job.id, {
        status: missingPin ? 'failed' : 'paused',
        pause_requested: 0,
        paused_at: nowMs(),
        error: missingPin ? 'pinned snapshot was lost; this export cannot be resumed' : job.error,
      });
      if (reset.length) {
        this.state.event(
          'recover',
          `startup recovery: ${reset.length} uncommitted window(s) of ${job.name} returned to pending; ` +
            'committed parts are untouched',
          job.id
        );
      }
    }
  }

  // ---- source ------------------------------------------------------------

  async generateSource(opts = {}) {
    if (this._genLock) {
      throw Object.assign(new Error('generation already running'), { statusCode: 409 });
    }
    // Regenerating replaces the data every live pin was registered against.
    const active = this.state
      .listJobs(500)
      .filter((j) => !['expired', 'canceled'].includes(j.status));
    if (active.length) {
      throw Object.assign(
        new Error(
          `source is pinned by ${active.length} job(s); expire or collect them before regenerating`
        ),
        { statusCode: 409, activeJobs: active.map((j) => j.id) }
      );
    }
    this._genLock = true;
    try {
      const res = await this.source.generate(opts);
      this.state.event(
        'source',
        `generated ${res.rows.toLocaleString()} rows at ~${res.payloadBytes} B payload (v${res.changes})`
      );
      return res;
    } finally {
      this._genLock = false;
    }
  }

  // ---- jobs --------------------------------------------------------------

  async createJob({
    strategy = 'snapshot',
    windowWidth = 1000,
    workers = 2,
    retentionSec = 900,
    name = null,
  } = {}) {
    if (this._genLock) {
      throw Object.assign(new Error('source is being regenerated'), { statusCode: 409 });
    }
    const { version, maxId } = this.source.topVersion();
    if (!maxId) throw Object.assign(new Error('source is empty'), { statusCode: 400 });
    const width = Math.max(1, Math.min(50_000, Math.trunc(windowWidth) || 1000));
    const windowCount = Math.ceil(maxId / width);
    const createdAt = nowMs();
    const jobId = newJobId();

    const job = {
      id: jobId,
      name: name || `Export ${new Date(createdAt).toISOString().slice(0, 19).replace('T', ' ')}`,
      status: 'pinned',
      strategy,
      pinnedVersion: version,
      maxId,
      windowWidth: width,
      windowCount,
      createdAt,
      workers: Math.max(1, Math.min(8, Math.trunc(workers) || 2)),
      retentionSec: Math.max(30, Math.trunc(retentionSec) || 900),
      sourceWitness: version,
      pinFloor: 0,
    };

    const pinStarted = nowMs();
    if (strategy === 'snapshot') {
      const snap = buildSnapshot(this, this.source, jobId, version, maxId);
      job.snapshotBytes = snap.bytes;
      job.snapshotMs = snap.elapsedMs;
      job.pinMs = snap.elapsedMs;
    } else {
      job.pinMs = 0;
    }

    this.state.createJob(job);
    this.state.updateJob(jobId, {
      status: 'pending',
      pin_state: 'active',
      pin_ms: job.pinMs,
      expires_at: createdAt + job.retentionSec * 1000,
    });
    this.state.event(
      'job-created',
      `${job.name} pinned at v${version} (${strategy === 'snapshot' ? 'materialized snapshot' : 'replay recipe'}); ${windowCount} windows x ${width} ids`,
      jobId
    );
    return this.state.getJob(jobId);
  }

  isRunning(jobId) {
    return this._running.has(jobId);
  }

  start(jobId, { workers = null } = {}) {
    const job = this.state.getJob(jobId);
    if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
    if (this._running.has(jobId)) return this._running.get(jobId);
    const engine = this.engine();
    const count = workers ?? job.workers;
    const promise = engine
      .run(jobId, { workers: count, workerId: `exp-${process.pid}-${jobId}` })
      .catch((err) => {
        if (err.statusCode === 409) {
          this.state.event('blocked', `worker could not start: ${err.message}`, jobId);
          return this.state.getJob(jobId);
        }
        this.state.event('error', `worker error: ${err.message}`, jobId);
        this.state.updateJob(jobId, {
          status: 'failed',
          error: err.message,
          finished_at: nowMs(),
        });
        return this.state.getJob(jobId);
      })
      .finally(() => {
        this._running.delete(jobId);
      });
    this._running.set(jobId, promise);
    return promise;
  }

  async waitFor(jobId) {
    const p = this._running.get(jobId);
    if (p) await p;
    return this.state.getJob(jobId);
  }

  touchRetention(jobId, extraMs = null) {
    const job = this.state.getJob(jobId);
    if (!job) return null;
    const base = Math.max(job.expires_at ?? 0, nowMs());
    const expiresAt =
      extraMs != null ? nowMs() + extraMs : base + Math.max(60, job.retention_sec) * 1000;
    this.state.updateJob(jobId, { expires_at: expiresAt });
    return this.state.getJob(jobId);
  }

  pause(jobId) {
    const job = this.state.getJob(jobId);
    if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
    if (!['running', 'pending', 'pinned', 'paused'].includes(job.status)) {
      throw Object.assign(new Error(`cannot pause a ${job.status} job`), { statusCode: 409 });
    }
    if (job.status === 'paused') return job;
    // With no worker in flight there is nothing to wait for: pause is immediate.
    // Otherwise set the flag and let the running workers finish their window.
    const running = this.isRunning(jobId);
    this.state.updateJob(
      jobId,
      running
        ? { pause_requested: 1 }
        : { pause_requested: 0, status: 'paused', paused_at: nowMs() }
    );
    this.state.event(
      'pause-requested',
      running ? 'pause requested; in-flight windows finish first' : 'export paused',
      jobId
    );
    return this.state.getJob(jobId);
  }

  resume(jobId, opts = {}) {
    const job = this.state.getJob(jobId);
    if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
    if (job.status === 'expired') {
      throw Object.assign(new Error('job expired; its files were removed'), { statusCode: 410 });
    }
    const pendingLeft = this.state
      .getWindows(jobId)
      .some((w) => ['pending', 'writing', 'failed'].includes(w.status));
    if (job.status === 'done' && !pendingLeft) return job;
    if (job.strategy === 'snapshot' && !fs.existsSync(snapshotPath(this, jobId))) {
      throw Object.assign(
        new Error('pinned snapshot is gone; this export can no longer produce a consistent archive'),
        { statusCode: 409 }
      );
    }
    this.touchRetention(jobId);
    this.state.updateJob(jobId, {
      pause_requested: 0,
      status: 'running',
      error: null,
    });
    this.state.event(
      'resumed',
      'resumed: committed parts kept as-is; pending windows recomputed from the pin',
      jobId
    );
    this.start(jobId, opts);
    return this.state.getJob(jobId);
  }

  cancel(jobId) {
    const job = this.state.getJob(jobId);
    if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
    this.state.updateJob(jobId, {
      status: 'canceled',
      pause_requested: 0,
      finished_at: nowMs(),
      pin_state: 'released',
    });
    this.state.event('canceled', 'export canceled; parts retained until cleanup', jobId);
    return this.state.getJob(jobId);
  }

  // ---- pin bookkeeping ---------------------------------------------------

  /**
   * Oldest change-log version any resumable job still needs.
   *
   * Only `replay` jobs read the log. A snapshot job holds its own copy, so it
   * does not constrain pruning - that is exactly the resource trade shown in
   * the cost view. Jobs expire with retention; once expired, they release
   * their claim (their files are being removed).
   */
  oldestActivePin() {
    let oldest = null;
    for (const job of this.state.listJobs(500)) {
      if (job.status === 'expired' || job.status === 'canceled') continue;
      if (job.strategy !== 'replay') continue;
      if (oldest == null || job.pinned_version < oldest) oldest = job.pinned_version;
    }
    return oldest;
  }

  pinFloors() {
    return this.state
      .listJobs(500)
      .filter((j) => j.strategy === 'replay' && !['expired', 'canceled'].includes(j.status))
      .map((j) => ({
        jobId: j.id,
        name: j.name,
        version: j.pinned_version,
        status: j.status,
        expiresAt: j.expires_at,
      }));
  }

  /** Snapshot space is held per job until that job expires. */
  heldSnapshotBytes() {
    let total = 0;
    for (const job of this.state.listJobs(500)) {
      if (job.strategy !== 'snapshot' || job.status === 'expired') continue;
      total += fileSize(snapshotPath(this, job.id)) ?? 0;
    }
    return total;
  }

  // ---- reporting ---------------------------------------------------------

  storage() {
    const dirBytes = (dir) => {
      let total = 0;
      let names = [];
      try {
        names = fs.readdirSync(dir, { withFileTypes: true });
      } catch {
        return 0;
      }
      for (const entry of names) {
        const p = path.join(dir, entry.name);
        if (entry.isDirectory()) total += dirBytes(p);
        else total += fileSize(p) ?? 0;
      }
      return total;
    };
    const src = this.source.stats();
    return {
      sourceBytes: src.dbBytes,
      sourceWalBytes: (fileSize(`${this.sourcePath}-wal`) ?? 0),
      stateBytes: (fileSize(this.statePath) ?? 0) + (fileSize(`${this.statePath}-wal`) ?? 0),
      partsBytes: dirBytes(this.partsDir),
      snapshotBytes: dirBytes(this.jobsDir),
      probeBytes: dirBytes(path.join(this.dataDir, 'probe')),
      changeHistoryBytes: src.changeBytes ?? null,
    };
  }

  serializeJob(job, { full = false } = {}) {
    const windows = full ? this.state.getWindows(job.id) : null;
    const counts = Object.fromEntries(
      this.state.windowCounts(job.id).map((r) => [r.status, r.n])
    );
    const committed = counts.committed ?? 0;
    const failed = counts.failed ?? 0;
    const pending = (counts.pending ?? 0) + (counts.writing ?? 0);
    const total = job.window_count || 1;
    return {
      id: job.id,
      name: job.name,
      status: job.status,
      strategy: job.strategy,
      pinnedVersion: job.pinned_version,
      maxId: job.max_id,
      windowWidth: job.window_width,
      windowCount: job.window_count,
      rowTotal: job.row_total,
      rowExported: job.row_exported,
      partsBytes: job.parts_bytes,
      partsRows: job.parts_rows,
      createdAt: job.created_at,
      startedAt: job.started_at,
      finishedAt: job.finished_at,
      pausedAt: job.paused_at,
      pauseRequested: !!job.pause_requested,
      error: job.error,
      expiresAt: job.expires_at,
      retentionSec: job.retention_sec,
      snapshotBytes: job.snapshot_bytes,
      snapshotMs: job.snapshot_ms,
      pinMs: job.pin_ms,
      pinState: job.pin_state,
      deriveMs: job.derive_ms,
      rebuildMs: job.rebuild_ms,
      rebuildRows: job.rebuild_rows,
      workers: job.workers,
      sourceWitness: job.source_witness,
      pinFloor: job.pin_floor,
      counts: {
        committed,
        failed,
        pending,
        writing: counts.writing ?? 0,
        skipped: counts.skipped ?? 0,
        expired: counts.expired ?? 0,
      },
      progress: {
        committed,
        failed,
        pending,
        total,
        ratio: total ? committed / total : 0,
        rowsExported: job.row_exported,
        bytes: job.parts_bytes,
        complete: pending === 0 && committed > 0 && failed === 0,
      },
      windows,
    };
  }
}

module.exports = { Workspace };
