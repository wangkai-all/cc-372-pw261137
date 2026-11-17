'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { nowMs, fileSize, removeQuiet, removeDirQuiet } = require('./util');
const { snapshotPath } = require('./pin');

/**
 * Retention / garbage collection.
 *
 * Expiry is a claim on a job's files, but a claim is only acted on when it is
 * safe. A job is protected while any of these hold:
 *   - an export lease (a worker is writing windows)
 *   - a download lease (bytes are on the wire, or verification is reading)
 *   - a revision lease (partitioning is mid-flight)
 *   - the job is still resumable: it has pending/failed windows and has not
 *     been explicitly released
 *
 * The check and the deletion marker happen inside one `gc` lease, so a
 * download that starts in the same instant either sees the job live and takes
 * a lease (blocking the collector) or sees it expired. Files are unlinked only
 * after the marker is committed; a crash between mark and unlink leaves an
 * expired job whose files the next pass sweeps.
 */
class Collector {
  constructor(workspace, { leaseTtlMs = 20_000 } = {}) {
    this.ws = workspace;
    this.leaseTtlMs = leaseTtlMs;
    this.owner = `gc-${process.pid}`;
  }

  jobPartsDir(jobId) {
    return path.join(this.ws.partsDir, jobId);
  }

  /** Why a job must not be cleaned right now, or null if it may be. */
  protection(job) {
    if (!job) return 'unknown job';
    const leases = this.ws.state.liveLeases(job.id);
    if (leases.length) {
      const purposes = [...new Set(leases.map((l) => l.purpose))].join('+');
      return `${purposes} lease held (${leases[0].owner})`;
    }
    if (this.ws.isRunning(job.id)) return 'export worker running in this process';
    if (job.status === 'expired') return null; // already collected, sweep leftovers
    if (job.pause_requested) return 'pause in progress';
    if (['running', 'pinned', 'pinning'].includes(job.status)) return 'export in progress';
    if (job.status === 'paused') return 'paused but resumable: resume it or cancel it first';
    return null;
  }

  /** Candidates for cleanup: anything past its retention deadline. */
  plan() {
    const out = [];
    for (const job of this.ws.state.listJobs(500)) {
      if (job.status === 'expired') {
        const leftovers = this.partsBytes(job.id) + (fileSize(snapshotPath(this.ws, job.id)) ?? 0);
        out.push({
          jobId: job.id,
          name: job.name,
          status: job.status,
          expiresAt: job.expires_at,
          expired: true,
          protected: false,
          reason: leftovers ? 'files remain from an earlier collection' : null,
          leftoversBytes: leftovers,
        });
        continue;
      }
      if (job.expires_at == null || nowMs() < job.expires_at) continue;
      const why = this.protection(job);
      out.push({
        jobId: job.id,
        name: job.name,
        status: job.status,
        expiresAt: job.expires_at,
        expired: true,
        protected: why != null,
        reason: why,
        partsBytes: this.partsBytes(job.id),
        snapshotBytes: fileSize(snapshotPath(this.ws, job.id)) ?? 0,
      });
    }
    return out;
  }

  partsBytes(jobId) {
    const dir = this.jobPartsDir(jobId);
    let total = 0;
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return 0;
    }
    for (const e of entries) {
      if (e.isFile()) total += fileSize(path.join(dir, e.name)) ?? 0;
    }
    return total;
  }

  /**
   * Delete a job's payload and mark it expired, unless protected.
   * Returns a structured outcome so the UI can explain a refusal.
   */
  collect(jobId, { force = false, reason = 'retention' } = {}) {
    const job = this.ws.state.getJob(jobId);
    if (!job) return { ok: false, code: 'not-found' };
    if (job.status !== 'expired') {
      const why = this.protection(job);
      if (why && !force) {
        this.ws.state.event(
          'gc-skipped',
          `cleanup refused for ${job.name}: ${why}`,
          jobId
        );
        return { ok: false, code: 'protected', reason: why };
      }
    }
    const lease = this.ws.state.acquireLease(jobId, this.owner, 'gc', this.leaseTtlMs);
    if (!lease.ok) {
      return { ok: false, code: 'protected', reason: `${lease.blocker.purpose} lease is live` };
    }
    try {
      // Re-check under the lease: a download may have started between the
      // first check and this point.
      const live = this.ws.state
        .liveLeases(jobId)
        .filter((l) => l.owner !== this.owner);
      if (live.length && !force) {
        return { ok: false, code: 'protected', reason: `${live[0].purpose} lease is live` };
      }
      const partsBytes = this.partsBytes(jobId);
      const snapshotBytes = fileSize(snapshotPath(this.ws, jobId)) ?? 0;

      // Commit the claim first, then delete. A crash here leaves files that
      // the next sweep removes because the job is already marked expired.
      this.ws.state.tx(() => {
        this.ws.state.db
          .prepare(
            `UPDATE windows SET status = 'expired', sha256 = NULL, crc32 = NULL, file = NULL
             WHERE job_id = ? AND status != 'expired'`
          )
          .run(jobId);
        this.ws.state.db
          .prepare(
            `UPDATE jobs SET status = 'expired', row_exported = 0, parts_bytes = 0,
               parts_rows = 0, pin_state = 'released', error = ?
             WHERE id = ?`
          )
          .run(`retention expired (${reason}); files removed`, jobId);
      });
      removeDirQuiet(this.jobPartsDir(jobId));
      const snap = snapshotPath(this.ws, jobId);
      for (const suffix of ['', '-wal', '-shm', '-journal']) removeQuiet(`${snap}${suffix}`);
      const freed = partsBytes + snapshotBytes;
      this.ws.state.event(
        'gc',
        `removed ${freed} bytes for expired job ${job.name} (${reason})`,
        jobId
      );
      return {
        ok: true,
        freedBytes: freed,
        partsBytes,
        snapshotBytes,
        reason,
        pinReleased: job.strategy === 'replay' ? job.pinned_version : null,
      };
    } finally {
      this.ws.state.releaseLease(jobId, this.owner);
    }
  }

  sweep({ limit = 20 } = {}) {
    const results = [];
    for (const item of this.plan()) {
      if (results.length >= limit) break;
      if (item.protected) {
        results.push({ ...item, collected: false });
        continue;
      }
      const res = this.collect(item.jobId, { reason: 'retention sweep' });
      results.push({
        jobId: item.jobId,
        name: item.name,
        collected: res.ok,
        freedBytes: res.freedBytes ?? 0,
        refusal: res.reason,
      });
    }
    return results;
  }
}

module.exports = { Collector };
