'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { nowMs, removeQuiet } = require('./util');

/**
 * Revision of a running or finished export.
 *
 * Two kinds of edit are possible, and they have different costs:
 *
 *  - Partition edits (split / merge) only touch windows that have NOT been
 *    written. Committed parts are untouched and no data is re-read.
 *  - Re-issue edits (rewrite a range that is already exported) delete the
 *    affected part files and return those windows to pending. The rows are
 *    re-derived from the same pinned version - a correction to the source
 *    after the pin is not picked up, and must not be, or the archive would
 *    mix two data versions.
 *
 * Revision requires quiescence: a `revise` lease excludes export workers, so
 * no window can be claimed while the partitioning changes underneath it.
 */
class Reviser {
  constructor(workspace, { leaseTtlMs = 20_000 } = {}) {
    this.ws = workspace;
    this.state = workspace.state;
    this.leaseTtlMs = leaseTtlMs;
    this.owner = `revise-${process.pid}`;
  }

  withLease(jobId, fn) {
    const lease = this.state.acquireLease(jobId, this.owner, 'revise', this.leaseTtlMs);
    if (!lease.ok) {
      throw Object.assign(
        new Error(`cannot revise while ${lease.blocker.purpose} holds the job`),
        { statusCode: 409 }
      );
    }
    try {
      return fn();
    } finally {
      this.state.releaseLease(jobId, this.owner);
    }
  }

  requireJob(jobId) {
    const job = this.state.getJob(jobId);
    if (!job) throw Object.assign(new Error('job not found'), { statusCode: 404 });
    if (job.status === 'expired') {
      throw Object.assign(new Error('job has expired; files were removed'), { statusCode: 409 });
    }
    return job;
  }

  /**
   * Split window `idx` into `parts` contiguous sub-windows.
   * Only unexported windows may be split. Committed windows keep their file
   * and name; only their display index shifts.
   */
  split(jobId, idx, parts = 2) {
    const n = Math.max(2, Math.min(64, Math.trunc(parts) || 2));
    return this.withLease(jobId, () => {
      const job = this.requireJob(jobId);
      const win = this.state.getWindow(jobId, idx);
      if (!win) throw Object.assign(new Error('window not found'), { statusCode: 404 });
      if (win.status === 'committed') {
        throw Object.assign(
          new Error('window is already exported; use re-issue to rewrite it'),
          { statusCode: 409 }
        );
      }
      const span = win.id_to - win.id_from + 1;
      if (span < n) {
        throw Object.assign(new Error(`window spans ${span} ids; cannot split into ${n}`), {
          statusCode: 400,
        });
      }
      const pieces = [];
      let from = win.id_from;
      const base = Math.floor(span / n);
      let remainder = span % n;
      for (let i = 0; i < n; i += 1) {
        const size = base + (remainder > 0 ? 1 : 0);
        if (remainder > 0) remainder -= 1;
        const to = from + size - 1;
        pieces.push({ id_from: from, id_to: to });
        from = to + 1;
      }
      this.state.tx(() => {
        // `idx` is display order; file names live in the `file` column, so every
        // later window can be renumbered - including committed ones, which keep
        // their immutable part file. The two-step offset avoids transient
        // (job_id, idx) collisions while shifting upwards.
        this.state.db
          .prepare('UPDATE windows SET idx = -(idx + 1) WHERE job_id = ? AND idx > ?')
          .run(jobId, idx);
        // idx was stored as -(idx + 1), so this restores it shifted by n - 1.
        this.state.db
          .prepare('UPDATE windows SET idx = -idx + ? WHERE job_id = ? AND idx < 0')
          .run(n - 2, jobId);
        // Only unexported windows need a fresh part allocation; a committed
        // window keeps its immutable file and its recorded name.
        this.state.db
          .prepare(
            `UPDATE windows SET part_name = NULL WHERE job_id = ? AND status != 'committed'`
          )
          .run(jobId);

        this.state.db
          .prepare('DELETE FROM windows WHERE job_id = ? AND idx = ?')
          .run(jobId, idx);
        const ins = this.state.db.prepare(
          `INSERT INTO windows (job_id, idx, id_from, id_to, status)
           VALUES (?, ?, ?, ?, 'pending')`
        );
        pieces.forEach((p, i) => ins.run(jobId, idx + i, p.id_from, p.id_to));
      });
      this.state.recomputeJobTotals(jobId);
      this.state.event(
        'revise',
        `split window #${idx} (ids ${win.id_from}-${win.id_to}) into ${n} windows; nothing was re-read`,
        jobId
      );
      return { job: this.state.getJob(jobId), windows: this.state.getWindows(jobId) };
    });
  }

  /**
   * Merge windows [idx, idx+count) into one. All must be unexported.
   */
  merge(jobId, idx, count = 2) {
    const n = Math.max(2, Math.min(64, Math.trunc(count) || 2));
    return this.withLease(jobId, () => {
      const job = this.requireJob(jobId);
      const wins = this.state.getWindows(jobId).filter((w) => w.idx >= idx && w.idx < idx + n);
      if (wins.length !== n) {
        throw Object.assign(new Error('not enough windows to merge'), { statusCode: 400 });
      }
      if (wins.some((w) => w.status === 'committed')) {
        throw Object.assign(
          new Error('one of the windows is already exported; re-issue it first'),
          { statusCode: 409 }
        );
      }
      const from = wins[0].id_from;
      const to = wins[n - 1].id_to;
      this.state.tx(() => {
        this.state.db
          .prepare('DELETE FROM windows WHERE job_id = ? AND idx >= ? AND idx < ?')
          .run(jobId, idx, idx + n);
        // Shift later windows down through a negative offset so no transient
        // duplicate (job_id, idx) exists, whatever order rows are visited in.
        this.state.db
          .prepare('UPDATE windows SET idx = -(idx + 1) WHERE job_id = ? AND idx >= ?')
          .run(jobId, idx + n);
        this.state.db
          .prepare('UPDATE windows SET idx = -idx - ? WHERE job_id = ? AND idx < 0')
          .run(n, jobId);
        this.state.db
          .prepare(
            `INSERT INTO windows (job_id, idx, id_from, id_to, status)
             VALUES (?, ?, ?, ?, 'pending')`
          )
          .run(jobId, idx, from, to);
      });
      this.state.recomputeJobTotals(jobId);
      this.state.event(
        'revise',
        `merged ${n} windows into ids ${from}-${to}; nothing was re-read`,
        jobId
      );
      return { job: this.state.getJob(jobId), windows: this.state.getWindows(jobId) };
    });
  }

  /**
   * Re-issue a range of windows: delete their parts and queue them again.
   * They are recomputed from the pinned version, never from the live source.
   */
  reissue(jobId, fromIdx, toIdx = null) {
    const lo = Math.max(0, Math.trunc(fromIdx));
    const hi = toIdx == null ? lo : Math.trunc(toIdx);
    if (hi < lo) throw Object.assign(new Error('invalid window range'), { statusCode: 400 });
    return this.withLease(jobId, () => {
      const job = this.requireJob(jobId);
      const wins = this.state.getWindows(jobId).filter((w) => w.idx >= lo && w.idx <= hi);
      if (!wins.length) {
        throw Object.assign(new Error('no windows in that range'), { statusCode: 404 });
      }
      let removedFiles = 0;
      let removedBytes = 0;
      const dir = path.join(this.ws.partsDir, jobId);
      for (const w of wins) {
        if (w.file) {
          const p = path.join(dir, w.file);
          try {
            removedBytes += fs.statSync(p).size;
          } catch {
            /* already gone */
          }
          removeQuiet(p);
          removedFiles += 1;
        }
      }
      this.state.tx(() => {
        const upd = this.state.db.prepare(
          `UPDATE windows SET status = 'pending', rows = 0, bytes = 0, sha256 = NULL,
             crc32 = NULL, file = NULL, part_name = NULL, first_id = NULL, last_id = NULL,
             claimed_by = NULL, committed_at = NULL, verified_at = NULL, verify_ok = NULL,
             verify_note = NULL
           WHERE job_id = ? AND idx >= ? AND idx <= ?`
        );
        upd.run(jobId, lo, hi);
      });
      this.state.recomputeJobTotals(jobId);
      // A re-issued job is no longer complete; it must transition to done again.
      this.state.updateJob(jobId, {
        status: job.status === 'done' ? 'paused' : job.status,
        finished_at: null,
        error: null,
      });
      this.state.event(
        'reissue',
        `re-issued windows ${lo}-${hi}: ${removedFiles} part(s), ${removedBytes} bytes removed; will be recomputed from the pin v${job.pinned_version}`,
        jobId
      );
      return {
        job: this.state.getJob(jobId),
        removedFiles,
        removedBytes,
        windows: this.state.getWindows(jobId),
      };
    });
  }

  /**
   * Replace one unexported window's id range (e.g. widen a partially exported
   * boundary). The range must stay within the job's pinned max id.
   */
  retarget(jobId, idx, idFrom, idTo) {
    const lo = Math.trunc(idFrom);
    const hi = Math.trunc(idTo);
    return this.withLease(jobId, () => {
      const job = this.requireJob(jobId);
      const win = this.state.getWindow(jobId, idx);
      if (!win) throw Object.assign(new Error('window not found'), { statusCode: 404 });
      if (win.status === 'committed') {
        throw Object.assign(new Error('cannot retarget an exported window'), { statusCode: 409 });
      }
      if (!(lo >= 1 && hi >= lo && hi <= job.max_id)) {
        throw Object.assign(
          new Error(`range must be within 1..${job.max_id}`),
          { statusCode: 400 }
        );
      }
      this.state.tx(() => {
        this.state.db
          .prepare('UPDATE windows SET id_from = ?, id_to = ?, verify_note = ? WHERE job_id = ? AND idx = ?')
          .run(lo, hi, 'retargeted', jobId, idx);
      });
      this.state.event('revise', `retargeted window #${idx} to ids ${lo}-${hi}`, jobId);
      return { job: this.state.getJob(jobId), windows: this.state.getWindows(jobId) };
    });
  }
}

module.exports = { Reviser };
