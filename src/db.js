'use strict';

const { DatabaseSync } = require('node:sqlite');

/**
 * Open a SQLite database. `node:sqlite` is synchronous, which we rely on:
 * every transaction block below is straight-line code with no `await` inside,
 * so the single Node thread can never interleave two transactions.
 */
function openDb(file, opts = {}) {
  const { readOnly = false, wal = true, busyTimeout = 8000 } = opts;
  const db = new DatabaseSync(file, { readOnly });
  db.exec(`PRAGMA busy_timeout = ${busyTimeout}`);
  if (!readOnly) {
    if (wal) {
      db.exec('PRAGMA journal_mode = WAL');
      db.exec('PRAGMA synchronous = NORMAL');
    }
    db.exec('PRAGMA foreign_keys = ON');
  }
  return db;
}

function tx(db, fn) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const out = fn();
    db.exec('COMMIT');
    return out;
  } catch (err) {
    try {
      db.exec('ROLLBACK');
    } catch {
      /* ignore */
    }
    throw err;
  }
}

/** Read-only consistent snapshot transaction. */
function readTx(db, fn) {
  db.exec('BEGIN');
  try {
    const out = fn();
  } finally {
    try {
      db.exec('COMMIT');
    } catch {
      /* ignore */
    }
  }
  return out;
}

function attached(db, name) {
  return db.prepare('PRAGMA database_list').all().some((r) => r.name === name);
}

module.exports = { openDb, tx, readTx, attached };
