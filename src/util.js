'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

function nowMs() {
  return Date.now();
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function fileSize(p) {
  try {
    return fs.statSync(p).size;
  } catch {
    return null;
  }
}

function removeQuiet(p) {
  try {
    fs.rmSync(p, { force: true });
  } catch {
    /* ignore */
  }
}

function removeDirQuiet(dir) {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {
    /* ignore */
  }
}

function sha256Hex(data) {
  return crypto.createHash('sha256').update(data).digest('hex');
}

/** Stream a file through sha256 without loading it into memory. */
async function sha256File(p) {
  const hash = crypto.createHash('sha256');
  const rs = fs.createReadStream(p);
  for await (const chunk of rs) hash.update(chunk);
  return hash.digest('hex');
}

function tick() {
  return new Promise((resolve) => setImmediate(resolve));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function asInt(value, fallback) {
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(n, lo, hi) {
  return Math.min(hi, Math.max(lo, n));
}

function humanBytes(n) {
  if (n == null) return '—';
  if (n < 1024) return `${n} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v < 10 ? v.toFixed(2) : v.toFixed(1)} ${units[i]}`;
}

/**
 * Reads a file line by line with a bounded buffer.
 * Each yielded line is raw text (no trailing newline).
 */
async function* readLines(p, { maxLine = 4 * 1024 * 1024 } = {}) {
  const rs = fs.createReadStream(p, { encoding: 'utf8' });
  let buf = '';
  for await (const chunk of rs) {
    buf += chunk;
    let idx;
    while ((idx = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, idx);
      buf = buf.slice(idx + 1);
      if (line.length > maxLine) throw new Error('line exceeds maxLine');
      yield line;
    }
    if (buf.length > maxLine) throw new Error('line exceeds maxLine');
  }
  if (buf.length > 0) yield buf;
}

/** JSON body reader with a hard size cap. */
function readJsonBody(req, limitBytes = 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > limitBytes) {
        reject(Object.assign(new Error('request body too large'), { statusCode: 413 }));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      if (!chunks.length) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        reject(Object.assign(new Error('invalid JSON body'), { statusCode: 400 }));
      }
    });
    req.on('error', reject);
  });
}

/** Deterministic xorshift32 PRNG for reproducible synthetic payloads. */
function makeRng(seed) {
  let s = (seed >>> 0) || 0x9e3779b9;
  return () => {
    s ^= s << 13;
    s >>>= 0;
    s ^= s >>> 17;
    s ^= s << 5;
    s >>>= 0;
    return s / 0x100000000;
  };
}

const PAYLOAD_ALPHABET =
  'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .-:;';

function makePayload(rng, bytes, tag = '') {
  // Synthetic, printable, single-line payload of a requested length.
  const target = Math.max(1, bytes);
  if (tag) {
    let out = `[${tag}] `;
    while (out.length < target) out += PAYLOAD_ALPHABET[(rng() * PAYLOAD_ALPHABET.length) | 0];
    return out.slice(0, target);
  }
  let out = '';
  while (out.length < target) out += PAYLOAD_ALPHABET[(rng() * PAYLOAD_ALPHABET.length) | 0];
  return out.slice(0, target);
}

function serializeRow(row) {
  // Canonical line format; used by both the writer and deep verification.
  return `${JSON.stringify({ id: row.id, seq: row.seq, payload: row.payload })}\n`;
}

module.exports = {
  nowMs,
  ensureDir,
  fileSize,
  removeQuiet,
  removeDirQuiet,
  sha256Hex,
  sha256File,
  tick,
  sleep,
  asInt,
  clamp,
  humanBytes,
  readLines,
  readJsonBody,
  makeRng,
  makePayload,
  serializeRow,
};
