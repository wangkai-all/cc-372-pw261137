'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { createReadStream } = require('node:fs');
const { Crc32 } = require('./crc32');
const { nowMs, sha256File } = require('./util');

const EXPECTED_KEYS = ['id', 'seq', 'payload'];

function isPlainIndex(line) {
  return line.charCodeAt(0) === 0x7b; // '{'
}

/**
 * Per-file verification. Every check is independent of the export writer:
 * the file is re-read from disk and tested against the job's pin and the
 * window's registered row range.
 *
 * Checks:
 *  1. file exists and its size matches the committed record
 *  2. byte checksum matches the committed record (SHA-256 + CRC-32)
 *  3. every line is well-formed JSON with exactly the row keys
 *  4. row ids are strictly increasing and unique
 *  5. every row id lies inside the window's declared [id_from, id_to]
 *  6. row count matches the window record and the pinned source count
 *  7. (deep) payload and seq equal the pinned version row-for-row
 *  8. every row's seq (its source change version) is <= the pinned version
 */
async function verifyWindow({
  job,
  win,
  reader,
  partPath,
  deep = false,
  expectedRows = null,
}) {
  const startedAt = nowMs();
  const failures = [];
  const add = (code, message) => failures.push({ code, message });
  let lines = 0;
  let bytes = 0;
  const hash = require('node:crypto').createHash('sha256');
  const crc = new Crc32();
  let prevId = null;
  let firstId = null;
  let lastId = null;
  let deepCursor = null;

  if (!fs.existsSync(partPath)) {
    return {
      idx: win.idx,
      ok: false,
      checked: 0,
      failures: [{ code: 'missing', message: 'part file not found on disk' }],
      ms: nowMs() - startedAt,
    };
  }

  const stream = createReadStream(partPath);
  let buffer = '';
  let lineNo = 0;
  const checkLine = (line) => {
    lineNo += 1;
    if (!line.length) return;
    let row;
    try {
      row = JSON.parse(line);
    } catch {
      add('json', `line ${lineNo}: not valid JSON`);
      return;
    }
    const keys = Object.keys(row);
    if (
      keys.length !== EXPECTED_KEYS.length ||
      !EXPECTED_KEYS.every((k) => Object.prototype.hasOwnProperty.call(row, k))
    ) {
      add('shape', `line ${lineNo}: unexpected row shape`);
      return;
    }
    if (!Number.isInteger(row.id)) {
      add('id', `line ${lineNo}: id is not an integer`);
      return;
    }
    if (row.id < win.id_from || row.id > win.id_to) {
      add(
        'range',
        `line ${lineNo}: id ${row.id} outside declared range [${win.id_from}, ${win.id_to}]`
      );
    }
    if (prevId != null && row.id <= prevId) {
      add('order', `line ${lineNo}: id ${row.id} is not greater than previous id ${prevId}`);
    }
    if (row.seq > job.pinned_version) {
      add(
        'version',
        `line ${lineNo}: row seq ${row.seq} is newer than pinned version ${job.pinned_version}`
      );
    }
    if (firstId == null) firstId = row.id;
    prevId = row.id;
    lastId = row.id;
    lines += 1;

    if (deep) {
      if (!deepCursor) deepCursor = reader.rows(win.id_from, win.id_to);
      const expected = deepCursor.next();
      if (expected.done) {
        add('rows', `line ${lineNo}: pinned source has fewer rows than the part`);
      } else if (expected.value.id !== row.id) {
        add('deep-id', `line ${lineNo}: expected id ${expected.value.id}, found ${row.id}`);
      } else {
        if (expected.value.seq !== row.seq) {
          add('deep-seq', `line ${lineNo}: id ${row.id} seq differs from pinned version`);
        }
        if (expected.value.payload !== row.payload) {
          add('deep-payload', `line ${lineNo}: id ${row.id} payload differs from pinned version`);
        }
      }
    }
  };

  for await (const chunk of stream) {
    hash.update(chunk);
    crc.update(chunk);
    bytes += chunk.length;
    buffer += chunk;
    let idx;
    while ((idx = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 1);
      checkLine(line);
    }
  }
  if (buffer.length) checkLine(buffer);

  const digest = hash.digest('hex');
  const crcValue = crc.digest();
  if (win.sha256 && digest !== win.sha256) add('sha256', 'sha256 differs from committed record');
  if (win.crc32 != null && crcValue !== win.crc32) add('crc32', 'crc32 differs from committed record');
  if (win.bytes && bytes !== win.bytes) add('size', 'file size differs from committed record');
  if (win.rows != null && lines !== win.rows) add('count', 'row count differs from committed record');
  if (win.first_id != null && firstId !== win.first_id) add('first', 'first row id differs');
  if (win.last_id != null && lastId !== win.last_id) add('last', 'last row id differs');
  if (expectedRows != null && lines !== expectedRows) {
    add(
      'pin-count',
      `row count ${lines} does not match pinned source count ${expectedRows} for this window`
    );
  }
  if (deep && deepCursor) {
    const extra = deepCursor.next();
    if (!extra.done) add('rows-missing', 'pinned source has more rows than the part');
  }

  return {
    idx: win.idx,
    ok: failures.length === 0,
    checked: lines,
    bytes,
    sha256: digest,
    crc32: crcValue,
    firstId,
    lastId,
    range: [win.id_from, win.id_to],
    failures: failures.slice(0, 20),
    failureCount: failures.length,
    ms: nowMs() - startedAt,
  };
}

module.exports = { verifyWindow };
