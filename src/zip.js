'use strict';

/**
 * Minimal streaming ZIP writer (store method, ZIP64-aware).
 *
 * A multi-GB archive must never be assembled in memory: entries are streamed
 * from disk and the central directory is emitted afterwards from the small
 * per-entry records collected along the way. CRC-32 and sizes come from the
 * committed part records, so no seek-back is required.
 */

const LOCAL_SIG = 0x04034b50;
const CENTRAL_SIG = 0x02014b50;
const END_SIG = 0x06054b50;
const ZIP64_END_SIG = 0x06064b50;
const ZIP64_LOCATOR_SIG = 0x07064b50;
const ZIP64_EXTRA_ID = 0x0001;
const U32_MAX = 0xffffffff;
const U16_MAX = 0xffff;

function dosDateTime(date) {
  const year = Math.max(1980, date.getFullYear());
  return {
    time:
      ((date.getHours() & 0x1f) << 11) |
      ((date.getMinutes() & 0x3f) << 5) |
      ((date.getSeconds() / 2) & 0x1f),
    date: (((year - 1980) & 0x7f) << 9) | (((date.getMonth() + 1) & 0x0f) << 5) | (date.getDate() & 0x1f),
  };
}

class ZipStream {
  constructor(res, { mtime = new Date(), onWrite = null } = {}) {
    this.res = res;
    this.offset = 0;
    this.entries = [];
    this.mtime = mtime;
    this.onWrite = onWrite;
  }

  async write(buf) {
    this.offset += buf.length;
    if (this.onWrite) this.onWrite(buf.length);
    if (!this.res.write(buf)) {
      await new Promise((resolve) => this.res.once('drain', resolve));
    }
  }

  /** Add a Buffer entry (used for the manifest). */
  async addBuffer(name, data) {
    await this.addStream(name, (async function* () {
      yield data;
    })(), { crc32: crc32(data), size: data.length });
  }

  /**
   * Stream one entry. `source` is an async iterable of Buffers (or a ReadStream).
   * Size and CRC must be known in advance; a mismatch aborts the archive.
   */
  async addStream(name, source, { crc32: knownCrc, size }) {
    if (knownCrc == null || size == null) throw new Error('zip entries need known crc32 and size');
    const nameBuf = Buffer.from(name, 'utf8');
    const { time, date } = dosDateTime(this.mtime);
    const needsZip64 = size > U32_MAX;

    const extra = needsZip64 ? Buffer.alloc(20) : Buffer.alloc(0);
    if (needsZip64) {
      extra.writeUInt16LE(ZIP64_EXTRA_ID, 0);
      extra.writeUInt16LE(16, 2);
      extra.writeBigUInt64LE(BigInt(size), 4);
      extra.writeBigUInt64LE(BigInt(size), 12);
    }

    const header = Buffer.alloc(30);
    header.writeUInt32LE(LOCAL_SIG, 0);
    header.writeUInt16LE(needsZip64 ? 45 : 20, 4);
    header.writeUInt16LE(0x0800, 6); // UTF-8 file names
    header.writeUInt16LE(0, 8); // store
    header.writeUInt16LE(time, 10);
    header.writeUInt16LE(date, 12);
    header.writeUInt32LE(knownCrc >>> 0, 14);
    header.writeUInt32LE(needsZip64 ? U32_MAX : size, 18);
    header.writeUInt32LE(needsZip64 ? U32_MAX : size, 22);
    header.writeUInt16LE(nameBuf.length, 26);
    header.writeUInt16LE(extra.length, 28);

    const headerOffset = this.offset;
    await this.write(header);
    await this.write(nameBuf);
    if (extra.length) await this.write(extra);

    let written = 0;
    for await (const chunk of source) {
      written += chunk.length;
      await this.write(chunk);
    }
    if (written !== size) throw new Error(`zip size mismatch for ${name}: ${written} != ${size}`);

    this.entries.push({
      name: nameBuf,
      crc: knownCrc >>> 0,
      size,
      headerOffset,
      time,
      date,
    });
  }

  async finish() {
    const centralStart = this.offset;
    for (const e of this.entries) {
      const needsZip64 = e.size > U32_MAX || e.headerOffset > U32_MAX;
      const extra = needsZip64 ? Buffer.alloc(28) : Buffer.alloc(0);
      if (needsZip64) {
        extra.writeUInt16LE(ZIP64_EXTRA_ID, 0);
        extra.writeUInt16LE(24, 2);
        extra.writeBigUInt64LE(BigInt(e.size), 4);
        extra.writeBigUInt64LE(BigInt(e.size), 12);
        extra.writeBigUInt64LE(BigInt(e.headerOffset), 20);
      }
      const h = Buffer.alloc(46);
      h.writeUInt32LE(CENTRAL_SIG, 0);
      h.writeUInt16LE(45, 4); // version made by (zip64)
      h.writeUInt16LE(needsZip64 ? 45 : 20, 6);
      h.writeUInt16LE(0x0800, 8);
      h.writeUInt16LE(0, 10);
      h.writeUInt16LE(e.time, 12);
      h.writeUInt16LE(e.date, 14);
      h.writeUInt32LE(e.crc, 16);
      h.writeUInt32LE(needsZip64 ? U32_MAX : e.size, 20);
      h.writeUInt32LE(needsZip64 ? U32_MAX : e.size, 24);
      h.writeUInt16LE(e.name.length, 28);
      h.writeUInt16LE(extra.length, 30);
      h.writeUInt16LE(0, 32);
      h.writeUInt16LE(0, 34);
      h.writeUInt16LE(0, 36);
      h.writeUInt32LE(0, 38);
      h.writeUInt32LE(needsZip64 ? U32_MAX : e.headerOffset, 42);
      await this.write(h);
      await this.write(e.name);
      if (extra.length) await this.write(extra);
    }
    const centralSize = this.offset - centralStart;
    const count = this.entries.length;
    const needsZip64End = count > U16_MAX || centralStart > U32_MAX || centralSize > U32_MAX;

    if (needsZip64End) {
      const z64 = Buffer.alloc(56);
      z64.writeUInt32LE(ZIP64_END_SIG, 0);
      z64.writeBigUInt64LE(44n, 4);
      z64.writeUInt16LE(45, 12);
      z64.writeUInt16LE(45, 14);
      z64.writeUInt32LE(0, 16);
      z64.writeUInt32LE(0, 20);
      z64.writeBigUInt64LE(BigInt(count), 24);
      z64.writeBigUInt64LE(BigInt(count), 32);
      z64.writeBigUInt64LE(BigInt(centralSize), 40);
      z64.writeBigUInt64LE(BigInt(centralStart), 48);
      const z64Offset = this.offset;
      await this.write(z64);

      const loc = Buffer.alloc(20);
      loc.writeUInt32LE(ZIP64_LOCATOR_SIG, 0);
      loc.writeUInt32LE(0, 4);
      loc.writeBigUInt64LE(BigInt(z64Offset), 8);
      loc.writeUInt32LE(1, 16);
      await this.write(loc);
    }

    const end = Buffer.alloc(22);
    end.writeUInt32LE(END_SIG, 0);
    end.writeUInt16LE(0, 4);
    end.writeUInt16LE(0, 6);
    end.writeUInt16LE(Math.min(count, U16_MAX), 8);
    end.writeUInt16LE(Math.min(count, U16_MAX), 10);
    end.writeUInt32LE(Math.min(centralSize, U32_MAX), 12);
    end.writeUInt32LE(Math.min(centralStart, U32_MAX), 16);
    end.writeUInt16LE(0, 20);
    await this.write(end);
  }
}

const CRC_TABLE = (() => {
  const table = new Int32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c;
  }
  return table;
})();

function crc32(buf) {
  let c = 0xffffffff;
  for (let i = 0; i < buf.length; i += 1) c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

module.exports = { ZipStream, crc32, dosDateTime };
