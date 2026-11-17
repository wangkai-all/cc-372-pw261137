'use strict';

const CRC_TABLE = (() => {
  const table = new Int32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c;
  }
  return table;
})();

/** Incremental CRC-32 (IEEE) with the same convention as ZIP. */
class Crc32 {
  constructor() {
    this.value = 0xffffffff;
  }

  update(buf) {
    let c = this.value;
    for (let i = 0; i < buf.length; i += 1) c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
    this.value = c;
    return this;
  }

  digest() {
    return (this.value ^ 0xffffffff) >>> 0;
  }
}

function crc32(buf) {
  return new Crc32().update(buf).digest();
}

module.exports = { Crc32, crc32 };
