/**
 * Audit benchmark harness (Milestone 1).
 * - warm-up, several measurement series, median, p95, throughput
 * - memory: rss, heapUsed, external, arrayBuffers
 * - no heavy dependencies, uses node --cpu-prof / --heap-prof externally
 *
 * Run: node benchmarks/audit-harness.cjs
 * Profiling: node --cpu-prof --heap-prof benchmarks/audit-harness.cjs
 */
const { performance, monitorEventLoopDelay } = require('node:perf_hooks');
const {
  buildSqlDtaWithExternalData,
} = require('../dist/cjs/converters/sqlDta.js');
const { packDdmObject } = require('../dist/cjs/network/ddm.js');
const { encodeRequestDss, readDss } = require('../dist/cjs/network/dss.js');
const { encodeCp500, decodeCp500 } = require('../dist/cjs/converters/ebcdic.js');
const { encodeDecfloat, decodeDecfloat } = require('../dist/cjs/converters/decfloat.js');
const { CodePoint } = require('../dist/cjs/network/codepoint.js');

function mem() {
  const m = process.memoryUsage();
  return { rss: m.rss, heapUsed: m.heapUsed, external: m.external, arrayBuffers: m.arrayBuffers };
}

function stats(times) {
  const sorted = [...times].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];
  const p95 = sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))];
  const mean = times.reduce((s, v) => s + v, 0) / times.length;
  return { median, p95, mean, min: sorted[0], max: sorted[sorted.length - 1] };
}

async function bench(name, { warmup = 50, series = 100, fn, bytesPerOp }) {
  // Warm-up for V8 (JIT, hidden classes).
  for (let i = 0; i < warmup; i++) await fn(i);
  if (global.gc) global.gc();
  const before = mem();
  const times = [];
  const hDelay = monitorEventLoopDelay({ resolutionMs: 1 });
  hDelay.enable();
  const eulStart = performance.eventLoopUtilization();
  for (let i = 0; i < series; i++) {
    const t0 = performance.now();
    await fn(i);
    times.push(performance.now() - t0);
  }
  const eul = performance.eventLoopUtilization(eulStart);
  hDelay.disable();
  const after = mem();
  const s = stats(times);
  const opsSec = 1000 / s.median;
  const mbSec = bytesPerOp ? (bytesPerOp * opsSec) / (1024 * 1024) : undefined;
  console.log(`\n=== ${name} ===`);
  console.log(`  series=${series} warmup=${warmup} median=${s.median.toFixed(3)}ms p95=${s.p95.toFixed(3)}ms mean=${s.mean.toFixed(3)}ms`);
  console.log(`  throughput: ${opsSec.toFixed(1)} ops/sec${mbSec !== undefined ? `, ${mbSec.toFixed(2)} MB/sec` : ''}`);
  console.log(`  mem before: rss=${(before.rss / 1048576).toFixed(1)}MB heap=${(before.heapUsed / 1048576).toFixed(1)}MB ext=${(before.external / 1048576).toFixed(1)}MB arrBuf=${(before.arrayBuffers / 1048576).toFixed(1)}MB`);
  console.log(`  mem after:  rss=${(after.rss / 1048576).toFixed(1)}MB heap=${(after.heapUsed / 1048576).toFixed(1)}MB ext=${(after.external / 1048576).toFixed(1)}MB arrBuf=${(after.arrayBuffers / 1048576).toFixed(1)}MB`);
  console.log(`  mem delta:  rss=${((after.rss - before.rss) / 1048576).toFixed(2)}MB heap=${((after.heapUsed - before.heapUsed) / 1048576).toFixed(2)}MB ext=${((after.external - before.external) / 1048576).toFixed(2)}MB arrBuf=${((after.arrayBuffers - before.arrayBuffers) / 1048576).toFixed(2)}MB`);
  console.log(`  eventLoopUtilization: idle=${eul.idle.toFixed(1)}ms active=${eul.active.toFixed(1)}ms util=${eul.utilization.toFixed(3)} delay p95=${(hDelay.percentile(95) / 1e6).toFixed(2)}ms`);
  return { name, ...s, opsSec, mbSec, before, after };
}

function intColumns(n, sqlType = 497) {
  const cols = [];
  for (let i = 0; i < n; i++) cols.push({ name: `C${i}`, sqlType, length: 4n, precision: 0, scale: 0, nullable: false });
  return cols;
}
function varcharColumns(n) {
  const cols = [];
  for (let i = 0; i < n; i++) cols.push({ name: `C${i}`, sqlType: 449, length: 20n, precision: 0, scale: 0, nullable: true });
  return cols;
}

class MemoryReader {
  constructor(frames) { this.frames = frames; this.idx = 0; this.off = 0; }
  async readExact(length) {
    // Simulate TCP fragmentation: frames arrive in small chunks.
    const out = new Uint8Array(length);
    let w = 0;
    while (w < length) {
      if (this.idx >= this.frames.length) throw new Error('EOF');
      const f = this.frames[this.idx];
      const avail = f.length - this.off;
      const take = Math.min(avail, length - w);
      out.set(f.subarray(this.off, this.off + take), w);
      w += take;
      this.off += take;
      if (this.off >= f.length) { this.idx++; this.off = 0; }
    }
    return out;
  }
}

function fragment(buffer, chunks) {
  const frames = [];
  const size = Math.ceil(buffer.length / chunks);
  for (let o = 0; o < buffer.length; o += size) frames.push(buffer.subarray(o, Math.min(o + size, buffer.length)));
  return frames;
}

(async () => {
  console.log('Node', process.version, process.arch);
  console.log('Tip: run with node --cpu-prof --heap-prof --trace-gc for profiles.');

  // --- Encoder benchmarks ---
  await bench('encode SQLDTA 1x INTEGER', {
    fn: () => buildSqlDtaWithExternalData(intColumns(1), [42]),
    bytesPerOp: 64,
  });
  await bench('encode SQLDTA 10x INTEGER', {
    fn: () => buildSqlDtaWithExternalData(intColumns(10), Array(10).fill(42)),
    bytesPerOp: 640,
  });
  await bench('encode SQLDTA 50x INTEGER', {
    fn: () => buildSqlDtaWithExternalData(intColumns(50), Array(50).fill(42)),
    bytesPerOp: 3200,
  });
  await bench('encode SQLDTA 10x VARCHAR(20)', {
    fn: () => buildSqlDtaWithExternalData(varcharColumns(10), Array(10).fill('abcdefghijklmnopqrst')),
    bytesPerOp: 10 * 44,
  });
  await bench('encode SQLDTA 50x VARCHAR(20)', {
    fn: () => buildSqlDtaWithExternalData(varcharColumns(50), Array(50).fill('abcdefghijklmnopqrst')),
    bytesPerOp: 50 * 44,
  });
  const mixedCols = [...intColumns(10), ...varcharColumns(10)];
  await bench('encode SQLDTA mixed 20 cols', {
    fn: () => buildSqlDtaWithExternalData(mixedCols, [...Array(10).fill(1), ...Array(10).fill('hello world test 123')]),
    bytesPerOp: 1024,
  });
  await bench('encode VARCHAR 1KB', {
    fn: () => buildSqlDtaWithExternalData(varcharColumns(1), ['x'.repeat(1024)]),
    bytesPerOp: 2048,
  });
  await bench('encode VARCHAR 32KB', {
    fn: () => buildSqlDtaWithExternalData([{ name: 'C', sqlType: 449, length: 32768n, precision: 0, scale: 0, nullable: true }], ['y'.repeat(16000)]),
    bytesPerOp: 32768,
  });
  await bench('encode VARBINARY 32KB', {
    fn: () => buildSqlDtaWithExternalData(
      [{ name: 'B', sqlType: 909, length: 32768n, precision: 0, scale: 0, nullable: true }],
      [new Uint8Array(32768).fill(0xab)],
    ),
    bytesPerOp: 32768,
  });
  await bench('packDdmObject 1KB', {
    fn: () => packDdmObject(CodePoint.SQLDTA, new Uint8Array(1024)),
    bytesPerOp: 1024,
  });
  await bench('encodeRequestDss 64KB', {
    fn: () => {
      const payload = packDdmObject(CodePoint.SQLDTA, new Uint8Array(65536 - 10));
      return encodeRequestDss(payload, 1, false, true);
    },
    bytesPerOp: 65536,
  });
  await bench('encodeCp500 100 chars', {
    fn: () => encodeCp500('ABCDEFGHIJ'.repeat(10)),
    bytesPerOp: 100,
  });
  await bench('encodeDecfloat16', {
    fn: () => encodeDecfloat('123.456', 8),
    bytesPerOp: 8,
  });
  await bench('encodeDecfloat34', {
    fn: () => encodeDecfloat('12345678901234567890.12345678901234', 16),
    bytesPerOp: 16,
  });

  // --- Decoder benchmarks (synthetic QRYDTA via readDss) ---
  const smallPayload = packDdmObject(CodePoint.EXCSAT, new Uint8Array(100));
  const smallDss = encodeRequestDss(smallPayload, 1, false, true).bytes;
  await bench('readDss 1 chunk (small)', {
    fn: () => readDss(new MemoryReader([smallDss])),
  });
  await bench('readDss 10 chunks (small fragmented)', {
    fn: () => readDss(new MemoryReader(fragment(smallDss, 10))),
  });
  const bigPayload = packDdmObject(CodePoint.EXTDTA, new Uint8Array(65536));
  const bigDss = encodeRequestDss(bigPayload, 1, false, true).bytes;
  await bench('readDss 64KB 1 chunk', {
    fn: () => readDss(new MemoryReader([bigDss])),
    bytesPerOp: 65536,
  });
  await bench('readDss 64KB 100 chunks', {
    fn: () => readDss(new MemoryReader(fragment(bigDss, 100))),
    bytesPerOp: 65536,
  });
  const huge = encodeRequestDss(packDdmObject(CodePoint.EXTDTA, new Uint8Array(1024 * 1024)), 1, false, true).bytes;
  await bench('readDss 1MB 1 chunk', {
    fn: () => readDss(new MemoryReader([huge])),
    bytesPerOp: 1024 * 1024,
  });
  await bench('readDss 1MB 1000 chunks', {
    fn: () => readDss(new MemoryReader(fragment(huge, 1000))),
    bytesPerOp: 1024 * 1024,
  });

  // --- CP500 / UTF ---
  await bench('cp500 decode 1KB', {
    fn: () => decodeCp500(new Uint8Array(1024).fill(0xc1)),
    bytesPerOp: 1024,
  });

  console.log('\nDone. Compare Before/After with the Definition-of-Done table.');
})().catch((e) => { console.error(e); process.exitCode = 1; });
