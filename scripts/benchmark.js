#!/usr/bin/env node
'use strict';

/**
 * Benchmark / load script.
 *
 * Measures enqueue throughput (jobs per second) by pushing a configurable
 * number of jobs as fast as possible, then optionally waiting for all of
 * them to complete and reporting end-to-end throughput.
 *
 * Usage:
 *   node scripts/benchmark.js [--jobs 5000] [--concurrency 50] [--wait]
 *
 *   --jobs N         Total jobs to enqueue (default 2000)
 *   --concurrency N  Parallel enqueue goroutines (default 50)
 *   --wait           Wait for all jobs to reach terminal state before exiting
 */

const { createClient } = require('../src/queue/client');
const { enqueue }      = require('../src/queue/enqueue');
const { getQueueStats } = require('../src/queue/stats');
const KEYS = require('../src/queue/keys');

const args = process.argv.slice(2);
const getArg = (name, def) => {
  const idx = args.indexOf(name);
  return idx !== -1 ? Number(args[idx + 1]) : def;
};
const TOTAL       = getArg('--jobs', 2000);
const CONCURRENCY = getArg('--concurrency', 50);
const WAIT        = args.includes('--wait');

const TYPES = ['sendEmail', 'resizeImage', 'flaky'];

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function countTerminal(redis) {
  const keys = await redis.keys('job:*');
  if (!keys.length) return 0;
  const pipeline = redis.pipeline();
  for (const k of keys) pipeline.hget(k, 'status');
  const res = await pipeline.exec();
  return res.filter(([, s]) => s === 'completed' || s === 'dead').length;
}

async function main() {
  const redis = createClient();

  console.log(`\n🏎  Benchmark: enqueuing ${TOTAL} jobs (concurrency=${CONCURRENCY})\n`);

  // ── Enqueue phase ─────────────────────────────────────────────────────────
  const t0 = Date.now();
  let enqueued = 0;

  // Run jobs in batches of CONCURRENCY
  for (let i = 0; i < TOTAL; i += CONCURRENCY) {
    const batch = Math.min(CONCURRENCY, TOTAL - i);
    await Promise.all(
      Array.from({ length: batch }, (_, j) => {
        const type = TYPES[(i + j) % TYPES.length];
        return enqueue(redis, { type, payload: { n: i + j }, maxAttempts: 3 });
      })
    );
    enqueued += batch;
  }

  const enqueueMs = Date.now() - t0;
  const enqueueRPS = Math.round((TOTAL / enqueueMs) * 1000);

  console.log(`📥 Enqueue complete`);
  console.log(`   Jobs    : ${TOTAL}`);
  console.log(`   Time    : ${enqueueMs} ms`);
  console.log(`   RPS     : ${enqueueRPS} enqueue/sec\n`);

  if (WAIT) {
    console.log('⏳ Waiting for all jobs to reach terminal state (requires workers running)…');
    const e2eStart = Date.now();
    const deadline = Date.now() + 300_000; // 5 min timeout
    let prev = 0;
    while (Date.now() < deadline) {
      const terminal = await countTerminal(redis);
      const stats = await getQueueStats(redis);
      if (terminal !== prev) {
        const pct = ((terminal / TOTAL) * 100).toFixed(1);
        console.log(`  ${terminal}/${TOTAL} (${pct}%)  pending=${stats.pending}  processing=${stats.processing}  delayed=${stats.delayed}  dead=${stats.dead}`);
        prev = terminal;
      }
      if (terminal >= TOTAL) break;
      await sleep(1000);
    }
    const e2eMs = Date.now() - e2eStart;
    const e2eRPS = Math.round((TOTAL / e2eMs) * 1000);
    console.log(`\n✅ End-to-end throughput: ${e2eRPS} jobs/sec  (${e2eMs} ms total)\n`);
  }

  await redis.quit();
}

main().catch(err => { console.error(err); process.exit(1); });

