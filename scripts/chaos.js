#!/usr/bin/env node
'use strict';

/**
 * Chaos script: enqueues 1000 jobs, kills a worker mid-run, and verifies
 * that zero jobs are permanently lost (they all eventually reach
 * completed or dead status after retries).
 *
 * Usage:
 *   node scripts/chaos.js
 *
 * Prerequisites: Redis running, at least 2 workers running in background.
 * The script kills one worker by spawning workers itself, then terminates one.
 */

const { createClient } = require('../src/queue/client');
const { enqueue }      = require('../src/queue/enqueue');
const { getQueueStats } = require('../src/queue/stats');
const KEYS = require('../src/queue/keys');
const { spawn } = require('child_process');
const path = require('path');

const TOTAL_JOBS = 1000;
const TYPES = ['sendEmail', 'resizeImage', 'flaky'];
const WORKER_SCRIPT = path.join(__dirname, '../src/worker/index.js');

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function countTerminalJobs(redis) {
  const keys = await redis.keys('job:*');
  if (keys.length === 0) return { completed: 0, dead: 0, other: 0, total: 0 };
  const pipeline = redis.pipeline();
  for (const k of keys) pipeline.hget(k, 'status');
  const results = await pipeline.exec();
  let completed = 0, dead = 0, other = 0;
  for (const [, status] of results) {
    if (status === 'completed') completed++;
    else if (status === 'dead') dead++;
    else other++;
  }
  return { completed, dead, other, total: keys.length };
}

async function main() {
  const redis = createClient();
  await redis.flushdb();
  console.log('🧹 Redis flushed — starting chaos test\n');

  // ── Spawn 3 workers ───────────────────────────────────────────────────────
  const workers = [];
  for (let i = 0; i < 3; i++) {
    const w = spawn(process.execPath, [WORKER_SCRIPT], {
      env: {
        ...process.env,
        WORKER_ID: `chaos-worker-${i}`,
        WORKER_CONCURRENCY: '5',
        VISIBILITY_TIMEOUT_MS: '10000',  // short for chaos test
        REAPER_INTERVAL_MS: '3000',
      },
      stdio: 'inherit',
    });
    workers.push(w);
    console.log(`🚀 Spawned worker ${i} (pid ${w.pid})`);
  }

  await sleep(1000); // let workers connect

  // ── Enqueue 1000 jobs ─────────────────────────────────────────────────────
  console.log(`\n📥 Enqueueing ${TOTAL_JOBS} jobs…`);
  const t0 = Date.now();
  const jobIds = [];
  for (let i = 0; i < TOTAL_JOBS; i++) {
    const type = TYPES[i % TYPES.length];
    const { jobId } = await enqueue(redis, { type, payload: { i }, maxAttempts: 3 });
    jobIds.push(jobId);
  }
  console.log(`✅ Enqueued ${TOTAL_JOBS} jobs in ${Date.now() - t0} ms`);

  // ── Kill one worker mid-run ───────────────────────────────────────────────
  await sleep(2000); // let workers pick up some jobs
  const victim = workers[0];
  console.log(`\n💀 Killing worker 0 (pid ${victim.pid}) mid-run…`);
  victim.kill('SIGKILL'); // SIGKILL bypasses graceful shutdown — simulates crash

  // ── Wait for all jobs to reach a terminal state ───────────────────────────
  console.log('\n⏳ Waiting for all jobs to reach terminal state…');
  const deadline = Date.now() + 120_000; // 2 min timeout
  let last = {};
  while (Date.now() < deadline) {
    const counts = await countTerminalJobs(redis);
    const stats  = await getQueueStats(redis);
    if (JSON.stringify(counts) !== JSON.stringify(last)) {
      console.log(`  terminal=${counts.completed + counts.dead}/${TOTAL_JOBS}  completed=${counts.completed}  dead=${counts.dead}  pending=${stats.pending}  processing=${stats.processing}  delayed=${stats.delayed}`);
      last = counts;
    }
    if (counts.completed + counts.dead >= TOTAL_JOBS) break;
    await sleep(2000);
  }

  const final = await countTerminalJobs(redis);
  const lostJobs = TOTAL_JOBS - final.completed - final.dead;

  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`📊 Final results:`);
  console.log(`   Total enqueued : ${TOTAL_JOBS}`);
  console.log(`   Completed      : ${final.completed}`);
  console.log(`   Dead (exhausted): ${final.dead}`);
  console.log(`   Still in-flight: ${final.other}`);
  console.log(`   LOST JOBS      : ${lostJobs}`);
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

  if (lostJobs === 0) {
    console.log('✅ CHAOS TEST PASSED — Zero lost jobs!');
  } else {
    console.error(`❌ CHAOS TEST FAILED — ${lostJobs} jobs were lost!`);
    process.exitCode = 1;
  }

  // Cleanup
  for (const w of workers) {
    try { w.kill('SIGTERM'); } catch {}
  }
  await sleep(1000);
  await redis.quit();
}

main().catch((err) => {
  console.error('Fatal chaos error:', err);
  process.exit(1);
});

