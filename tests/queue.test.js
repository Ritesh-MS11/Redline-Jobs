'use strict';

/**
 * Integration tests for the job queue system.
 *
 * These tests run against a REAL Redis instance (127.0.0.1:6379).
 * Each test suite uses an isolated key namespace (flushed before/after)
 * to avoid cross-test contamination.
 *
 * Run with: npm test
 * Requires Redis running locally or via docker-compose.
 */

const { createClient } = require('../src/queue/client');
const { enqueue }      = require('../src/queue/enqueue');
const { dequeue, acknowledge, removeFromProcessing } = require('../src/queue/dequeue');
const { scheduleRetry, moveToDead, promoteDueJobs, retryDeadJob, backoffDelay } = require('../src/queue/retry');
const { getQueueStats } = require('../src/queue/stats');
const KEYS = require('../src/queue/keys');

let redis;

beforeAll(async () => {
  redis = createClient();
  await redis.flushdb(); // start clean
});

afterAll(async () => {
  await redis.flushdb();
  await redis.quit();
});

beforeEach(async () => {
  await redis.flushdb();
});

// ─── Enqueue ─────────────────────────────────────────────────────────────────

describe('enqueue', () => {
  test('creates job hash and adds to pending list', async () => {
    const { jobId, duplicate } = await enqueue(redis, { type: 'sendEmail', payload: { to: 'a@b.com' } });

    expect(duplicate).toBe(false);
    expect(jobId).toMatch(/^[0-9a-f-]{36}$/);

    const job = await redis.hgetall(KEYS.job(jobId));
    expect(job.type).toBe('sendEmail');
    expect(job.status).toBe('pending');
    expect(Number(job.attempts)).toBe(0);

    const pendingLen = await redis.llen(KEYS.PENDING);
    expect(pendingLen).toBe(1);
    const head = await redis.lindex(KEYS.PENDING, 0);
    expect(head).toBe(jobId);
  });

  test('idempotency key prevents duplicate enqueue', async () => {
    const opts = { type: 'sendEmail', payload: {}, idempotencyKey: 'unique-key-1' };
    const first  = await enqueue(redis, opts);
    const second = await enqueue(redis, opts);

    expect(first.duplicate).toBe(false);
    expect(second.duplicate).toBe(true);
    expect(second.jobId).toBe(first.jobId);

    // Only one entry in pending
    const pendingLen = await redis.llen(KEYS.PENDING);
    expect(pendingLen).toBe(1);
  });

  test('different idempotency keys both enqueue', async () => {
    await enqueue(redis, { type: 'sendEmail', payload: {}, idempotencyKey: 'key-A' });
    await enqueue(redis, { type: 'sendEmail', payload: {}, idempotencyKey: 'key-B' });
    const pendingLen = await redis.llen(KEYS.PENDING);
    expect(pendingLen).toBe(2);
  });

  test('throws if type is missing', async () => {
    await expect(enqueue(redis, { payload: {} })).rejects.toThrow('type is required');
  });
});

// ─── Dequeue ─────────────────────────────────────────────────────────────────

describe('dequeue', () => {
  test('moves job from pending to processing atomically', async () => {
    const { jobId } = await enqueue(redis, { type: 'resizeImage', payload: {} });

    const dequeuedId = await dequeue(redis, 1);
    expect(dequeuedId).toBe(jobId);

    const pendingLen    = await redis.llen(KEYS.PENDING);
    const processingLen = await redis.llen(KEYS.PROCESSING);
    expect(pendingLen).toBe(0);
    expect(processingLen).toBe(1);
  });

  test('returns null when queue is empty (timeout)', async () => {
    const result = await dequeue(redis, 1);
    expect(result).toBeNull();
  });

  test('acknowledge removes from processing and sets status=completed', async () => {
    const { jobId } = await enqueue(redis, { type: 'sendEmail', payload: {} });
    await dequeue(redis, 1);
    await acknowledge(redis, jobId);

    const processingLen = await redis.llen(KEYS.PROCESSING);
    expect(processingLen).toBe(0);

    const job = await redis.hgetall(KEYS.job(jobId));
    expect(job.status).toBe('completed');
  });
});

// ─── Retry & Backoff ─────────────────────────────────────────────────────────

describe('retry backoff', () => {
  test('backoffDelay doubles each attempt and caps at 60s', () => {
    expect(backoffDelay(1)).toBe(1000);
    expect(backoffDelay(2)).toBe(2000);
    expect(backoffDelay(3)).toBe(4000);
    expect(backoffDelay(4)).toBe(8000);
    expect(backoffDelay(10)).toBe(60000); // capped
  });

  test('scheduleRetry adds job to delayed sorted set', async () => {
    const { jobId } = await enqueue(redis, { type: 'flaky', payload: {} });

    const { delay } = await scheduleRetry(redis, jobId, 1, 'transient error');

    expect(delay).toBe(1000);
    const score = await redis.zscore(KEYS.DELAYED, jobId);
    expect(Number(score)).toBeGreaterThan(Date.now() - 100);

    const job = await redis.hgetall(KEYS.job(jobId));
    expect(job.status).toBe('delayed');
    expect(job.lastError).toBe('transient error');
  });

  test('promoteDueJobs moves past-due jobs to pending', async () => {
    const { jobId } = await enqueue(redis, { type: 'flaky', payload: {} });
    // Score the job in the past so it's immediately due
    await redis.zadd(KEYS.DELAYED, Date.now() - 1000, jobId);
    await redis.hset(KEYS.job(jobId), 'status', 'delayed');
    // Remove from pending (we manually placed it in delayed)
    await redis.del(KEYS.PENDING);

    const promoted = await promoteDueJobs(redis);
    expect(promoted).toBe(1);

    const pendingLen = await redis.llen(KEYS.PENDING);
    expect(pendingLen).toBe(1);

    const job = await redis.hgetall(KEYS.job(jobId));
    expect(job.status).toBe('pending');
  });

  test('future delayed jobs are NOT promoted yet', async () => {
    const { jobId } = await enqueue(redis, { type: 'flaky', payload: {} });
    await redis.zadd(KEYS.DELAYED, Date.now() + 60_000, jobId); // 60 s in future
    await redis.del(KEYS.PENDING);

    const promoted = await promoteDueJobs(redis);
    expect(promoted).toBe(0);
    const pendingLen = await redis.llen(KEYS.PENDING);
    expect(pendingLen).toBe(0);
  });
});

// ─── Dead Letter ─────────────────────────────────────────────────────────────

describe('dead-letter queue', () => {
  test('moveToDead adds job to dead list and sets status=dead', async () => {
    const { jobId } = await enqueue(redis, { type: 'flaky', payload: {} });
    await moveToDead(redis, jobId, 3, 'gave up');

    const deadLen = await redis.llen(KEYS.DEAD);
    expect(deadLen).toBe(1);

    const job = await redis.hgetall(KEYS.job(jobId));
    expect(job.status).toBe('dead');
    expect(job.lastError).toBe('gave up');
  });

  test('retryDeadJob moves job back to pending and resets attempts', async () => {
    const { jobId } = await enqueue(redis, { type: 'flaky', payload: {} });
    // Drain pending (simulates worker having picked it up already)
    await redis.lrem(KEYS.PENDING, 1, jobId);
    await moveToDead(redis, jobId, 3, 'gave up');

    const ok = await retryDeadJob(redis, jobId);
    expect(ok).toBe(true);

    const deadLen    = await redis.llen(KEYS.DEAD);
    const pendingLen = await redis.llen(KEYS.PENDING);
    expect(deadLen).toBe(0);
    expect(pendingLen).toBe(1);

    const job = await redis.hgetall(KEYS.job(jobId));
    expect(job.status).toBe('pending');
    expect(Number(job.attempts)).toBe(0);
  });

  test('retryDeadJob returns false for non-existent job', async () => {
    const ok = await retryDeadJob(redis, 'non-existent-id');
    expect(ok).toBe(false);
  });
});

// ─── Stats ───────────────────────────────────────────────────────────────────

describe('queue stats', () => {
  test('returns correct counts after enqueue and dequeue', async () => {
    const jobId1 = (await enqueue(redis, { type: 'sendEmail', payload: {} })).jobId;
    const jobId2 = (await enqueue(redis, { type: 'sendEmail', payload: {} })).jobId;
    const jobId3 = (await enqueue(redis, { type: 'flaky',     payload: {} })).jobId;

    // Move jobId1 to processing by manually shifting it (avoid brpoplpush ordering surprise)
    await redis.lrem(KEYS.PENDING, 1, jobId1);
    await redis.rpush(KEYS.PROCESSING, jobId1);

    // Move jobId3 to dead (remove from pending first)
    await redis.lrem(KEYS.PENDING, 1, jobId3);
    await moveToDead(redis, jobId3, 3, 'oops');

    const stats = await getQueueStats(redis);
    expect(stats.pending).toBe(1);    // jobId2 still pending
    expect(stats.processing).toBe(1); // jobId1 processing
    expect(stats.dead).toBe(1);       // jobId3 dead
    expect(stats.delayed).toBe(0);
  });
});

// ─── Crash Recovery ──────────────────────────────────────────────────────────

describe('crash recovery (reaper simulation)', () => {
  test('a job stuck in processing with no live worker heartbeat gets requeued', async () => {
    // Enqueue + dequeue (simulates worker picking it up)
    const { jobId } = await enqueue(redis, { type: 'sendEmail', payload: {}, maxAttempts: 3 });
    // Simulate worker picking it up atomically
    await redis.brpoplpush(KEYS.PENDING, KEYS.PROCESSING, 1);

    // Mark it as old (simulate a worker that crashed)
    await redis.hmset(KEYS.job(jobId), {
      status: 'processing',
      attempts: 1,
      startedAt: Date.now() - 60_000,
    });

    // No heartbeat keys exist, so the Reaper should reclaim it
    const processingIds = await redis.lrange(KEYS.PROCESSING, 0, -1);
    expect(processingIds).toContain(jobId);

    // Simulate reaper logic: no live workers, job is old → schedule retry
    await removeFromProcessing(redis, jobId);
    await scheduleRetry(redis, jobId, 1, 'reaped: worker crashed');

    const inProcessing = await redis.lrange(KEYS.PROCESSING, 0, -1);
    expect(inProcessing).not.toContain(jobId);

    const score = await redis.zscore(KEYS.DELAYED, jobId);
    expect(score).not.toBeNull();
  });
});

