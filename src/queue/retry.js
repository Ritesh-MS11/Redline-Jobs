'use strict';

const KEYS = require('./keys');

/**
 * Retry + Dead-Letter queue helpers.
 *
 * Design decisions:
 *  - Exponential backoff: delay = baseMs * 2^(attempt-1), capped at maxDelayMs.
 *    This prevents thundering-herd when Redis or downstream services recover.
 *  - Delayed jobs live in a Sorted Set scored by their run-at unix-ms.
 *    A lightweight scheduler loop uses ZRANGEBYSCORE to promote due jobs back
 *    to queue:pending without scanning all jobs.
 *  - Dead-letter is a plain List (RPUSH) so dead jobs are ordered by when they
 *    died and can be paginated easily by the dashboard.
 */

const BASE_DELAY_MS = 1000;    // 1 s base
const MAX_DELAY_MS  = 60_000;  // 60 s ceiling

/**
 * Compute exponential backoff delay.
 * attempt=1 → 1 s, attempt=2 → 2 s, attempt=3 → 4 s, … capped at 60 s.
 */
function backoffDelay(attempt) {
  return Math.min(BASE_DELAY_MS * Math.pow(2, attempt - 1), MAX_DELAY_MS);
}

/**
 * Lua: atomically update job hash + add to delayed sorted set.
 * KEYS[1] = job:<id> hash
 * KEYS[2] = queue:delayed sorted set
 * ARGV[1] = jobId
 * ARGV[2] = new attempt count (string)
 * ARGV[3] = error message
 * ARGV[4] = run-at timestamp (ms, used as score)
 * ARGV[5] = now timestamp
 */
const SCHEDULE_RETRY_SCRIPT = `
redis.call('HSET', KEYS[1], 'status',    'delayed')
redis.call('HSET', KEYS[1], 'attempts',  ARGV[2])
redis.call('HSET', KEYS[1], 'lastError', ARGV[3])
redis.call('HSET', KEYS[1], 'updatedAt', ARGV[5])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
return 1
`;

/**
 * Schedule a failed job for retry via the delayed sorted set.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} jobId
 * @param {number} attempt  – The attempt number just failed (1-based).
 * @param {string} errorMsg
 */
async function scheduleRetry(redis, jobId, attempt, errorMsg) {
  const delay = backoffDelay(attempt);
  const runAt = Date.now() + delay;
  const now = Date.now();

  await redis.eval(
    SCHEDULE_RETRY_SCRIPT,
    2,
    KEYS.job(jobId),
    KEYS.DELAYED,
    jobId,
    String(attempt),
    errorMsg,
    String(runAt),
    String(now),
  );

  return { delay, runAt };
}

/**
 * Lua: atomically move job to dead-letter list and update hash.
 * KEYS[1] = job:<id> hash
 * KEYS[2] = queue:dead list
 * ARGV[1] = jobId
 * ARGV[2] = error message
 * ARGV[3] = now timestamp
 * ARGV[4] = attempt count
 */
const MOVE_TO_DEAD_SCRIPT = `
redis.call('HSET', KEYS[1], 'status',    'dead')
redis.call('HSET', KEYS[1], 'lastError', ARGV[2])
redis.call('HSET', KEYS[1], 'updatedAt', ARGV[3])
redis.call('HSET', KEYS[1], 'attempts',  ARGV[4])
redis.call('HSET', KEYS[1], 'deadAt',    ARGV[3])
redis.call('RPUSH', KEYS[2], ARGV[1])
return 1
`;

/**
 * Move a job to the dead-letter queue after exhausting all attempts.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} jobId
 * @param {number} attempt
 * @param {string} errorMsg
 */
async function moveToDead(redis, jobId, attempt, errorMsg) {
  const now = Date.now();
  await redis.eval(
    MOVE_TO_DEAD_SCRIPT,
    2,
    KEYS.job(jobId),
    KEYS.DEAD,
    jobId,
    errorMsg,
    String(now),
    String(attempt),
  );
}

/**
 * Promote all delayed jobs whose run-at score <= now into queue:pending.
 * Called by the scheduler loop in the worker process.
 *
 * Uses a Lua script for atomicity: ZRANGEBYSCORE + ZREM + RPUSH happen
 * together so a concurrent scheduler on another worker won't double-promote.
 *
 * KEYS[1] = queue:delayed
 * KEYS[2] = queue:pending
 * ARGV[1] = now (ms)
 * ARGV[2] = max jobs to promote per tick (batch size)
 * Returns: number of jobs promoted.
 */
const PROMOTE_DUE_SCRIPT = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due == 0 then return 0 end
redis.call('ZREM', KEYS[1], unpack(due))
for _, jobId in ipairs(due) do
  redis.call('HSET', 'job:' .. jobId, 'status', 'pending')
  redis.call('HSET', 'job:' .. jobId, 'updatedAt', ARGV[1])
  redis.call('RPUSH', KEYS[2], jobId)
end
return #due
`;

/**
 * @param {import('ioredis').Redis} redis
 * @param {number} batchSize
 * @returns {Promise<number>} number of jobs promoted
 */
async function promoteDueJobs(redis, batchSize = 100) {
  const count = await redis.eval(
    PROMOTE_DUE_SCRIPT,
    2,
    KEYS.DELAYED,
    KEYS.PENDING,
    String(Date.now()),
    String(batchSize),
  );
  return Number(count);
}

/**
 * Re-enqueue a dead job (manual retry from the dashboard / API).
 * Resets attempts to 0 and moves back to queue:pending.
 *
 * KEYS[1] = job:<id> hash
 * KEYS[2] = queue:dead list
 * KEYS[3] = queue:pending list
 * ARGV[1] = jobId
 * ARGV[2] = now
 */
const RETRY_DEAD_SCRIPT = `
local exists = redis.call('HGET', KEYS[1], 'id')
if not exists then return 0 end
redis.call('LREM', KEYS[2], 1, ARGV[1])
redis.call('HSET', KEYS[1], 'status',    'pending')
redis.call('HSET', KEYS[1], 'attempts',  0)
redis.call('HSET', KEYS[1], 'lastError', '')
redis.call('HSET', KEYS[1], 'updatedAt', ARGV[2])
redis.call('RPUSH', KEYS[3], ARGV[1])
return 1
`;

/**
 * @param {import('ioredis').Redis} redis
 * @param {string} jobId
 * @returns {Promise<boolean>} true if job was found and re-enqueued
 */
async function retryDeadJob(redis, jobId) {
  const result = await redis.eval(
    RETRY_DEAD_SCRIPT,
    3,
    KEYS.job(jobId),
    KEYS.DEAD,
    KEYS.PENDING,
    jobId,
    String(Date.now()),
  );
  return result === 1;
}

module.exports = { scheduleRetry, moveToDead, promoteDueJobs, retryDeadJob, backoffDelay };

