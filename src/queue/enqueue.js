'use strict';

const { v4: uuidv4 } = require('uuid');
const KEYS = require('./keys');

/**
 * Lua script: atomically check idempotency key and push to pending queue.
 *
 * KEYS[1] = idempotencyKey hash field (or '' if none)
 * KEYS[2] = queue:pending
 * KEYS[3] = idempotency:<key>  (only used when idempotencyKey provided)
 * ARGV[1] = jobId
 * ARGV[2] = idempotencyKey ('' if none)
 * ARGV[3] = idempotency TTL in seconds (86400 = 24 h)
 * Returns: 1 = enqueued, 0 = duplicate (idempotencyKey already exists)
 */
const ENQUEUE_SCRIPT = `
local ikey = ARGV[2]
if ikey ~= '' then
  local existing = redis.call('GET', KEYS[3])
  if existing then
    return {0, existing}
  end
  redis.call('SET', KEYS[3], ARGV[1], 'EX', ARGV[3])
end
redis.call('RPUSH', KEYS[2], ARGV[1])
return {1, ARGV[1]}
`;

/**
 * Enqueue a new job.
 *
 * @param {import('ioredis').Redis} redis
 * @param {{ type: string, payload: any, maxAttempts?: number, idempotencyKey?: string }} opts
 * @returns {Promise<{ jobId: string, duplicate: boolean }>}
 */
async function enqueue(redis, { type, payload, maxAttempts = 3, idempotencyKey = '' }) {
  if (!type) throw new Error('Job type is required');

  const jobId = uuidv4();
  const now = Date.now();

  // Store job metadata as a Redis hash
  const jobKey = KEYS.job(jobId);
  await redis.hmset(jobKey, {
    id: jobId,
    type,
    payload: JSON.stringify(payload),
    status: 'pending',
    attempts: 0,
    maxAttempts,
    createdAt: now,
    updatedAt: now,
    lastError: '',
    idempotencyKey,
  });

  const idempotencyRedisKey = idempotencyKey ? KEYS.idempotency(idempotencyKey) : '';

  // Atomically check idempotency + push to pending list
  const [result, resolvedId] = await redis.eval(
    ENQUEUE_SCRIPT,
    3,
    '',                        // KEYS[1] unused legacy slot
    KEYS.PENDING,              // KEYS[2]
    idempotencyRedisKey,       // KEYS[3]
    jobId,                     // ARGV[1]
    idempotencyKey,            // ARGV[2]
    86400,                     // ARGV[3]  idempotency TTL 24 h
  );

  if (result === 0) {
    // Duplicate — delete the hash we just created for the rejected job
    await redis.del(jobKey);
    return { jobId: resolvedId, duplicate: true };
  }

  return { jobId, duplicate: false };
}

module.exports = { enqueue };

