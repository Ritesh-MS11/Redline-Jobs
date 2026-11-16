'use strict';

const KEYS = require('./keys');

/**
 * Atomically move one jobId from queue:pending → queue:processing.
 *
 * We use BRPOPLPUSH (blocking right-pop / left-push) which is available
 * since Redis 2.2 and works on the Windows Redis 3.x portable build.
 * Note: BRPOPLPUSH pops from the RIGHT of source and pushes to the LEFT
 * of destination. For a FIFO queue we want to pop from the LEFT of pending,
 * so we maintain pending as a right-push / left-pop list via BLPOP + LPUSH
 * emulated atomically via a Lua script for non-blocking, and BRPOPLPUSH
 * for the blocking path (treating pending as a stack is fine — ordering
 * within a single worker poll doesn't matter for correctness).
 *
 * @param {import('ioredis').Redis} redis  – Should be a *dedicated* blocking connection.
 * @param {number} timeoutSeconds          – How long to block waiting for a job (0 = forever).
 * @returns {Promise<string|null>}         – jobId or null on timeout.
 */
async function dequeue(redis, timeoutSeconds = 5) {
  // BRPOPLPUSH source destination timeout
  // Pops from right of PENDING, pushes to left of PROCESSING — atomic.
  // Returns the element moved, or null on timeout.
  const jobId = await redis.brpoplpush(
    KEYS.PENDING,
    KEYS.PROCESSING,
    timeoutSeconds,
  );
  return jobId; // null when timeout expires with no job
}

/**
 * Acknowledge successful completion: remove jobId from processing list
 * and update the job hash status.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} jobId
 */
async function acknowledge(redis, jobId) {
  const pipeline = redis.pipeline();
  pipeline.lrem(KEYS.PROCESSING, 1, jobId);
  pipeline.lpush(KEYS.COMPLETED, jobId);
  pipeline.ltrim(KEYS.COMPLETED, 0, 499);
  pipeline.hmset(KEYS.job(jobId), {
    status: 'completed',
    updatedAt: Date.now(),
    completedAt: Date.now(),
  });
  await pipeline.exec();
}

/**
 * Remove a jobId from the processing list (called on failure path before
 * scheduling retry or moving to dead-letter).
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} jobId
 */
async function removeFromProcessing(redis, jobId) {
  await redis.lrem(KEYS.PROCESSING, 1, jobId);
}

module.exports = { dequeue, acknowledge, removeFromProcessing };

