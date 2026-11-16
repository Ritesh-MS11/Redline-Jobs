'use strict';

const KEYS = require('./keys');

/**
 * Returns queue length statistics for all queues.
 *
 * @param {import('ioredis').Redis} redis
 * @returns {Promise<{ pending: number, processing: number, delayed: number, dead: number }>}
 */
async function getQueueStats(redis) {
  const pipeline = redis.pipeline();
  pipeline.llen(KEYS.PENDING);
  pipeline.llen(KEYS.PROCESSING);
  pipeline.zcard(KEYS.DELAYED);
  pipeline.llen(KEYS.DEAD);
  pipeline.llen(KEYS.COMPLETED);
  const results = await pipeline.exec();
  // Each result is [error, value]
  const [pending, processing, delayed, dead, completed] = results.map(([, v]) => Number(v));
  return { pending, processing, delayed, dead, completed: completed || 0 };
}

/**
 * Fetch recent job hashes from queue lists for the dashboard.
 * Returns up to `limit` jobIds from each queue list.
 *
 * @param {import('ioredis').Redis} redis
 * @param {number} limit
 */
async function getRecentJobs(redis, limit = 20) {
  const pipeline = redis.pipeline();
  // LRANGE 0 limit-1 → most-recently-added (right end) of each list
  pipeline.lrange(KEYS.PENDING, -limit, -1);
  pipeline.lrange(KEYS.PROCESSING, -limit, -1);
  pipeline.lrange(KEYS.DEAD, -limit, -1);
  // For delayed, grab the soonest due (lowest score)
  pipeline.zrange(KEYS.DELAYED, 0, limit - 1);
  // For completed, grab the most recently completed (left end)
  pipeline.lrange(KEYS.COMPLETED, 0, limit - 1);

  const [[, pendingIds], [, processingIds], [, deadIds], [, delayedIds], [, completedIds]] =
    await pipeline.exec();

  // Fetch all job hashes in one round-trip
  const allIds = [...new Set([
    ...(pendingIds || []),
    ...(processingIds || []),
    ...(deadIds || []),
    ...(delayedIds || []),
    ...(completedIds || []),
  ])];
  if (allIds.length === 0) return { pending: [], processing: [], delayed: [], dead: [], completed: [] };

  const p2 = redis.pipeline();
  for (const id of allIds) p2.hgetall(KEYS.job(id));
  const rawJobs = await p2.exec();

  const jobMap = {};
  allIds.forEach((id, i) => {
    const job = rawJobs[i][1];
    if (job && job.id) {
      job.payload = safeParseJSON(job.payload);
      jobMap[id] = job;
    }
  });

  return {
    pending:    (pendingIds || []).map((id) => jobMap[id]).filter(Boolean),
    processing: (processingIds || []).map((id) => jobMap[id]).filter(Boolean),
    delayed:    (delayedIds || []).map((id) => jobMap[id]).filter(Boolean),
    dead:       (deadIds || []).map((id) => jobMap[id]).filter(Boolean),
    completed:  (completedIds || []).map((id) => jobMap[id]).filter(Boolean),
  };
}

function safeParseJSON(str) {
  try { return JSON.parse(str); } catch { return str; }
}

module.exports = { getQueueStats, getRecentJobs };

