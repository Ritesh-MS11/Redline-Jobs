'use strict';

const express = require('express');
const { enqueue } = require('../queue/enqueue');
const { retryDeadJob } = require('../queue/retry');
const { getQueueStats } = require('../queue/stats');
const KEYS = require('../queue/keys');
const logger = require('../logger');

/**
 * Build the Express router for the producer API.
 *
 * @param {import('ioredis').Redis} redis
 */
function buildRouter(redis) {
  const router = express.Router();

  // ── POST /jobs ────────────────────────────────────────────────────────────
  // Enqueue a new job. Returns immediately with the jobId (fire-and-forget
  // from the caller's perspective) — this is the fundamental advantage of a
  // job queue over in-process async: the HTTP response is decoupled from job
  // execution time.
  router.post('/jobs', async (req, res) => {
    try {
      const { type, payload, maxAttempts, idempotencyKey } = req.body;
      if (!type) return res.status(400).json({ error: 'type is required' });

      const { jobId, duplicate } = await enqueue(redis, {
        type,
        payload: payload ?? {},
        maxAttempts: maxAttempts ?? 3,
        idempotencyKey: idempotencyKey ?? '',
      });

      logger.info('job enqueued', { jobId, type, duplicate });
      return res.status(duplicate ? 200 : 201).json({ jobId, duplicate });
    } catch (err) {
      logger.error('enqueue error', { error: err.message });
      return res.status(500).json({ error: err.message });
    }
  });

  // ── GET /jobs/:id ─────────────────────────────────────────────────────────
  // Polling endpoint: clients poll this until status is completed/dead.
  // A production system would layer on WebSockets or SSE for push, but
  // polling is simpler and works well for infrequent status checks.
  router.get('/jobs/:id', async (req, res) => {
    try {
      const { id } = req.params;
      const job = await redis.hgetall(KEYS.job(id));
      if (!job || !job.id) return res.status(404).json({ error: 'Job not found' });

      // Parse payload JSON back to object for API consumers
      try { job.payload = JSON.parse(job.payload); } catch {}
      job.attempts = Number(job.attempts);
      job.maxAttempts = Number(job.maxAttempts);

      return res.json(job);
    } catch (err) {
      logger.error('get job error', { error: err.message });
      return res.status(500).json({ error: err.message });
    }
  });

  // ── GET /queue/stats ──────────────────────────────────────────────────────
  router.get('/queue/stats', async (_req, res) => {
    try {
      const stats = await getQueueStats(redis);
      return res.json(stats);
    } catch (err) {
      logger.error('stats error', { error: err.message });
      return res.status(500).json({ error: err.message });
    }
  });

  // ── POST /jobs/:id/retry ──────────────────────────────────────────────────
  // Re-enqueue a dead job (manual override). Resets attempt counter.
  router.post('/jobs/:id/retry', async (req, res) => {
    try {
      const { id } = req.params;
      const ok = await retryDeadJob(redis, id);
      if (!ok) return res.status(404).json({ error: 'Job not found or not in dead queue' });

      logger.info('dead job requeued', { jobId: id });
      return res.json({ jobId: id, status: 'pending' });
    } catch (err) {
      logger.error('retry error', { error: err.message });
      return res.status(500).json({ error: err.message });
    }
  });

  return router;
}

module.exports = { buildRouter };

