'use strict';

const { v4: uuidv4 } = require('uuid');
const { createClient } = require('../queue/client');
const { dequeue, acknowledge, removeFromProcessing } = require('../queue/dequeue');
const { scheduleRetry, moveToDead, promoteDueJobs } = require('../queue/retry');
const { registry } = require('../handlers');
const KEYS = require('../queue/keys');
const logger = require('../logger');

/** How long (ms) a job can stay in queue:processing before the Reaper reclaims it */
const VISIBILITY_TIMEOUT_MS = Number(process.env.VISIBILITY_TIMEOUT_MS) || 30_000; // 30 s

/** How often the worker heartbeats its in-flight job list (ms) */
const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS) || 5_000; // 5 s

/** How often the scheduler promotes due delayed jobs (ms) */
const SCHEDULER_INTERVAL_MS = Number(process.env.SCHEDULER_INTERVAL_MS) || 2_000; // 2 s

/** How often the Reaper scans for stuck processing jobs (ms) */
const REAPER_INTERVAL_MS = Number(process.env.REAPER_INTERVAL_MS) || 10_000; // 10 s

/** Max concurrent jobs this worker will run simultaneously */
const CONCURRENCY = Number(process.env.WORKER_CONCURRENCY) || 5;

class Worker {
  constructor() {
    this.workerId = process.env.WORKER_ID || `worker-${uuidv4().slice(0, 8)}`;
    this.log = logger.child({ workerId: this.workerId });

    // Separate connections:
    // - blockingRedis: used exclusively for BLMOVE (blocks the connection)
    // - redis: used for all other commands
    this.blockingRedis = createClient();
    this.redis = createClient();

    this.activeJobs = new Set();   // jobIds currently being processed by this worker
    this.running = false;
    this.shuttingDown = false;

    this._heartbeatTimer = null;
    this._schedulerTimer = null;
    this._reaperTimer = null;
  }

  async start() {
    this.running = true;
    this.log.info('worker starting', { concurrency: CONCURRENCY });

    this._startHeartbeat();
    this._startScheduler();
    this._startReaper();

    // Main loop: run up to CONCURRENCY jobs at once
    while (this.running) {
      if (this.shuttingDown) {
        // Wait for in-flight jobs to finish, then exit
        if (this.activeJobs.size === 0) break;
        await sleep(200);
        continue;
      }

      const available = CONCURRENCY - this.activeJobs.size;
      if (available <= 0) {
        // All slots busy — wait a tick
        await sleep(100);
        continue;
      }

      // Try to dequeue. BLMOVE blocks for up to 5 s if the queue is empty,
      // which avoids busy-looping while still allowing graceful shutdown checks.
      const jobId = await dequeue(this.blockingRedis, 2);
      if (!jobId) continue; // timeout — loop back and check shutdownflag

      // Run the job without awaiting so we can dequeue the next one immediately
      this._processJob(jobId);
    }

    await this._cleanup();
  }

  async _processJob(jobId) {
    this.activeJobs.add(jobId);
    const jobLog = this.log.child({ jobId });

    try {
      const rawJob = await this.redis.hgetall(KEYS.job(jobId));
      if (!rawJob || !rawJob.id) {
        jobLog.warn('job hash missing, skipping');
        await removeFromProcessing(this.redis, jobId);
        return;
      }

      const { type, maxAttempts } = rawJob;
      const attempts = Number(rawJob.attempts) + 1; // increment for this run

      // Update status → processing + attempts count + workerId
      await this.redis.hmset(KEYS.job(jobId), {
        status: 'processing',
        attempts,
        workerId: this.workerId,
        startedAt: Date.now(),
        updatedAt: Date.now(),
      });

      const handler = registry[type];
      if (!handler) {
        const errMsg = `No handler registered for job type "${type}"`;
        jobLog.error(errMsg, { type });
        await this._handleFailure(jobId, attempts, Number(maxAttempts), errMsg);
        return;
      }

      jobLog.info('processing job', { type, attempts, maxAttempts });

      let payload;
      try { payload = JSON.parse(rawJob.payload); } catch { payload = rawJob.payload; }

      const result = await handler(payload, jobId);

      // Success path
      await acknowledge(this.redis, jobId);
      jobLog.info('job completed', { type, attempts, result });

    } catch (err) {
      const rawJob = await this.redis.hgetall(KEYS.job(jobId));
      const attempts = Number(rawJob?.attempts ?? 1);
      const maxAttempts = Number(rawJob?.maxAttempts ?? 3);
      await removeFromProcessing(this.redis, jobId);
      await this._handleFailure(jobId, attempts, maxAttempts, err.message);
      this.log.child({ jobId }).error('job failed', { error: err.message, attempts, maxAttempts });
    } finally {
      this.activeJobs.delete(jobId);
    }
  }

  /**
   * Decide whether to retry (delayed queue) or dead-letter a failed job.
   */
  async _handleFailure(jobId, attempts, maxAttempts, errorMsg) {
    if (attempts < maxAttempts) {
      const { delay } = await scheduleRetry(this.redis, jobId, attempts, errorMsg);
      this.log.child({ jobId }).info('scheduled retry', { attempts, maxAttempts, delayMs: delay });
    } else {
      await moveToDead(this.redis, jobId, attempts, errorMsg);
      this.log.child({ jobId }).warn('job moved to dead-letter', { attempts, maxAttempts, errorMsg });
    }
  }

  // ── Heartbeat ─────────────────────────────────────────────────────────────
  // Each worker writes its set of in-flight jobIds + a timestamp to Redis.
  // The Reaper reads this to know which jobs are "legitimately" in processing.
  _startHeartbeat() {
    this._heartbeatTimer = setInterval(async () => {
      try {
        const hbKey = KEYS.workerHeartbeat(this.workerId);
        await this.redis.hmset(hbKey, {
          activeJobs: JSON.stringify([...this.activeJobs]),
          lastSeen:   Date.now(),
        });
        await this.redis.expire(hbKey, Math.ceil((VISIBILITY_TIMEOUT_MS * 2) / 1000));
      } catch (err) {
        this.log.error('heartbeat error', { error: err.message });
      }
    }, HEARTBEAT_INTERVAL_MS);
  }

  // ── Scheduler ─────────────────────────────────────────────────────────────
  // Promotes delayed jobs whose run-at time has passed back to queue:pending.
  // Running this inside the worker (vs. a separate process) keeps the system
  // self-contained. Multiple workers will race, but the Lua script is atomic
  // so only one will win per batch, preventing double-promotion.
  _startScheduler() {
    this._schedulerTimer = setInterval(async () => {
      try {
        const count = await promoteDueJobs(this.redis);
        if (count > 0) this.log.info('scheduler promoted delayed jobs', { count });
      } catch (err) {
        this.log.error('scheduler error', { error: err.message });
      }
    }, SCHEDULER_INTERVAL_MS);
  }

  // ── Reaper ────────────────────────────────────────────────────────────────
  // Scans queue:processing for jobs that have been there longer than
  // VISIBILITY_TIMEOUT_MS without a live worker heartbeat claiming them.
  // This handles crashed workers that never called acknowledge() or
  // scheduleRetry().
  _startReaper() {
    this._reaperTimer = setInterval(async () => {
      try {
        await this._reapStuckJobs();
      } catch (err) {
        this.log.error('reaper error', { error: err.message });
      }
    }, REAPER_INTERVAL_MS);
  }

  async _reapStuckJobs() {
    const processingIds = await this.redis.lrange(KEYS.PROCESSING, 0, -1);
    if (processingIds.length === 0) return;

    // Collect all job IDs that are claimed by a live worker
    const workerKeys = await this.redis.keys('worker:*:heartbeat');
    const liveJobIds = new Set();

    for (const wk of workerKeys) {
      const hb = await this.redis.hgetall(wk);
      if (!hb) continue;
      const lastSeen = Number(hb.lastSeen || 0);
      if (Date.now() - lastSeen < VISIBILITY_TIMEOUT_MS) {
        // Worker is alive — trust its activeJobs list
        try {
          const jobs = JSON.parse(hb.activeJobs || '[]');
          jobs.forEach((id) => liveJobIds.add(id));
        } catch {}
      }
    }

    for (const jobId of processingIds) {
      if (liveJobIds.has(jobId)) continue; // alive worker has it

      // Job is in processing but no live worker owns it
      const rawJob = await this.redis.hgetall(KEYS.job(jobId));
      if (!rawJob || !rawJob.id) {
        // Orphaned entry — just remove it
        await this.redis.lrem(KEYS.PROCESSING, 1, jobId);
        continue;
      }

      const startedAt = Number(rawJob.startedAt || 0);
      const age = Date.now() - startedAt;
      if (age < VISIBILITY_TIMEOUT_MS) continue; // too young to reap

      this.log.warn('reaper reclaiming stuck job', { jobId, ageMs: age });

      const attempts    = Number(rawJob.attempts   ?? 1);
      const maxAttempts = Number(rawJob.maxAttempts ?? 3);

      await removeFromProcessing(this.redis, jobId);
      await this._handleFailure(jobId, attempts, maxAttempts, 'reaped: worker crashed or timed out');
    }
  }

  // ── Graceful shutdown ─────────────────────────────────────────────────────
  async shutdown(signal) {
    this.log.info(`${signal} received — graceful shutdown initiated`);
    this.shuttingDown = true;

    clearInterval(this._heartbeatTimer);
    clearInterval(this._schedulerTimer);
    clearInterval(this._reaperTimer);

    // Give in-flight jobs up to 30 s to finish
    const deadline = Date.now() + 30_000;
    while (this.activeJobs.size > 0 && Date.now() < deadline) {
      this.log.info('waiting for in-flight jobs', { count: this.activeJobs.size });
      await sleep(500);
    }

    if (this.activeJobs.size > 0) {
      this.log.warn('shutdown deadline reached; some jobs may be re-reaped', {
        jobs: [...this.activeJobs],
      });
    }

    this.running = false;
  }

  async _cleanup() {
    try {
      // Remove heartbeat key so the Reaper doesn't falsely reclaim jobs
      await this.redis.del(KEYS.workerHeartbeat(this.workerId));
      await this.blockingRedis.quit();
      await this.redis.quit();
    } catch {}
    this.log.info('worker shutdown complete');
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

module.exports = { Worker };

