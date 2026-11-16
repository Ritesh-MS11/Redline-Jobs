/**
 * Central registry of Redis key names.
 * Keeping keys in one place prevents typo-driven bugs.
 */
'use strict';

const KEYS = {
  /** Job detail hash: job:<id> */
  job: (id) => `job:${id}`,

  /** Sorted set: idempotency key → jobId (score = expiry unix-ms) */
  idempotency: (key) => `idempotency:${key}`,

  /** Worker heartbeat hash: worker:<workerId> */
  workerHeartbeat: (workerId) => `worker:${workerId}:heartbeat`,

  PENDING: 'queue:pending',       // List  – FIFO queue of jobIds waiting to run
  PROCESSING: 'queue:processing', // List  – jobIds currently being executed
  DELAYED: 'queue:delayed',       // Sorted Set – jobIds scored by run-at timestamp (ms)
  DEAD: 'queue:dead',             // List  – jobIds that exhausted all retry attempts
  COMPLETED: 'queue:completed',   // List  – recently completed jobIds
  IDEMPOTENCY_INDEX: 'idempotency:index', // Set of active idempotency keys
};

module.exports = KEYS;

