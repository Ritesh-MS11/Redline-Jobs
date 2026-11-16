'use strict';

const Redis = require('ioredis');

/**
 * Creates a fresh ioredis connection.
 * We call this factory function instead of exporting a singleton so that
 * the worker process can open a *blocking* connection (used for BLMOVE) while
 * the API and scheduler use separate connections — ioredis blocks the whole
 * connection during a BLMOVE, so mixing blocking + non-blocking on one client
 * causes starvation.
 */
function createClient(options = {}) {
  const client = new Redis({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
    // Automatically reconnect with exponential backoff
    retryStrategy: (times) => Math.min(times * 100, 3000),
    // Surface connection errors to the caller instead of silently swallowing
    lazyConnect: false,
    ...options,
  });

  client.on('error', (err) => {
    // Don't crash on transient redis errors; the retry strategy handles reconnect
    console.error('[redis] connection error', err.message);
  });

  return client;
}

module.exports = { createClient };

