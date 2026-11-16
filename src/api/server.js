'use strict';

const express = require('express');
const path = require('path');
const { createClient } = require('../queue/client');
const { buildRouter } = require('./routes');
const { getQueueStats, getRecentJobs } = require('../queue/stats');
const logger = require('../logger');

const PORT = Number(process.env.PORT) || 3000;

async function main() {
  const redis = createClient();
  const app = express();

  app.use(express.json());

  // Serve static dashboard files
  app.use('/dashboard', express.static(path.join(__dirname, '../../dashboard')));

  // Dashboard data endpoint (used by the dashboard HTML via fetch)
  app.get('/dashboard/data', async (_req, res) => {
    try {
      const [stats, recent] = await Promise.all([
        getQueueStats(redis),
        getRecentJobs(redis, 30),
      ]);
      res.json({ stats, recent });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Mount the job API routes
  app.use('/', buildRouter(redis));

  // Global error handler
  app.use((err, _req, res, _next) => {
    logger.error('unhandled express error', { error: err.message, stack: err.stack });
    res.status(500).json({ error: 'Internal server error' });
  });

  const server = app.listen(PORT, () => {
    logger.info(`API server listening`, { port: PORT });
  });

  // Graceful shutdown: stop accepting new connections, then close Redis
  const shutdown = async (signal) => {
    logger.info(`${signal} received, shutting down API`);
    server.close(async () => {
      await redis.quit();
      logger.info('API shutdown complete');
      process.exit(0);
    });
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
}

main().catch((err) => {
  console.error('Fatal API error:', err);
  process.exit(1);
});

