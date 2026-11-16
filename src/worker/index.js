'use strict';

const { Worker } = require('./worker');

const worker = new Worker();

process.on('SIGTERM', async () => {
  await worker.shutdown('SIGTERM');
  process.exit(0);
});
process.on('SIGINT', async () => {
  await worker.shutdown('SIGINT');
  process.exit(0);
});

worker.start().catch((err) => {
  console.error('Worker fatal error:', err);
  process.exit(1);
});

