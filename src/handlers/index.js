'use strict';

const logger = require('../logger');

/**
 * Simulates sending an email.
 * Always succeeds after a brief async pause.
 *
 * @param {{ to: string, subject: string, body: string }} payload
 * @param {string} jobId
 */
async function sendEmail(payload, jobId) {
  const { to = 'unknown@example.com', subject = '(no subject)' } = payload;
  // Simulate network I/O latency (50-200 ms)
  await sleep(50 + Math.random() * 150);
  logger.info('email sent', { jobId, to, subject });
  return { sent: true, to, subject };
}

/**
 * Simulates resizing an image.
 * Always succeeds after a moderate delay.
 *
 * @param {{ url: string, width: number, height: number }} payload
 * @param {string} jobId
 */
async function resizeImage(payload, jobId) {
  const { url = 'unknown', width = 800, height = 600 } = payload;
  // Simulate CPU-ish work (100-400 ms)
  await sleep(100 + Math.random() * 300);
  logger.info('image resized', { jobId, url, width, height });
  return { resized: true, url, width, height };
}

/**
 * A deliberately flaky handler that fails ~40% of the time.
 * Useful for exercising the retry + dead-letter paths.
 *
 * @param {any} payload
 * @param {string} jobId
 */
async function flakyHandler(payload, jobId) {
  await sleep(50 + Math.random() * 100);
  if (Math.random() < 0.4) {
    throw new Error(`flaky failure [jobId=${jobId}] – this is intentional`);
  }
  logger.info('flaky handler succeeded', { jobId });
  return { flaky: true, succeeded: true };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Handler registry — maps job type strings to handler functions.
 * Adding a new job type is as simple as adding an entry here.
 *
 * Handler signature: async (payload, jobId) => result
 */
const registry = {
  sendEmail,
  resizeImage,
  flaky: flakyHandler,
};

module.exports = { registry };

