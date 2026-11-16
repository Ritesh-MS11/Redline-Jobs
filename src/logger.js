'use strict';

const winston = require('winston');

/**
 * Structured logger using winston.
 * Every log line is JSON so it's machine-parseable (useful when aggregating
 * logs from multiple worker instances in production).
 *
 * Usage:
 *   const logger = require('./logger').child({ workerId: 'w1', jobId: 'abc' });
 *   logger.info('processing started');
 */
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json(),
  ),
  defaultMeta: { service: 'job-queue' },
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
          const metaStr = Object.keys(meta).length ? ' ' + JSON.stringify(meta) : '';
          return `${timestamp} [${level}] ${message}${metaStr}`;
        }),
      ),
    }),
  ],
});

module.exports = logger;

