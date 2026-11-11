/* Copyright (C) 2016 NooBaa */
'use strict';

const config = require('../../../config.js');

/**
 * One background task queue, shared by producers (enqueue) and the executor (dequeue).
 * This is the common definition: queue name, scheduling weight, and connect defaults.
 * The executor-side behavior (building the worker that runs each message) lives in bg_task_executor,
 * because it references the replication code, which this shared module must not import or it would form a require cycle.
 * @typedef {{
 *   queue: string,
 *   weight: number,
 *   defaults: {
 *     visibility_ms: number,
 *     retry_limit: number,
 *     dead_retention_ms: number,
 *     retry_delay_ms: number,
 *   },
 * }} BgTaskSpec
 */

/**
 * Background task queues. Producers read queue/defaults to enqueue; the executor reads all of it to dequeue and run.
 * Tests replace this export with their own map.
 * @type {Record<string, BgTaskSpec>}
 */
const BG_TASK_QUEUES = {
    replication: {
        // Postgres table for the replication queue. Producers and the executor use this name.
        queue: 'replication_mq',
        weight: config.REPLICATION_EXECUTOR_WEIGHT,
        // Inline, not a nested config object: the config override mechanism updates the flat values, not an object.
        defaults: {
            visibility_ms: config.REPLICATION_QUEUE_VISIBILITY_MS,
            retry_limit: config.REPLICATION_QUEUE_RETRY_LIMIT,
            dead_retention_ms: config.REPLICATION_QUEUE_DEAD_RETENTION_MS,
            retry_delay_ms: config.REPLICATION_QUEUE_RETRY_DELAY_MS,
        },
    },
};

exports.BG_TASK_QUEUES = BG_TASK_QUEUES;
