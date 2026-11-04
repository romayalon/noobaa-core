/* Copyright (C) 2016 NooBaa */
'use strict';

/** @typedef {typeof import('../../sdk/nb')} nb */

const dbg = require('../../util/debug_module')(__filename);
const config = require('../../../config.js');
const message_queue_client = require('../../util/message_queue_client');
const system_store = require('../system_services/system_store').get_instance();
const system_utils = require('../utils/system_utils');
const background_scheduler = require('../../util/background_scheduler').get_instance();

/**
 * @typedef {{
 *   name: string,
 *   queue: string,
 *   execute: (message: nb.QueueMessage) => Promise<void>,
 * }} BgTaskType
 */

const CONSUME_RESULT = Object.freeze({ EMPTY: 'EMPTY', OK: 'OK', ERROR: 'ERROR' });

/**
 * Dequeues background tasks on config.BG_EXECUTOR_WORKERS loops.
 * Each turn a loop draws a random number and picks a queue from config.BG_EXECUTOR_WEIGHTS.
 */
class BgTaskExecutor {

    /**
     * @param {nb.APIClient} rpc_client
     * @param {BgTaskType[]} task_types
     */
    constructor(rpc_client, task_types) {
        if (!rpc_client) throw new Error('bg_task_executor: rpc_client is required');
        if (!Array.isArray(task_types)) throw new Error('bg_task_executor: task_types is required');
        /** @type {nb.APIClient} */
        this.rpc_client = rpc_client;
        /** @type {nb.MessageQueueClient | undefined} */
        this.mq_client = undefined;
        /** @type {BgTaskType[]} */
        this.task_types = task_types;
    }

    /**
     * Connect the queues that have a positive weight and start the configured workers.
     * @returns {Promise<void>}
     */
    async init() {
        const bands = weighted_tasks(this.task_types, config.BG_EXECUTOR_WEIGHTS);
        if (!bands.length) {
            dbg.log0('bg_task_executor: init: no weighted tasks');
            return;
        }
        const worker_count = config.BG_EXECUTOR_WORKERS;
        if (!(worker_count >= 1)) {
            dbg.log0('bg_task_executor: init: BG_EXECUTOR_WORKERS is', worker_count);
            return;
        }
        this.mq_client = message_queue_client.instance();
        if (!this.mq_client) throw new Error('bg_task_executor: init: message queue client is not configured');

        const queues = new Set(bands.map(band => band.task.queue));
        for (const queue of queues) {
            await this.mq_client.connect(queue);
        }

        for (let i = 0; i < worker_count; ++i) {
            background_scheduler.register_bg_worker({
                name: `bg executor ${i}`,
                delay: config.BG_EXECUTOR_EMPTY_DELAY,
                run_immediate: true,
            }, () => this.run_task());
        }
        dbg.log0('bg_task_executor: init:', worker_count, bands.map(band => band.task.name).join(', '));
    }

    /**
     * @returns {Promise<number>}
     */
    async run_task() {
        if (!this.can_run()) return config.BG_EXECUTOR_EMPTY_DELAY;
        const task_type = this.choose_task(Math.random());
        if (!task_type) return config.BG_EXECUTOR_EMPTY_DELAY;
        const result = await this.consume_message(task_type);
        if (result === CONSUME_RESULT.ERROR) return config.BG_EXECUTOR_ERROR_DELAY;
        if (result === CONSUME_RESULT.OK) return config.BG_EXECUTOR_BATCH_DELAY;
        return config.BG_EXECUTOR_EMPTY_DELAY;
    }

    /**
     * @param {number} random a draw in [0, 1)
     * @returns {BgTaskType | undefined}
     */
    choose_task(random) {
        return choose_weighted_task(this.task_types, config.BG_EXECUTOR_WEIGHTS, random);
    }

    /**
     * Dequeue one message for the chosen task and run its execute.
     * @param {BgTaskType} task_type
     * @returns {Promise<typeof CONSUME_RESULT.EMPTY | typeof CONSUME_RESULT.OK | typeof CONSUME_RESULT.ERROR>}
     */
    async consume_message(task_type) {
        const mq_client = this.mq_client;
        if (!mq_client) throw new Error('bg_task_executor: consume_message: message queue client is not configured');

        const message = await mq_client.dequeue(task_type.queue);
        if (!message) return CONSUME_RESULT.EMPTY;

        if (mq_client.is_retry_limit_exceeded(message)) {
            dbg.warn('bg_task_executor: consume_message: dropping task after retry limit',
                task_type.name, message._id, message.retry_count);
            const nacked = await this.nack_message(task_type, message, 'retry limit exceeded');
            return nacked ? CONSUME_RESULT.OK : CONSUME_RESULT.ERROR;
        }

        try {
            dbg.log1('bg_task_executor: consume_message: executing task', task_type.name, message._id);
            await task_type.execute(message);
        } catch (err) {
            dbg.error('bg_task_executor: consume_message: execution failed', task_type.name, message._id, err);
            const reason = err instanceof Error ? err.message : String(err);
            await this.nack_message(task_type, message, reason);
            return CONSUME_RESULT.ERROR;
        }

        try {
            await mq_client.ack(message);
        } catch (err) {
            dbg.error('bg_task_executor: consume_message: failed to ack', task_type.name, message._id, err);
            return CONSUME_RESULT.ERROR;
        }
        return CONSUME_RESULT.OK;
    }

    /**
     * @returns {boolean}
     */
    can_run() {
        if (!system_store.is_finished_initial_load) {
            dbg.log0('bg_task_executor: can_run:', 'system_store did not finish initial load');
            return false;
        }
        const system = system_store.data.systems[0];
        if (!system || system_utils.system_in_maintenance(system._id)) return false;
        return true;
    }

    /**
     * @param {BgTaskType} task
     * @param {nb.QueueMessage} message
     * @param {string} reason
     * @returns {Promise<boolean>}
     */
    async nack_message(task, message, reason) {
        const mq_client = this.mq_client;
        if (!mq_client) {
            dbg.error('bg_task_executor: nack_message: message queue client is not configured', task.name, message._id);
            return false;
        }
        try {
            await mq_client.nack(message, reason);
            return true;
        } catch (err) {
            dbg.error('bg_task_executor: nack_message: failed to nack', task.name, message._id, err);
            return false;
        }
    }
}

/** @type {BgTaskExecutor | undefined} */
let _instance;

/**
 * @param {nb.APIClient} rpc_client
 * @param {BgTaskType[]} task_types
 * @returns {BgTaskExecutor}
 */
function get_instance(rpc_client, task_types) {
    _instance = _instance || new BgTaskExecutor(rpc_client, task_types);
    return _instance;
}

exports.BgTaskExecutor = BgTaskExecutor;
exports.CONSUME_RESULT = CONSUME_RESULT;
exports.get_instance = get_instance;

/**
 * Tasks with a positive configured percent, in weight-map order.
 * @param {BgTaskType[]} task_types
 * @param {Readonly<Record<string, number>>} weights
 * @returns {{ task: BgTaskType, weight: number }[]}
 */
function weighted_tasks(task_types, weights) {
    /** @type {{ task: BgTaskType, weight: number }[]} */
    const bands = [];
    for (const name of Object.keys(weights || {})) {
        const task = task_types.find(item => item.name === name);
        const weight = Number(weights[name]);
        if (!task || !(weight > 0)) continue;
        bands.push({ task, weight });
    }
    return bands;
}

/**
 * The first map entry owns the low end of [0, 1). A draw above that share
 * falls through to the next entry. With transition at 20 and replication at 80,
 * a draw above 0.2 selects replication. A draw past the covered percent is idle.
 * @param {BgTaskType[]} task_types
 * @param {Readonly<Record<string, number>>} weights
 * @param {number} random
 * @returns {BgTaskType | undefined}
 */
function choose_weighted_task(task_types, weights, random) {
    const bands = weighted_tasks(task_types, weights);
    let cursor = 0;
    for (let i = 0; i < bands.length; ++i) {
        cursor += bands[i].weight / 100;
        const covers_rest = i === bands.length - 1 && cursor >= 1 - 1e-9;
        if (random <= cursor || covers_rest) return bands[i].task;
    }
    return undefined;
}
