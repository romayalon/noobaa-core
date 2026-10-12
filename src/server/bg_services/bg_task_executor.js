/* Copyright (C) 2016 NooBaa */
'use strict';

const dbg = require('../../util/debug_module')(__filename);
const config = require('../../../config.js');
const message_queue_client = require('../../util/message_queue_client');
const system_store = require('../system_services/system_store').get_instance();
const system_utils = require('../utils/system_utils');
const background_scheduler = require('../../util/background_scheduler').get_instance();
const { REPLICATION_TASK_KIND } = require('../utils/replication_utils');
const { ReplicationWorker } = require('./replication_scanner');
const { LogReplicationWorker } = require('./log_replication_scanner');

/**
 * @typedef {{
*      name: string, 
*      queue: string, 
*      execute: (message: nb.QueueMessage) => Promise<void>,
* }} BgTaskType
*/

/** @typedef {Parameters<ReplicationWorker['execute_task']>[0]} ScanReplicateTask */
/** @typedef {Parameters<LogReplicationWorker['execute_task']>[0]} LogReplicateTask */

const CONSUME_RESULT = Object.freeze({ EMPTY: 'EMPTY', OK: 'OK', ERROR: 'ERROR' });

/**
 * Dequeues background tasks and runs each task's execute method.
 * init connects the queue client and starts one worker per task.
 */
class BgTaskExecutor {

    /**
     * @param {nb.APIClient} rpc_client
     */
    constructor(rpc_client) {
        /** @type {nb.APIClient} RPC client. */
        this.rpc_client = rpc_client;
        /** @type {nb.MessageQueueClient | undefined} Message queue client. Set in init. */
        this.mq_client = undefined;
        this.replication_scanner = config.REPLICATION_ENABLED ? new ReplicationWorker({ name: 'replication worker', client: this.rpc_client, commit_scan_status: false }) : undefined;
        this.log_replication_scanner = config.LOG_REPLICATION_ENABLED ? new LogReplicationWorker({ name: 'log replication worker', client: this.rpc_client }) : undefined;
        /** @type {BgTaskType[]} Enabled background tasks. */
        this.task_types = this.get_enabled_bg_tasks();
    }

    /**
     * init connects the process queue client and starts a worker for each task type.
     * @returns {Promise<void>}
     */
    async init() {
        this.mq_client = message_queue_client.instance();
        if (!this.mq_client) throw new Error('bg_task_executor: init: message queue client is not configured');

        const queues = new Set(this.task_types.map(task_type => task_type.queue));
        for (const queue of queues) {
            await this.mq_client.connect(queue);
        }

        for (const task_type of this.task_types) {
            background_scheduler.register_bg_worker({
                name: `bg executor ${task_type.name}`,
                delay: config.BG_EXECUTOR_EMPTY_DELAY,
                run_immediate: true,
            }, () => this.run_task(task_type));
        }
        dbg.log0('bg_task_executor: init:', this.task_types.map(task_type => task_type.name).join(', '));
    }

    /**
     * consume_message is a general purpose method to dequeue one message for a task and run its execute.
     * @param {BgTaskType} task
     * @returns {Promise<typeof CONSUME_RESULT.EMPTY | typeof CONSUME_RESULT.OK | typeof CONSUME_RESULT.ERROR>}
     */
    async consume_message(task) {
        const mq_client = this.mq_client;
        if (!mq_client) throw new Error('bg_task_executor: consume_message: message queue client is not configured');

        const message = await mq_client.dequeue(task.queue);
        if (!message) return CONSUME_RESULT.EMPTY;

        if (mq_client.is_retry_limit_exceeded(message)) {
            dbg.warn('bg_task_executor: consume_message: dropping task after retry limit', task.name, message._id, message.retry_count);
            const nacked = await this.nack_message(task, message, 'retry limit exceeded');
            return nacked ? CONSUME_RESULT.OK : CONSUME_RESULT.ERROR;
        }

        try {
            dbg.log1('bg_task_executor: consume_message: executing task', task.name, message._id);
            await task.execute(message);
        } catch (err) {
            dbg.error('bg_task_executor: consume_message: execution failed', task.name, message._id, err);
            const reason = err instanceof Error ? err.message : String(err);
            await this.nack_message(task, message, reason);
            return CONSUME_RESULT.ERROR;
        }

        try {
            await mq_client.ack(message);
        } catch (err) {
            dbg.error('bg_task_executor: consume_message: failed to ack', task.name, message._id, err);
            return CONSUME_RESULT.ERROR;
        }
        return CONSUME_RESULT.OK;
    }

    /**
     * Run queued messages until the queue is empty or the limit is reached.
     * @param {BgTaskType} task
     * @param {number} [limit] Stops after this many messages. Defaults to BG_EXECUTOR_MAX_MESSAGES.
     * @returns {Promise<void>}
     */
    async consume_until_empty(task, limit = config.BG_EXECUTOR_MAX_MESSAGES) {
        for (let i = 0; i < limit; i++) {
            const result = await this.consume_message(task);
            if (result === CONSUME_RESULT.EMPTY) return;
            if (result === CONSUME_RESULT.ERROR) throw new Error(`task ${task.name} failed`);
        }
        throw new Error(`bg_task_executor: consume_until_empty: task ${task.name} stopped after ${limit} messages`);
    }

    /**
     * @param {BgTaskType} task
     * @returns {Promise<number | undefined>}
     */
    async run_task(task) {
        if (!this.can_run()) return config.BG_EXECUTOR_EMPTY_DELAY;
        const result = await this.consume_message(task);
        if (result === CONSUME_RESULT.ERROR) return config.BG_EXECUTOR_ERROR_DELAY;
        if (result === CONSUME_RESULT.OK) return config.BG_EXECUTOR_BATCH_DELAY;
        return config.BG_EXECUTOR_EMPTY_DELAY;
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
     * nack the claimed message. A queue error is logged here so the worker keeps running.
     * @param {BgTaskType} task
     * @param {nb.QueueMessage} message
     * @param {string} reason
     * @returns {Promise<boolean>}
     */
    async nack_message(task, message, reason) {
        try {
            await this.mq_client.nack(message, reason);
            return true;
        } catch (err) {
            dbg.error('bg_task_executor: nack_message: failed to nack', task.name, message._id, err);
            return false;
        }
    }

    /**
     * @returns {BgTaskType[]}
     */
    get_enabled_bg_tasks() {
        if (!config.REPLICATION_ENABLED && !config.LOG_REPLICATION_ENABLED) return [];

        // Both kinds share replication_mq, so one dequeue loop calls only an enabled worker.
        const task = {
            name: 'replicate',
            queue: config.REPLICATION_QUEUE_NAME,
            /**
             * @param {nb.QueueMessage} message
             * @returns {Promise<void>}
             */
            execute: async message => {
                const payload = message && message.payload;
                if (!payload || typeof payload !== 'object') throw new Error('task payload is missing');

                const kind = message.kind;
                if (kind === REPLICATION_TASK_KIND.SCAN) {
                    if (!this.replication_scanner) throw new Error(`task kind ${kind} is not enabled`);
                    await this.replication_scanner.execute_task(/** @type {ScanReplicateTask} */ (payload));
                } else if (kind === REPLICATION_TASK_KIND.LOG) {
                    if (!this.log_replication_scanner) throw new Error(`task kind ${kind} is not enabled`);
                    await this.log_replication_scanner.execute_task(/** @type {LogReplicateTask} */ (payload));
                } else {
                    throw new Error(`task kind ${kind} is not enabled`);
                }
            },
        };
        return [task];
    }
}

/** @type {BgTaskExecutor | undefined} */
let singleton;

/**
 * @returns {BgTaskExecutor}
 */
function get_instance() {
    if (!singleton) {
        const server_rpc = require('../server_rpc');
        singleton = new BgTaskExecutor(server_rpc.client);
    }
    return singleton;
}

exports.BgTaskExecutor = BgTaskExecutor;
exports.CONSUME_RESULT = CONSUME_RESULT;
exports.get_instance = get_instance;
