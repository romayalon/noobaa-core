/* Copyright (C) 2016 NooBaa */
'use strict';

const dbg = require('../../util/debug_module')(__filename);
const config = require('../../../config.js');
const message_queue_client = require('../../util/message_queue_client');
const server_rpc = require('../server_rpc');
const system_store = require('../system_services/system_store').get_instance();
const system_utils = require('../utils/system_utils');
const background_scheduler = require('../../util/background_scheduler').get_instance();
const replication_executor = require('./replication_executor');
const { ReplicationScanner } = require('./replication_scanner');
const { LogReplicationScanner } = require('./log_replication_scanner');

/**
 * One worker per task type. replicate is the first type: it dequeues the
 * replicate queue and runs the replication scanners' execute.
 * @returns {BgTaskType[]}
 */
function task_types() {
    return [
        {
            name: 'replicate',
            queue: config.REPLICATION_QUEUE_NAME,
            execute: payload => replication_executor.execute_payload(payload, scanners()),
        },
    ];
}

/**
 * Dequeue one message for this task type and run its executor.
 * @param {BgTaskType} task
 * @param {nb.MessageQueueClient} [queue]
 * @returns {Promise<'empty' | 'ok' | 'error'>}
 */
async function consume_message(task, queue) {
    const client = queue || message_queue_client.instance();
    await client.connect();
    const message = await client.dequeue(task.queue);
    if (!message) return 'empty';
    if (message_queue_client.is_exhausted_claim(message)) {
        dbg.warn('bg executor dropping exhausted task', task.name, message.id);
        await client.nack(message, 'visibility timeout');
        return 'ok';
    }
    const lease = message_queue_client.hold_lease(client, message);
    try {
        dbg.log1('bg executor running', task.name, message.id);
        await task.execute(message.payload);
        await client.ack(message);
        return 'ok';
    } catch (err) {
        dbg.error('bg executor failed', task.name, message.id, err);
        const reason = err instanceof Error ? err.message : String(err);
        try {
            await client.nack(message, reason);
        } catch (nack_err) {
            dbg.error('bg executor failed to nack', task.name, message.id, nack_err);
        }
        return 'error';
    } finally {
        lease.stop();
    }
}

/**
 * Run queued tasks until the queue is empty. Tests use this after a scan.
 * @param {BgTaskType} task
 * @param {nb.MessageQueueClient} [queue]
 * @returns {Promise<void>}
 */
async function drain_task(task, queue) {
    for (let i = 0; i < 1000; i++) {
        const result = await consume_message(task, queue);
        if (result === 'empty') return;
        if (result === 'error') throw new Error(`task ${task.name} failed`);
    }
    throw new Error(`task ${task.name} queue did not drain`);
}

function register_executors() {
    for (const task of task_types()) {
        background_scheduler.register_bg_worker({
            name: `bg executor ${task.name}`,
            delay: config.BG_EXECUTOR_EMPTY_DELAY,
            run_immediate: true,
        }, () => run_task(task));
    }
}

/**
 * @param {BgTaskType} task
 * @returns {Promise<number | undefined>}
 */
async function run_task(task) {
    if (!can_run()) return config.BG_EXECUTOR_EMPTY_DELAY;
    const result = await consume_message(task);
    if (result === 'error') return config.BG_EXECUTOR_ERROR_DELAY;
    if (result === 'ok') return config.BG_EXECUTOR_BATCH_DELAY;
    return config.BG_EXECUTOR_EMPTY_DELAY;
}

function can_run() {
    if (!system_store.is_finished_initial_load) {
        dbg.log0('bg executors: system_store did not finish initial load');
        return false;
    }
    const system = system_store.data.systems[0];
    if (!system || system_utils.system_in_maintenance(system._id)) return false;
    return true;
}

/** @type {{ replication: ReplicationScanner, log: LogReplicationScanner } | undefined} */
let executor_scanners;

/**
 * Scanners used only to run execute. Scanning stays on the default mode workers.
 * @returns {{ replication: ReplicationScanner, log: LogReplicationScanner }}
 */
function scanners() {
    if (!executor_scanners) {
        executor_scanners = {
            replication: new ReplicationScanner({
                name: 'replication executor',
                client: server_rpc.client,
            }),
            log: new LogReplicationScanner({
                name: 'log replication executor',
                client: server_rpc.client,
            }),
        };
    }
    return executor_scanners;
}

exports.consume_message = consume_message;
exports.drain_task = drain_task;
exports.task_types = task_types;
exports.register_executors = register_executors;

/**
 * @typedef {object} BgTaskType
 * @property {string} name
 * @property {string} queue
 * @property {(payload: object) => Promise<void>} execute
 */
