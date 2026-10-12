/* Copyright (C) 2016 NooBaa */
'use strict';

const mocha = require('mocha');
const assert = require('assert');

const config = require('../../../../config');
const replication_utils = require('../../../server/utils/replication_utils');
const { REPLICATION_TASK_KIND } = replication_utils;
const replication_store = require('../../../server/system_services/replication_store').instance();
const log_parser = require('../../../server/bg_services/replication_log_parser');
const replication_scanner = require('../../../server/bg_services/replication_scanner');
const { ReplicationWorker } = replication_scanner;
const { BgTaskExecutor, CONSUME_RESULT } = require('../../../server/bg_services/bg_task_executor');
const { LogReplicationWorker } = require('../../../server/bg_services/log_replication_scanner');

mocha.describe('replication executor', function() {

    mocha.it('scan enqueues a replicate task and the executor runs execute', async function() {
        const queue = new_memory_queue();
        const stubs = stub_replication_deps();
        const scanner = new_scan_probe(queue);
        const queue_type = config.MESSAGE_QUEUE_TYPE;
        config.MESSAGE_QUEUE_TYPE = 'postgres';
        try {
            const result = { had_work: false, had_errors: false };
            await scanner.scan_rule({
                replication_id: 'rep-1',
                rule: { rule_id: 'rule-1', destination_bucket: 'dst-id' },
            }, result);

            assert.strictEqual(scanner.execute_called, false);
            assert.strictEqual(result.had_work, true);
            assert.strictEqual(stubs.updates.length, 1);
            assert.strictEqual(stubs.updates[0].status.src_cont_token, 'next-src');
            assert.strictEqual(await queue.size(config.REPLICATION_QUEUE_NAME), 1);

            /** @type {boolean | undefined} */
            let commit_scan_status;
            const executor = new ReplicationWorker({
                name: 'replication worker',
                client: /** @type {nb.APIClient} */ ({}),
                commit_scan_status: false,
            });
            executor.execute = async function() {
                commit_scan_status = this.commit_scan_status;
                return false;
            };
            const outcome = await consume_one(replicate_task(executor), queue);

            assert.strictEqual(outcome, CONSUME_RESULT.OK);
            assert.strictEqual(commit_scan_status, false);
            assert.strictEqual(stubs.updates.length, 1);
            assert.strictEqual(await queue.size(config.REPLICATION_QUEUE_NAME), 0);
        } finally {
            config.MESSAGE_QUEUE_TYPE = queue_type;
            stubs.restore();
        }
    });

    mocha.it('log scan commits the marker and the executor does not commit it again', async function() {
        const queue = new_memory_queue();
        const stubs = stub_replication_deps();
        const queue_type = config.MESSAGE_QUEUE_TYPE;
        config.MESSAGE_QUEUE_TYPE = 'postgres';
        let done_count = 0;
        const original_candidates = log_parser.get_log_candidates;
        log_parser.get_log_candidates = async () => ({
            items: { 'a.txt': { action: 'copy', time: new Date() } },
            done: async () => {
                done_count += 1;
            },
        });
        const scanner = new_log_probe(queue);
        try {
            const result = { had_work: false, had_errors: false };
            await scanner.scan_rule(
                { _id: 'rep-1', rules: [] },
                { rule_id: 'rule-1', destination_bucket: 'dst-id', sync_deletions: true },
                result,
            );

            assert.strictEqual(scanner.execute_called, false);
            assert.strictEqual(done_count, 1);
            assert.strictEqual(result.had_work, true);

            const executor = new LogReplicationWorker({
                name: 'log replication worker',
                client: /** @type {nb.APIClient} */ ({}),
            });
            executor.noobaa_connection = /** @type {LogReplicationWorker['noobaa_connection']} */ ({});
            executor.execute = async (_src, _dst, candidates) => {
                const page = /** @type {{ done?: () => Promise<void> }} */ (candidates);
                if (page.done) await page.done();
                return { had_copy_errors: false, copy_keys: {}, delete_keys: [] };
            };
            const outcome = await consume_one(replicate_task(undefined, executor), queue);

            assert.strictEqual(outcome, CONSUME_RESULT.OK);
            assert.strictEqual(done_count, 1);
            assert.strictEqual(await queue.size(config.REPLICATION_QUEUE_NAME), 0);
        } finally {
            config.MESSAGE_QUEUE_TYPE = queue_type;
            log_parser.get_log_candidates = original_candidates;
            stubs.restore();
        }
    });

    mocha.it('an exhausted replicate claim is not executed', async function() {
        const queue = new_memory_queue();
        const id = await replication_scanner.enqueue(queue, REPLICATION_TASK_KIND.SCAN, {
            replication_id: 'rep-1',
            rule_id: 'rule-1',
            destination_bucket_id: 'dst-id',
            keys_diff_map: { a: [] },
            status: { last_cycle_start: 1 },
            src_cont_token: '',
            dst_cont_token: '',
        });
        queue.mark_terminal(id);
        let called = false;
        const outcome = await consume_one({
            name: 'replicate',
            queue: config.REPLICATION_QUEUE_NAME,
            execute: async () => {
                called = true;
            },
        }, queue);

        assert.strictEqual(outcome, CONSUME_RESULT.OK);
        assert.strictEqual(called, false);
        assert.strictEqual(queue.was_nacked(id), true);
    });

    mocha.it('scan enqueues one message per configured number of keys and commits the cursor once', async function() {
        const queue = new_memory_queue();
        const stubs = stub_replication_deps();
        const scanner = new_scan_probe(queue);
        const keys_diff_map = {};
        for (let i = 0; i < 120; i++) keys_diff_map[`key-${i}`] = [{}];
        scanner.get_rule_diff = async () => ({
            keys_diff_map,
            src_cont_token: 'next-src',
            dst_cont_token: 'next-dst',
        });
        const queue_type = config.MESSAGE_QUEUE_TYPE;
        const chunk_size = config.REPLICATION_QUEUE_OBJECTS_PER_MESSAGE;
        config.MESSAGE_QUEUE_TYPE = 'postgres';
        config.REPLICATION_QUEUE_OBJECTS_PER_MESSAGE = 50;
        try {
            await scanner.scan_rule({
                replication_id: 'rep-1',
                rule: { rule_id: 'rule-1', destination_bucket: 'dst-id' },
            }, { had_work: false, had_errors: false });

            assert.strictEqual(await queue.size(config.REPLICATION_QUEUE_NAME), 3);
            assert.strictEqual(stubs.updates.length, 1);
            assert.strictEqual(stubs.updates[0].status.src_cont_token, 'next-src');
            const sizes = [];
            for (let i = 0; i < 3; i++) {
                const message = await queue.dequeue(config.REPLICATION_QUEUE_NAME);
                sizes.push(Object.keys(message.payload.keys_diff_map).length);
            }
            assert.deepStrictEqual(sizes, [50, 50, 20]);
        } finally {
            config.MESSAGE_QUEUE_TYPE = queue_type;
            config.REPLICATION_QUEUE_OBJECTS_PER_MESSAGE = chunk_size;
            stubs.restore();
        }
    });
});

/**
 * @param {ReplicationWorker} [replication]
 * @param {LogReplicationWorker} [log]
 * @returns {{ name: string, queue: string, execute: (message: nb.QueueMessage) => Promise<void> }}
 */
function replicate_task(replication, log) {
    return {
        name: 'replicate',
        queue: config.REPLICATION_QUEUE_NAME,
        execute: async message => {
            const payload = message && message.payload;
            if (!payload || typeof payload !== 'object') throw new Error('task payload is missing');
            if (message.kind === REPLICATION_TASK_KIND.SCAN) {
                if (!replication) throw new Error(`task kind ${message.kind} is not enabled`);
                const scan_payload = /** @type {Parameters<ReplicationWorker['execute_task']>[0]} */ (payload);
                await replication.execute_task(scan_payload);
            } else if (message.kind === REPLICATION_TASK_KIND.LOG) {
                if (!log) throw new Error(`task kind ${message.kind} is not enabled`);
                const log_payload = /** @type {Parameters<LogReplicationWorker['execute_task']>[0]} */ (payload);
                await log.execute_task(log_payload);
            } else {
                throw new Error(`task kind ${message.kind} is not enabled`);
            }
        },
    };
}

function stub_replication_deps() {
    const original_find = replication_utils.find_src_and_dst_buckets;
    const original_update = replication_store.update_replication_status_by_id;
    /** @type {Array<{ id: string, rule_id: string, status: { src_cont_token?: string } }>} */
    const updates = [];
    replication_utils.find_src_and_dst_buckets = /** @type {typeof replication_utils.find_src_and_dst_buckets} */ (
        /** @type {unknown} */ (() => ({
            src_bucket: { _id: 'src', name: 'src-bucket' },
            dst_bucket: { _id: 'dst', name: 'dst-bucket' },
        }))
    );
    replication_store.update_replication_status_by_id = async (
        /** @type {string} */ id,
        /** @type {string} */ rule_id,
        /** @type {{ src_cont_token?: string }} */ status,
    ) => {
        updates.push({ id: String(id), rule_id: String(rule_id), status });
    };
    return {
        updates,
        restore() {
            replication_utils.find_src_and_dst_buckets = original_find;
            replication_store.update_replication_status_by_id = original_update;
        },
    };
}

/**
 * @param {nb.MessageQueueClient} queue
 * @returns {ReplicationWorker & { execute_called: boolean }}
 */
function new_scan_probe(queue) {
    const scanner = /** @type {ReplicationWorker & { execute_called: boolean }} */ (new ReplicationWorker({
        name: 'scan probe',
        client: /** @type {nb.APIClient} */ ({}),
        message_queue: queue,
    }));
    scanner.execute_called = false;
    scanner.validate_src = async () => true;
    scanner.validate_dst = async () => true;
    scanner.get_rule_diff = async () => ({
        keys_diff_map: { key: [{}] },
        src_cont_token: 'next-src',
        dst_cont_token: 'next-dst',
    });
    scanner.execute = async () => {
        scanner.execute_called = true;
        return false;
    };
    return scanner;
}

/**
 * @param {nb.MessageQueueClient} queue
 * @returns {LogReplicationWorker & { execute_called: boolean }}
 */
function new_log_probe(queue) {
    const scanner = /** @type {LogReplicationWorker & { execute_called: boolean }} */ (new LogReplicationWorker({
        name: 'log probe',
        client: /** @type {nb.APIClient} */ ({}),
        message_queue: queue,
    }));
    scanner.execute_called = false;
    scanner.noobaa_connection = /** @type {LogReplicationWorker['noobaa_connection']} */ ({});
    scanner.validate_src = async () => true;
    scanner.validate_dst = async () => true;
    scanner.execute = async () => {
        scanner.execute_called = true;
        return { had_copy_errors: false, copy_keys: {}, delete_keys: [] };
    };
    return scanner;
}

/**
 * @returns {nb.MessageQueueClient & { mark_terminal: (id: string) => void, was_nacked: (id: string) => boolean }}
 */
function new_memory_queue() {
    /** @type {Array<{ _id: string, queue: string, kind?: string, payload: object, terminal: boolean, claimed: boolean, acked: boolean, nacked: boolean }>} */
    const rows = [];
    let next_id = 1;

    /**
     * @param {{ _id: string }} message
     */
    function row_for(message) {
        const row = rows.find(item => item._id === message._id);
        if (!row) throw new Error(`missing message ${message._id}`);
        return row;
    }

    return {
        /**
         * @param {string} [_queue]
         * @returns {Promise<void>}
         */
        connect(_queue) {
            return Promise.resolve();
        },
        disconnect() {
            return Promise.resolve();
        },
        /**
         * @param {string} queue
         * @param {object} payload
         * @param {string} [kind]
         * @returns {Promise<string>}
         */
        enqueue(queue, payload, kind) {
            const _id = String(next_id);
            next_id += 1;
            rows.push({
                _id,
                queue,
                kind,
                payload: JSON.parse(JSON.stringify(payload)),
                terminal: false,
                claimed: false,
                acked: false,
                nacked: false,
            });
            return Promise.resolve(_id);
        },
        /**
         * @param {string} queue
         * @returns {Promise<nb.QueueMessage | null>}
         */
        dequeue(queue) {
            const row = rows.find(item => item.queue === queue && !item.claimed && !item.acked);
            if (!row) return Promise.resolve(null);
            row.claimed = true;
            const now = new Date();
            return Promise.resolve({
                _id: row._id,
                queue: row.queue,
                kind: row.kind,
                payload: row.payload,
                retry_count: 0,
                lock_token: row._id,
                enqueued_at: now,
                visible_at: now,
                locked_until: now,
            });
        },
        /**
         * @param {nb.QueueMessage} message
         * @returns {boolean}
         */
        is_retry_limit_exceeded(message) {
            return row_for(message).terminal;
        },
        /**
         * @param {nb.QueueMessage} message
         * @returns {Promise<void>}
         */
        ack(message) {
            row_for(message).acked = true;
            return Promise.resolve();
        },
        /**
         * @param {nb.QueueMessage} message
         * @param {string} [_reason]
         * @returns {Promise<{ dropped: boolean }>}
         */
        nack(message, _reason) {
            const row = row_for(message);
            row.claimed = false;
            row.nacked = true;
            return Promise.resolve({ dropped: false });
        },
        /**
         * @param {string} queue
         * @returns {Promise<number>}
         */
        size(queue) {
            return Promise.resolve(rows.filter(item => item.queue === queue && !item.acked).length);
        },
        /**
         * @param {number} [_limit]
         * @returns {Promise<number>}
         */
        remove_expired_dead_messages(_limit) {
            return Promise.resolve(0);
        },
        /**
         * @param {string} id
         */
        mark_terminal(id) {
            row_for({ _id: id }).terminal = true;
        },
        /**
         * @param {string} id
         * @returns {boolean}
         */
        was_nacked(id) {
            return row_for({ _id: id }).nacked;
        },
    };
}

/**
 * @param {{ name: string, queue: string, execute: (message: nb.QueueMessage) => Promise<void> }} task
 * @param {nb.MessageQueueClient} queue
 * @returns {Promise<typeof CONSUME_RESULT.EMPTY | typeof CONSUME_RESULT.OK | typeof CONSUME_RESULT.ERROR>}
 */
function consume_one(task, queue) {
    const task_executor = new BgTaskExecutor(/** @type {nb.APIClient} */ ({}));
    task_executor.mq_client = queue;
    return task_executor.consume_message(task);
}
