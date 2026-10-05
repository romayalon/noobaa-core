/* Copyright (C) 2016 NooBaa */
'use strict';

const mocha = require('mocha');
const assert = require('assert');

const config = require('../../../../config');
const replication_utils = require('../../../server/utils/replication_utils');
const replication_store = require('../../../server/system_services/replication_store').instance();
const log_parser = require('../../../server/bg_services/replication_log_parser');
const replication_executor = require('../../../server/bg_services/replication_executor');
const { consume_message } = require('../../../server/bg_services/bg_task_executor');
const { ReplicationScanner } = require('../../../server/bg_services/replication_scanner');
const { LogReplicationScanner } = require('../../../server/bg_services/log_replication_scanner');

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
            scanner.execute = async function() {
                commit_scan_status = this.commit_scan_status;
                return false;
            };
            const outcome = await consume_message(replicate_task(scanner), queue);

            assert.strictEqual(outcome, 'ok');
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

            scanner.execute = async (_src, _dst, candidates) => {
                const page = /** @type {{ done?: () => Promise<void> }} */ (candidates);
                if (page.done) await page.done();
                return { had_copy_errors: false, copy_keys: {}, delete_keys: [] };
            };
            const outcome = await consume_message(replicate_task(undefined, scanner), queue);

            assert.strictEqual(outcome, 'ok');
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
        const id = await replication_executor.enqueue(queue, {
            kind: 'scan',
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
        const outcome = await consume_message({
            name: 'replicate',
            queue: config.REPLICATION_QUEUE_NAME,
            execute: async () => {
                called = true;
            },
        }, queue);

        assert.strictEqual(outcome, 'ok');
        assert.strictEqual(called, false);
        assert.strictEqual(queue.was_nacked(id), true);
    });
});

/**
 * @param {ReplicationScanner} [replication]
 * @param {LogReplicationScanner} [log]
 * @returns {{ name: string, queue: string, execute: (payload: object) => Promise<void> }}
 */
function replicate_task(replication, log) {
    return {
        name: 'replicate',
        queue: config.REPLICATION_QUEUE_NAME,
        execute: payload => replication_executor.execute_payload(payload, { replication, log }),
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
 * @returns {ReplicationScanner & { execute_called: boolean }}
 */
function new_scan_probe(queue) {
    const scanner = /** @type {ReplicationScanner & { execute_called: boolean }} */ (new ReplicationScanner({
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
 * @returns {LogReplicationScanner & { execute_called: boolean }}
 */
function new_log_probe(queue) {
    const scanner = /** @type {LogReplicationScanner & { execute_called: boolean }} */ (new LogReplicationScanner({
        name: 'log probe',
        client: /** @type {nb.APIClient} */ ({}),
        message_queue: queue,
    }));
    scanner.execute_called = false;
    scanner.noobaa_connection = /** @type {LogReplicationScanner['noobaa_connection']} */ ({});
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
    /** @type {Array<{ id: string, queue: string, payload: object, terminal: boolean, claimed: boolean, acked: boolean, nacked: boolean, attempts: number }>} */
    const rows = [];
    let next_id = 1;

    /**
     * @param {{ id: string }} message
     */
    function row_for(message) {
        const row = rows.find(item => item.id === message.id);
        if (!row) throw new Error(`missing message ${message.id}`);
        return row;
    }

    return {
        connect() {
            return Promise.resolve();
        },
        disconnect() {
            return Promise.resolve();
        },
        /**
         * @param {string} queue
         * @param {object} payload
         * @returns {Promise<string>}
         */
        enqueue(queue, payload) {
            const id = String(next_id);
            next_id += 1;
            rows.push({
                id,
                queue,
                payload: JSON.parse(JSON.stringify(payload)),
                terminal: false,
                claimed: false,
                acked: false,
                nacked: false,
                attempts: 0,
            });
            return Promise.resolve(id);
        },
        /**
         * @param {string} queue
         * @returns {Promise<nb.QueueMessage | null>}
         */
        dequeue(queue) {
            const row = rows.find(item => item.queue === queue && !item.claimed && !item.acked);
            if (!row) return Promise.resolve(null);
            row.claimed = true;
            row.attempts += 1;
            return Promise.resolve({
                id: row.id,
                queue: row.queue,
                payload: row.payload,
                attempts: row.attempts,
                lock_token: `${row.id}:${row.attempts}`,
                terminal: row.terminal,
            });
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
         * @param {nb.QueueMessage} [_message]
         * @returns {Promise<boolean>}
         */
        extend(_message) {
            return Promise.resolve(true);
        },
        /**
         * @param {string} queue
         * @returns {Promise<number>}
         */
        size(queue) {
            return Promise.resolve(rows.filter(item => item.queue === queue && !item.acked).length);
        },
        /**
         * @param {string} id
         */
        mark_terminal(id) {
            row_for({ id }).terminal = true;
        },
        /**
         * @param {string} id
         * @returns {boolean}
         */
        was_nacked(id) {
            return row_for({ id }).nacked;
        },
    };
}
