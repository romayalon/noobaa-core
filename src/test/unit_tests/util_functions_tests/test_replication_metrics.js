/* Copyright (C) 2016 NooBaa */
'use strict';

/** @typedef {typeof import('../../../sdk/nb')} nb */

const mocha = require('mocha');
const assert = require('assert');

const { update_replication_metrics } = require('../../../server/bg_services/replication_server');
const replication_utils = require('../../../server/utils/replication_utils');
const replication_store = require('../../../server/system_services/replication_store').instance();
const { ReplicationScanner } = require('../../../server/bg_services/replication_scanner');

/**
 * These tests pin the metrics-reporting contract of the executor path:
 * when replication copies the same objects, the metrics published on the core report must be
 * identical whether the copy ran inline (none-queue, the pre-change behavior) or across one or
 * many executors that each report a chunk through the update_replication_metrics RPC handler.
 *
 * The owner process sums every executor report for a rule. Because that sum runs synchronously,
 * out-of-order delivery, retries and cross-cycle reports must all converge to the inline result.
 */
mocha.describe('replication metrics aggregation', function() {

    /** Capture what the owner would publish on the core report, without loading prometheus. */
    function capture_published() {
        const published = [];
        const original = replication_utils.update_replication_prom_report;
        replication_utils.update_replication_prom_report = (_bucket, _id, rule_status, bucket_status) =>
            published.push({ rule_status, bucket_status });
        return {
            published,
            last: () => published[published.length - 1],
            restore: () => { replication_utils.update_replication_prom_report = original; },
        };
    }

    /**
     * Build a page of `num_keys` keys, each with `versions_per_key` versions of `size` bytes.
     * @returns {Record<string, Array<{ Size: number }>>}
     */
    function make_page(num_keys, versions_per_key = 2, size = 10) {
        /** @type {Record<string, Array<{ Size: number }>>} */
        const page = {};
        for (let i = 0; i < num_keys; i++) {
            page[`key-${i}`] = Array.from({ length: versions_per_key }, () => ({ Size: size }));
        }
        return page;
    }

    const key_index = key => Number(key.split('-')[1]);

    /**
     * copy_res for a sub-map: every key whose index does not fail moves all of its versions.
     * `fails(i)` is a per-key predicate so the same keys fail regardless of how the page is split -
     * this is what makes the inline whole-page outcome and the chunked outcome comparable.
     */
    function copy_res_for(sub, fails, size = 10) {
        let moved_versions = 0;
        for (const key of Object.keys(sub)) {
            if (!fails(key_index(key))) moved_versions += sub[key].length;
        }
        return { num_of_objects: moved_versions, size_of_objects: moved_versions * size };
    }

    /** The pre-change (inline / none-queue) path: one whole-page status published directly. */
    function run_inline(replication_id, rule_id, token, page, fails) {
        const cap = capture_published();
        try {
            const whole = replication_utils.get_rule_and_bucket_status(rule_id, token, page, copy_res_for(page, fails));
            replication_utils.update_replication_prom_report('src', replication_id, whole.rule_status, whole.bucket_status);
            return cap.last();
        } finally {
            cap.restore();
        }
    }

    /**
     * Partition the page across `num_executors`, send each chunk through the RPC handler, and return
     * the final published sum. `order` controls the delivery order of the chunk reports.
     * @param {object} opts
     * @returns {{ rule_status: object, bucket_status: object }}
     */
    function run_executors({ replication_id, rule_id, token, page, fails, num_executors, chunk_size, cycle = 1, order = 'in-order' }) {
        replication_utils._reset_executor_replication_metrics();
        const cap = capture_published();
        try {
            const keys = Object.keys(page);
            // Build the per-chunk reports first; each chunk belongs to one executor and gets a stable
            // report_id of `exec/chunk` so a given executor's retry would replace that same report.
            const reports = [];
            let chunk_i = 0;
            for (let offset = 0; offset < keys.length; offset += chunk_size) {
                /** @type {Record<string, Array<{ Size: number }>>} */
                const chunk = {};
                for (const k of keys.slice(offset, offset + chunk_size)) chunk[k] = page[k];
                const executor = chunk_i % num_executors;
                const { rule_status, bucket_status } =
                    replication_utils.get_rule_and_bucket_status(rule_id, token, chunk, copy_res_for(chunk, fails));
                reports.push({
                    replication_id, rule_id, bucket_name: 'src',
                    report_id: `exec-${executor}/chunk-${chunk_i}`,
                    last_cycle_start: cycle, rule_status, bucket_status,
                });
                chunk_i += 1;
            }

            let delivery = reports;
            if (order === 'reversed') delivery = reports.slice().reverse();
            else if (order === 'shuffled') delivery = shuffle(reports.slice());
            for (const rpc_params of delivery) update_replication_metrics({ rpc_params });

            return cap.last();
        } finally {
            cap.restore();
        }
    }

    // Deterministic shuffle so a failure reproduces. MINSTD LCG, no bitwise ops.
    function shuffle(arr) {
        let seed = 1337;
        const rand = () => {
            seed = (seed * 48271) % 0x7fffffff;
            return seed / 0x7fffffff;
        };
        for (let i = arr.length - 1; i > 0; i--) {
            const j = Math.floor(rand() * (i + 1));
            [arr[i], arr[j]] = [arr[j], arr[i]];
        }
        return arr;
    }

    const SCENARIOS = [
        { name: 'all copied', keys: 120, chunk: 50, fails: () => false },
        { name: 'partial copy failures', keys: 55, chunk: 20, fails: i => i % 10 === 0 },
        { name: 'all failed', keys: 30, chunk: 7, fails: () => true },
        { name: 'single chunk', keys: 10, chunk: 50, fails: i => i % 3 === 0 },
    ];

    mocha.describe('parity: none-queue vs 1 executor vs many executors', function() {
        for (const sc of SCENARIOS) {
            const token = 'src-tok';
            const page_of = () => make_page(sc.keys);

            mocha.it(`1 executor (postgres) summed metrics equal none-queue whole-page metrics (${sc.name})`, function() {
                const rep = `rep-1x-${sc.name}`;
                const rule = `rule-1x-${sc.name}`;
                const inline = run_inline(rep, rule, token, page_of(), sc.fails);
                const one = run_executors({
                    replication_id: rep, rule_id: rule, token, page: page_of(),
                    fails: sc.fails, num_executors: 1, chunk_size: sc.chunk,
                });
                assert.deepStrictEqual(one.rule_status, inline.rule_status);
                assert.deepStrictEqual(one.bucket_status, inline.bucket_status);
            });

            mocha.it(`many executors (postgres) summed metrics equal none-queue whole-page metrics (${sc.name})`, function() {
                const rep = `rep-nx-${sc.name}`;
                const rule = `rule-nx-${sc.name}`;
                const inline = run_inline(rep, rule, token, page_of(), sc.fails);
                const many = run_executors({
                    replication_id: rep, rule_id: rule, token, page: page_of(),
                    fails: sc.fails, num_executors: 3, chunk_size: sc.chunk,
                });
                assert.deepStrictEqual(many.rule_status, inline.rule_status);
                assert.deepStrictEqual(many.bucket_status, inline.bucket_status);
            });
        }
    });

    mocha.describe('concurrency: delivery order must not change the result', function() {
        const token = 'src-tok';
        const sc = { name: 'partial', keys: 83, chunk: 9, fails: i => i % 7 === 0 };

        mocha.it('in-order, reversed and shuffled delivery converge to the same sum', function() {
            const rep = 'rep-order';
            const rule = 'rule-order';
            const base = {
                replication_id: rep, rule_id: rule, token,
                page: make_page(sc.keys), fails: sc.fails, num_executors: 4, chunk_size: sc.chunk,
            };
            const in_order = run_executors({ ...base, order: 'in-order' });
            const reversed = run_executors({ ...base, order: 'reversed' });
            const shuffled = run_executors({ ...base, order: 'shuffled' });
            assert.deepStrictEqual(reversed, in_order);
            assert.deepStrictEqual(shuffled, in_order);
            // and all of them equal the inline whole-page result
            const inline = run_inline(rep, rule, token, make_page(sc.keys), sc.fails);
            assert.deepStrictEqual(in_order.rule_status, inline.rule_status);
            assert.deepStrictEqual(in_order.bucket_status, inline.bucket_status);
        });
    });

    mocha.describe('retries, late reports and cycle boundaries', function() {
        mocha.beforeEach(function() {
            replication_utils._reset_executor_replication_metrics();
        });

        const rule_status = (writes, token) => ({
            last_cycle_rule_id: 'rule-sum',
            last_cycle_writes_num: writes,
            last_cycle_writes_size: writes,
            last_cycle_error_writes_num: 0,
            last_cycle_error_writes_size: 0,
            last_cycle_src_cont_token: token,
        });
        const bucket_status = (total, replicated) => ({
            bucket_last_cycle_total_objects_num: total,
            bucket_last_cycle_replicated_objects_num: replicated,
            bucket_last_cycle_error_objects_num: total - replicated,
        });
        const send = (report_id, cycle, rs, bs) => update_replication_metrics({
            rpc_params: {
                replication_id: 'rep-sum', rule_id: 'rule-sum', bucket_name: 'src',
                report_id, last_cycle_start: cycle, rule_status: rs, bucket_status: bs,
            },
        });

        mocha.it('a retry with the same report_id replaces that executor numbers, not adds them', function() {
            const cap = capture_published();
            try {
                send('chunk-a', 1, rule_status(1, 'page-1'), bucket_status(1, 1));
                send('chunk-b', 1, rule_status(2, 'page-1'), bucket_status(4, 2));
                assert.strictEqual(cap.last().rule_status.last_cycle_writes_num, 3);
                assert.strictEqual(cap.last().bucket_status.bucket_last_cycle_total_objects_num, 5);

                // chunk-b retried with smaller numbers: the sum must drop, not grow.
                send('chunk-b', 1, rule_status(1, 'page-1'), bucket_status(1, 1));
                assert.strictEqual(cap.last().rule_status.last_cycle_writes_num, 2);
                assert.strictEqual(cap.last().bucket_status.bucket_last_cycle_total_objects_num, 2);
            } finally {
                cap.restore();
            }
        });

        mocha.it('a strictly newer cycle starts a fresh sum', function() {
            const cap = capture_published();
            try {
                send('chunk-a', 1, rule_status(5, 'page-1'), bucket_status(5, 5));
                send('chunk-b', 1, rule_status(5, 'page-1'), bucket_status(5, 5));
                assert.strictEqual(cap.last().rule_status.last_cycle_writes_num, 10);

                send('chunk-a', 2, rule_status(4, 'page-2'), bucket_status(4, 4));
                assert.strictEqual(cap.last().rule_status.last_cycle_writes_num, 4);
                assert.strictEqual(cap.last().rule_status.last_cycle_src_cont_token, 'page-2');
                assert.strictEqual(cap.last().bucket_status.bucket_last_cycle_total_objects_num, 4);
            } finally {
                cap.restore();
            }
        });

        mocha.it('a late report from an older cycle is dropped and does not regress the current sum', function() {
            const cap = capture_published();
            try {
                send('chunk-a', 2, rule_status(4, 'page-2'), bucket_status(4, 4));
                const before = cap.published.length;
                // A straggler from cycle 1 arrives after cycle 2 already started.
                send('chunk-late', 1, rule_status(99, 'page-1'), bucket_status(99, 99));
                // It must not publish anything new, and the current sum stays at cycle 2.
                assert.strictEqual(cap.published.length, before);
                assert.strictEqual(cap.last().rule_status.last_cycle_writes_num, 4);
                assert.strictEqual(cap.last().rule_status.last_cycle_src_cont_token, 'page-2');
            } finally {
                cap.restore();
            }
        });

        mocha.it('rejects a report that is missing an id', function() {
            assert.throws(() => send('', 1, rule_status(1, 'page-1'), bucket_status(1, 1)),
                /replication metrics report is missing its id/);
        });
    });

    // Producer side: the scanner decides where a rule's metrics go based on BG_WORKERS_MODE.
    mocha.describe('scanner picks the metrics destination by worker mode', function() {
        /** Stub the DB write and the copy so execute() runs without a server. */
        function stub_scanner_deps(copy_res) {
            const original = {
                update_status: replication_store.update_replication_status_by_id,
                copy_objects: replication_utils.copy_objects,
                send: replication_utils.send_replication_metrics,
                report: replication_utils.update_replication_prom_report,
                mode: process.env.BG_WORKERS_MODE,
            };
            replication_store.update_replication_status_by_id = async () => undefined;
            replication_utils.copy_objects = async () => copy_res;
            return {
                restore() {
                    replication_store.update_replication_status_by_id = original.update_status;
                    replication_utils.copy_objects = original.copy_objects;
                    replication_utils.send_replication_metrics = original.send;
                    replication_utils.update_replication_prom_report = original.report;
                    if (original.mode === undefined) delete process.env.BG_WORKERS_MODE;
                    else process.env.BG_WORKERS_MODE = original.mode;
                },
            };
        }

        const src_bucket = /** @type {{ name: nb.SensitiveString }} */ ({ name: 'src' });
        const dst_bucket = /** @type {{ name: nb.SensitiveString }} */ ({ name: 'dst' });

        mocha.it('executor mode sends the metrics over RPC and does not publish locally', async function() {
            const stubs = stub_scanner_deps({ num_of_objects: 1, size_of_objects: 10 });
            const sent = [];
            const published = [];
            replication_utils.send_replication_metrics = async (_client, entry) => { sent.push(entry); };
            replication_utils.update_replication_prom_report = () => { published.push(1); };
            process.env.BG_WORKERS_MODE = 'executor';
            const scanner = new ReplicationScanner({ name: 'exec', client: /** @type {nb.APIClient} */ ({}) });
            try {
                await scanner.execute(
                    src_bucket, dst_bucket, { a: [{ Size: 10 }] }, 'rep-x', 'rule-x',
                    { last_cycle_start: 7 }, { src_cont_token: 'tok', dst_cont_token: 'dtok', report_id: 'msg-1' });
                assert.strictEqual(published.length, 0, 'an executor must not publish to the core report itself');
                assert.strictEqual(sent.length, 1);
                assert.strictEqual(sent[0].report_id, 'msg-1');
                assert.strictEqual(sent[0].last_cycle_start, 7);
                assert.strictEqual(sent[0].rule_status.last_cycle_writes_num, 1);
                assert.strictEqual(sent[0].bucket_status.bucket_last_cycle_total_objects_num, 1);
            } finally {
                stubs.restore();
            }
        });

        mocha.it('default mode publishes locally and does not send over RPC', async function() {
            const stubs = stub_scanner_deps({ num_of_objects: 1, size_of_objects: 10 });
            const sent = [];
            const published = [];
            replication_utils.send_replication_metrics = async (_client, entry) => { sent.push(entry); };
            replication_utils.update_replication_prom_report = () => { published.push(1); };
            delete process.env.BG_WORKERS_MODE; // default
            const scanner = new ReplicationScanner({ name: 'def', client: /** @type {nb.APIClient} */ ({}) });
            try {
                await scanner.execute(
                    src_bucket, dst_bucket, { a: [{ Size: 10 }] }, 'rep-y', 'rule-y',
                    { last_cycle_start: 7 }, { src_cont_token: 'tok', dst_cont_token: 'dtok' });
                assert.strictEqual(sent.length, 0, 'the default process must not send metrics over RPC');
                assert.strictEqual(published.length, 1);
            } finally {
                stubs.restore();
            }
        });
    });
});
