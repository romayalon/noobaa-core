/* Copyright (C) 2016 NooBaa */
'use strict';

/** @typedef {typeof import('../../../sdk/nb')} nb */

// Real server over postgres (coretest). This test drives the update_replication_metrics RPC end to end:
// real schema validation, real in-process RPC dispatch to replication_server, and the real owner-side
// aggregation in replication_utils. It proves the published metrics match the pre-change inline path
// whether one or many "executors" report - here an executor is a concurrent RPC sender, which is exactly
// how a replication executor process reports its chunk in production (the queue itself is out of scope
// for this branch, so no executor processes are spawned).

const coretest = require('../../utils/coretest/coretest');
coretest.setup();

const mocha = require('mocha');
const assert = require('assert');

const replication_utils = require('../../../server/utils/replication_utils');

mocha.describe('replication metrics RPC (postgres, real server)', function() {
    this.timeout(60000); // eslint-disable-line no-invalid-this

    const BUCKET = 'metrics-src';

    /** Spy on the owner-side publish so we can read back exactly what the server would export. */
    function capture_published() {
        const published = [];
        const original = replication_utils.update_replication_prom_report;
        replication_utils.update_replication_prom_report = (_bucket, _id, rule_status, bucket_status) =>
            published.push({ rule_status, bucket_status });
        return {
            last: () => published[published.length - 1],
            restore: () => { replication_utils.update_replication_prom_report = original; },
        };
    }

    function make_page(num_keys, versions_per_key = 2, size = 10) {
        /** @type {Record<string, Array<{ Size: number }>>} */
        const page = {};
        for (let i = 0; i < num_keys; i++) {
            page[`key-${i}`] = Array.from({ length: versions_per_key }, () => ({ Size: size }));
        }
        return page;
    }

    const key_index = key => Number(key.split('-')[1]);

    function copy_res_for(sub, fails, size = 10) {
        let moved = 0;
        for (const key of Object.keys(sub)) if (!fails(key_index(key))) moved += sub[key].length;
        return { num_of_objects: moved, size_of_objects: moved * size };
    }

    /** The pre-change inline metrics for the whole page - the baseline every executor layout must match. */
    function inline_baseline(rule_id, token, page, fails) {
        return replication_utils.get_rule_and_bucket_status(rule_id, token, page, copy_res_for(page, fails));
    }

    /** Split the page into per-chunk RPC payloads, assigning chunks round-robin to `num_executors`. */
    function build_reports({ replication_id, rule_id, token, page, fails, num_executors, chunk_size, cycle }) {
        const keys = Object.keys(page);
        const reports = [];
        let chunk_i = 0;
        for (let offset = 0; offset < keys.length; offset += chunk_size) {
            /** @type {Record<string, Array<{ Size: number }>>} */
            const chunk = {};
            for (const k of keys.slice(offset, offset + chunk_size)) chunk[k] = page[k];
            const { rule_status, bucket_status } =
                replication_utils.get_rule_and_bucket_status(rule_id, token, chunk, copy_res_for(chunk, fails));
            reports.push({
                replication_id,
                rule_id,
                bucket_name: BUCKET,
                report_id: `exec-${chunk_i % num_executors}/chunk-${chunk_i}`,
                last_cycle_start: cycle,
                rule_status,
                bucket_status,
            });
            chunk_i += 1;
        }
        return reports;
    }

    const token = 'pg-src-tok';
    const cycle = 1;
    const SCENARIOS = [
        { name: 'all copied', keys: 60, chunk: 25, fails: () => false },
        { name: 'partial failures', keys: 47, chunk: 13, fails: i => i % 5 === 0 },
    ];
    const EXECUTOR_COUNTS = [1, 4];

    for (const sc of SCENARIOS) {
        for (const num_executors of EXECUTOR_COUNTS) {
            mocha.it(`${num_executors} executor(s) over RPC match the inline baseline (${sc.name})`, async function() {
                replication_utils._reset_executor_replication_metrics();
                const cap = capture_published();
                const replication_id = `pg-rep-${num_executors}-${sc.name.replace(/\s+/g, '-')}`;
                const rule_id = `pg-rule-${num_executors}-${sc.name.replace(/\s+/g, '-')}`;
                try {
                    const baseline = inline_baseline(rule_id, token, make_page(sc.keys), sc.fails);
                    const reports = build_reports({
                        replication_id, rule_id, token, page: make_page(sc.keys),
                        fails: sc.fails, num_executors, chunk_size: sc.chunk, cycle,
                    });

                    // Fire every chunk report concurrently - this is the concurrent delivery the owner must
                    // aggregate correctly. The real RPC validates each payload against replication_api.
                    await Promise.all(reports.map(params =>
                        coretest.rpc_client.replication.update_replication_metrics(params)));

                    const got = cap.last();
                    assert.ok(got, 'the server must publish at least one metrics report');
                    assert.deepStrictEqual(got.rule_status, baseline.rule_status);
                    assert.deepStrictEqual(got.bucket_status, baseline.bucket_status);
                } finally {
                    cap.restore();
                }
            });
        }
    }

    mocha.it('the RPC rejects a payload that is missing required fields', async function() {
        await assert.rejects(
            // @ts-expect-error intentionally missing required fields to exercise schema validation
            coretest.rpc_client.replication.update_replication_metrics({ replication_id: 'x' }),
            err => err.rpc_code === 'INVALID_SCHEMA' || (/SCHEMA|required/i).test(String(err.message)));
    });
});
