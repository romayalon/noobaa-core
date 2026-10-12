/* Copyright (C) 2026 NooBaa */
'use strict';

// Compares bucket replication with MESSAGE_QUEUE_TYPE=none (the scanner copies
// in this process) against MESSAGE_QUEUE_TYPE=postgres drained by 1, 2, and 5
// bg executor processes.
//
// An executor process exits immediately when the queue type is none, because
// there is nothing to dequeue. That combination is not a separate row.
//
// Not part of npm test. Run:
//  node --allow-natives-syntax ./node_modules/.bin/_mocha --timeout 600000 src/test/integration_tests/api/s3/test_replication_perf.js
//
// REPLICATION_PERF_OBJECT_COUNT   default 50
// REPLICATION_PERF_PAGE_SIZE      default 100  (REPLICATION_MAX_KEYS)
// REPLICATION_PERF_OBJECT_BYTES   default 1024
// REPLICATION_PERF_EXECUTORS      default 1,2,5
// REPLICATION_PERF_TIMEOUT_MS     default max(120000, objects * 3000)
// REPLICATION_PERF_BATCH_DELAY_MS default 1     (executor only, minimum 1)
// REPLICATION_PERF_EMPTY_DELAY_MS default 20    (executor only)
// REPLICATION_QUEUE_OBJECTS_PER_MESSAGE default 50 (0 keeps the whole page in one message)
//
// While the source list is truncated, the next page is queued before the copies
// of this page finish, so several pages can sit in the queue together.
// The last page has an empty source token. The test waits for the queue to
// drain before scanning again, so that page is not queued twice.
//
// The source bucket is uploaded once. Each scenario creates an empty destination
// and points the source replication rule at it.

const mocha = require('mocha');
const assert = require('assert');
const path = require('path');
const http = require('http');
const child_process = require('child_process');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const { S3 } = require('@aws-sdk/client-s3');

const config = require('../../../../../config');
const P = require('../../../../util/promise');
const cloud_utils = require('../../../../util/cloud_utils');
const db_client = require('../../../../util/db_client');
const postgres_message_queue = require('../../../../util/postgres_message_queue');
const { ReplicationWorker } = require('../../../../server/bg_services/replication_scanner');
const coretest = require('../../../utils/coretest/coretest');
const { rpc_client, EMAIL } = coretest;

coretest.setup({ pools_to_create: [coretest.POOL_LIST[1]] });

const object_count = positive_int(process.env.REPLICATION_PERF_OBJECT_COUNT, 50);
const page_size = positive_int(process.env.REPLICATION_PERF_PAGE_SIZE, 100);
const object_bytes = positive_int(process.env.REPLICATION_PERF_OBJECT_BYTES, 1024);
const timeout_ms = positive_int(
    process.env.REPLICATION_PERF_TIMEOUT_MS,
    Math.max(120000, object_count * 3000),
);
const batch_delay_ms = positive_int(process.env.REPLICATION_PERF_BATCH_DELAY_MS, 1);
const empty_delay_ms = positive_int(process.env.REPLICATION_PERF_EMPTY_DELAY_MS, 20);
const executor_counts = (process.env.REPLICATION_PERF_EXECUTORS || '1,2,5')
    .split(',')
    .map(part => Number(part.trim()))
    .filter(count => Number.isInteger(count) && count > 0);
const repo_root = path.resolve(__dirname, '../../../../../');
const executor_script = path.join(__dirname, 'replication_perf_executor.js');
const original_queue_type = config.MESSAGE_QUEUE_TYPE;

/** @type {Array<{ queue: string, executors: number, startup_ms: number, replicate_ms: number, copied: number, objects: number }>} */
const results = [];

// Root after hooks run in registration order. This one is registered after
// coretest's, so the table is printed after coretest teardown, just before
// mocha's summary. Printing inside the suite after is buried in that teardown.
mocha.after('replication performance results', function() {
    if (!results.length) return;
    print_results();
});

/** @type {ExecutorProc[]} */
let live_executors = [];
let bucket_seq = 0;
/** @type {S3 | undefined} */
let s3_owner;
/** @type {ReplicationWorker | undefined} */
let scanner;
/** @type {string | undefined} */
let source_bucket;

mocha.describe('replication queue performance', function() {
    if (config.DB_TYPE !== 'postgres') {
        console.log('replication perf skipped: DB_TYPE is not postgres');
        return;
    }
    if (!executor_counts.length) {
        throw new Error('REPLICATION_PERF_EXECUTORS has no counts');
    }

    mocha.before('s3 client and scanner', async function() {
        const self = this; // eslint-disable-line no-invalid-this
        self.timeout(timeout_ms + 180000);
        process.env.REPLICATION_MAX_KEYS = String(page_size);
        const admin_account = await rpc_client.account.read_account({ email: EMAIL });
        const admin_keys = admin_account.access_keys;
        s3_owner = new S3({
            forcePathStyle: true,
            region: 'us-east-1',
            endpoint: coretest.get_http_address(),
            credentials: {
                accessKeyId: admin_keys[0].access_key.unwrap(),
                secretAccessKey: admin_keys[0].secret_key.unwrap(),
            },
            requestHandler: new NodeHttpHandler({
                httpsAgent: new http.Agent({ keepAlive: false }),
            }),
        });
        cloud_utils.set_noobaa_s3_connection = () => s3_client();
        scanner = new ReplicationWorker({
            name: 'replication_scanner',
            client: rpc_client,
        });
        scanner.noobaa_connection = s3_client();
        console.log('replication performance');
        console.log(`objects=${object_count} page_size=${page_size} bytes=${object_bytes} queue_objects_per_message=${config.REPLICATION_QUEUE_OBJECTS_PER_MESSAGE}`);
        console.log(`executor_batch_delay_ms=${batch_delay_ms} executor_empty_delay_ms=${empty_delay_ms}`);
        source_bucket = 'perf-src';
        await rpc_client.bucket.create_bucket({ name: source_bucket });
        await upload_objects(source_bucket, object_count);
        console.log('warmup: copy that source, none then postgres with 1 executor');
    });

    mocha.before('warmup', async function() {
        const self = this; // eslint-disable-line no-invalid-this
        self.timeout(timeout_ms + 180000);
        await run_scenario({ queue: 'none', executors: 0, record: false });
        await run_scenario({ queue: 'postgres', executors: 1, record: false });
    });

    mocha.after('stop executors and print', async function() {
        const self = this; // eslint-disable-line no-invalid-this
        self.timeout(Math.max(180000, timeout_ms));
        await stop_executors(live_executors);
        if (source_bucket) await delete_buckets([source_bucket]);
        config.MESSAGE_QUEUE_TYPE = original_queue_type;
        delete process.env.REPLICATION_MAX_KEYS;
    });

    mocha.it('message queue none, scanner copies in process', async function() {
        const self = this; // eslint-disable-line no-invalid-this
        self.timeout(timeout_ms + 180000);
        await run_scenario({ queue: 'none', executors: 0, record: true });
    });

    for (const executors of executor_counts) {
        mocha.it(`message queue postgres, ${executors} executor processes`, async function() {
            const self = this; // eslint-disable-line no-invalid-this
            self.timeout(timeout_ms + 180000);
            await run_scenario({ queue: 'postgres', executors, record: true });
        });
    }
});

/**
 * @param {{ queue: string, executors: number, record: boolean }} scenario
 * @returns {Promise<void>}
 */
async function run_scenario(scenario) {
    const src = source_bucket;
    if (!src) throw new Error('replication perf source bucket is not ready');
    const dst = next_bucket('dst', scenario);
    const row = {
        queue: scenario.queue,
        executors: scenario.executors,
        startup_ms: 0,
        replicate_ms: 0,
        copied: 0,
        objects: object_count,
    };
    console.log(`scenario queue=${scenario.queue} executors=${scenario.executors} objects=${object_count}`);
    config.MESSAGE_QUEUE_TYPE = scenario.queue;
    try {
        await clear_queue();
        await rpc_client.bucket.create_bucket({ name: dst });
        // Drop the previous rule so its scan cursor cannot skip keys on this empty destination.
        await rpc_client.bucket.delete_bucket_replication({ name: src });
        await rpc_client.bucket.put_bucket_replication({
            name: src,
            replication_policy: {
                rules: [{
                    rule_id: 'perf-rule',
                    destination_bucket: dst,
                    sync_versions: false,
                }],
            },
        });
        const started_executors = Date.now();
        live_executors = await start_executors(scenario.executors);
        row.startup_ms = scenario.executors ? Date.now() - started_executors : 0;
        const timed = await replicate_until_copied(live_executors, dst, object_count);
        row.replicate_ms = timed.replicate_ms;
        row.copied = timed.copied;
    } finally {
        await stop_executors(live_executors);
        await clear_queue();
        await delete_buckets([dst]);
        config.MESSAGE_QUEUE_TYPE = original_queue_type;
    }
    if (scenario.record) {
        results.push(row);
        console.log(`replication perf result ${format_result_row(row)}`);
    }
    assert.strictEqual(
        row.copied,
        object_count,
        `replicated ${row.copied} of ${object_count} ` +
        `(queue=${scenario.queue} executors=${scenario.executors})`,
    );
}

/**
 * @param {ExecutorProc[]} executors
 * @param {string} dst
 * @param {number} expected
 * @returns {Promise<{ replicate_ms: number, copied: number }>}
 */
async function replicate_until_copied(executors, dst, expected) {
    const started = Date.now();
    let copied = 0;
    while (Date.now() - started < timeout_ms) {
        assert_executors_alive(executors);
        const batch_scanner = replication_scanner();
        await batch_scanner.run_batch();
        // A source token means this page was truncated. Queue the next page now,
        // while these copies are still in flight, so the executors can share them.
        if (batch_scanner.last_src_cont_token) continue;
        await wait_until_queued_copies_finish(executors, started);
        copied = await count_objects(dst);
        if (copied >= expected) {
            return { replicate_ms: Date.now() - started, copied };
        }
        await P.delay(50);
    }
    return { replicate_ms: Date.now() - started, copied };
}

/**
 * @param {number} count
 * @returns {Promise<ExecutorProc[]>}
 */
async function start_executors(count) {
    if (!count) return [];
    await postgres_message_queue.instance().connect(config.REPLICATION_QUEUE_NAME);
    /** @type {ExecutorProc[]} */
    const procs = [];
    live_executors = procs;
    for (let i = 0; i < count; i++) {
        procs.push(spawn_executor());
    }
    await Promise.all(procs.map(wait_for_executor));
    return procs;
}

/**
 * @returns {ExecutorProc}
 */
function spawn_executor() {
    const child = child_process.spawn(process.execPath, [...process.execArgv, executor_script], {
        cwd: repo_root,
        env: executor_env(),
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    /** @type {ExecutorProc} */
    const proc = { child, logs: '' };
    /** @param {Buffer} chunk */
    const capture = chunk => {
        const text = String(chunk);
        proc.logs = (proc.logs + text).slice(-12000);
        process.stdout.write(text);
    };
    on_data(child.stdout, capture);
    on_data(child.stderr, capture);
    return proc;
}

/**
 * @param {ExecutorProc} proc
 * @returns {Promise<void>}
 */
function wait_for_executor(proc) {
    if (proc.logs.includes('replication_perf_executor: ready')) return Promise.resolve();
    return new Promise((resolve, reject) => {
        let settled = false;
        const timer = setTimeout(() => {
            finish(new Error(`executor did not start\n${proc.logs}`));
        }, 60000);

        /**
         * @param {Error} [err]
         */
        function finish(err) {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            if (err) reject(err);
            else resolve();
        }

        const on_text = () => {
            if (proc.logs.includes('replication_perf_executor: ready')) finish();
        };
        on_data(proc.child.stdout, on_text);
        on_data(proc.child.stderr, on_text);
        proc.child.once('exit', (code, signal) => {
            finish(new Error(`executor exited ${code} ${signal} before ready\n${proc.logs}`));
        });
    });
}

/**
 * @param {ExecutorProc[]} procs
 */
function assert_executors_alive(procs) {
    for (const proc of procs) {
        const child = proc.child;
        if (child.exitCode === null && child.signalCode === null) continue;
        throw new Error(`executor exited ${child.exitCode} ${child.signalCode}\n${proc.logs}`);
    }
}

/**
 * @param {ExecutorProc[]} procs
 * @returns {Promise<void>}
 */
async function stop_executors(procs) {
    const pending = procs.splice(0, procs.length);
    for (const proc of pending) {
        if (proc.child.exitCode === null && proc.child.signalCode === null) {
            proc.child.kill('SIGTERM');
        }
    }
    await Promise.all(pending.map(proc => wait_for_exit(proc, 5000)));
    for (const proc of pending) {
        if (proc.child.exitCode === null && proc.child.signalCode === null) {
            proc.child.kill('SIGKILL');
        }
    }
    await Promise.all(pending.map(proc => wait_for_exit(proc, 5000)));
}

/**
 * @param {ExecutorProc} proc
 * @param {number} limit_ms
 * @returns {Promise<void>}
 */
function wait_for_exit(proc, limit_ms) {
    const child = proc.child;
    if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve();
    return new Promise(resolve => {
        const timer = setTimeout(resolve, limit_ms);
        child.once('exit', () => {
            clearTimeout(timer);
            resolve();
        });
    });
}

/**
 * @returns {NodeJS.ProcessEnv}
 */
function executor_env() {
    const db_name = db_client.instance().get_db_name();
    const rpc_address = coretest.get_http_address().replace(/^http/, 'ws');
    return {
        ...process.env,
        MESSAGE_QUEUE_TYPE: 'postgres',
        BG_WORKERS_MODE: 'executor',
        SYSTEM_STORE_SOURCE: 'DB',
        POSTGRES_DBNAME: db_name,
        REPLICATION_PERF_DB_NAME: db_name,
        REPLICATION_PERF_RPC_ADDRESS: rpc_address,
        REPLICATION_PERF_ROOT_KEY_MOUNT: config.ROOT_KEY_MOUNT,
        REPLICATION_PERF_BATCH_DELAY_MS: String(batch_delay_ms),
        REPLICATION_PERF_EMPTY_DELAY_MS: String(empty_delay_ms),
    };
}

async function clear_queue() {
    try {
        await postgres_message_queue.instance().connect(config.REPLICATION_QUEUE_NAME);
        await db_client.instance().executeSQL(`DELETE FROM ${config.REPLICATION_QUEUE_NAME}`, []);
    } catch (err) {
        console.log('clear replicate queue:', err instanceof Error ? err.message : err);
    }
}

/**
 * @param {string} bucket
 * @param {number} count
 * @returns {Promise<void>}
 */
async function upload_objects(bucket, count) {
    const body = Buffer.alloc(object_bytes, 97);
    const keys = Array.from({ length: count }, (_value, index) => index);
    const started = Date.now();
    const s3 = s3_client();
    await P.map_with_concurrency(16, keys, index => s3.putObject({
        Bucket: bucket,
        Key: `obj-${String(index).padStart(6, '0')}`,
        Body: body,
    }));
    console.log(`uploaded ${count} objects in ${Date.now() - started}ms`);
}

/**
 * @param {string} bucket
 * @returns {Promise<number>}
 */
async function count_objects(bucket) {
    const s3 = s3_client();
    let count = 0;
    let marker = '';
    let truncated = true;
    while (truncated) {
        /** @type {{ Bucket: string, Marker?: string }} */
        const params = { Bucket: bucket };
        if (marker) params.Marker = marker;
        const response = await s3.listObjects(params);
        const contents = response.Contents || [];
        count += contents.length;
        truncated = Boolean(response.IsTruncated) && contents.length > 0;
        const last = contents[contents.length - 1];
        if (truncated && last && last.Key) marker = last.Key;
    }
    return count;
}

/**
 * @param {string[]} names
 * @returns {Promise<void>}
 */
async function delete_buckets(names) {
    for (const name of names.reverse()) {
        try {
            await rpc_client.bucket.delete_bucket_replication({ name });
        } catch (err) {
            const message = err instanceof Error ? err.message : err;
            console.log('delete_bucket_replication', name, message);
        }
        try {
            await empty_bucket(name);
            await rpc_client.bucket.delete_bucket({ name });
        } catch (err) {
            const message = err instanceof Error ? err.message : err;
            console.error('delete bucket', name, message);
        }
    }
}

/**
 * @param {string} bucket
 * @returns {Promise<void>}
 */
async function empty_bucket(bucket) {
    const s3 = s3_client();
    let marker = '';
    let truncated = true;
    while (truncated) {
        /** @type {{ Bucket: string, MaxKeys: number, Marker?: string }} */
        const params = { Bucket: bucket, MaxKeys: 1000 };
        if (marker) params.Marker = marker;
        const response = await s3.listObjects(params);
        const keys = (response.Contents || []).map(item => item.Key || '').filter(key => key);
        await P.map_with_concurrency(16, keys, key => s3.deleteObject({ Bucket: bucket, Key: key }));
        truncated = Boolean(response.IsTruncated) && keys.length > 0;
        const last = keys[keys.length - 1];
        if (truncated && last) marker = last;
    }
}

/**
 * @param {'src' | 'dst'} role
 * @param {{ queue: string, executors: number }} scenario
 * @returns {string}
 */
function next_bucket(role, scenario) {
    bucket_seq += 1;
    return `perf-${role}-${scenario.queue}-${scenario.executors}-${bucket_seq}`;
}

/**
 * @returns {S3}
 */
function s3_client() {
    if (!s3_owner) throw new Error('replication perf s3 client is not ready');
    return s3_owner;
}

/**
 * @returns {ReplicationWorker}
 */
function replication_scanner() {
    if (!scanner) throw new Error('replication perf scanner is not ready');
    return scanner;
}

/**
 * @param {import('stream').Readable | null} stream
 * @param {(chunk: Buffer) => void} fn
 */
function on_data(stream, fn) {
    if (!stream) throw new Error('executor stdio is not piped');
    stream.on('data', fn);
}

/**
 * @param {{ queue: string, executors: number, startup_ms: number, replicate_ms: number, copied: number, objects: number }} row
 * @returns {string}
 */
function format_result_row(row) {
    const seconds = row.replicate_ms / 1000;
    const rate = seconds > 0 ? (row.copied / seconds).toFixed(1) : '0.0';
    return [
        row.queue.padEnd(12),
        String(row.executors).padStart(9),
        String(row.startup_ms).padStart(11),
        String(row.replicate_ms).padStart(13),
        String(`${row.copied}/${row.objects}`).padStart(7),
        rate.padStart(16),
    ].join('  ');
}

function print_results() {
    const line = '='.repeat(88);
    console.log('\n' + line);
    console.log('replication performance');
    console.log(`objects=${object_count} page_size=${page_size} bytes=${object_bytes}`);
    console.log(`executor_batch_delay_ms=${batch_delay_ms} executor_empty_delay_ms=${empty_delay_ms}`);
    console.log('queue        executors  startup_ms  replicate_ms  copied  objects_per_sec');
    for (const row of results) {
        console.log(format_result_row(row));
    }
    console.log(line);
}

/**
 * @param {string | undefined} raw
 * @param {number} fallback
 * @returns {number}
 */
function positive_int(raw, fallback) {
    const value = Number(raw);
    if (!Number.isFinite(value) || value < 1) return fallback;
    return Math.floor(value);
}

/**
 * @typedef {object} ExecutorProc
 * @property {import('child_process').ChildProcess} child
 * @property {string} logs
 */

/**
 * The last page has been queued. Wait until every queued page is acked, then
 * the caller lists the destination.
 * @param {ExecutorProc[]} executors
 * @param {number} started
 * @returns {Promise<void>}
 */
async function wait_until_queued_copies_finish(executors, started) {
    if (config.MESSAGE_QUEUE_TYPE === 'none') return;
    while (Date.now() - started < timeout_ms) {
        assert_executors_alive(executors);
        const queued = await postgres_message_queue.instance().size(config.REPLICATION_QUEUE_NAME);
        if (!queued) return;
        await P.delay(50);
    }
}
