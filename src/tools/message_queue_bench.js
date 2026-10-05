/* Copyright (C) 2016 NooBaa */
/** @typedef {typeof import('../sdk/nb')} nb */
/**
 * @typedef {{
 *   type: string,
 *   op: string,
 *   count: number,
 *   seconds: number,
 *   ops_s: number,
 *   p50_ms: number,
 *   p99_ms: number,
 *   dequeue_p50_ms: number,
 *   ack_p50_ms: number,
 * }} ReportRow
 */
'use strict';

// node src/tools/message_queue_bench.js --type postgres,graphile,pgboss --count 1000 --concurrency 4 --bytes 128 --pool 4 --producers 5
//
// For each message queue client, on the NooBaa Postgres database:
//   1. enqueue, then drain, with --concurrency workers
//   2. --producers pushers and twice that many poppers, at the same time

const argv = require('minimist')(process.argv.slice(2));

const config = require('../../config');
const db_client = require('../util/db_client');
const { PostgresMessageQueue } = require('../util/postgres_message_queue');
const { GraphileMessageQueue } = require('../util/graphile_message_queue');
const { PgBossMessageQueue } = require('../util/pgboss_message_queue');
const console_wrapper = require('../util/console_wrapper');

main().catch(err => {
    out_error('message queue benchmark failed', err);
    process.exit(1);
});

async function main() {
    if (argv.help) {
        out('node src/tools/message_queue_bench.js [--type postgres,graphile,pgboss] [--count 1000] [--concurrency 4] [--bytes 128] [--pool 4] [--producers 5]');
        process.exit(0);
    }
    const types = String(argv.type || 'postgres,graphile,pgboss').split(',').map(type => type.trim()).filter(Boolean);
    const count = positive_int(argv.count, 1000, 'count');
    const concurrency = positive_int(argv.concurrency, 4, 'concurrency');
    const bytes = positive_int(argv.bytes, 128, 'bytes');
    const producers = positive_int(argv.producers, 5, 'producers');
    const consumers = producers * 2;
    for (const type of types) {
        if (type !== 'postgres' && type !== 'graphile' && type !== 'pgboss') {
            throw new Error(`unknown message queue type: ${type}`);
        }
    }
    const pool_floor = producers + consumers;
    const pool_size = argv.pool ? positive_int(argv.pool, pool_floor, 'pool') : pool_floor;
    config.POSTGRES_DEFAULT_MAX_CLIENTS = Math.max(config.POSTGRES_DEFAULT_MAX_CLIENTS, pool_size);
    config.MESSAGE_QUEUE_GRAPHILE_POOL_MAX = Math.max(config.MESSAGE_QUEUE_GRAPHILE_POOL_MAX, pool_size);
    config.MESSAGE_QUEUE_PGBOSS_POOL_MAX = Math.max(config.MESSAGE_QUEUE_PGBOSS_POOL_MAX, pool_size);

    /** @type {ReportRow[]} */
    const rows = [];
    let failed = false;
    await db_client.instance().connect();
    out('message queue benchmark', JSON.stringify({
        types,
        count,
        concurrency,
        producers,
        consumers,
        bytes,
        postgres_pool: config.POSTGRES_DEFAULT_MAX_CLIENTS,
        graphile_pool: config.MESSAGE_QUEUE_GRAPHILE_POOL_MAX,
        pgboss_pool: config.MESSAGE_QUEUE_PGBOSS_POOL_MAX,
    }));
    for (const type of types) {
        try {
            rows.push(...await bench_one(type, count, concurrency, producers, consumers, bytes));
        } catch (err) {
            failed = true;
            out_error(type, 'failed', err);
        }
    }
    print_report(rows);
    process.exit(failed ? 1 : 0);
}

/**
 * @param {string} type
 * @param {number} count
 * @param {number} concurrency
 * @param {number} producers
 * @param {number} consumers
 * @param {number} bytes
 * @returns {Promise<ReportRow[]>}
 */
async function bench_one(type, count, concurrency, producers, consumers, bytes) {
    const queue = create_client(type);
    const connect_ms = await elapsed(() => queue.connect());
    const warmup = Math.min(20, count);
    const warm_name = queue_name(type, 'warm');
    await run_enqueue(queue, warm_name, warmup, 1, bytes);
    await run_drain(queue, warm_name, 1);
    const name = queue_name(type, 'run');
    const enqueue = await run_enqueue(queue, name, count, concurrency, bytes);
    const drain = await run_drain(queue, name, concurrency);
    const left = await queue.size(name);
    const live_name = queue_name(type, 'live');
    const live = await run_live(queue, live_name, count, producers, consumers, bytes);
    const live_left = await queue.size(live_name);
    await queue.disconnect();
    if (drain.acked !== count || left !== 0) {
        throw new Error(`${type} drained ${drain.acked} of ${count}, ${left} left in the queue`);
    }
    if (live.acked !== count || live_left !== 0) {
        throw new Error(`${type} live run acked ${live.acked} of ${count}, ${live_left} left in the queue`);
    }
    return [
        report_row(type, 'connect', 1, connect_ms, []),
        report_row(type, 'enqueue', count, enqueue.ms, enqueue.latencies),
        report_row(type, 'drain', count, drain.ms, drain.latencies, drain.dequeue_latencies, drain.ack_latencies),
        report_row(type, `push/${producers}`, count, live.push_ms, live.enqueue_latencies),
        report_row(type, `pop/${consumers}`, count, live.ms, live.latencies, live.dequeue_latencies, live.ack_latencies),
    ];
}

/**
 * @param {nb.MessageQueueClient} queue
 * @param {string} name
 * @param {number} count
 * @param {number} concurrency
 * @param {number} bytes
 */
async function run_enqueue(queue, name, count, concurrency, bytes) {
    const pad = 'x'.repeat(bytes);
    let next = 0;
    /** @type {number[]} */
    const latencies = [];
    const ms = await elapsed(() => worker_pool(concurrency, async () => {
        for (;;) {
            const n = next;
            next += 1;
            if (n >= count) return;
            const sample = await timed(() => queue.enqueue(name, { n, pad }));
            latencies.push(sample.ms);
        }
    }));
    return { ms, latencies };
}

/**
 * @param {nb.MessageQueueClient} queue
 * @param {string} name
 * @param {number} concurrency
 */
async function run_drain(queue, name, concurrency) {
    let acked = 0;
    /** @type {number[]} */
    const latencies = [];
    /** @type {number[]} */
    const dequeue_latencies = [];
    /** @type {number[]} */
    const ack_latencies = [];
    const ms = await elapsed(() => worker_pool(concurrency, async () => {
        for (;;) {
            const dequeued = await timed(() => queue.dequeue(name));
            const message = dequeued.value;
            if (!message) return;
            const acked_sample = await timed(() => queue.ack(message));
            dequeue_latencies.push(dequeued.ms);
            ack_latencies.push(acked_sample.ms);
            latencies.push(dequeued.ms + acked_sample.ms);
            acked += 1;
        }
    }));
    return { acked, ms, latencies, dequeue_latencies, ack_latencies };
}

/**
 * Producers and consumers run together. Consumers keep polling after an empty
 * dequeue, because the producers may not have published the next message yet.
 * @param {nb.MessageQueueClient} queue
 * @param {string} name
 * @param {number} count
 * @param {number} producers
 * @param {number} consumers
 * @param {number} bytes
 */
async function run_live(queue, name, count, producers, consumers, bytes) {
    const pad = 'x'.repeat(bytes);
    let next = 0;
    let inflight = 0;
    let acked = 0;
    let producers_done = false;
    /** @type {number[]} */
    const enqueue_latencies = [];
    /** @type {number[]} */
    const latencies = [];
    /** @type {number[]} */
    const dequeue_latencies = [];
    /** @type {number[]} */
    const ack_latencies = [];
    const started = process.hrtime.bigint();
    /** @type {number} */
    let push_ms = 0;
    const ms = await elapsed(() => {
        const pushing = worker_pool(producers, async () => {
            for (;;) {
                const n = next;
                next += 1;
                if (n >= count) return;
                const sample = await timed(() => queue.enqueue(name, { n, pad }));
                enqueue_latencies.push(sample.ms);
            }
        }).then(() => {
            producers_done = true;
            push_ms = Number(process.hrtime.bigint() - started) / 1e6;
        });
        const popping = worker_pool(consumers, async () => {
            for (;;) {
                // An empty read started before the producers finished does not
                // mean the queue is done; they may have published during it.
                const producers_done_before_poll = producers_done;
                const dequeued = await timed(() => queue.dequeue(name));
                const message = dequeued.value;
                if (!message) {
                    if (producers_done_before_poll && inflight === 0) return;
                    continue;
                }
                inflight += 1;
                const acked_sample = await timed(() => queue.ack(message));
                inflight -= 1;
                acked += 1;
                dequeue_latencies.push(dequeued.ms);
                ack_latencies.push(acked_sample.ms);
                latencies.push(dequeued.ms + acked_sample.ms);
            }
        });
        return Promise.all([pushing, popping]);
    });
    return { acked, ms, push_ms, latencies, enqueue_latencies, dequeue_latencies, ack_latencies };
}

/**
 * @param {string} type
 * @returns {nb.MessageQueueClient}
 */
function create_client(type) {
    switch (type) {
        case 'postgres':
            return new PostgresMessageQueue();
        case 'graphile':
            return new GraphileMessageQueue();
        case 'pgboss':
            return new PgBossMessageQueue();
        default:
            throw new Error(`unknown message queue type: ${type}`);
    }
}

/**
 * @param {string} type
 * @param {string} op
 * @param {number} count
 * @param {number} ms
 * @param {number[]} latencies
 * @param {number[]} [dequeue_latencies]
 * @param {number[]} [ack_latencies]
 * @returns {ReportRow}
 */
function report_row(type, op, count, ms, latencies, dequeue_latencies, ack_latencies) {
    const seconds = ms / 1000;
    return {
        type,
        op,
        count,
        seconds,
        ops_s: seconds > 0 ? count / seconds : 0,
        p50_ms: percentile(latencies, 50),
        p99_ms: percentile(latencies, 99),
        dequeue_p50_ms: percentile(dequeue_latencies || [], 50),
        ack_p50_ms: percentile(ack_latencies || [], 50),
    };
}

/**
 * @param {ReportRow[]} rows
 */
function print_report(rows) {
    const header = ['type', 'op', 'count', 'seconds', 'ops/s', 'p50 ms', 'p99 ms', 'dequeue p50', 'ack p50'];
    /** @type {string[][]} */
    const body = rows.map(item => [
        item.type,
        item.op,
        String(item.count),
        item.seconds.toFixed(3),
        item.op === 'connect' ? '' : item.ops_s.toFixed(1),
        item.op === 'connect' ? '' : item.p50_ms.toFixed(2),
        item.op === 'connect' ? '' : item.p99_ms.toFixed(2),
        item.op === 'drain' || item.op.startsWith('pop/') ? item.dequeue_p50_ms.toFixed(2) : '',
        item.op === 'drain' || item.op.startsWith('pop/') ? item.ack_p50_ms.toFixed(2) : '',
    ]);
    const widths = header.map((name, index) => Math.max(name.length, ...body.map(line => line[index].length)));
    /** @param {string[]} line */
    const format = line => line.map((cell, index) => cell.padEnd(widths[index])).join('  ');
    out(format(header));
    for (const line of body) out(format(line));
}

function out(...args) {
    console_wrapper.original_console();
    console.log(...args);
}

function out_error(...args) {
    console_wrapper.original_console();
    console.error(...args);
}

/**
 * @param {number} concurrency
 * @param {() => Promise<void>} worker
 */
function worker_pool(concurrency, worker) {
    /** @type {Array<Promise<void>>} */
    const workers = [];
    for (let i = 0; i < concurrency; i += 1) workers.push(worker());
    return Promise.all(workers);
}

/**
 * @param {() => Promise<any>} func
 * @returns {Promise<number>}
 */
async function elapsed(func) {
    const sample = await timed(func);
    return sample.ms;
}

/**
 * @template T
 * @param {() => Promise<T>} func
 * @returns {Promise<{ value: T, ms: number }>}
 */
async function timed(func) {
    const start = process.hrtime.bigint();
    const value = await func();
    return { value, ms: Number(process.hrtime.bigint() - start) / 1e6 };
}

/**
 * @param {number[]} samples
 * @param {number} pct
 */
function percentile(samples, pct) {
    if (samples.length === 0) return 0;
    const sorted = samples.slice().sort((a, b) => a - b);
    const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil((pct / 100) * sorted.length) - 1));
    return sorted[index];
}

/**
 * @param {string | number | undefined} value
 * @param {number} fallback
 * @param {string} name
 */
function positive_int(value, fallback, name) {
    const parsed = value === undefined ? fallback : Number(value);
    if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`${name} must be a positive integer`);
    return parsed;
}

/**
 * @param {string} type
 * @param {string} phase
 */
function queue_name(type, phase) {
    return `bench_${type}_${phase}_${process.pid}_${Date.now().toString(36)}`;
}
