/* Copyright (C) 2016 NooBaa */
'use strict';

const coretest = require('../../utils/coretest/coretest');
coretest.setup();

const mocha = require('mocha');
const assert = require('assert');

const P = require('../../../util/promise');
const config = require('../../../../config');
const db_client = require('../../../util/db_client');
const message_queue_client = require('../../../util/message_queue_client');
const { PostgresMessageQueue } = require('../../../util/postgres_message_queue');
const { GraphileMessageQueue } = require('../../../util/graphile_message_queue');
const { PgBossMessageQueue } = require('../../../util/pgboss_message_queue');
const { chunk_list } = require('../../../server/bg_services/objects_reclaimer');

mocha.describe('message queue', function() {
    // graphile migrate opens its own pool against the coretest database
    this.timeout(120000); // eslint-disable-line no-invalid-this

    /** @type {PostgresMessageQueue} */
    let postgres_queue;
    /** @type {GraphileMessageQueue} */
    let graphile_queue;
    /** @type {PgBossMessageQueue} */
    let pgboss_queue;

    mocha.before(async function() {
        postgres_queue = new PostgresMessageQueue();
        graphile_queue = new GraphileMessageQueue();
        pgboss_queue = new PgBossMessageQueue();
        await postgres_queue.connect();
        await graphile_queue.connect();
        await pgboss_queue.connect();
    });

    mocha.after(async function() {
        if (graphile_queue) await graphile_queue.disconnect();
        if (pgboss_queue) await pgboss_queue.disconnect();
    });

    mocha.describe('chunk_list', function() {
        mocha.it('splits object ids into configurable batches', function() {
            assert.deepStrictEqual(chunk_list(['a', 'b', 'c', 'd', 'e'], 2), [
                ['a', 'b'],
                ['c', 'd'],
                ['e'],
            ]);
            assert.deepStrictEqual(chunk_list(['only'], 100), [['only']]);
            assert.deepStrictEqual(chunk_list([], 100), []);
        });
    });

    mocha.it('factory none client does not store messages', async function() {
        const orig = config.MESSAGE_QUEUE_TYPE;
        config.MESSAGE_QUEUE_TYPE = 'none';
        try {
            const queue = message_queue_client.instance();
            await queue.enqueue('objects_reclaimer', { kind: 'deleted', object_ids: ['a'] });
            assert.strictEqual(await queue.dequeue('objects_reclaimer'), null);
            assert.strictEqual(await queue.size('objects_reclaimer'), 0);
        } finally {
            config.MESSAGE_QUEUE_TYPE = orig;
        }
    });

    mocha.it('factory rejects an unknown backend', function() {
        const orig = config.MESSAGE_QUEUE_TYPE;
        // @ts-expect-error unknown backend must be rejected at runtime
        config.MESSAGE_QUEUE_TYPE = 'redis';
        try {
            assert.throws(() => message_queue_client.instance(), /NON SUPPORTED MESSAGE_QUEUE_TYPE/);
        } finally {
            config.MESSAGE_QUEUE_TYPE = orig;
        }
    });

    mocha.it('postgres queue locks, retries, and drops JSON batches', async function() {
        await exercise_queue(postgres_queue, 'pg');
    });

    mocha.it('postgres queue ignores a stale ack after the lock expires', async function() {
        await exercise_stale_ack(postgres_queue);
    });

    mocha.it('postgres queue dead-letters a crashed attempt that used the last try', async function() {
        const id = await exercise_terminal_claim(postgres_queue);
        const res = await db_client.instance().executeSQL(
            `SELECT last_error, dead_at IS NOT NULL AS dead
             FROM nb_message_queue WHERE id = $1::bigint`,
            [id],
        );
        assert.strictEqual(res.rows[0].dead, true);
        assert.strictEqual(res.rows[0].last_error, 'visibility timeout');
    });

    mocha.it('postgres queue extend keeps the claim past the original lock', async function() {
        await exercise_extend(postgres_queue);
    });

    mocha.it('postgres one producer and one consumer drain one queue together', async function() {
        await exercise_one_producer_one_consumer(postgres_queue, 'pg');
    });

    mocha.it('postgres many producers and many consumers claim each message once', async function() {
        await exercise_many_producers_many_consumers(postgres_queue, 'pg');
    });

    mocha.it('postgres a second consumer takes the message only after the lease stops', async function() {
        await exercise_second_consumer_after_lease(postgres_queue);
    });

    mocha.it('graphile queue locks, retries, and drops JSON batches', async function() {
        await exercise_queue(graphile_queue, 'gw');
    });

    mocha.it('graphile queue ignores a stale ack after the lock expires', async function() {
        await exercise_stale_ack(graphile_queue);
    });

    mocha.it('graphile queue returns a crashed last attempt so the caller can drop it', async function() {
        await exercise_terminal_claim(graphile_queue);
    });

    mocha.it('graphile queue extend keeps the claim past the original lock', async function() {
        await exercise_extend(graphile_queue);
    });

    mocha.it('graphile one producer and one consumer drain one queue together', async function() {
        await exercise_one_producer_one_consumer(graphile_queue, 'gw');
    });

    mocha.it('graphile many producers and many consumers claim each message once', async function() {
        await exercise_many_producers_many_consumers(graphile_queue, 'gw');
    });

    mocha.it('graphile a second consumer takes the message only after the lease stops', async function() {
        await exercise_second_consumer_after_lease(graphile_queue);
    });

    mocha.it('pg-boss queue locks, retries, and drops JSON batches', async function() {
        await exercise_queue(pgboss_queue, 'boss');
    });

    mocha.it('pg-boss queue ignores a stale ack after another claim', async function() {
        await exercise_pgboss_stale_ack(pgboss_queue);
    });

    mocha.it('pg-boss queue extend matches only the active attempt', async function() {
        await exercise_pgboss_extend(pgboss_queue);
    });

    mocha.it('pg-boss one producer and one consumer drain one queue together', async function() {
        await exercise_one_producer_one_consumer(pgboss_queue, 'boss');
    });

    mocha.it('pg-boss many producers and many consumers claim each message once', async function() {
        await exercise_many_producers_many_consumers(pgboss_queue, 'boss');
    });

    mocha.it('factory returns the pg-boss client', function() {
        const orig = config.MESSAGE_QUEUE_TYPE;
        config.MESSAGE_QUEUE_TYPE = 'pgboss';
        try {
            const queue = message_queue_client.instance();
            assert.ok(queue instanceof PgBossMessageQueue);
        } finally {
            config.MESSAGE_QUEUE_TYPE = orig;
        }
    });

    mocha.it('pg-boss nack delay hides the message until it is due', async function() {
        const name = `boss_delay_${Date.now().toString(36)}`;
        const orig_delay = config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS;
        config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS = 400;
        try {
            await pgboss_queue.enqueue(name, { kind: 'deleted', object_ids: ['obj-d'] });
            const message = await pgboss_queue.dequeue(name);
            if (!message) throw new Error('expected a message to delay');
            const nacked = await pgboss_queue.nack(message, 'later');
            assert.strictEqual(nacked.dropped, false);
            assert.strictEqual(await pgboss_queue.dequeue(name), null);
            await P.delay(700);
            const again = await pgboss_queue.dequeue(name);
            assert.ok(again);
            assert.strictEqual(again.id, message.id);
            assert.strictEqual(again.attempts, 2);
            await pgboss_queue.ack(again);
            assert.strictEqual(await pgboss_queue.size(name), 0);
        } finally {
            config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS = orig_delay;
        }
    });
});

/**
 * @param {nb.MessageQueueClient} queue
 * @param {string} prefix
 */
async function exercise_queue(queue, prefix) {
    const name = `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    assert.strictEqual(await queue.size(name), 0);
    assert.strictEqual(await queue.dequeue(name), null);
    await assert.rejects(() => queue.enqueue(name, /** @type {any} */ (null)), /JSON object/);

    const enqueued_id = await queue.enqueue(name, {
        kind: 'deleted',
        object_ids: ['obj-1', 'obj-2'],
    });
    assert.ok(enqueued_id);
    await queue.enqueue(name, { kind: 'expired_restore', object_ids: ['obj-3'] });
    assert.strictEqual(await queue.size(name), 2);

    const first = await queue.dequeue(name);
    const second = await queue.dequeue(name);
    assert.ok(first && second);
    assert.notStrictEqual(first.id, second.id);
    assert.strictEqual(await queue.dequeue(name), null);
    const first_ids = /** @type {{ object_ids: string[] }} */ (first.payload).object_ids;
    const second_ids = /** @type {{ object_ids: string[] }} */ (second.payload).object_ids;
    const ids = first_ids.concat(second_ids);
    assert.deepStrictEqual(ids.sort(), ['obj-1', 'obj-2', 'obj-3']);
    assert.strictEqual(first.attempts, 1);
    assert.strictEqual(second.attempts, 1);

    await queue.ack(first);

    const orig_delay = config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS;
    const orig_attempts = config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS;
    config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS = 0;
    try {
        const retried = await queue.nack(second, 'try again');
        assert.strictEqual(retried.dropped, false);
        const again = await queue.dequeue(name);
        assert.ok(again);
        assert.strictEqual(again.id, second.id);
        assert.strictEqual(again.attempts, 2);
        assert.deepStrictEqual(again.payload, second.payload);
        await queue.ack(again);
        assert.strictEqual(await queue.size(name), 0);

        config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS = 1;
        await queue.enqueue(name, { kind: 'transition_source', object_ids: ['obj-9'] });
        const doomed = await queue.dequeue(name);
        if (!doomed) throw new Error('expected a message to drop');
        assert.strictEqual(doomed.attempts, 1);
        const dropped = await queue.nack(doomed, 'give up');
        assert.strictEqual(dropped.dropped, true);
        assert.strictEqual(await queue.dequeue(name), null);
        assert.strictEqual(await queue.size(name), 0);
    } finally {
        config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS = orig_delay;
        config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS = orig_attempts;
    }
}

/**
 * @param {nb.MessageQueueClient} queue
 */
async function exercise_stale_ack(queue) {
    const name = `pg_stale_${Date.now().toString(36)}`;
    const orig_visibility = config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS;
    config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = 1;
    try {
        await queue.enqueue(name, { kind: 'deleted', object_ids: ['obj-s'] });
        const first = await queue.dequeue(name);
        if (!first) throw new Error('expected a message');
        await P.delay(50);
        const second = await queue.dequeue(name);
        if (!second) throw new Error('expected the message to be claimed again');
        assert.strictEqual(second.id, first.id);
        assert.strictEqual(second.attempts, 2);
        assert.notStrictEqual(second.lock_token, first.lock_token);
        await assert.rejects(() => queue.ack(first), /lost the lock/);
        assert.strictEqual(await queue.size(name), 1);
        await queue.ack(second);
        assert.strictEqual(await queue.size(name), 0);
    } finally {
        config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = orig_visibility;
    }
}

/**
 * The claim that already used the last attempt comes back with terminal set,
 * and nack drops it instead of running the batch again.
 * @param {nb.MessageQueueClient} queue
 * @returns {Promise<string>}
 */
async function exercise_terminal_claim(queue) {
    const name = `term_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
    const orig_visibility = config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS;
    const orig_attempts = config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS;
    config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = 1;
    config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS = 1;
    try {
        await queue.enqueue(name, { kind: 'deleted', object_ids: ['obj-dead'] });
        await queue.enqueue(name, { kind: 'deleted', object_ids: ['obj-live'] });
        const crashed = await queue.dequeue(name);
        if (!crashed) throw new Error('expected a message');
        assert.strictEqual(crashed.attempts, 1);
        assert.ok(!crashed.terminal);
        await P.delay(50);
        const expired = await queue.dequeue(name);
        if (!expired) throw new Error('expected the exhausted message to be returned');
        assert.strictEqual(expired.id, crashed.id);
        assert.strictEqual(expired.terminal, true);
        const dropped = await queue.nack(expired, 'visibility timeout');
        assert.strictEqual(dropped.dropped, true);
        const live = await queue.dequeue(name);
        if (!live) throw new Error('expected the next live message');
        assert.notStrictEqual(live.id, crashed.id);
        assert.ok(!live.terminal);
        const live_ids = /** @type {{ object_ids: string[] }} */ (live.payload).object_ids;
        assert.deepStrictEqual(live_ids, ['obj-live']);
        await queue.ack(live);
        assert.strictEqual(await queue.dequeue(name), null);
        assert.strictEqual(await queue.size(name), 0);
        return crashed.id;
    } finally {
        config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = orig_visibility;
        config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS = orig_attempts;
    }
}

/**
 * @param {nb.MessageQueueClient} queue
 */
async function exercise_extend(queue) {
    const name = `ext_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
    const orig_visibility = config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS;
    config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = 2000;
    try {
        await queue.enqueue(name, { kind: 'deleted', object_ids: ['obj-e'] });
        const message = await queue.dequeue(name);
        if (!message) throw new Error('expected a message');
        await P.delay(400);
        assert.strictEqual(await queue.extend(message), true);
        await P.delay(1800);
        assert.strictEqual(await queue.dequeue(name), null);
        await queue.ack(message);
        assert.strictEqual(await queue.extend(message), false);
        assert.strictEqual(await queue.size(name), 0);
    } finally {
        config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = orig_visibility;
    }
}

/**
 * @param {nb.MessageQueueClient} queue
 */
async function exercise_pgboss_extend(queue) {
    const name = `boss_ext_${Date.now().toString(36)}`;
    await queue.enqueue(name, { kind: 'deleted', object_ids: ['obj-e'] });
    const message = await queue.dequeue(name);
    if (!message) throw new Error('expected a message');
    assert.strictEqual(await queue.extend(message), true);
    await queue.ack(message);
    assert.strictEqual(await queue.extend(message), false);
}

/**
 * @param {nb.MessageQueueClient} queue
 */
async function exercise_pgboss_stale_ack(queue) {
    const name = `boss_stale_${Date.now().toString(36)}`;
    const orig_delay = config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS;
    config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS = 0;
    try {
        await queue.enqueue(name, { kind: 'deleted', object_ids: ['obj-s'] });
        const first = await queue.dequeue(name);
        if (!first) throw new Error('expected a message');
        const nacked = await queue.nack(first, 'again');
        assert.strictEqual(nacked.dropped, false);
        const second = await queue.dequeue(name);
        if (!second) throw new Error('expected the message to be claimed again');
        assert.strictEqual(second.id, first.id);
        assert.strictEqual(second.attempts, first.attempts + 1);
        assert.notStrictEqual(second.retry_count, first.retry_count);
        await assert.rejects(() => queue.ack(first), /could not ack/);
        assert.strictEqual(await queue.size(name), 1);
        await queue.ack(second);
        assert.strictEqual(await queue.size(name), 0);
    } finally {
        config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS = orig_delay;
    }
}

/**
 * One producer task and one consumer task run at the same time.
 * The consumer keeps polling until the producer has finished and every
 * message has been acked once.
 * @param {nb.MessageQueueClient} queue
 * @param {string} prefix
 */
async function exercise_one_producer_one_consumer(queue, prefix) {
    await exercise_concurrent_workers(queue, prefix, 1, 1, 40);
}

/**
 * Several producers insert while several consumers claim. Each message id
 * is acked once.
 * @param {nb.MessageQueueClient} queue
 * @param {string} prefix
 */
async function exercise_many_producers_many_consumers(queue, prefix) {
    await exercise_concurrent_workers(queue, prefix, 4, 8, 8);
}

/**
 * @param {nb.MessageQueueClient} queue
 * @param {string} prefix
 * @param {number} producers
 * @param {number} consumers
 * @param {number} per_producer
 */
async function exercise_concurrent_workers(queue, prefix, producers, consumers, per_producer) {
    const name = `${prefix}_conc_${producers}p${consumers}c_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
    const count = producers * per_producer;
    /** @type {Set<string>} */
    const claimed = new Set();
    /** @type {Set<number>} */
    const payloads = new Set();
    let enqueued = 0;
    let producers_done = 0;

    await Promise.all([
        ...Array.from({ length: producers }, (_, producer) => produce_batch(
            queue,
            name,
            producer,
            per_producer,
            () => {
                enqueued += 1;
            },
            () => {
                producers_done += 1;
            },
        )),
        ...Array.from({ length: consumers }, () => consume_until_done(
            queue,
            name,
            count,
            claimed,
            payloads,
            () => producers_done === producers && enqueued === count,
        )),
    ]);

    assert.strictEqual(claimed.size, count);
    assert.strictEqual(payloads.size, count);
    assert.strictEqual(await queue.dequeue(name), null);
    assert.strictEqual(await queue.size(name), 0);
}

/**
 * @param {nb.MessageQueueClient} queue
 * @param {string} name
 * @param {number} producer
 * @param {number} per_producer
 * @param {() => void} on_enqueue
 * @param {() => void} on_done
 */
async function produce_batch(queue, name, producer, per_producer, on_enqueue, on_done) {
    for (let n = 0; n < per_producer; n++) {
        await queue.enqueue(name, {
            kind: 'deleted',
            object_ids: [`p${producer}-n${n}`],
            n: (producer * per_producer) + n,
        });
        on_enqueue();
    }
    on_done();
}

/**
 * @param {nb.MessageQueueClient} queue
 * @param {string} name
 * @param {number} count
 * @param {Set<string>} claimed
 * @param {Set<number>} payloads
 * @param {() => boolean} producers_finished
 */
async function consume_until_done(queue, name, count, claimed, payloads, producers_finished) {
    while (claimed.size < count) {
        const finished_before_poll = producers_finished();
        const message = await queue.dequeue(name);
        if (!message) {
            if (finished_before_poll && claimed.size === count) return;
            await P.delay(5);
            continue;
        }
        assert.ok(!claimed.has(message.id), `message ${message.id} was claimed twice`);
        claimed.add(message.id);
        const n = /** @type {{ n: number }} */ (message.payload).n;
        assert.ok(!payloads.has(n), `payload ${n} was claimed twice`);
        payloads.add(n);
        await queue.ack(message);
    }
}

/**
 * The first consumer holds the lease past the original visibility timeout.
 * The second consumer gets nothing until that lease stops, then claims the
 * same message. The first consumer's ack is rejected.
 * @param {nb.MessageQueueClient} queue
 */
async function exercise_second_consumer_after_lease(queue) {
    const name = `lease2_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
    const orig_visibility = config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS;
    config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = 2000;
    /** @type {{ stop: () => void } | undefined} */
    let lease;
    try {
        await queue.enqueue(name, { kind: 'deleted', object_ids: ['obj-lease'] });
        const first = await queue.dequeue(name);
        if (!first) throw new Error('expected a message');
        lease = message_queue_client.hold_lease(queue, first);
        // Visibility is 2000ms and the lease refreshes every 1000ms, so this
        // sample is after the original deadline while the first consumer still holds it.
        await P.delay(2500);
        assert.strictEqual(await queue.dequeue(name), null);
        lease.stop();
        lease = undefined;
        const second = await dequeue_until(queue, name, 6000);
        if (!second) throw new Error('expected the second consumer to claim the message');
        assert.strictEqual(second.id, first.id);
        assert.strictEqual(second.attempts, 2);
        assert.notStrictEqual(second.lock_token, first.lock_token);
        await assert.rejects(() => queue.ack(first), /lost the lock/);
        assert.strictEqual(await queue.size(name), 1);
        await queue.ack(second);
        assert.strictEqual(await queue.size(name), 0);
    } finally {
        if (lease) lease.stop();
        config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = orig_visibility;
    }
}

/**
 * @param {nb.MessageQueueClient} queue
 * @param {string} name
 * @param {number} timeout_ms
 * @returns {Promise<nb.QueueMessage | null>}
 */
async function dequeue_until(queue, name, timeout_ms) {
    const deadline = Date.now() + timeout_ms;
    while (Date.now() < deadline) {
        const message = await queue.dequeue(name);
        if (message) return message;
        await P.delay(100);
    }
    return null;
}
