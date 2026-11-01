/* Copyright (C) 2016 NooBaa */
'use strict';

const coretest = require('../../utils/coretest/coretest');
coretest.setup();

const mocha = require('mocha');
const assert = require('assert');

const config = require('../../../../config');
const db_client = require('../../../util/db_client');
const message_queue_client = require('../../../util/message_queue_client');
const { PostgresMessageQueue } = require('../../../util/postgres_message_queue');
const {
    exercise_queue,
    exercise_stale_ack,
    exercise_exhausted_claim,
    exercise_max_size,
    exercise_one_producer_one_consumer,
    exercise_many_producers_many_consumers,
} = require('./message_queue_exercises');

mocha.describe('message queue', function() {
    this.timeout(120000); // eslint-disable-line no-invalid-this

    /** @type {PostgresMessageQueue} */
    let postgres_queue;

    mocha.before(async function() {
        postgres_queue = new PostgresMessageQueue();
    });

    mocha.it('factory none does not create a queue', function() {
        const orig = config.MESSAGE_QUEUE_TYPE;
        config.MESSAGE_QUEUE_TYPE = 'none';
        try {
            assert.strictEqual(message_queue_client.instance(), undefined);
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

    mocha.it('postgres queue rejects enqueue when the queue is full', async function() {
        const orig_capacity = config.MESSAGE_QUEUE_DEFAULT_CAPACITY;
        config.MESSAGE_QUEUE_DEFAULT_CAPACITY = 1;
        try {
            await exercise_max_size(postgres_queue);
        } finally {
            config.MESSAGE_QUEUE_DEFAULT_CAPACITY = orig_capacity;
        }
    });

    mocha.it('postgres queue dead-letters a crashed attempt that used the last try', async function() {
        const { _id, queue } = await exercise_exhausted_claim(postgres_queue);
        const res = await db_client.instance().executeSQL(
            `SELECT data->>'last_error' AS last_error,
                    data->'dead_at' IS NOT NULL AS dead
             FROM ${queue} WHERE _id = $1`,
            [_id],
        );
        assert.strictEqual(res.rows[0].dead, true);
        assert.strictEqual(res.rows[0].last_error, 'visibility timeout');
    });

    mocha.it('postgres queue holds a nacked message and then claims the next _id', async function() {
        const name = `order_${Date.now().toString(36)}`;
        await postgres_queue.connect(name, undefined, undefined, undefined, 60000);
        const older_id = await postgres_queue.enqueue(name, { kind: 'deleted', object_ids: ['older'] });
        const newer_id = await postgres_queue.enqueue(name, { kind: 'deleted', object_ids: ['newer'] });

        const claimed = await postgres_queue.dequeue(name);
        assert.ok(claimed);
        assert.strictEqual(claimed._id, older_id);
        const requeued = await postgres_queue.nack(claimed, 'retry');
        assert.strictEqual(requeued.dropped, false);

        const next = await postgres_queue.dequeue(name);
        assert.ok(next);
        assert.strictEqual(next._id, newer_id);
        await postgres_queue.ack(next);
        assert.strictEqual(await postgres_queue.dequeue(name), null);
    });

    mocha.it('postgres queue removes only dead messages older than retention', async function() {
        const queue_name = `dead_clean_${Date.now().toString(36)}`;
        const retention_ms = 60 * 60 * 1000;
        const orig_registered = config.MESSAGE_QUEUE_REGISTERED;
        config.MESSAGE_QUEUE_REGISTERED = Object.freeze([queue_name, 'missing_queue_not_created']);
        try {
            // Retry limit 0 drops the message on the first nack.
            // Retention is the connect argument, shorter than the config default.
            await postgres_queue.connect(queue_name, undefined, 0, retention_ms);

            const recent_dead_id = await enqueue_dead_message(postgres_queue, queue_name, 'recent');
            const expired_dead_id = await enqueue_dead_message(postgres_queue, queue_name, 'expired');
            await set_dead_at(queue_name, expired_dead_id, retention_ms + 1000);
            const live_id = await postgres_queue.enqueue(queue_name, { kind: 'deleted', object_ids: ['live'] });

            const { removed, had_errors } = await postgres_queue.remove_expired_dead_messages(100);
            assert.ok(removed >= 1);
            assert.strictEqual(had_errors, true);

            const left = await db_client.instance().executeSQL(
                `SELECT _id, data->'dead_at' IS NOT NULL AS dead FROM ${queue_name}`,
            );
            const dead_by_id = new Map(left.rows.map(row => [String(row._id).trim(), row.dead]));
            assert.strictEqual(dead_by_id.size, 2);
            assert.strictEqual(dead_by_id.get(live_id), false);
            assert.strictEqual(dead_by_id.get(recent_dead_id), true);
            assert.strictEqual(dead_by_id.has(expired_dead_id), false);
        } finally {
            config.MESSAGE_QUEUE_REGISTERED = orig_registered;
        }
    });

    mocha.it('postgres one producer and one consumer drain one queue together', async function() {
        await exercise_one_producer_one_consumer(postgres_queue, 'pg');
    });

    mocha.it('postgres many producers and many consumers claim each message once', async function() {
        await exercise_many_producers_many_consumers(postgres_queue, 'pg');
    });
});

/**
 * Enqueue one message and drop it on the first nack.
 * The queue must have no other live message, because dequeue claims the smallest _id.
 * @param {PostgresMessageQueue} queue
 * @param {string} name
 * @param {string} object_id
 * @returns {Promise<string>}
 */
async function enqueue_dead_message(queue, name, object_id) {
    const enqueued_id = await queue.enqueue(name, { kind: 'deleted', object_ids: [object_id] });
    const message = await queue.dequeue(name);
    assert.ok(message);
    assert.strictEqual(message._id, enqueued_id);
    const dropped = await queue.nack(message, 'dropped in test');
    assert.strictEqual(dropped.dropped, true);
    return enqueued_id;
}

/**
 * Set dead_at to age_ms before now. Cleanup deletes a dead row when this age exceeds the queue retention.
 * @param {string} name
 * @param {string} id
 * @param {number} age_ms
 * @returns {Promise<void>}
 */
async function set_dead_at(name, id, age_ms) {
    await db_client.instance().executeSQL(
        `UPDATE ${name}
         SET data = jsonb_set(
             data,
             '{dead_at}',
             to_jsonb(now() - ($2::text || ' milliseconds')::interval)
         )
         WHERE _id = $1`,
        [id, String(age_ms)],
    );
}
