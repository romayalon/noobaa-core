/* Copyright (C) 2016 NooBaa */
'use strict';

/** @typedef {typeof import('../../../sdk/nb')} nb */

const coretest = require('../../utils/coretest/coretest');
coretest.setup();

const mocha = require('mocha');
const assert = require('assert');

const P = require('../../../util/promise');
const config = require('../../../../config');
const { PostgresMessageQueue } = require('../../../util/postgres_message_queue');
const { ObjectsReclaimer } = require('../../../server/bg_services/objects_reclaimer');

mocha.describe('objects reclaimer message queue', function() {
    this.timeout(60000); // eslint-disable-line no-invalid-this

    /** @type {PostgresMessageQueue} */
    let queue;

    mocha.before(async function() {
        queue = new PostgresMessageQueue();
        await queue.connect();
    });

    mocha.it('run_batch acks the message it enqueued and releases the claim', async function() {
        await exercise_reclaimer_run_batch(queue);
    });

    mocha.it('keeps the claim when a batch is nacked for retry', async function() {
        await exercise_reclaimer_retry(queue);
    });

    mocha.it('drops a terminal message and releases the claim without processing', async function() {
        await exercise_reclaimer_terminal(queue);
    });

    mocha.it('keeps the claim when a stale ack loses the lock', async function() {
        await exercise_reclaimer_stale_ack(queue);
    });
});

class ProbeReclaimer extends ObjectsReclaimer {

    /**
     * @param {nb.MessageQueueClient} message_queue
     */
    constructor(message_queue) {
        super({
            name: 'test_object_reclaimer',
            client: /** @type {nb.APIClient} */ ({}),
            message_queue,
        });
        /** @type {{ kind: 'deleted' | 'expired_restore' | 'transition_source', object_ids: string[] }[]} */
        this.processed = [];
        /** @type {string[][]} */
        this.released = [];
        this.fail = false;
        /** @type {{ kind: 'deleted' | 'expired_restore' | 'transition_source', object_ids: string[] } | undefined} */
        this.enqueue_payload = undefined;
        /** @type {(() => Promise<void>) | undefined} */
        this.before_return = undefined;
    }

    _can_run() {
        return true;
    }

    /**
     * @param {nb.MessageQueueClient} queue
     * @returns {Promise<{ had_work: boolean, had_errors: boolean }>}
     */
    async _enqueue_reclaim_batches(queue) {
        if (!this.enqueue_payload) return { had_work: false, had_errors: false };
        await queue.enqueue(config.OBJECT_RECLAIMER_QUEUE_NAME, this.enqueue_payload);
        return { had_work: true, had_errors: false };
    }

    /**
     * @param {object} payload
     * @returns {Promise<{ had_work: boolean, had_errors: boolean }>}
     */
    async _process_reclaim_message(payload) {
        if (this.before_return) await this.before_return();
        const body = /** @type {{ kind: 'deleted' | 'expired_restore' | 'transition_source', object_ids: string[] }} */ (payload);
        this.processed.push({ kind: body.kind, object_ids: body.object_ids.slice() });
        if (this.fail) return { had_work: true, had_errors: true };
        return { had_work: true, had_errors: false };
    }

    /**
     * @param {object} payload
     */
    async _release_reclaim_claim(payload) {
        const object_ids = /** @type {{ object_ids?: string[] }} */ (payload).object_ids || [];
        this.released.push(object_ids.slice());
    }
}

/**
 * @param {nb.MessageQueueClient} queue
 * @param {(name: string) => Promise<void>} fn
 */
async function with_reclaimer_queue(queue, fn) {
    const orig_name = config.OBJECT_RECLAIMER_QUEUE_NAME;
    const name = `objrec_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
    config.OBJECT_RECLAIMER_QUEUE_NAME = name;
    try {
        await fn(name);
        assert.strictEqual(await queue.size(name), 0);
    } finally {
        config.OBJECT_RECLAIMER_QUEUE_NAME = orig_name;
    }
}

/**
 * @param {nb.MessageQueueClient} queue
 */
async function exercise_reclaimer_run_batch(queue) {
    await with_reclaimer_queue(queue, async name => {
        const reclaimer = new ProbeReclaimer(queue);
        reclaimer.enqueue_payload = { kind: 'deleted', object_ids: ['obj-batch'] };
        const delay = await reclaimer.run_batch();
        assert.strictEqual(delay, config.OBJECT_RECLAIMER_BATCH_DELAY);
        assert.strictEqual(reclaimer.processed.length, 1);
        assert.deepStrictEqual(reclaimer.processed[0].object_ids, ['obj-batch']);
        assert.deepStrictEqual(reclaimer.released, [['obj-batch']]);
        assert.strictEqual(await queue.dequeue(name), null);
    });
}

/**
 * @param {nb.MessageQueueClient} queue
 */
async function exercise_reclaimer_retry(queue) {
    const orig_delay = config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS;
    config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS = 0;
    try {
        await with_reclaimer_queue(queue, async name => {
            const reclaimer = new ProbeReclaimer(queue);
            reclaimer.fail = true;
            const payload = { kind: 'expired_restore', object_ids: ['obj-r'] };
            await queue.enqueue(name, payload);
            const result = await reclaimer._consume_reclaim_batch(queue);
            assert.deepStrictEqual(result, { had_work: true, had_errors: true });
            assert.strictEqual(reclaimer.processed.length, 1);
            assert.deepStrictEqual(reclaimer.released, []);
            const again = await queue.dequeue(name);
            if (!again) throw new Error('expected the nacked message to be claimed again');
            assert.strictEqual(again.attempts, 2);
            assert.deepStrictEqual(/** @type {{ object_ids: string[] }} */ (again.payload).object_ids, ['obj-r']);
            await queue.ack(again);
        });
    } finally {
        config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS = orig_delay;
    }
}

/**
 * @param {nb.MessageQueueClient} queue
 */
async function exercise_reclaimer_terminal(queue) {
    const orig_visibility = config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS;
    const orig_attempts = config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS;
    config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = 1;
    config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS = 1;
    try {
        await with_reclaimer_queue(queue, async name => {
            const reclaimer = new ProbeReclaimer(queue);
            const payload = { kind: 'transition_source', object_ids: ['obj-t'] };
            await queue.enqueue(name, payload);
            const crashed = await queue.dequeue(name);
            if (!crashed) throw new Error('expected a message');
            await P.delay(50);
            const result = await reclaimer._consume_reclaim_batch(queue);
            assert.deepStrictEqual(result, { had_work: true, had_errors: true });
            assert.strictEqual(reclaimer.processed.length, 0);
            assert.deepStrictEqual(reclaimer.released, [['obj-t']]);
        });
    } finally {
        config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = orig_visibility;
        config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS = orig_attempts;
    }
}

/**
 * @param {nb.MessageQueueClient} queue
 */
async function exercise_reclaimer_stale_ack(queue) {
    const orig_visibility = config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS;
    const orig_delay = config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS;
    config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = 1;
    config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS = 0;
    try {
        await with_reclaimer_queue(queue, async name => {
            const reclaimer = new ProbeReclaimer(queue);
            const payload = { kind: 'deleted', object_ids: ['obj-s'] };
            await queue.enqueue(name, payload);
            /** @type {nb.QueueMessage | undefined} */
            let second;
            reclaimer.before_return = async () => {
                await P.delay(50);
                const message = await queue.dequeue(name);
                if (!message) throw new Error('expected the lock to expire');
                second = message;
            };
            const result = await reclaimer._consume_reclaim_batch(queue);
            assert.deepStrictEqual(result, { had_work: true, had_errors: true });
            assert.strictEqual(reclaimer.processed.length, 1);
            assert.deepStrictEqual(reclaimer.released, []);
            if (!second) throw new Error('expected a second claim');
            assert.strictEqual(second.attempts, 2);
            assert.strictEqual(await queue.size(name), 1);
            await queue.ack(second);
        });
    } finally {
        config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS = orig_visibility;
        config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS = orig_delay;
    }
}
