/* Copyright (C) 2016 NooBaa */
'use strict';

/** @typedef {typeof import('../../../sdk/nb')} nb */
/** @typedef {import('../../../server/bg_services/bg_task_executor').BgTaskType} BgTaskType */

const mocha = require('mocha');
const assert = require('assert');
const sinon = require('sinon');

const config = require('../../../../config');
const message_queue_client = require('../../../util/message_queue_client');
const background_scheduler = require('../../../util/background_scheduler').get_instance();
const { BgTaskExecutor } = require('../../../server/bg_services/bg_task_executor');

mocha.describe('bg_task_executor', function() {
    const weights = config.BG_EXECUTOR_WEIGHTS;
    const worker_count = config.BG_EXECUTOR_WORKERS;

    mocha.afterEach(function() {
        config.BG_EXECUTOR_WEIGHTS = weights;
        config.BG_EXECUTOR_WORKERS = worker_count;
        sinon.restore();
    });

    mocha.it('requires an rpc client and a task list', function() {
        assert.throws(
            () => new BgTaskExecutor(/** @type {nb.APIClient} */ (/** @type {unknown} */ (undefined)), []),
            /rpc_client is required/,
        );
        assert.throws(
            () => new BgTaskExecutor(rpc_client(), /** @type {BgTaskType[]} */ (/** @type {unknown} */ (undefined))),
            /task_types is required/,
        );
    });

    mocha.it('gives the first weight the low end of the draw', function() {
        config.BG_EXECUTOR_WEIGHTS = { transition: 20, replication: 80 };
        const executor = executor_with_tasks();
        assert.strictEqual(name_of(executor.choose_task(0)), 'transition');
        assert.strictEqual(name_of(executor.choose_task(0.2)), 'transition');
        assert.strictEqual(name_of(executor.choose_task(0.21)), 'replication');
        assert.strictEqual(name_of(executor.choose_task(0.99)), 'replication');
    });

    mocha.it('walks the weight map in insertion order', function() {
        config.BG_EXECUTOR_WEIGHTS = { replication: 80, transition: 20 };
        const executor = executor_with_tasks();
        assert.strictEqual(name_of(executor.choose_task(0.2)), 'replication');
        assert.strictEqual(name_of(executor.choose_task(0.8)), 'replication');
        assert.strictEqual(name_of(executor.choose_task(0.81)), 'transition');
    });

    mocha.it('stays idle when the draw is past the covered percent', function() {
        config.BG_EXECUTOR_WEIGHTS = { replication: 80 };
        const executor = executor_with_tasks();
        assert.strictEqual(name_of(executor.choose_task(0.8)), 'replication');
        assert.strictEqual(executor.choose_task(0.81), undefined);
    });

    mocha.it('skips a task whose weight is not positive', function() {
        config.BG_EXECUTOR_WEIGHTS = { transition: 0, replication: 100 };
        const executor = executor_with_tasks();
        assert.strictEqual(name_of(executor.choose_task(0)), 'replication');
        assert.strictEqual(name_of(executor.choose_task(0.99)), 'replication');
    });

    mocha.it('dequeues the queue selected by the draw', async function() {
        config.BG_EXECUTOR_WEIGHTS = { transition: 20, replication: 80 };
        /** @type {string[]} */
        const seen = [];
        const executor = new BgTaskExecutor(rpc_client(), task_types(seen));
        executor.mq_client = fake_queue(seen, true);
        sinon.stub(executor, 'can_run').returns(true);
        const random = sinon.stub(Math, 'random');

        random.returns(0.1);
        assert.strictEqual(await executor.run_task(), config.BG_EXECUTOR_BATCH_DELAY);
        random.returns(0.5);
        assert.strictEqual(await executor.run_task(), config.BG_EXECUTOR_BATCH_DELAY);

        assert.deepStrictEqual(seen, [
            'dequeue:transition_q',
            'execute:transition',
            'ack:transition_q',
            'dequeue:replication_q',
            'execute:replication',
            'ack:replication_q',
        ]);
    });

    mocha.it('waits when the chosen queue is empty', async function() {
        config.BG_EXECUTOR_WEIGHTS = { replication: 100 };
        const executor = executor_with_tasks();
        /** @type {string[]} */
        const seen = [];
        executor.mq_client = fake_queue(seen, false);
        sinon.stub(executor, 'can_run').returns(true);
        sinon.stub(Math, 'random').returns(0.4);

        assert.strictEqual(await executor.run_task(), config.BG_EXECUTOR_EMPTY_DELAY);
        assert.deepStrictEqual(seen, ['dequeue:replication_q']);
    });

    mocha.it('does not dequeue while the system cannot run', async function() {
        config.BG_EXECUTOR_WEIGHTS = { replication: 100 };
        const executor = executor_with_tasks();
        /** @type {string[]} */
        const seen = [];
        executor.mq_client = fake_queue(seen, true);
        sinon.stub(executor, 'can_run').returns(false);

        assert.strictEqual(await executor.run_task(), config.BG_EXECUTOR_EMPTY_DELAY);
        assert.deepStrictEqual(seen, []);
    });

    mocha.it('nacks a failed task and backs off', async function() {
        config.BG_EXECUTOR_WEIGHTS = { replication: 100 };
        const tasks = task_types();
        tasks[1].execute = async () => {
            throw new Error('copy failed');
        };
        const executor = new BgTaskExecutor(rpc_client(), tasks);
        /** @type {string[]} */
        const seen = [];
        executor.mq_client = fake_queue(seen, true);
        sinon.stub(executor, 'can_run').returns(true);
        sinon.stub(Math, 'random').returns(0.4);

        assert.strictEqual(await executor.run_task(), config.BG_EXECUTOR_ERROR_DELAY);
        assert.deepStrictEqual(seen, ['dequeue:replication_q', 'nack:replication_q']);
    });

    mocha.it('starts one loop per configured worker', async function() {
        config.BG_EXECUTOR_WEIGHTS = { replication: 100 };
        config.BG_EXECUTOR_WORKERS = 3;
        const executor = executor_with_tasks();
        sinon.stub(message_queue_client, 'instance').returns(queue_client());
        const register = sinon.stub(background_scheduler, 'register_bg_worker');

        await executor.init();

        assert.strictEqual(register.callCount, 3);
        assert.deepStrictEqual(register.getCalls().map(call => call.args[0].name), [
            'bg executor 0',
            'bg executor 1',
            'bg executor 2',
        ]);
    });

    mocha.it('does not connect a queue when nothing is weighted', async function() {
        config.BG_EXECUTOR_WEIGHTS = {};
        const executor = executor_with_tasks();
        await executor.init();
        assert.strictEqual(executor.mq_client, undefined);
    });
});

/**
 * @returns {nb.APIClient}
 */
function rpc_client() {
    return /** @type {nb.APIClient} */ ({});
}

/**
 * @param {string[]} [seen]
 * @returns {BgTaskType[]}
 */
function task_types(seen) {
    return [
        {
            name: 'transition',
            queue: 'transition_q',
            execute: async () => {
                if (seen) seen.push('execute:transition');
            },
        },
        {
            name: 'replication',
            queue: 'replication_q',
            execute: async () => {
                if (seen) seen.push('execute:replication');
            },
        },
    ];
}

/**
 * @returns {BgTaskExecutor}
 */
function executor_with_tasks() {
    return new BgTaskExecutor(rpc_client(), task_types());
}

/**
 * @param {BgTaskType | undefined} task
 * @returns {string}
 */
function name_of(task) {
    assert.ok(task);
    return task.name;
}

/**
 * @param {string[]} seen
 * @param {boolean} has_message
 * @returns {nb.MessageQueueClient}
 */
function fake_queue(seen, has_message) {
    return /** @type {nb.MessageQueueClient} */ (/** @type {unknown} */ ({
        /**
         * @param {string} queue
         * @returns {Promise<nb.QueueMessage | null>}
         */
        async dequeue(queue) {
            seen.push(`dequeue:${queue}`);
            if (!has_message) return null;
            return {
                _id: `${queue}-id`,
                queue,
                payload: {},
                retry_count: 0,
                lock_token: 'lock',
                enqueued_at: new Date(),
                locked_until: new Date(),
            };
        },
        /**
         * @param {nb.QueueMessage} message
         */
        async ack(message) {
            seen.push(`ack:${message.queue}`);
        },
        /**
         * @param {nb.QueueMessage} message
         * @returns {Promise<nb.MessageQueueNackResult>}
         */
        async nack(message) {
            seen.push(`nack:${message.queue}`);
            return { dropped: false };
        },
        is_retry_limit_exceeded() {
            return false;
        },
    }));
}

/**
 * @returns {nb.MessageQueueClient}
 */
function queue_client() {
    return /** @type {nb.MessageQueueClient} */ (/** @type {unknown} */ ({
        async connect() {
            return undefined;
        },
    }));
}
