/* Copyright (C) 2016 NooBaa */
/** @typedef {typeof import('../sdk/nb')} nb */
'use strict';

const dbg = require('./debug_module')(__filename);
const config = require('../../config');

/**
 * Backends that instance() can construct. MESSAGE_QUEUE_TYPE selects one per process.
 * @enum {nb.MessageQueueType}
 */
const MessageQueueType = {
    POSTGRES: 'postgres',
    GRAPHILE: 'graphile',
    PGBOSS: 'pgboss',
    NONE: 'none',
};

/**
 * Persistent JSON message queue.
 *
 * enqueue writes a JSON object. dequeue locks one message so other workers
 * skip it until ack, nack, or the visibility timeout. extend pushes that
 * timeout forward while the worker still holds the claim. size counts live
 * messages, including ones currently locked. A dropped message is not counted.
 *
 * @implements {nb.MessageQueueClient}
 */
class MessageQueueClient {

    /**
     * Open this backend. Postgres creates nb_message_queue. Graphile and
     * pg-boss open their own pools and install their schemas. A second call
     * waits for the first one.
     * @returns {Promise<void>} Resolves when the backend can accept calls.
     */
    connect() {
        return Promise.resolve();
    }

    /**
     * Close pools this client opened. The shared NooBaa Postgres pool stays open.
     * @returns {Promise<void>} Resolves when those pools are closed.
     */
    disconnect() {
        return Promise.resolve();
    }

    /**
     * Store one JSON object on a queue.
     * @param {string} queue Queue name, 1 to 128 characters. One name per worker.
     * @param {object} payload JSON object. Nested dates are stored as strings.
     * @returns {Promise<string>} Id of the new message. A later claim of the same row returns this id.
     */
    enqueue(queue, payload) {
        return this._unsupported('enqueue');
    }

    /**
     * Claim one visible message. Other workers skip it until ack, nack, or the
     * visibility timeout. The returned object is what ack, nack, and extend take.
     * @param {string} queue Queue name passed to enqueue.
     * @returns {Promise<nb.QueueMessage | null>} The claim, or null when no message is visible.
     */
    dequeue(queue) {
        return this._unsupported('dequeue');
    }

    /**
     * Count messages still in the queue, including ones a worker currently holds.
     * Dropped messages are excluded.
     * @param {string} queue Queue name passed to enqueue.
     * @returns {Promise<number>} Live message count. Zero when the queue is empty.
     */
    size(queue) {
        return this._unsupported('size');
    }

    /**
     * Delete a message this worker still holds.
     * Throws when the claim no longer matches, so a worker whose lock expired
     * cannot delete the next worker's claim.
     * @param {nb.QueueMessage} message Claim returned by dequeue.
     * @returns {Promise<void>} Resolves when the row is gone.
     */
    ack(message) {
        return this._unsupported('ack');
    }

    /**
     * Give up a claim. Under the attempt limit the message stays queued and
     * becomes visible after the retry delay, and dropped is false. At the
     * limit the message leaves the live queue, and dropped is true.
     * Throws when this worker no longer holds the claim.
     * @param {nb.QueueMessage} message Claim returned by dequeue.
     * @param {string} [reason] Stored as the message's last error. Omit it to store an empty string.
     * @returns {Promise<nb.MessageQueueNackResult>} dropped is true when the message left the live queue.
     */
    nack(message, reason) {
        return this._unsupported('nack');
    }

    /**
     * Move this claim's visibility deadline to now plus the visibility timeout.
     * hold_lease calls this on a timer while a worker is still processing.
     * @param {nb.QueueMessage} message Claim returned by dequeue.
     * @returns {Promise<boolean>} True when this worker still holds the claim. False when the lock was lost.
     */
    extend(message) {
        return this._unsupported('extend');
    }

    /**
     * Reject a queue name that is not a string of 1 to 128 characters.
     * @param {string} queue Name passed to enqueue, dequeue, or size.
     * @returns {void}
     */
    _validate_queue_name(queue) {
        if (typeof queue !== 'string' || queue.length === 0 || queue.length > 128) {
            throw new Error('invalid message queue name');
        }
    }

    /**
     * Copy a payload into plain JSON so every backend stores the same shape.
     * @param {object} payload Value passed to enqueue. Must be a non-array object.
     * @returns {object} A JSON copy of payload.
     */
    _normalize_payload(payload) {
        if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
            throw new Error('message queue payload must be a JSON object');
        }
        return JSON.parse(JSON.stringify(payload));
    }

    /**
     * Reject a call on the base class. Each backend overrides the operations it supports.
     * @param {string} op Method name that was called.
     * @returns {Promise<never>} Rejects. The promise does not resolve.
     */
    _unsupported(op) {
        return Promise.reject(new Error(`${this.constructor.name} does not implement ${op}`));
    }

}

/**
 * Client for MESSAGE_QUEUE_TYPE=none.
 * Same calls as the other backends, and nothing is stored. A process that
 * turns the queue off can still call instance() without a Postgres schema.
 * Mirrors DB_TYPE=none.
 * @extends {MessageQueueClient}
 */
class NoneMessageQueue extends MessageQueueClient {

    /**
     * Accept the payload and drop it.
     * @param {string} queue Queue name, validated and then ignored.
     * @param {object} payload JSON object, validated and then ignored.
     * @returns {Promise<string>} The id '0'. It does not refer to a row.
     */
    enqueue(queue, payload) {
        this._validate_queue_name(queue);
        this._normalize_payload(payload);
        return Promise.resolve('0');
    }

    /**
     * Report that the queue has nothing to claim.
     * @param {string} [_queue] Ignored. There is no storage.
     * @returns {Promise<null>} Always null.
     */
    dequeue(_queue) {
        return Promise.resolve(null);
    }

    /**
     * Report an empty queue.
     * @param {string} queue Queue name, validated and then ignored.
     * @returns {Promise<number>} Always 0.
     */
    size(queue) {
        this._validate_queue_name(queue);
        return Promise.resolve(0);
    }

    /**
     * Accept an ack. There is no row to delete.
     * @param {nb.QueueMessage} [_message] Ignored.
     * @returns {Promise<void>}
     */
    ack(_message) {
        return Promise.resolve();
    }

    /**
     * Accept a nack. Nothing is dropped because nothing was stored.
     * @param {nb.QueueMessage} [_message] Ignored.
     * @param {string} [_reason] Ignored.
     * @returns {Promise<nb.MessageQueueNackResult>} dropped is false.
     */
    nack(_message, _reason) {
        return Promise.resolve({ dropped: false });
    }

    /**
     * Pretend the claim is still held.
     * @param {nb.QueueMessage} [_message] Ignored.
     * @returns {Promise<boolean>} Always true.
     */
    extend(_message) {
        return Promise.resolve(true);
    }

}

/** @type {Record<string, MessageQueueClient>} */
const clients_by_type = {};

/**
 * Return the one client for this process. The first call builds it from
 * MESSAGE_QUEUE_TYPE and later calls return that same object.
 * @returns {nb.MessageQueueClient} Postgres, Graphile, pg-boss, or the none client.
 */
function instance() {
    const type = config.MESSAGE_QUEUE_TYPE || MessageQueueType.POSTGRES;
    if (!clients_by_type[type]) {
        clients_by_type[type] = _create(type);
    }
    return clients_by_type[type];
}

/**
 * Build the client for one MESSAGE_QUEUE_TYPE value.
 * The backend modules extend MessageQueueClient from this file, so requiring
 * them here instead of at the top avoids a cycle. graphile-worker and pg-boss
 * stay unloaded until this process selects that backend.
 * @param {string} type One of MessageQueueType.
 * @returns {MessageQueueClient}
 */
function _create(type) {
    switch (type) {
        case MessageQueueType.POSTGRES: {
            // eslint-disable-next-line global-require
            return require('./postgres_message_queue').instance();
        }
        case MessageQueueType.GRAPHILE: {
            // eslint-disable-next-line global-require
            return require('./graphile_message_queue').instance();
        }
        case MessageQueueType.PGBOSS: {
            // eslint-disable-next-line global-require
            return require('./pgboss_message_queue').instance();
        }
        case MessageQueueType.NONE:
            return new NoneMessageQueue();
        default: {
            const str = `NON SUPPORTED MESSAGE_QUEUE_TYPE ${type}`;
            dbg.error(str);
            throw new Error(str);
        }
    }
}

/**
 * True when this claim already used every attempt, so the caller should nack
 * it and skip the payload.
 * Postgres and Graphile set message.terminal when the visibility lock expired
 * on the last attempt. pg-boss can return attempts past the max after a crash
 * on that attempt.
 * The replication executor checks this after dequeue, nacks the message,
 * and does not call execute.
 * @param {nb.QueueMessage} message Claim returned by dequeue.
 * @returns {boolean} True when the payload must not run.
 */
function is_exhausted_claim(message) {
    return Boolean(message.terminal) || message.attempts > config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS;
}

/**
 * Keep a claim invisible while the caller processes its payload.
 *
 * dequeue hides the message only until MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS
 * (10 minutes unless the process changes it). A batch that runs longer is
 * claimed by another worker, and this worker's later ack throws. The timer
 * calls extend every half of that timeout, and at least once a second, so
 * the deadline stays ahead of a live worker.
 *
 * The replication executor starts this after dequeue, only for a claim that
 * still has attempts left, and calls stop() in a finally after ack or nack.
 * The lease test uses the same timer to show a second consumer waits until
 * stop().
 *
 * The timer is unref'd, so it does not keep the process alive. A failed
 * extend is logged. stop() does not ack or nack.
 * @param {nb.MessageQueueClient} queue Client that returned the claim.
 * @param {nb.QueueMessage} message Claim returned by dequeue.
 * @returns {{ stop: () => void }} stop() clears the timer. Call it when processing finishes, including on failure.
 */
function hold_lease(queue, message) {
    const interval_ms = Math.max(1000, Math.floor(config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS / 2));
    const timer = setInterval(() => {
        queue.extend(message).catch(err => {
            dbg.error('message queue failed to extend the lease', message.id, err);
        });
    }, interval_ms);
    timer.unref();
    return {
        stop() {
            clearInterval(timer);
        },
    };
}

exports.MessageQueueType = MessageQueueType;
exports.MessageQueueClient = MessageQueueClient;
exports.NoneMessageQueue = NoneMessageQueue;
exports.instance = instance;
exports.hold_lease = hold_lease;
exports.is_exhausted_claim = is_exhausted_claim;
