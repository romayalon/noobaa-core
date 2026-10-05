/* Copyright (C) 2016 NooBaa */
/** @typedef {typeof import('../sdk/nb')} nb */
'use strict';

const dbg = require('./debug_module')(__filename);
const config = require('../../config');

/**
 * Persistent JSON message queue.
 *
 * enqueue writes a JSON object. dequeue locks one message so other workers
 * skip it until ack, nack, or the visibility timeout. extend pushes that
 * timeout forward while the worker still holds the claim. size counts live
 * messages, including ones currently locked. A dead-lettered message is
 * not counted.
 *
 * @implements {nb.MessageQueueClient}
 */
class MessageQueueClient {

    /**
     * @returns {Promise<void>}
     */
    connect() {
        return Promise.resolve();
    }

    /**
     * @returns {Promise<void>}
     */
    disconnect() {
        return Promise.resolve();
    }

    /**
     * @param {string} queue
     * @param {object} payload
     * @returns {Promise<string>}
     */
    enqueue(queue, payload) {
        return this._unsupported('enqueue');
    }

    /**
     * @param {string} queue
     * @returns {Promise<nb.QueueMessage | null>}
     */
    dequeue(queue) {
        return this._unsupported('dequeue');
    }

    /**
     * @param {string} queue
     * @returns {Promise<number>}
     */
    size(queue) {
        return this._unsupported('size');
    }

    /**
     * @param {nb.QueueMessage} message
     * @returns {Promise<void>}
     */
    ack(message) {
        return this._unsupported('ack');
    }

    /**
     * @param {nb.QueueMessage} message
     * @param {string} [reason]
     * @returns {Promise<nb.MessageQueueNackResult>}
     */
    nack(message, reason) {
        return this._unsupported('nack');
    }

    /**
     * Keep a claimed message invisible to other workers. Returns false when
     * this worker no longer holds the claim.
     * @param {nb.QueueMessage} message
     * @returns {Promise<boolean>}
     */
    extend(message) {
        return this._unsupported('extend');
    }

    /**
     * @param {string} queue
     */
    _assert_queue(queue) {
        if (typeof queue !== 'string' || queue.length === 0 || queue.length > 128) {
            throw new Error('invalid message queue name');
        }
    }

    /**
     * Copy a payload into plain JSON so both backends store the same shape.
     * @param {object} payload
     * @returns {object}
     */
    _normalize_payload(payload) {
        if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
            throw new Error('message queue payload must be a JSON object');
        }
        return JSON.parse(JSON.stringify(payload));
    }

    /**
     * @param {string} op
     * @returns {Promise<any>}
     */
    /**
     * @param {string} op
     * @returns {Promise<never>}
     */
    _unsupported(op) {
        return Promise.reject(new Error(`${this.constructor.name} does not implement ${op}`));
    }

}

/**
 * No-op queue used when MESSAGE_QUEUE_TYPE=none.
 * @extends {MessageQueueClient}
 */
class NoneMessageQueue extends MessageQueueClient {

    /** @param {string} queue @param {object} payload */
    enqueue(queue, payload) {
        this._assert_queue(queue);
        this._normalize_payload(payload);
        return Promise.resolve('0');
    }

    /** @returns {Promise<null>} */
    dequeue() {
        return Promise.resolve(null);
    }

    /** @param {string} queue */
    size(queue) {
        this._assert_queue(queue);
        return Promise.resolve(0);
    }

    /** @returns {Promise<void>} */
    ack() {
        return Promise.resolve();
    }

    /** @returns {Promise<nb.MessageQueueNackResult>} */
    nack() {
        return Promise.resolve({ dropped: false });
    }

    /** @returns {Promise<boolean>} */
    extend() {
        return Promise.resolve(true);
    }

}

/** @type {Record<string, MessageQueueClient>} */
const clients_by_type = {};

/**
 * @returns {nb.MessageQueueClient}
 */
function instance() {
    const type = config.MESSAGE_QUEUE_TYPE || 'postgres';
    if (!clients_by_type[type]) {
        clients_by_type[type] = _create(type);
    }
    return clients_by_type[type];
}

/**
 * @param {string} type
 * @returns {MessageQueueClient}
 */
function _create(type) {
    switch (type) {
        case 'postgres':
            // Lazy so this module can be loaded by the backend classes.
            // eslint-disable-next-line global-require
            return require('./postgres_message_queue').instance();
        case 'graphile':
            // eslint-disable-next-line global-require
            return require('./graphile_message_queue').instance();
        case 'pgboss':
            // eslint-disable-next-line global-require
            return require('./pgboss_message_queue').instance();
        case 'none':
            return new NoneMessageQueue();
        default: {
            const str = `NON SUPPORTED MESSAGE_QUEUE_TYPE ${type}`;
            dbg.error(str);
            throw new Error(str);
        }
    }
}

/**
 * A crashed worker already used the last attempt. The message is only here so
 * the caller can drop it. pg-boss fetches one more time after that crash, so
 * attempts lands past the max.
 * @param {nb.QueueMessage} message
 * @returns {boolean}
 */
function is_terminal_message(message) {
    return Boolean(message.terminal) || message.attempts > config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS;
}

/**
 * Slide the visibility lock forward for as long as the caller is processing
 * this claim. Any background worker uses the same timer. stop() belongs in
 * the caller's finally, including when processing throws.
 * @param {nb.MessageQueueClient} queue
 * @param {nb.QueueMessage} message
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

exports.MessageQueueClient = MessageQueueClient;
exports.NoneMessageQueue = NoneMessageQueue;
exports.instance = instance;
exports.hold_lease = hold_lease;
exports.is_terminal_message = is_terminal_message;
