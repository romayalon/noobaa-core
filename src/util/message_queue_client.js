/* Copyright (C) 2016 NooBaa */
'use strict';

/** @typedef {typeof import('../sdk/nb')} nb */
const dbg = require('./debug_module')(__filename);
const config = require('../../config');
const postgres_message_queue = require('./postgres_message_queue');

/**
 * @enum {nb.MessageQueueType}
 */
const MessageQueueType = {
    POSTGRES: 'postgres',
    NONE: 'none',
};

/**
 * Return this process's queue client.
 * When MESSAGE_QUEUE_TYPE is none, the process has no queue and this returns undefined.
 * Otherwise the first call creates the client for the configured queue type, and later calls return that same client.
 * @returns {nb.MessageQueueClient | undefined}
 */
function instance() {
    const type = config.MESSAGE_QUEUE_TYPE || MessageQueueType.POSTGRES;
    switch (type) {
        case MessageQueueType.POSTGRES:
            return postgres_message_queue.instance();
        case MessageQueueType.NONE:
            return undefined;
        default: {
            const str = `NON SUPPORTED MESSAGE_QUEUE_TYPE ${type}`;
            dbg.error(str);
            throw new Error(str);
        }
    }
}

/**
 * Return true when the message must not be processed.
 * Call nack and skip the payload.
 * terminal is true when the lock ran out after the last allowed retry.
 * retry_count above the limit means the same thing.
 * @param {nb.QueueMessage} message Object returned by dequeue.
 * @returns {boolean}
 */
function is_exhausted_claim(message) {
    return Boolean(message.terminal) || message.retry_count > config.MESSAGE_QUEUE_DEFAULT_RETRY_LIMIT;
}

exports.MessageQueueType = MessageQueueType;
exports.instance = instance;
exports.is_exhausted_claim = is_exhausted_claim;
