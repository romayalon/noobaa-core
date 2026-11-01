/* Copyright (C) 2016 NooBaa */
'use strict';

const dbg = require('../../util/debug_module')(__filename);
const config = require('../../../config');
const message_queue_client = require('../../util/message_queue_client');

/**
 * Delete dead messages whose retention has elapsed.
 * A pass that deletes rows waits MESSAGE_QUEUE_CLEANER_CYCLE.
 * A pass that deletes nothing waits MESSAGE_QUEUE_CLEANER_EMPTY_DELAY.
 * A failed pass waits MESSAGE_QUEUE_CLEANER_ERROR_DELAY.
 * This does not wait on md_aggregator, system_store, or maintenance mode.
 * @returns {Promise<number>}
 */
async function background_worker() {
    const queue_client = message_queue_client.instance();
    if (!queue_client) return config.MESSAGE_QUEUE_CLEANER_EMPTY_DELAY;
    try {
        const { removed, had_errors } = await queue_client.remove_expired_dead_messages(
            config.MESSAGE_QUEUE_CLEANER_BATCH_SIZE,
        );
        if (removed) dbg.log0(`MESSAGE_QUEUE_CLEANER: removed ${removed} dead messages`);
        if (had_errors) return config.MESSAGE_QUEUE_CLEANER_ERROR_DELAY;
        if (!removed) return config.MESSAGE_QUEUE_CLEANER_EMPTY_DELAY;
        return config.MESSAGE_QUEUE_CLEANER_CYCLE;
    } catch (err) {
        dbg.error('MESSAGE_QUEUE_CLEANER: failed', err);
        return config.MESSAGE_QUEUE_CLEANER_ERROR_DELAY;
    }
}

exports.background_worker = background_worker;
