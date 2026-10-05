/* Copyright (C) 2016 NooBaa */
'use strict';

module.exports = {
    $id: 'message_queue_schema',
    type: 'object',
    required: [
        '_id',
        'queue_name',
        'payload',
        'enqueued_at',
        'visible_at',
        'attempts',
    ],
    properties: {
        _id: { objectid: true },
        queue_name: { type: 'string' },
        // JSON object passed to enqueue. Shape belongs to the worker.
        payload: {
            type: 'object',
            properties: {},
            additionalProperties: true,
        },
        enqueued_at: { date: true },
        // Not claimable before this time. nack pushes it forward.
        visible_at: { date: true },
        // Claim deadline. extend sets it forward. Absent when the row is free.
        locked_until: { date: true },
        attempts: { type: 'integer', minimum: 0 },
        // Current claim. ack, nack, and extend must present it.
        lock_token: { type: 'string' },
        last_error: { type: 'string' },
        // Set by a dropping nack. The live index omits these rows.
        dead_at: { date: true },
    },
};
