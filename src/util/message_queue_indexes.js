/* Copyright (C) 2016 NooBaa */
'use strict';

module.exports = [
    {
        // Live rows for one queue. dequeue still orders by _id.
        fields: {
            queue_name: 1,
            visible_at: 1,
        },
        options: {
            name: 'live',
            partialFilterExpression: {
                dead_at: null,
            },
        },
    },
];
