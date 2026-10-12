/* Copyright (C) 2016 NooBaa */
'use strict';

module.exports = [
    {
        // dequeue and size skip rows with dead_at set. This index covers the rest:
        // one queue, in the order those messages become claimable.
        fields: {
            queue_name: 1,
            visible_at: 1,
        },
        options: {
            name: 'queue_visible_at_index',
            partialFilterExpression: {
                dead_at: null,
            },
        },
    },
];
