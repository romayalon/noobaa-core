/* Copyright (C) 2016 NooBaa */
'use strict';

const NoobaaEvent = require('./nc_events_utils').NoobaaEvent;

// TODO : define list & status types
/**
 * @typedef {{
 *      code?: string, 
 *      http_code: number,
 *      list?: object,
 *      status?: object,  
 * }} NooBaaCLIResponseSpec
 */

class NooBaaCLIResponse {

    /**
     * @param {NooBaaCLIResponseSpec} response_spec 
     */
    constructor({ code, status, list }) {
        this.code = code;
        this.http_code = 200;
        this.status = status;
        this.list = list;
    }

    to_string(detail) {
        const json = {
            response: {
                code: this.code,
            }
        };
        if (this.list || this.status) json.response.reply = typeof detail === 'string' ? JSON.parse(detail) : detail;
        return JSON.stringify(json, null, 2);
    }
}

// See NooBaa CLI error codes docs - TODO: add docs

///////////////////////////////
// IPS WHITE LIST RESPONSES ///
///////////////////////////////
NooBaaCLIResponse.WhiteListIPUpdated = Object.freeze({
    code: 'WhiteListIPUpdated',
    status: {}
});

////////////////////////
// ACCOUNT RESPONSES ///
////////////////////////

NooBaaCLIResponse.AccountCreated = Object.freeze({
    code: 'AccountCreated',
    status: {}
});

NooBaaCLIResponse.AccountDeleted = Object.freeze({
    code: 'AccountDeleted',
});

NooBaaCLIResponse.AccountUpdated = Object.freeze({
    code: 'AccountUpdated',
    status: {}
});

NooBaaCLIResponse.AccountStatus = Object.freeze({
    code: 'AccountStatus',
    status: {}
});

NooBaaCLIResponse.AccountList = Object.freeze({
    code: 'AccountList',
    list: {}
});

////////////////////////
/// BUCKET RESPONSES ///
////////////////////////

NooBaaCLIResponse.BucketCreated = Object.freeze({
    code: 'BucketCreated',
    status: {}
});

NooBaaCLIResponse.BucketDeleted = Object.freeze({
    code: 'BucketDeleted',
});

NooBaaCLIResponse.BucketUpdated = Object.freeze({
    code: 'BucketUpdated',
    status: {}
});

NooBaaCLIResponse.BucketStatus = Object.freeze({
    code: 'BucketStatus',
    status: {}
});

NooBaaCLIResponse.BucketList = Object.freeze({
    code: 'BucketList',
    list: {}
});

const NSFS_CLI_SUCCESS_EVENT_MAP = {
    AccountCreated: NoobaaEvent.ACCOUNT_CREATED,
    AccountDeleted: NoobaaEvent.ACCOUNT_DELETED,
    BucketCreated: NoobaaEvent.BUCKET_CREATED,
    BucketDeleted: NoobaaEvent.BUCKET_DELETE,
    WhiteListIPUpdated: NoobaaEvent.WHITELIST_UPDATED,
};

exports.NooBaaCLIResponse = NooBaaCLIResponse;
exports.NSFS_CLI_SUCCESS_EVENT_MAP = NSFS_CLI_SUCCESS_EVENT_MAP;
