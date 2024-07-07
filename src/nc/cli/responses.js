/* Copyright (C) 2016 NooBaa */
'use strict';

const NoobaaEvent = require('../events_utils').NoobaaEvent;

// TODO : define list & status types
/**
 * @typedef {{
 *      code?: string, 
 *      http_code: number,
 *      list?: object,
 *      status?: object,  
 * }} ManageCLIResponseSpec
 */

class ManageCLIResponse {

    /**
     * @param {ManageCLIResponseSpec} response_spec 
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
ManageCLIResponse.WhiteListIPUpdated = Object.freeze({
    code: 'WhiteListIPUpdated',
    status: {}
});

////////////////////////
// ACCOUNT RESPONSES ///
////////////////////////

ManageCLIResponse.AccountCreated = Object.freeze({
    code: 'AccountCreated',
    status: {}
});

ManageCLIResponse.AccountDeleted = Object.freeze({
    code: 'AccountDeleted',
});

ManageCLIResponse.AccountUpdated = Object.freeze({
    code: 'AccountUpdated',
    status: {}
});

ManageCLIResponse.AccountStatus = Object.freeze({
    code: 'AccountStatus',
    status: {}
});

ManageCLIResponse.AccountList = Object.freeze({
    code: 'AccountList',
    list: {}
});

////////////////////////
/// BUCKET RESPONSES ///
////////////////////////

ManageCLIResponse.BucketCreated = Object.freeze({
    code: 'BucketCreated',
    status: {}
});

ManageCLIResponse.BucketDeleted = Object.freeze({
    code: 'BucketDeleted',
});

ManageCLIResponse.BucketUpdated = Object.freeze({
    code: 'BucketUpdated',
    status: {}
});

ManageCLIResponse.BucketStatus = Object.freeze({
    code: 'BucketStatus',
    status: {}
});

ManageCLIResponse.BucketList = Object.freeze({
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

exports.ManageCLIResponse = ManageCLIResponse;
exports.NSFS_CLI_SUCCESS_EVENT_MAP = NSFS_CLI_SUCCESS_EVENT_MAP;
