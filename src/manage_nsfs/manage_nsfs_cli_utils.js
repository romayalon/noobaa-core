/* Copyright (C) 2024 NooBaa */
'use strict';

const dbg = require('../util/debug_module')(__filename);
const nb_native = require('../util/nb_native');
const native_fs_utils = require('../util/native_fs_utils');
const ManageCLIError = require('../manage_nsfs/manage_nsfs_cli_errors').ManageCLIError;
const NSFS_CLI_ERROR_EVENT_MAP = require('../manage_nsfs/manage_nsfs_cli_errors').NSFS_CLI_ERROR_EVENT_MAP;
const ManageCLIResponse = require('../manage_nsfs/manage_nsfs_cli_responses').ManageCLIResponse;
const NSFS_CLI_SUCCESS_EVENT_MAP = require('../manage_nsfs/manage_nsfs_cli_responses').NSFS_CLI_SUCCESS_EVENT_MAP;
const { BOOLEAN_STRING_VALUES } = require('../manage_nsfs/manage_nsfs_constants');
const NoobaaEvent = require('../manage_nsfs/manage_nsfs_events_utils').NoobaaEvent;
const mongo_utils = require('../util/mongo_utils');


function throw_cli_error(error, detail, event_arg) {
    const error_event = NSFS_CLI_ERROR_EVENT_MAP[error.code];
    if (error_event) {
        new NoobaaEvent(error_event).create_event(undefined, event_arg, JSON.stringify(error));
    }
    const err = new ManageCLIError({ ...error, detail });
    throw err;
}

function write_stdout_response(response_code, detail, event_arg) {
    const response_event = NSFS_CLI_SUCCESS_EVENT_MAP[response_code.code];
    if (response_event) {
        new NoobaaEvent(response_event).create_event(undefined, event_arg, undefined);
    }
    const res = new ManageCLIResponse(response_code).to_string(detail);
    process.stdout.write(res + '\n', () => {
        process.exit(0);
    });
}

/**
 * get_bucket_owner_account will return the account of the bucket_owner
 * otherwise it would throw an error
 * @param {import('../sdk/config_fs').ConfigFS} config_fs
 * @param {string} [bucket_owner]
 * @param {string} [owner_account_id]
 */
async function get_bucket_owner_account(config_fs, bucket_owner, owner_account_id) {
    try {
        const account = bucket_owner ?
            await config_fs.get_account_by_name(bucket_owner) :
            await config_fs.get_identity_by_id(owner_account_id);
        return account;
    } catch (err) {
        if (err.code === 'ENOENT') {
            const detail_msg = bucket_owner ?
                `bucket owner name ${bucket_owner} does not exists` :
                `bucket owner id ${owner_account_id} does not exists`;
            throw_cli_error(ManageCLIError.BucketSetForbiddenBucketOwnerNotExists, detail_msg, {bucket_owner: bucket_owner});
        }
        throw err;
    }
}

/**
 * get_boolean_or_string_value will check if the value
 * 1. if the value is undefined - it returns false.
 * 2. (the value is defined) if it a string 'true' or 'false' = then we set boolean respectively.
 * 3. (the value is defined) then we set true (Boolean convert of this case will be true).
 * @param {boolean|string} value
 */
function get_boolean_or_string_value(value) {
    if (value === undefined) {
        return false;
    } else if (typeof value === 'string' && BOOLEAN_STRING_VALUES.includes(value.toLowerCase())) {
        return value.toLowerCase() === 'true';
    } else { // boolean type
        return Boolean(value);
    }
}

/**
 * get_options_from_file will read a JSON file that include key-value of the options 
 * (instead of flags) and return its content
 * @param {string} file_path
 */
async function get_options_from_file(file_path) {
    // we don't pass neither config_root_backend nor fs_backend
    const fs_context = native_fs_utils.get_process_fs_context();
    try {
        const input_options_with_data = await native_fs_utils.read_file(fs_context, file_path);
        return input_options_with_data;
    } catch (err) {
        if (err.code === 'ENOENT') throw_cli_error(ManageCLIError.InvalidFilePath, file_path);
        if (err instanceof SyntaxError) throw_cli_error(ManageCLIError.InvalidJSONFile, file_path);
        throw err;
    }
}

/**
 * has_access_keys will return if the array has at least one object of access keys
 * (depending on the access key length)
 * Note: when there is no access key array it might indicate that it is anonymous account
 * @param {object[]} access_keys
 */
function has_access_keys(access_keys) {
    return access_keys.length > 0;
}

/**
 * set_debug_level will set the debug log level
 * @param {string} debug
 */
function set_debug_level(debug) {
    const debug_level = Number(debug) || 5;
    dbg.set_module_level(debug_level, 'core');
    nb_native().fs.set_debug_level(debug_level);
}

/**
 * generate_id will generate an id that we use to identify entities (such as account, bucket, etc.). 
 */
// TODO: 
// - reuse this function in NC NSFS where we used the mongo_utils module
// - this function implantation should be db_client.new_object_id(), 
//   but to align with manage nsfs we won't change it now
function generate_id() {
    return mongo_utils.mongoObjectId();
}

/**
 * check_root_account_owns_user checks if an account is owned by root account
 * @param {object} root_account
 * @param {object} account
 */
function check_root_account_owns_user(root_account, account) {
    if (account.owner === undefined) return false;
    return root_account._id === account.owner;
}


/**
 * is_name_update returns true if a new_name flag was provided and it's not equal to 
 * the current name
 * @param {Object} data
 * @returns {Boolean} 
 */
function is_name_update(data) {
    const cur_name = data.name;
    const new_name = data.new_name;
    return new_name && cur_name && new_name !== cur_name;
}

/**
 * is_access_key_update returns true if a new_access_key flag was provided and it's not equal to 
 * the current access_key at index 0
 * @param {Object} data
 * @returns {Boolean} 
 */
function is_access_key_update(data) {
    const cur_access_key = has_access_keys(data.access_keys) ? data.access_keys[0].access_key.unwrap() : undefined;
    const new_access_key = data.new_access_key;
    return new_access_key && cur_access_key && new_access_key !== cur_access_key;
}

// EXPORTS
exports.throw_cli_error = throw_cli_error;
exports.write_stdout_response = write_stdout_response;
exports.get_boolean_or_string_value = get_boolean_or_string_value;
exports.get_bucket_owner_account = get_bucket_owner_account;
exports.get_options_from_file = get_options_from_file;
exports.has_access_keys = has_access_keys;
exports.generate_id = generate_id;
exports.set_debug_level = set_debug_level;
exports.check_root_account_owns_user = check_root_account_owns_user;
exports.is_name_update = is_name_update;
exports.is_access_key_update = is_access_key_update;
