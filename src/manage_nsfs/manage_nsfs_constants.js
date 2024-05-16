/* Copyright (C) 2024 NooBaa */
'use strict';

const TYPES = {
    ACCOUNT: 'account',
    BUCKET: 'bucket',
    IP_WHITELIST: 'whitelist',
    GLACIER: 'glacier',
    HEALTH: 'health'
};

const ACTIONS = {
    ADD: 'add',
    UPDATE: 'update',
    DELETE: 'delete',
    LIST: 'list',
    STATUS: 'status'
};

const GLACIER_ACTIONS = {
    MIGRATE: 'migrate',
    RESTORE: 'restore',
    EXPIRY: 'expiry',
};

const CONFIG_SUBDIRS = {
    ACCOUNTS: 'accounts',
    BUCKETS: 'buckets',
    ACCESS_KEYS: 'access_keys'
};

const GLOBAL_CONFIG_ROOT = 'config_root';
const GLOBAL_CONFIG_OPTIONS = new Set([GLOBAL_CONFIG_ROOT, 'config_root_backend']);
const FROM_FILE = 'from_file';

const VALID_OPTIONS_ACCOUNT = {
    'add': new Set(['name', 'uid', 'gid', 'new_buckets_path', 'user', 'access_key', 'secret_key', 'fs_backend', 'allow_bucket_creation', 'force_md5_etag', FROM_FILE, ...GLOBAL_CONFIG_OPTIONS]),
    'update': new Set(['name', 'uid', 'gid', 'new_buckets_path', 'user', 'access_key', 'secret_key', 'fs_backend', 'allow_bucket_creation', 'force_md5_etag', 'new_name', 'regenerate', ...GLOBAL_CONFIG_OPTIONS]),
    'delete': new Set(['name', ...GLOBAL_CONFIG_OPTIONS]),
    'list': new Set(['wide', 'show_secrets', 'gid', 'uid', 'user', 'name', 'access_key', ...GLOBAL_CONFIG_OPTIONS]),
    'status': new Set(['name', 'access_key', 'show_secrets', ...GLOBAL_CONFIG_OPTIONS]),
};

const VALID_OPTIONS_BUCKET = {
    'add': new Set(['name', 'owner', 'path', 'bucket_policy', 'fs_backend', 'force_md5_etag', FROM_FILE, ...GLOBAL_CONFIG_OPTIONS]),
    'update': new Set(['name', 'owner', 'path', 'bucket_policy', 'fs_backend', 'new_name', 'force_md5_etag', ...GLOBAL_CONFIG_OPTIONS]),
    'delete': new Set(['name', 'force', ...GLOBAL_CONFIG_OPTIONS]),
    'list': new Set(['wide', 'name', ...GLOBAL_CONFIG_OPTIONS]),
    'status': new Set(['name', ...GLOBAL_CONFIG_OPTIONS]),
};

const VALID_OPTIONS_GLACIER = {
    'migrate': new Set([ GLOBAL_CONFIG_ROOT]),
    'restore': new Set([ GLOBAL_CONFIG_ROOT]),
    'expiry': new Set([ GLOBAL_CONFIG_ROOT]),
};

const VALID_OPTIONS_HEALTH = new Set(['https_port', 'deployment_type', 'all_account_details', 'all_bucket_details', 'check_syslog_ng', ...GLOBAL_CONFIG_OPTIONS]);
const VALID_HEALTH_DEPLOYMENT_TYPE = ['nc'];

const VALID_OPTIONS_WHITELIST = new Set(['ips', ...GLOBAL_CONFIG_OPTIONS]);

const VALID_OPTIONS_FROM_FILE = new Set(['from_file', ...GLOBAL_CONFIG_OPTIONS]);

const VALID_OPTIONS = {
    account_options: VALID_OPTIONS_ACCOUNT,
    bucket_options: VALID_OPTIONS_BUCKET,
    glacier_options: VALID_OPTIONS_GLACIER,
    whitelist_options: VALID_OPTIONS_WHITELIST,
    from_file_options: VALID_OPTIONS_FROM_FILE,
    health_options: VALID_OPTIONS_HEALTH
};

const OPTION_TYPE = {
    name: 'string',
    owner: 'string',
    uid: 'number',
    gid: 'number',
    new_buckets_path: 'string',
    user: 'string',
    access_key: 'string',
    secret_key: 'string',
    fs_backend: 'string',
    allow_bucket_creation: 'boolean',
    force_md5_etag: 'boolean',
    config_root: 'string',
    from_file: 'string',
    config_root_backend: 'string',
    path: 'string',
    bucket_policy: 'string',
    new_name: 'string',
    regenerate: 'boolean',
    wide: 'boolean',
    show_secrets: 'boolean',
    ips: 'string',
    force: 'boolean',
    deployment_type: 'string',
    all_account_details: 'boolean',
    all_bucket_details: 'boolean',
    https_port: 'number',
    check_syslog_ng: 'boolean'
};

const BOOLEAN_STRING_VALUES = ['true', 'false'];

//options that can be unset using ''
const LIST_UNSETABLE_OPTIONS = ['fs_backend', 's3_policy', 'force_md5_etag'];

const LIST_ACCOUNT_FILTERS = ['uid', 'gid', 'user', 'name', 'access_key'];
const LIST_BUCKET_FILTERS = ['name'];

// EXPORTS
exports.TYPES = TYPES;
exports.ACTIONS = ACTIONS;
exports.GLACIER_ACTIONS = GLACIER_ACTIONS;
exports.CONFIG_SUBDIRS = CONFIG_SUBDIRS;
exports.VALID_OPTIONS = VALID_OPTIONS;
exports.OPTION_TYPE = OPTION_TYPE;
exports.FROM_FILE = FROM_FILE;
exports.BOOLEAN_STRING_VALUES = BOOLEAN_STRING_VALUES;
exports.LIST_UNSETABLE_OPTIONS = LIST_UNSETABLE_OPTIONS;

exports.LIST_ACCOUNT_FILTERS = LIST_ACCOUNT_FILTERS;
exports.LIST_BUCKET_FILTERS = LIST_BUCKET_FILTERS;
