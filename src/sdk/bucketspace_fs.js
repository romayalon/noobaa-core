/* Copyright (C) 2020 NooBaa */
'use strict';

const path = require('path');
const config = require('../../config');
const nb_native = require('../util/nb_native');
const SensitiveString = require('../util/sensitive_string');
const { S3Error } = require('../endpoint/s3/s3_errors');
const RpcError = require('../rpc/rpc_error');
const js_utils = require('../util/js_utils');
const P = require('../util/promise');
const BucketSpaceSimpleFS = require('./bucketspace_simple_fs');
const _ = require('lodash');
const util = require('util');
const bucket_policy_utils = require('../endpoint/s3/s3_bucket_policy_utils');
const { default: Ajv } = require('ajv');
const bucket_schema = require('../server/object_services/schemas/nsfs_bucket_schema');
const account_schema = require('../server/nc_nsfs_services/schemas/nsfs_account_schema');

const KeysSemaphore = require('../util/keys_semaphore');
const native_fs_utils = require('../util/native_fs_utils');

const dbg = require('../util/debug_module')(__filename);

const BUCKET_PATH = 'buckets';
const ACCOUNT_PATH = 'accounts';
const ACCESS_KEYS_PATH = 'access_keys';
const ajv = new Ajv({ verbose: true, allErrors: true });

const bucket_semaphore = new KeysSemaphore(1);

//TODO:  dup from namespace_fs - need to handle and not dup code

/**
 * @param {nb.ObjectSDK} object_sdk 
 * @returns {nb.NativeFSContext}
 */
function prepare_fs_context(object_sdk) {
    const fs_context = object_sdk?.requesting_account?.nsfs_account_config;
    if (!fs_context) throw new RpcError('UNAUTHORIZED', 'nsfs_account_config is missing');
    fs_context.warn_threshold_ms = config.NSFS_WARN_THRESHOLD_MS;
    // TODO: 
    //fs_context.backend = this.fs_backend || '';
    //if (this.stats) fs_context.report_fs_stats = this.stats.update_fs_stats;
    return fs_context;
}


class BucketSpaceFS extends BucketSpaceSimpleFS {
    constructor({config_root}) {
        super({ fs_root: ''});
        this.fs_root = '';
        this.accounts_dir = path.join(config_root, ACCOUNT_PATH);
        this.access_keys_dir = path.join(config_root, ACCESS_KEYS_PATH);
        this.bucket_schema_dir = path.join(config_root, BUCKET_PATH);
        this.config_root = config_root;
        this.fs_context = {
            uid: process.getuid(),
            gid: process.getgid(),
            warn_threshold_ms: config.NSFS_WARN_THRESHOLD_MS,
        };
    }

    _get_bucket_config_path(bucket_name) {
       return path.join(this.bucket_schema_dir, bucket_name + '.json');
    }

    _get_account_config_path(name) {
        return path.join(this.accounts_dir, name + '.json');
    }

    _get_access_keys_config_path(access_key) {
        return path.join(this.access_keys_dir, access_key + '.symlink');
    }

    async _get_account_by_name(name) {
        const access_key_config_path = this._get_account_config_path(name);
        try {
            await nb_native().fs.stat(this.fs_context, access_key_config_path);
            return true;
        } catch (err) {
            return false;
        }
    }

    _translate_bucket_error_codes(err) {
        if (err.rpc_code) return err;
        if (err.code === 'ENOENT') err.rpc_code = 'NO_SUCH_BUCKET';
        if (err.code === 'EEXIST') err.rpc_code = 'BUCKET_ALREADY_EXISTS';
        if (err.code === 'EPERM' || err.code === 'EACCES') err.rpc_code = 'UNAUTHORIZED';
        if (err.code === 'IO_STREAM_ITEM_TIMEOUT') err.rpc_code = 'IO_STREAM_ITEM_TIMEOUT';
        if (err.code === 'INTERNAL_ERROR') err.rpc_code = 'INTERNAL_ERROR';
        return err;
    }

    async read_account_by_access_key({ access_key }) {
        try {
            if (!access_key) throw new Error('no access key');
            const iam_path = this._get_access_keys_config_path(access_key);
            const { data } = await nb_native().fs.readFile(this.fs_context, iam_path);
            const account = JSON.parse(data.toString());
            this.validate_account_schema(account);
            account.name = new SensitiveString(account.name);
            account.email = new SensitiveString(account.email);
            for (const k of account.access_keys) {
                k.access_key = new SensitiveString(k.access_key);
                k.secret_key = new SensitiveString(k.secret_key);
            }
            //account newly created
            dbg.event({
                code: "noobaa_account_created",
                entity_type: "NODE",
                event_type: "INFO",
                message: String("New noobaa account created."),
                scope: "NODE",
                severity: "INFO",
                state: "HEALTHY",
                arguments: {account_name: account.name.unwrap()},
            });
            return account;
        } catch (err) {
            dbg.error('BucketSpaceFS.read_account_by_access_key: failed with error', err);
            if (err.code === 'ENOENT') {
                throw new RpcError('NO_SUCH_ACCOUNT', `Account with access_key not found.`, err);
            }
            throw new RpcError('NO_SUCH_ACCOUNT', err.message);
        }
    }

    validate_account_schema(account) {
        const valid = ajv.validate(account_schema, account);
        if (!valid) throw new RpcError('INVALID_SCHEMA', ajv.errors[0]?.message);
    }

    validate_bucket_schema(bucket) {
        const valid = ajv.validate(bucket_schema, bucket);
        if (!valid) throw new RpcError('INVALID_SCHEMA', ajv.errors[0]?.message);
    }

    async read_bucket_sdk_info({ name }) {
        try {
            const bucket_config_path = this._get_bucket_config_path(name);
            dbg.log0('BucketSpaceFS.read_bucket_sdk_info: bucket_config_path', bucket_config_path);
            const { data } = await nb_native().fs.readFile(this.fs_context, bucket_config_path);
            const bucket = JSON.parse(data.toString());
            this.validate_bucket_schema(bucket);
            const is_valid = await this.check_bucket_config(bucket);
            if (!is_valid) {
                dbg.warn('BucketSpaceFS: one or more bucket config check is failed for bucket : ', name);
            }
            const nsr = {
                resource: {
                    fs_root_path: this.fs_root,
                },
                path: bucket.path
            };
            bucket.namespace = {
                read_resources: [nsr],
                write_resource: nsr,
                should_create_underlying_storage: bucket.should_create_underlying_storage,
            };

            bucket.bucket_info = {
                versioning: bucket.versioning
            };

            bucket.system_owner = new SensitiveString(bucket.system_owner);
            bucket.bucket_owner = new SensitiveString(bucket.bucket_owner);
            if (bucket.s3_policy) {
                for (const [s_index, statement] of bucket.s3_policy.Statement.entries()) {
                    const statement_principal = statement.Principal || statement.NotPrincipal;
                    if (statement_principal.AWS) {
                        const sensitive_arr = _.flatten([statement_principal.AWS]).map(principal => new SensitiveString(principal));
                        if (statement.Principal) bucket.s3_policy.Statement[s_index].Principal.AWS = sensitive_arr;
                        if (statement.NotPrincipal) bucket.s3_policy.Statement[s_index].NotPrincipal.AWS = sensitive_arr;
                    } else {
                        bucket.s3_policy.Statement = new SensitiveString(statement_principal);
                    }
                }
            }
            return bucket;
        } catch (err) {
            dbg.event({
                code: "noobaa_bucket_not_found",
                entity_type: "NODE",
                event_type: "ERROR",
                message: String("Noobaa bucket " + name + " get failed."),
                description: String("Noobaa bucket " + name + " get failed.. error : " + err),
                scope: "NODE",
                severity: "ERROR",
                state: "HEALTHY",
                arguments: {bucket_name: name},
            });
            throw this._translate_bucket_error_codes(err);
        }
    }

    async check_bucket_config(bucket) {
        const bucket_storage_path = path.join(this.fs_root, bucket.path);
        const bucket_dir_stat = await nb_native().fs.stat(this.fs_context, bucket_storage_path);
        if (!bucket_dir_stat) {
            return false;
        }
        //TODO: Bucket owner check
        return true;
    }


    ////////////
    // BUCKET //
    ////////////

    async list_buckets(object_sdk) {
        try {
            const entries = await nb_native().fs.readdir(this.fs_context, this.bucket_schema_dir);
            const bucket_config_files = entries.filter(entree => !native_fs_utils.isDirectory(entree) && entree.name.endsWith('.json'));
            //TODO : we need to add pagination support to list buckets for more than 1000 buckets.
            let buckets = await P.map(bucket_config_files, bucket_config_file => this.get_bucket_name(bucket_config_file.name));
            buckets = buckets.filter(bucket => bucket.name.unwrap());
            return this.validate_bucket_access(buckets, object_sdk);
        } catch (err) {
            if (err.code === 'ENOENT') {
                dbg.error('BucketSpaceFS: root dir not found', err);
                throw new S3Error(S3Error.NoSuchBucket);
            }
            throw this._translate_bucket_error_codes(err);
        }
    }

    async validate_bucket_access(buckets, object_sdk) {
        const has_access_buckets = (await P.all(_.map(
            buckets,
            async bucket => {
                const ns = await object_sdk.read_bucket_sdk_namespace_info(bucket.name.unwrap());
                const has_access_to_bucket = object_sdk.is_nsfs_bucket(ns) ?
                    await this._has_access_to_nsfs_dir(ns, object_sdk) : false;
                return has_access_to_bucket && bucket;
            }))).filter(bucket => bucket);
            return { buckets: has_access_buckets };
    }

    async _has_access_to_nsfs_dir(ns, object_sdk) {
        const account = object_sdk.requesting_account;
        dbg.log0('_has_access_to_nsfs_dir: nsr: ', ns, 'account.nsfs_account_config: ', account && account.nsfs_account_config);
        // nsfs bucket
        if (!account || !account.nsfs_account_config || _.isUndefined(account.nsfs_account_config.uid) ||
            _.isUndefined(account.nsfs_account_config.gid)) return false;
        try {
            dbg.log0('_has_access_to_nsfs_dir: checking access:', ns.write_resource, account.nsfs_account_config.uid, account.nsfs_account_config.gid);
            //TODO:  Operation not permitted error on change_user() with non root user, and 
            //proccess exit with error
            await nb_native().fs.checkAccess({
                uid: account.nsfs_account_config.uid,
                gid: account.nsfs_account_config.gid,
                backend: ns.write_resource.resource.fs_backend,
                warn_threshold_ms: config.NSFS_WARN_THRESHOLD_MS,
            }, path.join(ns.write_resource.resource.fs_root_path, ns.write_resource.path || ''));

            return true;
        } catch (err) {
            dbg.log0('_has_access_to_nsfs_dir: failed', err);
            if (err.code === 'ENOENT' || err.code === 'EACCES' || (err.code === 'EPERM' && err.message === 'Operation not permitted')) return false;
            throw err;
        }
    }

    async get_bucket_name(bucket_config_file_name) {
        const bucket_config_path = path.join(this.bucket_schema_dir, bucket_config_file_name);
        const { data } = await nb_native().fs.readFile(this.fs_context, bucket_config_path);
        const bucket = JSON.parse(data.toString());
        return { name: new SensitiveString(bucket.name) };
    }

    async read_bucket(params) {
        return this.read_bucket_sdk_info(params);
    }


    async create_bucket(params, sdk) {
        return bucket_semaphore.surround_key(String(params.name), async () => {
            if (!sdk.requesting_account.nsfs_account_config || !sdk.requesting_account.nsfs_account_config.new_buckets_path) {
                throw new RpcError('MISSING_NSFS_ACCOUNT_CONFIGURATION');
            }
            const fs_context = prepare_fs_context(sdk);
            native_fs_utils.validate_bucket_creation(params);

            const { name } = params;
            const bucket_config_path = this._get_bucket_config_path(name);
            const bucket_storage_path = path.join(sdk.requesting_account.nsfs_account_config.new_buckets_path, name);

            dbg.log0(`BucketSpaceFS.create_bucket 
                requesting_account=${util.inspect(sdk.requesting_account)},
                bucket_config_path=${bucket_config_path},
                bucket_storage_path=${bucket_storage_path}`);

            // initiate bucket defaults
            const create_uls = true;
            const bucket = this.new_bucket_defaults(sdk.requesting_account, params, create_uls, bucket_storage_path);
            const bucket_config = JSON.stringify(bucket);

            // create bucket configuration file
            try {
                await native_fs_utils.create_config_file(fs_context, this.bucket_schema_dir, bucket_config_path, bucket_config);
            } catch (err) {
                dbg.event({
                    code: "noobaa_bucket_creation_failed",
                    entity_type: "NODE",
                    event_type: "ERROR",
                    message: String("BucketSpaceFS: Could not create underlying config file " + name),
                    description: String("BucketSpaceFS: Could not create underlying config file " + name + " directory, Check for permission and dir path. error : " + err),
                    scope: "NODE",
                    severity: "ERROR",
                    state: "DEGRADED",
                    arguments: {bucket_name: name}
                });
                throw this._translate_bucket_error_codes(err);
            }

            // create bucket's underlying storage directory
            try {
                await nb_native().fs.mkdir(fs_context, bucket_storage_path, native_fs_utils.get_umasked_mode(config.BASE_MODE_DIR));
            } catch (err) {
                dbg.error('BucketSpaceFS: create_bucket could not create underlying directory - nsfs, deleting bucket', err);
                dbg.event({
                    code: "noobaa_bucket_creation_failed",
                    entity_type: "NODE",
                    event_type: "ERROR",
                    message: String("BucketSpaceFS: Could not create underlying bucket " + name + " directory."),
                    description: String("BucketSpaceFS: Could not create underlying bucket " + name + " directory, Check for permission and dir path. error : " + err),
                    scope: "NODE",
                    severity: "ERROR",
                    state: "DEGRADED",
                    arguments: {bucket: name, path: bucket_storage_path}
                });
                await nb_native().fs.unlink(fs_context, bucket_config_path);
                throw this._translate_bucket_error_codes(err);
            }
        });
    }


    new_bucket_defaults(account, { name, tag, lock_enabled, force_md5_etag }, create_uls, bucket_storage_path) {
        return {
            name,
            tag: js_utils.default_value(tag, ''),
            owner_account: account._id,
            system_owner: new SensitiveString(account.email),
            bucket_owner: new SensitiveString(account.email),
            versioning: config.NSFS_VERSIONING_ENABLED && lock_enabled ? 'ENABLED' : 'DISABLED',
            object_lock_configuration: config.WORM_ENABLED ? {
                object_lock_enabled: lock_enabled ? 'Enabled' : 'Disabled',
            } : undefined,
            creation_date: new Date().toISOString(),
            force_md5_etag: force_md5_etag,
            path: bucket_storage_path,
            should_create_underlying_storage: create_uls,
        };
    }


    async delete_bucket(params, object_sdk) {
        return bucket_semaphore.surround_key(String(params.name), async () => {
            const { name } = params;
            const bucket_path = this._get_bucket_config_path(name);
            try {
                const namespace_bucket_config = await object_sdk.read_bucket_sdk_namespace_info(params.name);
                dbg.log1('BucketSpaceFS.delete_bucket: namespace_bucket_config', namespace_bucket_config);
                if (namespace_bucket_config && namespace_bucket_config.should_create_underlying_storage) {
                    const ns = await object_sdk._get_bucket_namespace(params.name);
                    // delete underlying storage = the directory which represents the bucket
                    dbg.log1('BucketSpaceFS.delete_bucket: deleting uls', this.fs_root, namespace_bucket_config.write_resource.path);
                    await ns.delete_uls({
                        name,
                        full_path: path.join(this.fs_root, namespace_bucket_config.write_resource.path) // includes write_resource.path + bucket name (s3 flow)
                    }, object_sdk);
                }
                dbg.log1(`BucketSpaceFS: delete_fs_bucket ${bucket_path}`);

                // delete bucket config json file
                const fs_context = prepare_fs_context(object_sdk);
                await native_fs_utils.delete_config_file(fs_context, this.bucket_schema_dir, bucket_path);
            } catch (err) {
                dbg.event({
                    code: "noobaa_bucket_delete_failed",
                    entity_type: "NODE",
                    event_type: "ERROR",
                    message: String("BucketSpaceFS: Could not delete underlying bucket " + params.name),
                    description: String("BucketSpaceFS: Could not create underlying bucket " + params.name + ". error : " + err),
                    scope: "NODE",
                    severity: "ERROR",
                    state: "DEGRADED",
                    arguments: {bucket_name: params.name, bucket_path: bucket_path}
                });
                dbg.error('BucketSpaceFS: delete_bucket error', err);
                throw this._translate_bucket_error_codes(err);
            }
        });
    }


    //////////////////////
    // BUCKET LIFECYCLE //
    //////////////////////

    async get_bucket_lifecycle_configuration_rules(params) {
        // TODO
    }

    async set_bucket_lifecycle_configuration_rules(params) {
        // TODO
    }

    async delete_bucket_lifecycle(params) {
        // TODO
    }

    ///////////////////////
    // BUCKET VERSIONING //
    ///////////////////////

    async set_bucket_versioning(params, object_sdk) {
        try {
            const ns = await object_sdk._get_bucket_namespace(params.name);
            await ns.set_bucket_versioning(params.versioning, object_sdk);
            const { name, versioning } = params;
            dbg.log0('BucketSpaceFS.set_bucket_versioning: Bucket name, versioning', name, versioning);
            const bucket_config_path = this._get_bucket_config_path(name);
            const fs_context = prepare_fs_context(object_sdk);
            const { data } = await nb_native().fs.readFile(fs_context, bucket_config_path);
            const bucket = JSON.parse(data.toString());
            bucket.versioning = versioning;
            const update_bucket = JSON.stringify(bucket);
            await nb_native().fs.writeFile(
                fs_context,
                bucket_config_path,
                Buffer.from(update_bucket), {
                    mode: native_fs_utils.get_umasked_mode(config.BASE_MODE_CONFIG_FILE)
                }
            );
        } catch (err) {
            throw this._translate_bucket_error_codes(err);
        }
    }

    ////////////////////
    // BUCKET TAGGING //
    ////////////////////

    async put_bucket_tagging(params) {
        // TODO
    }

    async delete_bucket_tagging(params) {
        // TODO
    }

    async get_bucket_tagging(params) {
        // TODO
    }

    ///////////////////////
    // BUCKET ENCRYPTION //
    ///////////////////////

    async put_bucket_encryption(params) {
        try {
            const { name, encryption } = params;
            dbg.log0('BucketSpaceFS.put_bucket_encryption: Bucket name, encryption', name, encryption);
            const bucket_config_path = this._get_bucket_config_path(name);
            const { data } = await nb_native().fs.readFile(this.fs_context, bucket_config_path);
            const bucket = JSON.parse(data.toString());
            bucket.encryption = encryption;
            const update_bucket = JSON.stringify(bucket);
            await nb_native().fs.writeFile(
                this.fs_context,
                bucket_config_path,
                Buffer.from(update_bucket), {
                    mode: native_fs_utils.get_umasked_mode(config.BASE_MODE_CONFIG_FILE)
                }
            );
        } catch (err) {
            throw this._translate_bucket_error_codes(err);
        }
    }

    async get_bucket_encryption(params) {
        try {
            const { name } = params;
            dbg.log0('BucketSpaceFS.get_bucket_encryption: Bucket name', name);
            const bucket_config_path = this._get_bucket_config_path(name);
            const { data } = await nb_native().fs.readFile(this.fs_context, bucket_config_path);
            const bucket = JSON.parse(data.toString());
            return bucket.encryption;
        } catch (err) {
            throw this._translate_bucket_error_codes(err);
        }
    }

    async delete_bucket_encryption(params) {
        try {
            const { name } = params;
            dbg.log0('BucketSpaceFS.delete_bucket_encryption: Bucket name', name);
            const bucket_config_path = this._get_bucket_config_path(name);
            const { data } = await nb_native().fs.readFile(this.fs_context, bucket_config_path);
            const bucket = JSON.parse(data.toString());
            bucket.encryption = undefined;
            const update_bucket = JSON.stringify(bucket);
            await nb_native().fs.writeFile(
                this.fs_context,
                bucket_config_path,
                Buffer.from(update_bucket), {
                    mode: native_fs_utils.get_umasked_mode(config.BASE_MODE_CONFIG_FILE)
                }
            );
        } catch (err) {
            throw this._translate_bucket_error_codes(err);
        }
    }

    ////////////////////
    // BUCKET WEBSITE //
    ////////////////////

    async put_bucket_website(params) {
        try {
            const { name, website } = params;
            dbg.log0('BucketSpaceFS.put_bucket_website: Bucket name, website', name, website);
            const bucket_config_path = this._get_bucket_config_path(name);
            const { data } = await nb_native().fs.readFile(this.fs_context, bucket_config_path);
            const bucket = JSON.parse(data.toString());
            bucket.website = website;
            const update_bucket = JSON.stringify(bucket);
            await nb_native().fs.writeFile(
                this.fs_context,
                bucket_config_path,
                Buffer.from(update_bucket), {
                    mode: native_fs_utils.get_umasked_mode(config.BASE_MODE_CONFIG_FILE)
                }
            );
        } catch (err) {
            throw this._translate_bucket_error_codes(err);
        }
    }

    async delete_bucket_website(params) {
        try {
            const { name } = params;
            dbg.log0('BucketSpaceFS.delete_bucket_website: Bucket name', name);
            const bucket_config_path = this._get_bucket_config_path(name);
            const { data } = await nb_native().fs.readFile(this.fs_context, bucket_config_path);
            const bucket = JSON.parse(data.toString());
            bucket.website = undefined;
            const update_bucket = JSON.stringify(bucket);
            await nb_native().fs.writeFile(
                this.fs_context,
                bucket_config_path,
                Buffer.from(update_bucket), {
                    mode: native_fs_utils.get_umasked_mode(config.BASE_MODE_CONFIG_FILE)
                }
            );
        } catch (err) {
            throw this._translate_bucket_error_codes(err);
        }
    }

    async get_bucket_website(params) {
        try {
            const { name } = params;
            dbg.log0('BucketSpaceFS.get_bucket_website: Bucket name', name);
            const bucket_config_path = this._get_bucket_config_path(name);
            const { data } = await nb_native().fs.readFile(this.fs_context, bucket_config_path);
            const bucket = JSON.parse(data.toString());
            return bucket.website;
        } catch (err) {
            throw this._translate_bucket_error_codes(err);
        }
    }

    ////////////////////
    // BUCKET POLICY  //
    ////////////////////


    async put_bucket_policy(params) {
        try {
            const { name, policy } = params;
            dbg.log0('BucketSpaceFS.put_bucket_policy: Bucket name, policy', name, policy);
            const bucket_config_path = this._get_bucket_config_path(name);
            const { data } = await nb_native().fs.readFile(this.fs_context, bucket_config_path);
            const bucket = JSON.parse(data.toString());
            await bucket_policy_utils.validate_s3_policy(policy, bucket.name, async principal => this._get_account_by_name(principal));
            bucket.s3_policy = policy;
            const update_bucket = JSON.stringify(bucket);
            await nb_native().fs.writeFile(
                this.fs_context,
                bucket_config_path,
                Buffer.from(update_bucket), {
                    mode: native_fs_utils.get_umasked_mode(config.BASE_MODE_CONFIG_FILE)
                }
            );
        } catch (err) {
            throw this._translate_bucket_error_codes(err);
        }
    }

    async delete_bucket_policy(params) {
        try {
            const { name } = params;
            dbg.log0('BucketSpaceFS.delete_bucket_policy: Bucket name', name);
            const bucket_config_path = this._get_bucket_config_path(name);
            const { data } = await nb_native().fs.readFile(this.fs_context, bucket_config_path);
            const bucket = JSON.parse(data.toString());
            bucket.s3_policy = undefined;
            const update_bucket = JSON.stringify(bucket);
            await nb_native().fs.writeFile(
                this.fs_context,
                bucket_config_path,
                Buffer.from(update_bucket), {
                    mode: native_fs_utils.get_umasked_mode(config.BASE_MODE_CONFIG_FILE)
                }
            );
        } catch (err) {
            throw this._translate_bucket_error_codes(err);
        }
    }

    async get_bucket_policy(params) {
        try {
            const { name } = params;
            dbg.log0('BucketSpaceFS.get_bucket_policy: Bucket name', name);
            const bucket_path = this._get_bucket_config_path(name);
            const { data } = await nb_native().fs.readFile(this.fs_context, bucket_path);
            const bucket = JSON.parse(data.toString());
            dbg.log0('BucketSpaceFS.get_bucket_policy: policy', bucket.s3_policy);
            return {
                policy: bucket.s3_policy
            };
        } catch (err) {
            throw this._translate_bucket_error_codes(err);
        }
    }

    /////////////////////////
    // DEFAULT OBJECT LOCK //
    /////////////////////////

    async get_object_lock_configuration(params, object_sdk) {
        // TODO
    }

    async put_object_lock_configuration(params, object_sdk) {
        // TODO
    }
}

module.exports = BucketSpaceFS;
