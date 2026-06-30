/* Copyright (C) 2024 NooBaa */
'use strict';

const _ = require('lodash');
const P = require('../util/promise');
const s3_utils = require('../endpoint/s3/s3_utils');
const S3Error = require('../endpoint/s3/s3_errors').S3Error;

const EXCEPT_REASONS = [
    'NO_SUCH_OBJECT'
];

/**
 * NamespaceMultiStorageClass routes operations to different namespaces based on
 * the object's storage class — analogous to NamespaceMerge, but keyed by
 * storage_class instead of read/write resources.
 *
 * Example:
 *   new NamespaceMultiStorageClass({
 *     namespaces: {
 *       STANDARD: namespace_nb,
 *       DEEP_ARCHIVE: namespace_deep_archive,
 *       GLACIER: namespace_deep_archive,
 *     },
 *   })
 *
 * Write path (PutObject, CreateMultipartUpload, …):
 *   params.storage_class → matching namespace (falls back to default / STANDARD)
 *
 * Read path (HeadObject, GetObject, …):
 *   probe all unique namespaces (like NamespaceMerge), then re-bind md.ns to
 *   the namespace that owns that storage_class so subsequent stream/delete
 *   calls hit the right backend.
 *
 * List objects / versions / uploads:
 *   served only from the default (STANDARD) namespace — object and upload metadata
 *   for every storage class is owned there. Archive backends like NamespaceDeepArchive
 *   are not queried for listings. Direct access under `restored_objects/` is rejected
 *   (AccessDenied); the MD list query always excludes that internal prefix.
 *
 * Adding a new storage class later is just another entry in `namespaces`.
 *
 * @implements {nb.Namespace}
 */
class NamespaceMultiStorageClass {

    /**
     * @param {{
     *   namespaces: { [storage_class: string]: nb.Namespace },
     *   default_storage_class?: string,
     * }} args
     */
    constructor({ namespaces, default_storage_class }) {
        if (!namespaces || !Object.keys(namespaces).length) {
            throw new Error('NamespaceMultiStorageClass requires a non-empty namespaces map');
        }
        this.namespaces = namespaces;
        this.default_storage_class = default_storage_class || s3_utils.STORAGE_CLASS_STANDARD;
        // Preserve insertion order while deduplicating (same ns may be mapped
        // to several storage classes, e.g. GLACIER + DEEP_ARCHIVE).
        this._unique_namespaces = _.uniq(Object.values(namespaces));
    }

    /**
     * Returns this router as the write target.
     * Used by ObjectSDK copy to resolve the actual write backend for server-side copy checks.
     * @returns {nb.Namespace}
     */
    get_write_resource() {
        return this;
    }

    /**
     * Server-side copy across the router is disabled (same restriction as NamespaceMerge).
     * @param {nb.Namespace} other
     * @param {nb.ObjectInfo} other_md
     * @param {object} params
     * @returns {boolean}
     */
    is_server_side_copy(other, other_md, params) {
        return false;
    }

    /**
     * @param {string} bucket
     * @returns {string}
     */
    get_bucket(bucket) {
        return bucket;
    }

    /**
     * True only when every unique backend namespace is read-only.
     * @returns {boolean}
     */
    is_readonly_namespace() {
        return this._unique_namespaces.every(ns => ns.is_readonly_namespace());
    }

    /////////////////
    // OBJECT LIST //
    /////////////////

    /**
     * Lists objects from the default (STANDARD) namespace only.
     * Metadata for all storage classes is stored there, so fan-out would duplicate
     * entries (and conflict with backends like NamespaceDeepArchive that share NB).
     * `restored_objects/` is rejected on direct access; the MD list query always
     * excludes that internal prefix.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async list_objects(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_for_storage_class(this.default_storage_class)
            .list_objects(params, object_sdk);
    }

    /**
     * Lists in-progress multipart uploads from the default (STANDARD) namespace only.
     * Same rationale as {@link list_objects}.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async list_uploads(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_for_storage_class(this.default_storage_class)
            .list_uploads(params, object_sdk);
    }

    /**
     * Lists object versions from the default (STANDARD) namespace only.
     * Same rationale as {@link list_objects}.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async list_object_versions(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_for_storage_class(this.default_storage_class)
            .list_object_versions(params, object_sdk);
    }

    /////////////////
    // OBJECT READ //
    /////////////////

    /**
     * Probes all unique namespaces for the object, then binds `md.ns` to the
     * namespace that owns `md.storage_class` so follow-up stream/delete calls
     * hit the correct backend.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<nb.ObjectInfo>}
     */
    async read_object_md(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        const replies = await this._ns_map(ns => ns.read_object_md(params, object_sdk)
            .then(res => {
                res.ns = res.ns || ns;
                return res;
            }), EXCEPT_REASONS);

        // Prefer a reply whose storage_class is owned by the namespace that
        // produced it (e.g. DeepArchive over a shared NB metadata record).
        const owned = replies.filter(r =>
            this._ns_for_storage_class(r.storage_class) === r.ns
        );
        const working_set = _.sortBy(owned.length ? owned : replies, 'create_time');
        const md = _.last(working_set);
        md.ns = this._ns_for_storage_class(md.storage_class) || md.ns;
        return md;
    }

    /**
     * Streams object data via `params.object_md.ns` when available (set by
     * {@link read_object_md}); otherwise probes unique namespaces.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<import('stream').Readable>}
     */
    async read_object_stream(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        params = _.omit(params, 'noobaa_trigger_agent');
        if (params.object_md && params.object_md.ns) {
            return params.object_md.ns.read_object_stream(params, object_sdk);
        }
        return this._ns_get(ns => ns.read_object_stream(params, object_sdk));
    }

    ///////////////////
    // OBJECT UPLOAD //
    ///////////////////

    /**
     * Routes the upload to the namespace for `params.storage_class`.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async upload_object(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_put(params, ns => ns.upload_object(params, object_sdk));
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    upload_blob_block(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_put(params, ns => ns.upload_blob_block(params, object_sdk));
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    commit_blob_block_list(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_put(params, ns => ns.commit_blob_block_list(params, object_sdk));
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    get_blob_block_lists(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_put(params, ns => ns.get_blob_block_lists(params, object_sdk));
    }

    /////////////////////////////
    // OBJECT MULTIPART UPLOAD //
    /////////////////////////////

    /**
     * Routes CreateMultipartUpload to the namespace for `params.storage_class`.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    create_object_upload(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_put(params, ns => ns.create_object_upload(params, object_sdk));
    }

    /**
     * Follow-up MPU ops have no storage_class on the request — probes namespaces
     * until one accepts the upload_id (same pattern as Merge reads).
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    upload_multipart(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.upload_multipart(params, object_sdk));
    }

    /**
     * Follow-up MPU ops have no storage_class on the request — probes namespaces
     * until one accepts the upload_id (same pattern as Merge reads).
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async list_multiparts(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.list_multiparts(params, object_sdk));
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async complete_object_upload(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.complete_object_upload(params, object_sdk));
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    abort_object_upload(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.abort_object_upload(params, object_sdk));
    }

    ///////////////////
    // OBJECT DELETE //
    ///////////////////

    /**
     * Fans out delete across unique namespaces (NO_SUCH_OBJECT is ignored).
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async delete_object(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        const reply = await this._ns_map(ns => ns.delete_object(params, object_sdk), EXCEPT_REASONS);
        return _.first(reply);
    }

    /**
     * Fans out multi-delete across unique namespaces and merges per-object results.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object[]>}
     */
    async delete_multiple_objects(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        const deleted_res = await this._ns_map(ns => ns.delete_multiple_objects(params, object_sdk));
        const merged_res = this._merge_multiple_delete_responses({
            deleted_res,
            total_objects: params.objects.length
        });
        return _.map(merged_res, obj => obj.res);
    }

    /**
     * Merges per-namespace multi-delete reply arrays into a single per-object conclusion.
     * @param {{ deleted_res: object[][], total_objects: number }} params
     * @returns {Array<{ success: boolean, res: object }>}
     */
    _merge_multiple_delete_responses(params) {
        const { deleted_res } = params;
        let ns_conclusion;
        for (let ns = 0; ns < deleted_res.length; ++ns) {
            const deleted_ns = deleted_res[ns];
            const ns_merged = this._handle_single_namespace_deletes({ deleted_ns });
            if (ns_conclusion) {
                for (let obj_index = 0; obj_index < ns_conclusion.length; obj_index++) {
                    ns_conclusion[obj_index] =
                        this._pick_ns_obj_reply({ curr: ns_conclusion[obj_index], cand: ns_merged[obj_index] });
                }
            } else {
                ns_conclusion = ns_merged;
            }
        }
        return ns_conclusion;
    }

    /**
     * Normalizes a single namespace's multi-delete replies into `{ success, res }` entries.
     * @param {{ deleted_ns: object[] }} params
     * @returns {Array<{ success: boolean, res: object }>}
     */
    _handle_single_namespace_deletes(params) {
        const response = [];
        const { deleted_ns } = params;
        for (let i = 0; i < deleted_ns.length; ++i) {
            const res = deleted_ns[i];
            if (_.isUndefined(res && res.err_code)) {
                response.push({ success: true, res });
            } else {
                response.push({ success: false, res });
            }
        }
        return response;
    }

    /**
     * Picks between two per-object delete replies, preferring success over failure.
     * @param {{ curr: { success: boolean, res: object }, cand: { success: boolean, res: object } }} params
     * @returns {{ success: boolean, res: object }}
     */
    _pick_ns_obj_reply(params) {
        const { curr, cand } = params;
        const STATUSES = {
            FAILED_WITHOUT_INFO: 1,
            SUCCEEDED_WITHOUT_INFO: 0
        };
        const get_object_status = object => {
            if (object.success) return STATUSES.SUCCEEDED_WITHOUT_INFO;
            return STATUSES.FAILED_WITHOUT_INFO;
        };
        const curr_status = get_object_status(curr);
        const cand_status = get_object_status(cand);

        if (curr_status > cand_status) return curr;
        if (cand_status > curr_status) return cand;
        return curr;
    }

    ////////////////////
    // OBJECT TAGGING //
    ////////////////////

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    get_object_tagging(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.get_object_tagging(params, object_sdk));
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    delete_object_tagging(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.delete_object_tagging(params, object_sdk));
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    put_object_tagging(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.put_object_tagging(params, object_sdk));
    }

    //////////
    // ACLs //
    //////////

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    get_object_acl(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.get_object_acl(params, object_sdk));
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    put_object_acl(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.put_object_acl(params, object_sdk));
    }

    ///////////////////
    //  OBJECT LOCK  //
    ///////////////////

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async get_object_legal_hold(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.get_object_legal_hold(params, object_sdk));
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async put_object_legal_hold(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.put_object_legal_hold(params, object_sdk));
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async get_object_retention(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.get_object_retention(params, object_sdk));
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async put_object_retention(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.put_object_retention(params, object_sdk));
    }

    ///////////////////
    //      ULS      //
    ///////////////////

    /**
     * @returns {Promise<never>}
     */
    async create_uls() {
        throw new Error('TODO');
    }

    /**
     * @returns {Promise<never>}
     */
    async delete_uls() {
        throw new Error('TODO');
    }

    ////////////////////
    // OBJECT RESTORE //
    ////////////////////

    /**
     * Probes unique namespaces until one handles RestoreObject
     * (typically NamespaceDeepArchive for glacier classes).
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async restore_object(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        return this._ns_get(ns => ns.restore_object(params, object_sdk));
    }

    //////////////////////////
    //  OBJECT ATTRIBUTES   //
    //////////////////////////

    /**
     * Resolves object md via {@link read_object_md}, then delegates to the bound
     * namespace's get_object_attributes when available.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<nb.ObjectInfo>}
     */
    async get_object_attributes(params, object_sdk) {
        this._assert_no_internal_restored_access(params);
        const md = await this.read_object_md(params, object_sdk);
        if (md.ns && md.ns.get_object_attributes) {
            return md.ns.get_object_attributes(params, object_sdk);
        }
        return md;
    }

    //////////////
    // INTERNAL //
    //////////////

    /**
     * Rejects user access to the internal `restored_objects/` directory.
     * Checks key, prefix, copy_source key, and multi-delete keys.
     * Internal NooBaa paths (DeepArchive restore reads, BG worker writes) use
     * NamespaceNB directly and bypass this router.
     * @param {object} [params]
     */
    _assert_no_internal_restored_access(params) {
        if (!params) return;
        const paths = [
            params.key,
            params.prefix,
            params.copy_source?.key,
            ...(params.objects || []).map(obj => obj.key),
        ];
        for (const path of paths) {
            if (path && path.startsWith(s3_utils.RESTORED_OBJECTS_DIR)) {
                throw new S3Error(S3Error.AccessDenied);
            }
        }
    }

    /**
     * Resolves the namespace for a write based on `storage_class`.
     * Falls back to `default_storage_class` when unset or unmapped.
     * @param {string} [storage_class]
     * @returns {nb.Namespace}
     */
    _ns_for_storage_class(storage_class) {
        const sc = s3_utils.parse_storage_class(storage_class) || this.default_storage_class;
        return this.namespaces[sc] || this.namespaces[this.default_storage_class];
    }

    /**
     * Dispatches a write to the namespace selected by `params.storage_class`.
     * @param {object} params
     * @param {(ns: nb.Namespace) => Promise<*>} func
     * @returns {Promise<*>}
     */
    async _ns_put(params, func) {
        const ns = this._ns_for_storage_class(params.storage_class);
        if (!ns) {
            throw new S3Error(S3Error.InvalidStorageClass);
        }
        return func(ns);
    }

    /**
     * Tries `func` on each unique namespace until one succeeds.
     * @param {(ns: nb.Namespace) => Promise<*>} func
     * @returns {Promise<*>}
     */
    async _ns_get(func) {
        for (const ns of this._unique_namespaces) {
            try {
                return await func(ns);
            } catch (err) {
                continue;
            }
        }
        throw new Error('NamespaceMultiStorageClass._ns_get exhausted');
    }

    /**
     * Runs `func` on all unique namespaces in parallel and returns successful replies,
     * throwing on unexpected failures (except those listed in `except_reasons`).
     * @param {(ns: nb.Namespace) => Promise<*>} func
     * @param {string[]} [except_reasons]
     * @param {(err: Error) => Error} [cast_error_func]
     * @returns {Promise<*[]>}
     */
    async _ns_map(func, except_reasons, cast_error_func = null) {
        const replies = await P.map(this._unique_namespaces, async ns => {
            try {
                const res = await func(ns);
                return { reply: res, success: true };
            } catch (err) {
                return {
                    error: cast_error_func ? cast_error_func(err) : err,
                    success: false
                };
            }
        });
        return this._throw_if_any_failed_or_get_succeeded(replies, except_reasons);
    }

    /**
     * @param {Array<{ success: boolean, reply?: *, error?: Error }>} reply_array
     * @returns {*[]}
     */
    _get_succeeded_responses(reply_array) {
        return reply_array.filter(res => res.success).map(rec => rec.reply);
    }

    /**
     * @param {Array<{ success: boolean, reply?: *, error?: Error }>} reply_array
     * @param {string[]} [except_reasons]
     * @returns {Error[]}
     */
    _get_failed_responses(reply_array, except_reasons) {
        return reply_array.filter(
                res => !res.success &&
                !_.includes(except_reasons || [], res.error.rpc_code || res.error.code || 'UNKNOWN_ERR')
            )
            .map(rec => rec.error);
    }

    /**
     * Throws the first unexpected failure; otherwise returns succeeded replies.
     * If nothing succeeded, throws the first error (even if it was an excepted reason).
     * @param {Array<{ success: boolean, reply?: *, error?: Error }>} reply_array
     * @param {string[]} [except_reasons]
     * @returns {*[]}
     */
    _throw_if_any_failed_or_get_succeeded(reply_array, except_reasons) {
        const failed = this._get_failed_responses(reply_array, except_reasons);
        if (!_.isEmpty(failed)) throw _.first(failed);
        const succeeded = this._get_succeeded_responses(reply_array);
        if (_.isEmpty(succeeded)) throw _.first(reply_array).error;
        return succeeded;
    }

    /**
     * Merges list replies from multiple namespaces into a single S3-style listing
     * (newest object wins per key; same approach as NamespaceMerge).
     * @param {object[]} res
     * @param {object} params
     * @returns {object}
     */
    _handle_list(res, params) {
        if (res.length === 1) return res[0];
        let i;
        let j;
        const map = {};
        let is_truncated;
        for (i = 0; i < res.length; ++i) {
            for (j = 0; j < res[i].objects.length; ++j) {
                const obj = res[i].objects[j];
                if (!map[obj.key] ||
                    (map[obj.key] && obj.create_time > map[obj.key].create_time)
                ) map[obj.key] = obj;
            }
            for (j = 0; j < res[i].common_prefixes.length; ++j) {
                const prefix = res[i].common_prefixes[j];
                map[prefix] = prefix;
            }
            if (res[i].is_truncated) is_truncated = true;
        }
        const all_names = Object.keys(map);
        all_names.sort();
        const names = all_names.slice(0, params.limit || 1000);
        const objects = [];
        const common_prefixes = [];
        for (i = 0; i < names.length; ++i) {
            const name = names[i];
            const obj_or_prefix = map[name];
            if (typeof obj_or_prefix === 'string') {
                common_prefixes.push(obj_or_prefix);
            } else {
                objects.push(obj_or_prefix);
            }
        }
        if (names.length < all_names.length) {
            is_truncated = true;
        }
        const next_marker = is_truncated ? names[names.length - 1] : undefined;
        const last_obj_or_prefix = map[names[names.length - 1]];
        const next_version_id_marker =
            is_truncated && (typeof last_obj_or_prefix === 'object') ?
            last_obj_or_prefix.version_id : undefined;
        const next_upload_id_marker =
            is_truncated && (typeof last_obj_or_prefix === 'object') ?
            last_obj_or_prefix.obj_id : undefined;

        return {
            objects,
            common_prefixes,
            is_truncated,
            next_marker,
            next_version_id_marker,
            next_upload_id_marker
        };
    }

    /**
     * Maps known cloud "bucket missing" errors onto S3 NoSuchBucket.
     * @param {Error & { code?: string }} err
     * @returns {Error|undefined}
     */
    cast_err_to_s3err(err) {
        if (!err) return;
        const err_to_s3err_map = {
            'NoSuchBucket': S3Error.NoSuchBucket,
            'ContainerNotFound': S3Error.NoSuchBucket,
        };
        const exist = err_to_s3err_map[err.code];
        if (!exist) return err;
        const s3error = new S3Error(exist);
        s3error.message = err.message;
        return s3error;
    }
}


module.exports = NamespaceMultiStorageClass;
