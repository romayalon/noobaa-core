/* Copyright (C) 2024 NooBaa */
'use strict';

const _ = require('lodash');
const util = require('util');

const dbg = require('../util/debug_module')(__filename);
const s3_utils = require('../endpoint/s3/s3_utils');
const S3Error = require('../endpoint/s3/s3_errors').S3Error;

const XATTR_RESTORE_ONGOING = 'noobaa-deep-archive.restore.ongoing';
const XATTR_RESTORE_EXPIRY = 'noobaa-deep-archive.restore.expiry';
const XATTR_STORAGE_CLASS = 'noobaa-deep-archive.storage-class';

/**
 * NamespaceDeepArchive is the archive backend for GLACIER / DEEP_ARCHIVE objects.
 *
 * It is intended to be registered under those storage classes in a
 * NamespaceMultiStorageClass router — it does NOT handle STANDARD objects itself.
 *
 * Inner namespaces:
 *   - deep_archive_ns (NamespaceS3) — remote Deep Archive / S3-compatible endpoint
 *     where object data lives with the archive storage class.
 *   - namespace_nb (NamespaceNB) — NooBaa metadata DB for object metadata,
 *     including restore_status and storage_class (via xattr).
 *
 * Restore status is persisted via object xattr metadata keys:
 *   - noobaa-deep-archive.restore.ongoing — "true" while restore is in progress
 *   - noobaa-deep-archive.restore.expiry — ISO date string of restore expiry
 *   - noobaa-deep-archive.storage-class — the storage class (DEEP_ARCHIVE / GLACIER)
 *
 * Data flow:
 *   - PutObject / CompleteMultipartUpload: data → deep_archive_ns, metadata → namespace_nb
 *   - HeadObject / GetObjectAttributes: served from namespace_nb
 *   - ListObjects / ListObjectVersions / ListMultipartUploads: owned by
 *     NamespaceMultiStorageClass via the STANDARD (NamespaceNB) namespace — not listed here
 *   - GetObject: if not restored → InvalidObjectState; else read the temporary copy from
 *     namespace_nb at key `restored_objects/<original_key>` (written by the restore BG worker)
 *   - RestoreObject: update namespace_nb restore_status + call S3 RestoreObject on archive
 *   - DeleteObject: delete archive data from deep_archive_ns (metadata delete owned by STANDARD ns)
 *   - CopyObject (source archived): if not restored → InvalidObjectState; else copy via deep_archive_ns
 *
 * @implements {nb.Namespace}
 */
class NamespaceDeepArchive {

    /**
     * @param {{
     *      deep_archive_ns: nb.Namespace & { s3: any, bucket: string },
     *      namespace_nb: nb.Namespace,
     *      stats: import('./endpoint_stats_collector').EndpointStatsCollector,
     * }} args
     */
    constructor({ deep_archive_ns, namespace_nb, stats }) {
        this.deep_archive_ns = deep_archive_ns;
        this.namespace_nb = namespace_nb;
        this.stats = stats;
    }

    /**
     * Returns this namespace as the write target.
     * Used by ObjectSDK copy to resolve the actual write backend for server-side copy checks.
     * @returns {nb.Namespace}
     */
    get_write_resource() {
        return this;
    }

    /**
     * Server-side copy is disabled.
     * NamespaceMultiStorageClass already returns false for the same reason as NamespaceMerge
     * (composite routing). Even if this method were reached directly, archive copies need
     * restore checks and storage-class-aware routing that a remote CopyObject path does not handle.
     * @param {nb.Namespace} other
     * @param {nb.ObjectInfo} other_md
     * @param {object} params
     * @returns {boolean}
     */
    is_server_side_copy(other, other_md, params) {
        return false;
    }

    /**
     * Returns the underlying deep-archive bucket name used for remote S3 operations.
     * @returns {string}
     */
    get_bucket() {
        return this.deep_archive_ns.get_bucket();
    }

    /**
     * @returns {boolean}
     */
    is_readonly_namespace() {
        return this.deep_archive_ns.is_readonly_namespace();
    }


    /////////////////
    // OBJECT LIST //
    /////////////////

    /**
     * Not used for object listing under NamespaceMultiStorageClass — that router
     * lists only via the STANDARD (NamespaceNB) namespace, where metadata for every
     * storage class already lives. Returning empty keeps this method harmless if
     * ever invoked via a fan-out path.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async list_objects(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.list_objects: skipped (listed via STANDARD ns by MultiStorageClass)');
        return { objects: [], common_prefixes: [], is_truncated: false };
    }

    /**
     * Not used for upload listing under NamespaceMultiStorageClass — that router
     * lists only via the STANDARD (NamespaceNB) namespace.
     * Returning empty keeps this method harmless if ever invoked via a fan-out path.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async list_uploads(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.list_uploads: skipped (listed via STANDARD ns by MultiStorageClass)');
        return { objects: [], common_prefixes: [], is_truncated: false };
    }

    /**
     * Not used for version listing under NamespaceMultiStorageClass
     * (same rationale as {@link list_objects}).
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async list_object_versions(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.list_object_versions: skipped (listed via STANDARD ns by MultiStorageClass)');
        return { objects: [], common_prefixes: [], is_truncated: false };
    }


    /////////////////
    // OBJECT READ //
    /////////////////

    /**
     * Reads object metadata from namespace_nb and populates archive/restore fields.
     * Throws NO_SUCH_OBJECT for non-archived objects so NamespaceMultiStorageClass
     * falls through to the STANDARD namespace.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<nb.ObjectInfo>}
     */
    async read_object_md(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.read_object_md:', inspect(params));
        const md = await this.namespace_nb.read_object_md(params, object_sdk);
        this._populate_archive_fields(md);
        // Only claim objects that actually belong to an archive storage class.
        // NamespaceMultiStorageClass probes all namespaces; returning NoSuchObject
        // for STANDARD objects lets the STANDARD namespace win.
        if (!s3_utils.GLACIER_STORAGE_CLASSES.includes(md.storage_class)) {
            const err = new Error('Object is not archived');
            err.rpc_code = 'NO_SUCH_OBJECT';
            throw err;
        }
        return md;
    }

    /**
     * Streams object data after a successful restore.
     * Reads the temporary restored copy from namespace_nb at
     * `restored_objects/<original_key>` (same bucket). Throws InvalidObjectState
     * if the object is not restored.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<import('stream').Readable>}
     */
    async read_object_stream(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.read_object_stream:', inspect(params));
        const object_md = params.object_md || await this.namespace_nb.read_object_md(params, object_sdk);
        this._assert_restored(object_md, 'read_object_stream');
        // Restored copy lives under restored_objects/<key>; omit archive object_md.
        return this.namespace_nb.read_object_stream({
            ...params,
            key: s3_utils.RESTORED_OBJECTS_DIR + params.key,
            object_md: undefined,
        }, object_sdk);
    }


    ///////////////////
    // OBJECT UPLOAD //
    ///////////////////

    /**
     * Uploads object data to deep_archive_ns and writes a metadata shadow to namespace_nb
     * (including XATTR_STORAGE_CLASS for later routing/restore).
     * Copy sources that are archived must already be restored.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async upload_object(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.upload_object:', inspect(params));

        if (params.copy_source) {
            const source_md = params.copy_source.object_md ||
                await this.namespace_nb.read_object_md({
                    bucket: params.copy_source.bucket,
                    key: params.copy_source.key,
                    version_id: params.copy_source.version_id,
                }, object_sdk);

            if (s3_utils.GLACIER_STORAGE_CLASSES.includes(source_md.storage_class)) {
                this._assert_restored(source_md, 'upload_object (copy_source)');
            }
        }

        const storage_class = s3_utils.parse_storage_class(params.storage_class) ||
            s3_utils.STORAGE_CLASS_DEEP_ARCHIVE;
        params.storage_class = storage_class;

        const archive_res = await this.deep_archive_ns.upload_object(params, object_sdk);

        const nb_params = _.defaults({
            storage_class,
            xattr: _.defaults({
                [XATTR_STORAGE_CLASS]: storage_class,
            }, params.xattr),
        }, params);
        try {
            await this.namespace_nb.upload_object(nb_params, object_sdk);
        } catch (err) {
            dbg.warn('NamespaceDeepArchive.upload_object: NB metadata write failed, archive data may be orphaned', err);
        }

        return archive_res;
    }


    ////////////////////////
    // BLOCK BLOB UPLOADS //
    ////////////////////////

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<never>}
     */
    async upload_blob_block(params, object_sdk) {
        throw new S3Error(S3Error.NotImplemented);
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<never>}
     */
    async commit_blob_block_list(params, object_sdk) {
        throw new S3Error(S3Error.NotImplemented);
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<never>}
     */
    async get_blob_block_lists(params, object_sdk) {
        throw new S3Error(S3Error.NotImplemented);
    }


    /////////////////////////////
    // OBJECT MULTIPART UPLOAD //
    /////////////////////////////

    /**
     * Starts a multipart upload on deep_archive_ns.
     * Defaults storage_class to DEEP_ARCHIVE when unset.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async create_object_upload(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.create_object_upload:', inspect(params));
        params.storage_class = s3_utils.parse_storage_class(params.storage_class) ||
            s3_utils.STORAGE_CLASS_DEEP_ARCHIVE;
        return this.deep_archive_ns.create_object_upload(params, object_sdk);
    }

    /**
     * Uploads a multipart part to deep_archive_ns.
     * Copy-part sources that are archived must already be restored.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async upload_multipart(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.upload_multipart:', inspect(params));
        if (params.copy_source) {
            const source_md = params.copy_source.object_md ||
                await this.namespace_nb.read_object_md({
                    bucket: params.copy_source.bucket,
                    key: params.copy_source.key,
                    version_id: params.copy_source.version_id,
                }, object_sdk);

            if (s3_utils.GLACIER_STORAGE_CLASSES.includes(source_md.storage_class)) {
                this._assert_restored(source_md, 'upload_multipart (copy_source)');
            }
        }
        return this.deep_archive_ns.upload_multipart(params, object_sdk);
    }

    /**
     * Lists uploaded parts for an in-progress multipart upload on deep_archive_ns.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async list_multiparts(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.list_multiparts:', inspect(params));
        return this.deep_archive_ns.list_multiparts(params, object_sdk);
    }

    /**
     * Completes a multipart upload on deep_archive_ns and writes a metadata shadow
     * to namespace_nb (same pattern as {@link upload_object}).
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async complete_object_upload(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.complete_object_upload:', inspect(params));
        const archive_res = await this.deep_archive_ns.complete_object_upload(params, object_sdk);

        const storage_class = s3_utils.parse_storage_class(params.storage_class) ||
            s3_utils.STORAGE_CLASS_DEEP_ARCHIVE;
        const nb_params = _.defaults({
            storage_class,
            xattr: _.defaults({
                [XATTR_STORAGE_CLASS]: storage_class,
            }, params.xattr),
        }, params);
        try {
            await this.namespace_nb.complete_object_upload(nb_params, object_sdk);
        } catch (err) {
            dbg.warn('NamespaceDeepArchive.complete_object_upload: NB metadata write failed, archive data may be orphaned', err);
        }

        return archive_res;
    }

    /**
     * Aborts an in-progress multipart upload on deep_archive_ns.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async abort_object_upload(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.abort_object_upload:', inspect(params));
        return this.deep_archive_ns.abort_object_upload(params, object_sdk);
    }


    ////////////////////
    // OBJECT TAGGING //
    ////////////////////

    /**
     * Tagging is stored with the metadata shadow in namespace_nb.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async put_object_tagging(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.put_object_tagging:', inspect(params));
        return this.namespace_nb.put_object_tagging(params, object_sdk);
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async delete_object_tagging(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.delete_object_tagging:', inspect(params));
        return this.namespace_nb.delete_object_tagging(params, object_sdk);
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async get_object_tagging(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.get_object_tagging:', inspect(params));
        return this.namespace_nb.get_object_tagging(params, object_sdk);
    }

    //////////
    // ACLs //
    //////////

    /**
     * ACLs are stored with the metadata shadow in namespace_nb.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async get_object_acl(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.get_object_acl:', inspect(params));
        return this.namespace_nb.get_object_acl(params, object_sdk);
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async put_object_acl(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.put_object_acl:', inspect(params));
        return this.namespace_nb.put_object_acl(params, object_sdk);
    }

    ///////////////////
    // OBJECT DELETE //
    ///////////////////

    /**
     * Deletes archive data from deep_archive_ns for archived objects only.
     * Metadata deletion is owned by the STANDARD (NamespaceNB) side of the
     * NamespaceMultiStorageClass router to avoid double-deleting the shared NB record.
     * Throws NO_SUCH_OBJECT for non-archived objects so the router can ignore this ns.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async delete_object(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.delete_object:', inspect(params));

        // Metadata delete is owned by the STANDARD (NamespaceNB) side of the
        // NamespaceMultiStorageClass router. Here we only remove archive data, and
        // only for objects that are actually archived.
        const md = await this.namespace_nb.read_object_md({
            bucket: params.bucket,
            key: params.key,
            version_id: params.version_id,
        }, object_sdk);
        this._populate_archive_fields(md);
        if (!s3_utils.GLACIER_STORAGE_CLASSES.includes(md.storage_class)) {
            const err = new Error('Object is not archived');
            err.rpc_code = 'NO_SUCH_OBJECT';
            throw err;
        }

        try {
            return await this.deep_archive_ns.delete_object(params, object_sdk);
        } catch (err) {
            dbg.warn('NamespaceDeepArchive.delete_object: archive S3 delete failed, may leave orphaned data', err);
            return {};
        }
    }

    /**
     * Deletes archive data for the archived subset of `params.objects`.
     * Returns a per-object result array aligned with input order (Merge-compatible).
     * Non-archived entries are marked as NoSuchKey so the STANDARD ns owns those deletes.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object[]>}
     */
    async delete_multiple_objects(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.delete_multiple_objects:', inspect(params));

        // Metadata delete is owned by STANDARD/NamespaceNB. Only purge archive
        // data for archived objects; return a per-object result aligned with params.
        const md_results = await Promise.allSettled(
            params.objects.map(obj =>
                this.namespace_nb.read_object_md({
                    bucket: params.bucket,
                    key: obj.key,
                    version_id: obj.version_id,
                }, object_sdk)
            )
        );

        const archived_objects = [];
        const archived_indexes = [];
        for (let i = 0; i < params.objects.length; i++) {
            if (md_results[i].status === 'fulfilled') {
                const md = md_results[i].value;
                this._populate_archive_fields(md);
                if (s3_utils.GLACIER_STORAGE_CLASSES.includes(md.storage_class)) {
                    archived_objects.push(params.objects[i]);
                    archived_indexes.push(i);
                }
            }
        }

        const results = params.objects.map(() => ({ err_code: 'NoSuchKey', err_message: 'Not archived' }));

        if (archived_objects.length === 0) return results;

        let archive_res;
        try {
            archive_res = await this.deep_archive_ns.delete_multiple_objects({
                ...params,
                objects: archived_objects,
            }, object_sdk);
        } catch (err) {
            dbg.warn('NamespaceDeepArchive.delete_multiple_objects: archive S3 delete failed, may leave orphaned data', err);
            archive_res = archived_objects.map(() => ({}));
        }

        for (let j = 0; j < archived_indexes.length; j++) {
            results[archived_indexes[j]] = archive_res[j] || {};
        }
        return results;
    }


    ///////////////////
    //  OBJECT LOCK  //
    ///////////////////

    /**
     * Object-lock state is stored with the metadata shadow in namespace_nb.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async get_object_legal_hold(params, object_sdk) {
        return this.namespace_nb.get_object_legal_hold(params, object_sdk);
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async put_object_legal_hold(params, object_sdk) {
        return this.namespace_nb.put_object_legal_hold(params, object_sdk);
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async get_object_retention(params, object_sdk) {
        return this.namespace_nb.get_object_retention(params, object_sdk);
    }

    /**
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async put_object_retention(params, object_sdk) {
        return this.namespace_nb.put_object_retention(params, object_sdk);
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
     * Initiates (or extends) a restore for an archived object.
     * Updates restore xattrs on namespace_nb and issues RestoreObject on deep_archive_ns.
     * @param {object} params
     * @param {number} params.days Number of days the restored copy should remain available
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<{ accepted: boolean }>} `accepted: true` if a new restore was started;
     *   `accepted: false` if an existing restore expiry was extended
     */
    async restore_object(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.restore_object:', inspect(params));

        const md = await this.namespace_nb.read_object_md({
            bucket: params.bucket,
            key: params.key,
            version_id: params.version_id,
        }, object_sdk);
        this._populate_archive_fields(md);

        if (!s3_utils.GLACIER_STORAGE_CLASSES.includes(md.storage_class)) {
            throw new S3Error(S3Error.InvalidObjectStorageClass);
        }

        if (md.restore_status) {
            if (md.restore_status.ongoing) {
                throw new S3Error(S3Error.RestoreAlreadyInProgress);
            }

            if (md.restore_status.expiry_time && new Date(md.restore_status.expiry_time) > new Date()) {
                const new_expiry = new Date();
                new_expiry.setDate(new_expiry.getDate() + params.days);
                dbg.log0('NamespaceDeepArchive.restore_object: already restored, extending expiry to', new_expiry);

                await object_sdk.rpc_client.object.update_object_md({
                    bucket: params.bucket,
                    key: params.key,
                    xattr: {
                        [XATTR_RESTORE_ONGOING]: '',
                        [XATTR_RESTORE_EXPIRY]: new_expiry.toISOString(),
                    },
                });
                return { accepted: false };
            }
        }

        await object_sdk.rpc_client.object.update_object_md({
            bucket: params.bucket,
            key: params.key,
            xattr: {
                [XATTR_RESTORE_ONGOING]: 'true',
                [XATTR_RESTORE_EXPIRY]: '',
            },
        });

        try {
            await this.deep_archive_ns.s3.restoreObject({
                Bucket: this.deep_archive_ns.bucket,
                Key: params.key,
                VersionId: params.version_id,
                RestoreRequest: {
                    Days: params.days,
                },
            });
        } catch (err) {
            dbg.warn('NamespaceDeepArchive.restore_object: archive S3 RestoreObject call failed', err);
            throw err;
        }

        return { accepted: true };
    }


    //////////////////////////
    //  OBJECT ATTRIBUTES   //
    //////////////////////////

    /**
     * Returns object attributes from the namespace_nb metadata shadow,
     * with archive/restore fields populated.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<nb.ObjectInfo>}
     */
    async get_object_attributes(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.get_object_attributes:', inspect(params));
        const md = await this.namespace_nb.read_object_md(params, object_sdk);
        this._populate_archive_fields(md);
        return md;
    }


    ///////////////
    // INTERNALS //
    ///////////////

    /**
     * Reads xattr-encoded archive fields and populates storage_class and
     * restore_status on the object metadata in-place.
     *
     * @param {nb.ObjectInfo} md
     */
    _populate_archive_fields(md) {
        const xattr = md.xattr || {};

        if (xattr[XATTR_STORAGE_CLASS]) {
            md.storage_class = s3_utils.parse_storage_class(xattr[XATTR_STORAGE_CLASS]);
        }
        if (!md.storage_class) {
            md.storage_class = s3_utils.STORAGE_CLASS_DEEP_ARCHIVE;
        }

        const ongoing_val = xattr[XATTR_RESTORE_ONGOING];
        const expiry_val = xattr[XATTR_RESTORE_EXPIRY];

        if (ongoing_val || expiry_val) {
            const ongoing = ongoing_val === 'true';
            const expiry_time = expiry_val ? new Date(expiry_val) : undefined;
            const valid_expiry = (expiry_time && !isNaN(expiry_time.getTime())) ? expiry_time : undefined;

            let state = 'CAN_RESTORE';
            if (ongoing) {
                state = 'ONGOING';
            } else if (valid_expiry && valid_expiry > new Date()) {
                state = 'RESTORED';
            }

            md.restore_status = {
                state,
                ongoing,
                expiry_time: valid_expiry,
            };
        }
    }

    /**
     * Asserts that an archived object has been restored (ie. restore_status.expiry_time
     * is set and in the future). Throws InvalidObjectState if the object is still
     * archived or restore is ongoing. No-ops for non-glacier storage classes.
     *
     * @param {nb.ObjectInfo} object_md
     * @param {string} caller Name of the calling method (for log context)
     */
    _assert_restored(object_md, caller) {
        this._populate_archive_fields(object_md);

        if (!s3_utils.GLACIER_STORAGE_CLASSES.includes(object_md.storage_class)) return;

        if (object_md.restore_status?.ongoing) {
            dbg.warn(`NamespaceDeepArchive.${caller}: object restore is ongoing`, object_md.key);
            throw new S3Error(S3Error.InvalidObjectState);
        }

        if (!object_md.restore_status?.expiry_time) {
            dbg.warn(`NamespaceDeepArchive.${caller}: object is not restored`, object_md.key);
            throw new S3Error(S3Error.InvalidObjectState);
        }

        const expiry = new Date(object_md.restore_status.expiry_time);
        if (expiry <= new Date()) {
            dbg.warn(`NamespaceDeepArchive.${caller}: object restore has expired`, object_md.key, expiry);
            throw new S3Error(S3Error.InvalidObjectState);
        }
    }
}

/**
 * Inspect helper that omits streaming fields for safer debug logging.
 * @param {object} x
 * @returns {string}
 */
function inspect(x) {
    return util.inspect(_.omit(x, 'source_stream'), true, 5, true);
}

module.exports = NamespaceDeepArchive;
