/* Copyright (C) 2024 NooBaa */
'use strict';

const _ = require('lodash');
const util = require('util');

const dbg = require('../util/debug_module')(__filename);
const s3_utils = require('../endpoint/s3/s3_utils');
const S3Error = require('../endpoint/s3/s3_errors').S3Error;

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
 *     including restore_status and storage_class.
 *
 * All restore state is stored as first-class DB fields on the object record:
 *   - restore_status.ongoing  — true while restore is in progress
 *   - restore_status.expiry_time — Date when the temporary restored copy expires
 *   - storage_class — 'DEEP_ARCHIVE' or 'GLACIER'
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
     * Reads object metadata from namespace_nb.
     * Throws NO_SUCH_OBJECT for non-archived objects so NamespaceMultiStorageClass
     * falls through to the STANDARD namespace.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<nb.ObjectInfo>}
     */
    async read_object_md(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.read_object_md:', inspect(params));
        const md = await this.namespace_nb.read_object_md(params, object_sdk);
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
     * Uploads object data to deep_archive_ns and writes a metadata-only record to the NB DB.
     * Copy sources that are archived must already be restored.
     * Both writes must succeed — a failure on either side is propagated to the caller.
     *
     * The NB metadata record is created via create_object_upload + complete_object_upload RPC
     * (no data blocks stored in NB). This avoids reading the already-consumed source_stream a
     * second time and properly handles versioning via _put_object_handle_latest_with_retries.
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

        await this._write_nb_metadata(params, object_sdk, {
            storage_class,
            etag: archive_res.etag,
            size: params.size,
        });

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
     * Starts a multipart upload on BOTH deep_archive_ns (data) and NB (metadata).
     *
     * The two upload IDs are encoded into a single opaque string returned to the client.
     * All subsequent multipart operations decode this string to route each half correctly.
     *
     * Encoding: `{archive_upload_id}#{nb_obj_id}`
     *   - archive_upload_id : opaque S3 upload ID (no `#` in standard S3 upload IDs)
     *   - nb_obj_id         : 24-char hex MongoDB ObjectId (always fixed length)
     *
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async create_object_upload(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.create_object_upload:', inspect(params));
        const storage_class = s3_utils.parse_storage_class(params.storage_class) ||
            s3_utils.STORAGE_CLASS_DEEP_ARCHIVE;
        params.storage_class = storage_class;

        const [archive_res, nb_upload] = await Promise.all([
            this.deep_archive_ns.create_object_upload(params, object_sdk),
            object_sdk.rpc_client.object.create_object_upload({
                bucket: params.bucket,
                key: params.key,
                content_type: params.content_type,
                storage_class,
                xattr: params.xattr,
                tagging: params.tagging,
            }),
        ]);

        const encoded_upload_id = this._encode_upload_id(archive_res.obj_id, nb_upload.obj_id);
        return { ...archive_res, obj_id: encoded_upload_id };
    }

    /**
     * Uploads a multipart part to deep_archive_ns (data) and records its metadata in NB.
     *
     * NB stores part metadata (etag, size, num) via create_multipart + complete_multipart RPCs
     * so that list_multiparts can be served from NB. No actual data blocks are written to NB.
     *
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

        const { archive_upload_id, nb_obj_id } = this._decode_upload_id(params.obj_id);

        const archive_res = await this.deep_archive_ns.upload_multipart(
            { ...params, obj_id: archive_upload_id },
            object_sdk
        );

        if (nb_obj_id) {
            const { multipart_id } = await object_sdk.rpc_client.object.create_multipart({
                obj_id: nb_obj_id,
                bucket: params.bucket,
                key: params.key,
                num: params.num,
            });

            // Standard S3 part ETags are MD5 hex digests.
            // Convert to md5_b64 so that get_etag() and _complete_object_multiparts can
            // reconstruct the original hex etag without needing a separate schema field.
            const etag_hex = (archive_res.etag || '').replace(/"/g, '');
            const md5_b64 = Buffer.from(etag_hex, 'hex').toString('base64');

            await object_sdk.rpc_client.object.complete_multipart({
                obj_id: nb_obj_id,
                bucket: params.bucket,
                key: params.key,
                num: params.num,
                multipart_id,
                md5_b64,
                size: archive_res.size,
                num_parts: 1,
            });
        }

        return archive_res;
    }

    /**
     * Lists uploaded parts from NB (which is the source of truth for part metadata).
     * NB parts are recorded during upload_multipart via create_multipart + complete_multipart.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async list_multiparts(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.list_multiparts:', inspect(params));
        const { nb_obj_id } = this._decode_upload_id(params.obj_id);
        if (nb_obj_id) {
            return object_sdk.rpc_client.object.list_multiparts({
                obj_id: nb_obj_id,
                bucket: params.bucket,
                key: params.key,
                num_marker: params.num_marker,
                max: params.max,
            });
        }
        // Fallback for legacy uploads created before dual-tracking was introduced
        return this.deep_archive_ns.list_multiparts(
            { ...params, obj_id: this._decode_upload_id(params.obj_id).archive_upload_id },
            object_sdk
        );
    }

    /**
     * Completes the multipart upload:
     *   1. Complete data upload on deep_archive_ns.
     *   2. Write a final metadata-only object record to NB via _write_nb_metadata.
     *   3. Abort the in-progress NB tracking upload (nb_obj_id) to clean up part records.
     *
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async complete_object_upload(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.complete_object_upload:', inspect(params));

        const { archive_upload_id, nb_obj_id } = this._decode_upload_id(params.obj_id);

        const archive_res = await this.deep_archive_ns.complete_object_upload(
            { ...params, obj_id: archive_upload_id },
            object_sdk
        );

        const storage_class = s3_utils.parse_storage_class(params.storage_class) ||
            s3_utils.STORAGE_CLASS_DEEP_ARCHIVE;

        await this._write_nb_metadata(params, object_sdk, {
            storage_class,
            etag: archive_res.etag,
            // prefer size from archive response; fall back to params (e.g. for copy-part uploads)
            size: archive_res.size ?? params.size,
            num_parts: params.multiparts?.length,
        });

        // Clean up the in-progress NB tracking upload and its part records now that the
        // final object record has been committed above by _write_nb_metadata.
        if (nb_obj_id) {
            await object_sdk.rpc_client.object.abort_object_upload({
                obj_id: nb_obj_id,
                bucket: params.bucket,
                key: params.key,
            }).catch(err => dbg.warn('NamespaceDeepArchive.complete_object_upload: NB tracking upload cleanup failed', err));
        }

        return archive_res;
    }

    /**
     * Aborts the in-progress multipart upload on both deep_archive_ns and NB.
     * Aborting NB also removes all part records recorded during upload_multipart.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<object>}
     */
    async abort_object_upload(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.abort_object_upload:', inspect(params));
        const { archive_upload_id, nb_obj_id } = this._decode_upload_id(params.obj_id);

        await Promise.all([
            this.deep_archive_ns.abort_object_upload(
                { ...params, obj_id: archive_upload_id },
                object_sdk
            ),
            nb_obj_id && object_sdk.rpc_client.object.abort_object_upload({
                obj_id: nb_obj_id,
                bucket: params.bucket,
                key: params.key,
            }),
        ]);
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

        const md = await this.namespace_nb.read_object_md({
            bucket: params.bucket,
            key: params.key,
            version_id: params.version_id,
        }, object_sdk);

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
     * Updates restore_status on namespace_nb directly in the DB and issues
     * RestoreObject on deep_archive_ns.
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
                    version_id: params.version_id,
                    restore_status: {
                        ongoing: false,
                        expiry_time: new_expiry.getTime(),
                    },
                });
                return { accepted: false };
            }
        }

        await object_sdk.rpc_client.object.update_object_md({
            bucket: params.bucket,
            key: params.key,
            version_id: params.version_id,
            restore_status: {
                ongoing: true,
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
     * Returns object attributes from the namespace_nb metadata shadow.
     * @param {object} params
     * @param {nb.ObjectSDK} object_sdk
     * @returns {Promise<nb.ObjectInfo>}
     */
    async get_object_attributes(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.get_object_attributes:', inspect(params));
        return this.namespace_nb.read_object_md(params, object_sdk);
    }


    ///////////////
    // INTERNALS //
    ///////////////

    /**
     * Encodes the archive upload_id and the NB obj_id into a single opaque string.
     *
     * Format: `{archive_upload_id}#{nb_obj_id}`
     * - Standard S3 upload IDs (base64url) never contain `#`.
     * - NB obj_id is always a 24-char lowercase hex MongoDB ObjectId.
     *
     * @param {string} archive_upload_id
     * @param {string} nb_obj_id
     * @returns {string}
     */
    _encode_upload_id(archive_upload_id, nb_obj_id) {
        return `${archive_upload_id}#${nb_obj_id}`;
    }

    /**
     * Decodes an upload_id previously produced by {@link _encode_upload_id}.
     * If the string does not contain the expected suffix pattern (backward-compat for any
     * legacy uploads created before dual-tracking), the entire string is treated as the
     * archive_upload_id and nb_obj_id is returned as null.
     *
     * @param {string} encoded_upload_id
     * @returns {{ archive_upload_id: string, nb_obj_id: string | null }}
     */
    _decode_upload_id(encoded_upload_id) {
        // nb_obj_id is always exactly 24 lowercase hex chars (MongoDB ObjectId)
        const match = encoded_upload_id.match(/^(.*?)#([0-9a-f]{24})$/);
        if (!match) {
            return { archive_upload_id: encoded_upload_id, nb_obj_id: null };
        }
        return { archive_upload_id: match[1], nb_obj_id: match[2] };
    }

    /**
     * Writes a metadata-only object record to the NB DB after data has been written to the archive.
     *
     * We cannot reuse namespace_nb.upload_object or namespace_nb.complete_object_upload here because:
     *   - upload_object: the source_stream is already consumed by deep_archive_ns; reading it again
     *     would produce a 0-byte object in NB.
     *   - complete_object_upload: there is no matching NB multipart upload (the upload was created
     *     only on deep_archive_ns), so the RPC lookup would fail.
     *
     * Instead we drive the lightweight create → complete RPC pair directly, passing the etag and
     * size returned by the archive. This creates a metadata-only record with no data blocks in NB
     * and goes through _put_object_handle_latest_with_retries for correct versioning behavior.
     *
     * @param {object} params Original operation params (bucket, key, content_type, xattr, tagging, …)
     * @param {nb.ObjectSDK} object_sdk
     * @param {{ storage_class: string, etag: string, size: number, num_parts?: number }} meta Metadata from archive response
     */
    async _write_nb_metadata(params, object_sdk, { storage_class, etag, size, num_parts }) {
        const nb_upload = await object_sdk.rpc_client.object.create_object_upload({
            bucket: params.bucket,
            key: params.key,
            content_type: params.content_type,
            content_encoding: params.content_encoding,
            storage_class,
            xattr: params.xattr,
            tagging: params.tagging,
        });

        await object_sdk.rpc_client.object.complete_object_upload({
            obj_id: nb_upload.obj_id,
            bucket: params.bucket,
            key: params.key,
            etag,
            size,
            // num_parts lets HeadObject return x-amz-mp-parts-count correctly.
            // For multipart uploads the caller passes params.multiparts.length;
            // for simple PutObject it is left undefined (defaults to 0 on the server).
            num_parts,
        });
    }

    /**
     * Asserts that an archived object has been restored (i.e. restore_status.expiry_time
     * is set and in the future). Throws InvalidObjectState if the object is still
     * archived or restore is ongoing. No-ops for non-glacier storage classes.
     *
     * @param {nb.ObjectInfo} object_md
     * @param {string} caller Name of the calling method (for log context)
     */
    _assert_restored(object_md, caller) {
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
