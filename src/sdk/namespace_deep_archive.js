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
 * NamespaceDeepArchive is a composite namespace for buckets with an archive_policy.
 *
 * It manages two inner namespaces:
 *   - archive_ns (NamespaceS3) — the remote IBM Deep Archive / S3-compatible endpoint
 *     where object data is stored with DEEP_ARCHIVE storage class.
 *   - namespace_nb (NamespaceNB) — NooBaa's internal metadata DB for object metadata,
 *     including restore_status and storage_class.
 *
 * Restore status is persisted via object xattr metadata keys:
 *   - noobaa-deep-archive.restore.ongoing — "true" while restore is in progress
 *   - noobaa-deep-archive.restore.expiry — ISO date string of restore expiry
 *   - noobaa-deep-archive.storage-class — the storage class (DEEP_ARCHIVE)
 *
 * Data flow:
 *   - PutObject / CompleteMultipartUpload: metadata → NB, data → archive S3
 *   - HeadObject / ListObjects / GetObjectAttributes: served from NB metadata
 *   - GetObject: if not restored → InvalidObjectState; else read from archive S3
 *   - RestoreObject: update NB restore_status + call S3 RestoreObject on archive
 *   - DeleteObject: delete from NB + delete from archive S3
 *   - CopyObject (source archived): if not restored → InvalidObjectState; else copy via archive S3
 *
 * @implements {nb.Namespace}
 */
class NamespaceDeepArchive {

    /**
     * @param {{
     *      archive_ns: nb.Namespace & { s3: any, bucket: string },
     *      namespace_nb: nb.Namespace,
     *      stats: import('./endpoint_stats_collector').EndpointStatsCollector,
     * }} args
     */
    constructor({ archive_ns, namespace_nb, stats }) {
        this.archive_ns = archive_ns;
        this.namespace_nb = namespace_nb;
        this.stats = stats;
    }

    get_write_resource() {
        return this;
    }

    is_server_side_copy(other, other_md, params) {
        return other instanceof NamespaceDeepArchive &&
            this.archive_ns.is_server_side_copy(other.archive_ns, other_md, params);
    }

    get_bucket() {
        return this.archive_ns.get_bucket();
    }

    is_readonly_namespace() {
        return this.archive_ns.is_readonly_namespace();
    }


    /////////////////
    // OBJECT LIST //
    /////////////////

    async list_objects(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.list_objects:', inspect(params));
        return this.namespace_nb.list_objects(params, object_sdk);
    }

    async list_uploads(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.list_uploads:', inspect(params));
        return this.namespace_nb.list_uploads(params, object_sdk);
    }

    async list_object_versions(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.list_object_versions:', inspect(params));
        return this.namespace_nb.list_object_versions(params, object_sdk);
    }


    /////////////////
    // OBJECT READ //
    /////////////////

    async read_object_md(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.read_object_md:', inspect(params));
        const md = await this.namespace_nb.read_object_md(params, object_sdk);
        this._populate_archive_fields(md);
        return md;
    }

    async read_object_stream(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.read_object_stream:', inspect(params));
        const object_md = params.object_md || await this.namespace_nb.read_object_md(params, object_sdk);
        this._assert_restored(object_md, 'read_object_stream');
        return this.archive_ns.read_object_stream(params, object_sdk);
    }


    ///////////////////
    // OBJECT UPLOAD //
    ///////////////////

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

        params.storage_class = params.storage_class || s3_utils.STORAGE_CLASS_DEEP_ARCHIVE;

        const archive_res = await this.archive_ns.upload_object(params, object_sdk);

        const nb_params = _.defaults({
            storage_class: s3_utils.STORAGE_CLASS_DEEP_ARCHIVE,
            xattr: _.defaults({
                [XATTR_STORAGE_CLASS]: s3_utils.STORAGE_CLASS_DEEP_ARCHIVE,
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

    async upload_blob_block(params, object_sdk) {
        throw new S3Error(S3Error.NotImplemented);
    }

    async commit_blob_block_list(params, object_sdk) {
        throw new S3Error(S3Error.NotImplemented);
    }

    async get_blob_block_lists(params, object_sdk) {
        throw new S3Error(S3Error.NotImplemented);
    }


    /////////////////////////////
    // OBJECT MULTIPART UPLOAD //
    /////////////////////////////

    async create_object_upload(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.create_object_upload:', inspect(params));
        params.storage_class = params.storage_class || s3_utils.STORAGE_CLASS_DEEP_ARCHIVE;
        return this.archive_ns.create_object_upload(params, object_sdk);
    }

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
        return this.archive_ns.upload_multipart(params, object_sdk);
    }

    async list_multiparts(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.list_multiparts:', inspect(params));
        return this.archive_ns.list_multiparts(params, object_sdk);
    }

    async complete_object_upload(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.complete_object_upload:', inspect(params));
        const archive_res = await this.archive_ns.complete_object_upload(params, object_sdk);

        const nb_params = _.defaults({
            storage_class: s3_utils.STORAGE_CLASS_DEEP_ARCHIVE,
            xattr: _.defaults({
                [XATTR_STORAGE_CLASS]: s3_utils.STORAGE_CLASS_DEEP_ARCHIVE,
            }, params.xattr),
        }, params);
        try {
            await this.namespace_nb.complete_object_upload(nb_params, object_sdk);
        } catch (err) {
            dbg.warn('NamespaceDeepArchive.complete_object_upload: NB metadata write failed, archive data may be orphaned', err);
        }

        return archive_res;
    }

    async abort_object_upload(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.abort_object_upload:', inspect(params));
        return this.archive_ns.abort_object_upload(params, object_sdk);
    }


    ////////////////////
    // OBJECT TAGGING //
    ////////////////////

    async put_object_tagging(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.put_object_tagging:', inspect(params));
        return this.namespace_nb.put_object_tagging(params, object_sdk);
    }

    async delete_object_tagging(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.delete_object_tagging:', inspect(params));
        return this.namespace_nb.delete_object_tagging(params, object_sdk);
    }

    async get_object_tagging(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.get_object_tagging:', inspect(params));
        return this.namespace_nb.get_object_tagging(params, object_sdk);
    }

    //////////
    // ACLs //
    //////////

    async get_object_acl(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.get_object_acl:', inspect(params));
        return this.namespace_nb.get_object_acl(params, object_sdk);
    }

    async put_object_acl(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.put_object_acl:', inspect(params));
        return this.namespace_nb.put_object_acl(params, object_sdk);
    }

    ///////////////////
    // OBJECT DELETE //
    ///////////////////

    async delete_object(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.delete_object:', inspect(params));

        const nb_res = await this.namespace_nb.delete_object(params, object_sdk);

        try {
            await this.archive_ns.delete_object(params, object_sdk);
        } catch (err) {
            dbg.warn('NamespaceDeepArchive.delete_object: archive S3 delete failed, may leave orphaned data', err);
        }

        return nb_res;
    }

    async delete_multiple_objects(params, object_sdk) {
        dbg.log0('NamespaceDeepArchive.delete_multiple_objects:', inspect(params));

        const nb_res = await this.namespace_nb.delete_multiple_objects(params, object_sdk);

        try {
            await this.archive_ns.delete_multiple_objects(params, object_sdk);
        } catch (err) {
            dbg.warn('NamespaceDeepArchive.delete_multiple_objects: archive S3 delete failed, may leave orphaned data', err);
        }

        return nb_res;
    }


    ///////////////////
    //  OBJECT LOCK  //
    ///////////////////

    async get_object_legal_hold(params, object_sdk) {
        return this.namespace_nb.get_object_legal_hold(params, object_sdk);
    }

    async put_object_legal_hold(params, object_sdk) {
        return this.namespace_nb.put_object_legal_hold(params, object_sdk);
    }

    async get_object_retention(params, object_sdk) {
        return this.namespace_nb.get_object_retention(params, object_sdk);
    }

    async put_object_retention(params, object_sdk) {
        return this.namespace_nb.put_object_retention(params, object_sdk);
    }

    ///////////////////
    //      ULS      //
    ///////////////////

    async create_uls() {
        throw new Error('TODO');
    }
    async delete_uls() {
        throw new Error('TODO');
    }


    ////////////////////
    // OBJECT RESTORE //
    ////////////////////

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
            await this.archive_ns.s3.restoreObject({
                Bucket: this.archive_ns.bucket,
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
     * restore_status on the object metadata.
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
     * archived or restore is ongoing.
     *
     * @param {nb.ObjectInfo} object_md
     * @param {string} caller
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

function inspect(x) {
    return util.inspect(_.omit(x, 'source_stream'), true, 5, true);
}

module.exports = NamespaceDeepArchive;
