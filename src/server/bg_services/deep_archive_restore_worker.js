/* Copyright (C) 2026 NooBaa */
'use strict';

const config = require('../../../config');
const dbg = require('../../util/debug_module')(__filename);
const P = require('../../util/promise');
const system_store = require('../system_services/system_store').get_instance();
const system_utils = require('../utils/system_utils');
const auth_server = require('../common_services/auth_server');
const pool_server = require('../system_services/pool_server');
const { MDStore } = require('../object_services/md_store');
const ObjectIO = require('../../sdk/object_io');
const ObjectSDK = require('../../sdk/object_sdk');
const NamespaceNB = require('../../sdk/namespace_nb');
const NamespaceS3 = require('../../sdk/namespace_s3');
const noobaa_s3_client = require('../../sdk/noobaa_s3_client/noobaa_s3_client');
const s3_utils = require('../../endpoint/s3/s3_utils');
const deep_archive_utils = require('../../sdk/deep_archive_utils');

const {
    XATTR_RESTORE_DAYS_DB,
    parse_s3_restore_header,
    finalize_restore_xattr_patch,
    merge_db_xattr,
} = deep_archive_utils;

/**
 * Background worker that finalizes async Deep Archive restores:
 * polls objects with restore.ongoing=true, copies data from the archive
 * namespace into restored_objects/<key> on NamespaceNB, then sets expiry.
 */
class DeepArchiveRestoreWorker {

    /**
     * @param {{ name: string; client: nb.APIClient }} params
     */
    constructor({ name, client }) {
        this.name = name;
        this.client = client;
        this.object_io = new ObjectIO();
    }

    async run_batch() {
        if (!this._can_run()) return;

        const objects = await MDStore.instance().find_objects_with_restore_ongoing(
            config.DEEP_ARCHIVE_RESTORE_BATCH_SIZE
        );
        if (!objects || !objects.length) {
            dbg.log0('deep_archive_restore_worker: no ongoing restores');
            return config.DEEP_ARCHIVE_RESTORE_EMPTY_DELAY;
        }

        let has_errors = false;
        let did_work = false;
        dbg.log0('deep_archive_restore_worker: processing', objects.map(o => o.key).join(', '));

        await P.map(objects, async obj => {
            try {
                const finished = await this._process_object(obj);
                if (finished) did_work = true;
            } catch (err) {
                has_errors = true;
                dbg.error('deep_archive_restore_worker: failed for object', obj.key, err);
            }
        });

        if (has_errors) return config.DEEP_ARCHIVE_RESTORE_ERROR_DELAY;
        if (did_work) return config.DEEP_ARCHIVE_RESTORE_BATCH_DELAY;
        return config.DEEP_ARCHIVE_RESTORE_BUSY_DELAY;
    }

    _can_run() {
        if (!system_store.is_finished_initial_load) {
            dbg.log0('deep_archive_restore_worker: system_store did not finish initial load');
            return false;
        }
        const system = system_store.data.systems[0];
        if (!system || system_utils.system_in_maintenance(system._id)) return false;
        return true;
    }

    /**
     * @param {nb.ObjectMD} obj
     * @returns {Promise<boolean>} true if restore was finalized
     */
    async _process_object(obj) {
        const bucket = system_store.data.get_by_id(obj.bucket);
        if (!bucket || !bucket.archive_policy?.deep_archive_resource) {
            dbg.warn('deep_archive_restore_worker: bucket missing archive_policy', obj.bucket, obj.key);
            return false;
        }

        const archive_ns = this._create_archive_namespace(bucket);
        if (!archive_ns) return false;

        const object_sdk = this._create_object_sdk();
        const bucket_name = bucket.name.unwrap();

        let head;
        try {
            head = await archive_ns.s3.headObject({
                Bucket: archive_ns.bucket,
                Key: obj.key,
            });
        } catch (err) {
            // Still archived / not restorable yet — retry next cycle
            const code = err.code || err.Code || err.name;
            if (code === 'InvalidObjectState' || err.$metadata?.httpStatusCode === 403) {
                dbg.log1('deep_archive_restore_worker: archive not ready yet', obj.key, code);
                return false;
            }
            throw err;
        }

        const restore_status = parse_s3_restore_header(head.Restore);
        if (!restore_status.ready) {
            dbg.log1('deep_archive_restore_worker: restore still ongoing on archive', obj.key, head.Restore);
            return false;
        }

        const days = Number(obj.xattr?.[XATTR_RESTORE_DAYS_DB]);
        if (!days || Number.isNaN(days) || days < 1) {
            throw new Error(`missing or invalid restore days xattr for ${obj.key}`);
        }

        const source_stream = await archive_ns.read_object_stream({
            bucket: bucket_name,
            key: obj.key,
            object_md: {
                key: obj.key,
                size: head.ContentLength ?? obj.size,
                etag: s3_utils.parse_etag(head.ETag) || obj.etag,
                content_type: head.ContentType || obj.content_type,
            },
        }, object_sdk);

        const namespace_nb = new NamespaceNB();
        await namespace_nb.upload_object({
            bucket: bucket_name,
            key: s3_utils.RESTORED_OBJECTS_DIR + obj.key,
            size: head.ContentLength ?? obj.size,
            content_type: head.ContentType || obj.content_type || 'application/octet-stream',
            source_stream,
            xattr: {},
        }, object_sdk);

        await MDStore.instance().update_object_by_id(obj._id, {
            xattr: merge_db_xattr(obj.xattr, finalize_restore_xattr_patch(days)),
        });

        dbg.log0('deep_archive_restore_worker: finalized restore for', obj.key);
        return true;
    }

    /**
     * @param {nb.Bucket} bucket
     * @returns {(nb.Namespace & { s3: any, bucket: string }) | null}
     */
    _create_archive_namespace(bucket) {
        const resource_ref = bucket.archive_policy.deep_archive_resource.resource;
        const nsr = typeof resource_ref === 'object' && resource_ref.connection ?
            resource_ref :
            system_store.data.get_by_id(resource_ref);
        if (!nsr) {
            dbg.warn('deep_archive_restore_worker: namespace resource not found', resource_ref);
            return null;
        }
        const r = pool_server.get_namespace_resource_extended_info(nsr);
        return new NamespaceS3({
            namespace_resource_id: r.id,
            s3_params: {
                endpoint: r.endpoint,
                aws_sts_arn: r.aws_sts_arn,
                credentials: {
                    accessKeyId: r.access_key.unwrap(),
                    secretAccessKey: r.secret_key.unwrap(),
                },
                region: r.region || config.DEFAULT_REGION,
                forcePathStyle: true,
                requestHandler: noobaa_s3_client.get_requestHandler_with_suitable_agent(r.endpoint),
                requestChecksumCalculation: 'WHEN_REQUIRED',
                access_mode: r.access_mode,
            },
            bucket: r.target_bucket,
        });
    }

    _create_object_sdk() {
        const system = system_store.data.systems[0];
        const object_sdk = new ObjectSDK({
            rpc_client: this.client,
            internal_rpc_client: this.client,
            object_io: this.object_io,
        });
        object_sdk.set_auth_token(auth_server.make_auth_token({
            system_id: system._id,
            account_id: system.owner._id,
            role: 'admin',
        }));
        return object_sdk;
    }
}

exports.DeepArchiveRestoreWorker = DeepArchiveRestoreWorker;
