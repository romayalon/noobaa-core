/* Copyright (C) 2024 NooBaa */
'use strict';

const dbg = require('../util/debug_module')(__filename);
const s3_utils = require('../endpoint/s3/s3_utils');
const S3Error = require('../endpoint/s3/s3_errors').S3Error;

/** Prefix for temporary restored copies of deep-archive objects (internal to NooBaa). */
const RESTORED_OBJECTS_DIR = 'restored_objects/';

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
function encode_upload_id(archive_upload_id, nb_obj_id) {
    return `${archive_upload_id}#${nb_obj_id}`;
}

/**
 * Decodes an upload_id previously produced by {@link encode_upload_id}.
 * If the string does not contain the expected suffix pattern (backward-compat),
 * the entire string is treated as the archive_upload_id and nb_obj_id is null.
 *
 * @param {string} encoded_upload_id
 * @returns {{ archive_upload_id: string, nb_obj_id: string | null }}
 */
function decode_upload_id(encoded_upload_id) {
    // nb_obj_id is always exactly 24 lowercase hex chars (MongoDB ObjectId)
    const match = encoded_upload_id.match(/^(.*?)#([0-9a-f]{24})$/);
    if (!match) {
        return { archive_upload_id: encoded_upload_id, nb_obj_id: null };
    }
    return { archive_upload_id: match[1], nb_obj_id: match[2] };
}

/**
 * Writes a metadata-only object record to the NB DB after data has been written
 * to the archive, using the create_object_upload → complete_object_upload RPC pair.
 *
 * We cannot reuse namespace_nb.upload_object here because the source_stream is already
 * consumed by the archive namespace; reading it again would produce a 0-byte NB object.
 *
 * @param {object} params Original operation params (bucket, key, content_type, xattr, tagging, …)
 * @param {nb.ObjectSDK} object_sdk
 * @param {{ storage_class: string, etag: string, size: number, num_parts?: number, last_modified_time?: Date }} meta
 */
async function write_nb_metadata(params, object_sdk, { storage_class, etag, size, num_parts, last_modified_time }) {
    const nb_upload = await object_sdk.rpc_client.object.create_object_upload({
        bucket: params.bucket,
        key: params.key,
        content_type: params.content_type,
        content_encoding: params.content_encoding,
        storage_class,
        xattr: params.xattr,
        tagging: params.tagging,
        encryption: params.encryption,
        lock_settings: params.lock_settings,
    });

    try {
        await object_sdk.rpc_client.object.complete_object_upload({
            obj_id: nb_upload.obj_id,
            bucket: params.bucket,
            key: params.key,
            etag,
            size,
            md5_b64: params.md5_b64,
            sha256_b64: params.sha256_b64,
            last_modified_time,
            // num_parts lets HeadObject return x-amz-mp-parts-count correctly.
            num_parts,
        });
    } catch (err) {
        dbg.warn('deep_archive_utils.write_nb_metadata: complete_object_upload failed, aborting NB upload', err);
        await object_sdk.rpc_client.object.abort_object_upload({
            obj_id: nb_upload.obj_id,
            bucket: params.bucket,
            key: params.key,
        }).catch(err2 => dbg.warn('deep_archive_utils.write_nb_metadata: abort_object_upload also failed', err2));
        throw err;
    }
}

/**
 * Asserts that an archived object has been restored (restore_status.expiry_time is set
 * and in the future). Throws InvalidObjectState otherwise.
 * No-op for non-glacier storage classes.
 *
 * @param {nb.ObjectInfo} object_md
 * @param {string} caller Name of the calling method (for log context)
 */
function assert_restored(object_md, caller) {
    if (!s3_utils.GLACIER_STORAGE_CLASSES.includes(object_md.storage_class)) return;

    if (object_md.restore_status?.ongoing) {
        dbg.warn(`deep_archive_utils.assert_restored (${caller}): object restore is ongoing`, object_md.key);
        throw new S3Error(S3Error.InvalidObjectState);
    }

    if (!object_md.restore_status?.expiry_time) {
        dbg.warn(`deep_archive_utils.assert_restored (${caller}): object is not restored`, object_md.key);
        throw new S3Error(S3Error.InvalidObjectState);
    }

    const expiry = new Date(object_md.restore_status.expiry_time);
    if (expiry <= new Date()) {
        dbg.warn(`deep_archive_utils.assert_restored (${caller}): object restore has expired`, object_md.key, expiry);
        throw new S3Error(S3Error.InvalidObjectState);
    }
}

exports.RESTORED_OBJECTS_DIR = RESTORED_OBJECTS_DIR;
exports.encode_upload_id = encode_upload_id;
exports.decode_upload_id = decode_upload_id;
exports.write_nb_metadata = write_nb_metadata;
exports.assert_restored = assert_restored;
