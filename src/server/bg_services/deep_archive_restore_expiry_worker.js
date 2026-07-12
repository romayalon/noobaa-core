/* Copyright (C) 2026 NooBaa */
'use strict';

const config = require('../../../config');
const dbg = require('../../util/debug_module')(__filename);
const P = require('../../util/promise');
const system_store = require('../system_services/system_store').get_instance();
const system_utils = require('../utils/system_utils');
const { MDStore } = require('../object_services/md_store');
const s3_utils = require('../../endpoint/s3/s3_utils');
const deep_archive_utils = require('../../sdk/deep_archive_utils');

const {
    clear_restore_xattr_patch,
    merge_db_xattr,
} = deep_archive_utils;

/**
 * Background worker that expires temporary Deep Archive restores:
 * clears restore xattrs on the archive metadata object and marks the
 * restored_objects/<key> copy with data_expired for ObjectsReclaimer.
 */
class DeepArchiveRestoreExpiryWorker {

    /**
     * @param {{ name: string; client: nb.APIClient }} params
     */
    constructor({ name, client }) {
        this.name = name;
        this.client = client;
    }

    async run_batch() {
        if (!this._can_run()) return;

        const now = new Date();
        const objects = await MDStore.instance().find_objects_with_restore_expired(
            now,
            config.DEEP_ARCHIVE_RESTORE_EXPIRY_BATCH_SIZE
        );
        if (!objects || !objects.length) {
            dbg.log0('deep_archive_restore_expiry_worker: no expired restores');
            return config.DEEP_ARCHIVE_RESTORE_EXPIRY_EMPTY_DELAY;
        }

        let has_errors = false;
        dbg.log0('deep_archive_restore_expiry_worker: processing', objects.map(o => o.key).join(', '));

        await P.map(objects, async obj => {
            try {
                await this._expire_object(obj, now);
            } catch (err) {
                has_errors = true;
                dbg.error('deep_archive_restore_expiry_worker: failed for object', obj.key, err);
            }
        });

        if (has_errors) return config.DEEP_ARCHIVE_RESTORE_EXPIRY_ERROR_DELAY;
        return config.DEEP_ARCHIVE_RESTORE_EXPIRY_BATCH_DELAY;
    }

    _can_run() {
        if (!system_store.is_finished_initial_load) {
            dbg.log0('deep_archive_restore_expiry_worker: system_store did not finish initial load');
            return false;
        }
        const system = system_store.data.systems[0];
        if (!system || system_utils.system_in_maintenance(system._id)) return false;
        return true;
    }

    /**
     * @param {nb.ObjectMD} obj Archive metadata object with expired restore
     * @param {Date} now
     */
    async _expire_object(obj, now) {
        // Clear restore status first so GET/HEAD stop serving the temporary copy.
        await MDStore.instance().update_object_by_id(obj._id, {
            xattr: merge_db_xattr(obj.xattr, clear_restore_xattr_patch()),
        });

        const restored_key = s3_utils.RESTORED_OBJECTS_DIR + obj.key;
        const restored_obj = await MDStore.instance().find_object_latest(obj.bucket, restored_key);
        if (!restored_obj) {
            dbg.warn('deep_archive_restore_expiry_worker: restored copy not found', restored_key);
            return;
        }

        await MDStore.instance().update_object_by_id(restored_obj._id, {
            data_expired: now,
        });
        dbg.log0('deep_archive_restore_expiry_worker: marked data_expired for', restored_key);
    }
}

exports.DeepArchiveRestoreExpiryWorker = DeepArchiveRestoreExpiryWorker;
