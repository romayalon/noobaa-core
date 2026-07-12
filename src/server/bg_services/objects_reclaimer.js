/* Copyright (C) 2016 NooBaa */
'use strict';

const config = require('../../../config');
const dbg = require('../../util/debug_module')(__filename);
const MDStore = require('../object_services/md_store').MDStore;
const system_store = require('../system_services/system_store').get_instance();
const system_utils = require('../utils/system_utils');
const map_deleter = require('../object_services/map_deleter');
const P = require('../../util/promise');
const s3_utils = require('../../endpoint/s3/s3_utils');
const deep_archive_utils = require('../../sdk/deep_archive_utils');

const {
    clear_restore_xattr_patch,
    merge_db_xattr,
} = deep_archive_utils;

class ObjectsReclaimer {

    constructor({ name, client }) {
        this.name = name;
        this.client = client;
    }

    async run_batch() {
        if (!this._can_run()) return;

        const unreclaimed_objects = await MDStore.instance().find_unreclaimed_objects(config.OBJECT_RECLAIMER_BATCH_SIZE);
        const data_expired_objects = await MDStore.instance().find_objects_with_data_expired(config.OBJECT_RECLAIMER_BATCH_SIZE);

        const by_id = new Map();
        for (const obj of unreclaimed_objects || []) {
            by_id.set(String(obj._id), obj);
        }
        for (const obj of data_expired_objects || []) {
            by_id.set(String(obj._id), obj);
        }
        const objects = [...by_id.values()];

        if (!objects.length) {
            dbg.log0('no objects in "unreclaimed" or data_expired state. nothing to do');
            return config.OBJECT_RECLAIMER_EMPTY_DELAY;
        }

        let has_errors = false;
        dbg.log0('object_reclaimer: starting batch work on objects: ', objects.map(o => o.key).join(', '));
        const reclaimed_objects_ids = [];
        await P.all(objects.map(async obj => {
            try {
                await map_deleter.delete_object_mappings(obj);
                if (obj.key && obj.key.startsWith(s3_utils.RESTORED_OBJECTS_DIR)) {
                    await this._reset_parent_restore_status(obj);
                }
                reclaimed_objects_ids.push(obj._id);
            } catch (err) {
                dbg.error(`got error when trying to delete object ${obj.key} mappings :`, err);
                has_errors = true;
            }
        }));
        await MDStore.instance().update_objects_by_ids(reclaimed_objects_ids, { reclaimed: new Date() });

        if (has_errors) {
            return config.OBJECT_RECLAIMER_ERROR_DELAY;
        }
        return config.OBJECT_RECLAIMER_BATCH_DELAY;

    }

    /**
     * Idempotently clear restore xattrs on the archive metadata object for a
     * restored_objects/<key> temporary copy (safety if expiry worker already cleared them).
     * @param {nb.ObjectMD} restored_obj
     */
    async _reset_parent_restore_status(restored_obj) {
        const parent_key = restored_obj.key.slice(s3_utils.RESTORED_OBJECTS_DIR.length);
        if (!parent_key) return;
        const parent = await MDStore.instance().find_object_latest(restored_obj.bucket, parent_key);
        if (!parent) return;
        await MDStore.instance().update_object_by_id(parent._id, {
            xattr: merge_db_xattr(parent.xattr, clear_restore_xattr_patch()),
        });
    }

    _can_run() {
        if (!system_store.is_finished_initial_load) {
            dbg.log0('ObjectsReclaimer: system_store did not finish initial load');
            return false;
        }

        const system = system_store.data.systems[0];
        if (!system || system_utils.system_in_maintenance(system._id)) return false;

        return true;
    }

}


exports.ObjectsReclaimer = ObjectsReclaimer;
