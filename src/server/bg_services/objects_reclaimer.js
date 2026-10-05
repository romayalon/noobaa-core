/* Copyright (C) 2016 NooBaa */
'use strict';

const config = require('../../../config');
const dbg = require('../../util/debug_module')(__filename);
const db_client = require('../../util/db_client');
const message_queue_client = require('../../util/message_queue_client');
const MDStore = require('../object_services/md_store').MDStore;
const CONSTANTS = require('../../common/constants');
const system_store = require('../system_services/system_store').get_instance();
const system_utils = require('../utils/system_utils');
const map_deleter = require('../object_services/map_deleter');
const auth_server = require('../common_services/auth_server');
const deep_archive_utils = require('../../util/deep_archive_utils');
const P = require('../../util/promise');

class ObjectsReclaimer {

    /**
     * @param {{ name: string, client: nb.APIClient, message_queue?: nb.MessageQueueClient | null }} params
     */
    constructor({ name, client, message_queue = null }) {
        this.name = name;
        this.client = client;
        this.message_queue = message_queue;
    }

    /**
     * Scans reclaim work into the message queue, then processes one message.
     * Other workers can dequeue the remaining messages and scale the drain.
     * @returns {Promise<number | undefined>}
     */
    async run_batch() {
        if (!this._can_run()) return;

        const queue = this._queue();
        await queue.connect();
        const enqueued = await this._enqueue_reclaim_batches(queue);
        const consumed = await this._consume_reclaim_batch(queue);
        const pending = await queue.size(config.OBJECT_RECLAIMER_QUEUE_NAME);
        return this._next_delay([
            enqueued,
            consumed,
            { had_work: pending > 0, had_errors: false },
        ]);
    }

    /**
     * Reclaim unreclaimed deleted objects:
     * - STANDARD / tiering: map_deleter, then mark reclaimed.
     * - Incomplete glacier or deep-archive multipart upload (in-progress upload
     *   with an archive upload id): mapping/multipart cleanup, then abort the
     *   archive multipart. If abort fails, the object stays unreclaimed for retry.
     * - Remote archive with restore_status: full map_deleter (local restore copy),
     *   then delete remote keys and mark reclaimed.
     * - Remote archive with unreclaimed transition source: full map_deleter
     *   (local source copy), then delete remote keys and mark reclaimed.
     * - Remote archive without a local copy but with target data
     *   (archive upload id): soft-delete leftover multiparts only, then
     *   delete remote keys and mark reclaimed.
     *   Mapping cleanup must succeed before enqueue so we do not mark reclaimed
     *   while local copies may still remain.
     * @param {nb.ObjectMD[]} [objects] When set, reclaim these objects instead of scanning.
     * @returns {Promise<{ had_work: boolean, had_errors: boolean }>}
     */
    async reclaim_deleted_objects(objects) {
        const unreclaimed_objects = Array.isArray(objects) ?
            objects.filter(is_deleted_unreclaimed) :
            await MDStore.instance().find_unreclaimed_objects(config.OBJECT_RECLAIMER_BATCH_SIZE);
        if (!unreclaimed_objects || !unreclaimed_objects.length) {
            dbg.log0('no objects in "unreclaimed" state. nothing to do');
            return { had_work: false, had_errors: false };
        }

        let had_errors = false;
        dbg.log0('object_reclaimer: starting batch work on objects: ', unreclaimed_objects.map(o => o.key).join(', '));
        const reclaimed_objects_ids = [];
        /** @type {object[]} */
        const pending_archive_aborts = [];
        /** @type {{ [bucket_id: string]: object[] }} */
        const pending_archive_deletes_by_bucket = {};

        await P.all(unreclaimed_objects.map(async obj => {
            try {
                const bucket = system_store.data.get_by_id(obj.bucket);
                const is_remote_data = deep_archive_utils.is_remote_archive_object(obj, bucket);
                const has_local_copy = Boolean(obj.restore_status) || deep_archive_utils.is_transition_source_pending_purge(obj);
                const should_delete_mappings = is_remote_data ? has_local_copy : true;
                const is_md_only_multipart_upload = Boolean(obj.target_data_info?.upload_id);
                const should_delete_md_only_multiparts = is_remote_data && !should_delete_mappings && is_md_only_multipart_upload;
                if (should_delete_mappings) {
                    await map_deleter.delete_object_mappings(obj);
                } else if (should_delete_md_only_multiparts) {
                    await map_deleter.delete_object_multiparts(obj);
                }
                if (is_remote_data) {
                    if (obj.upload_started && is_md_only_multipart_upload) {
                        pending_archive_aborts.push(obj);
                        return;
                    }
                    const bucket_id = String(obj.bucket);
                    (pending_archive_deletes_by_bucket[bucket_id] ??= []).push(obj);
                    return;
                }
                reclaimed_objects_ids.push(obj._id);
            } catch (err) {
                dbg.error(`got error when trying to delete object ${obj.key} mappings :`, err);
                had_errors = true;
            }
        }));

        if (pending_archive_aborts.length) {
            const abort_result = await this._abort_archive_multiparts(pending_archive_aborts);
            reclaimed_objects_ids.push(...abort_result.reclaimed_ids);
            had_errors = had_errors || abort_result.had_errors;
        }

        if (Object.keys(pending_archive_deletes_by_bucket).length) {
            const archive_result = await this._delete_archived_objects(pending_archive_deletes_by_bucket);
            reclaimed_objects_ids.push(...archive_result.reclaimed_ids);
            had_errors = had_errors || archive_result.had_errors;
        }

        await MDStore.instance().update_objects_by_ids(reclaimed_objects_ids, { reclaimed: new Date() });

        return { had_work: true, had_errors };
    }

    /**
     * Purge STANDARD restore copies past restore_status.expiry_time and clear
     * restore_status. Does not soft-delete the object or delete archive keys.
     *
     * Delete mappings first so a failed cleanup stays retryable (restore_status
     * still expired). RestoreObject is blocked while expired restore_status
     * remains (see update_object_md), so unset after delete is safe.
     * @param {nb.ObjectMD[]} [objects] When set, reclaim these objects instead of scanning.
     * @returns {Promise<{ had_work: boolean, had_errors: boolean }>}
     */
    async reclaim_expired_restores(objects) {
        const now = new Date();
        const batch_size = config.OBJECT_RECLAIMER_EXPIRE_RESTORE_BATCH_SIZE;
        const expired_restores = Array.isArray(objects) ?
            objects.filter(obj => is_expired_restore(obj, now)) :
            await MDStore.instance().find_expired_restore_objects(batch_size, now);
        if (!expired_restores || !expired_restores.length) {
            dbg.log0('no expired restore objects. nothing to do');
            return { had_work: false, had_errors: false };
        }

        let had_errors = false;
        dbg.log0('object_reclaimer: reclaiming expired restores:',
            expired_restores.map(o => o.key).join(', '));

        await P.all(expired_restores.map(async obj => {
            try {
                await map_deleter.delete_object_mappings_for_expired_restore_or_transition(obj);
                await MDStore.instance().update_object_by_id(obj._id, undefined, { restore_status: 1 });
            } catch (err) {
                dbg.error(`got error when reclaiming expired restore for object ${obj.key}:`, err);
                had_errors = true;
            }
        }));

        return { had_work: true, had_errors };
    }

    /**
     * Purge source storage-class copies after transition and set
     * transition_info.source_info.reclaimed so the object leaves the unreclaimed find/index.
     *
     * Delete mappings first so a failed cleanup stays retryable. RestoreObject is
     * blocked while unreclaimed source data remains, so marking reclaimed
     * after delete is safe.
     * @param {nb.ObjectMD[]} [objects] When set, reclaim these objects instead of scanning.
     * @returns {Promise<{ had_work: boolean, had_errors: boolean }>}
     */
    async reclaim_transition_source_data(objects) {
        const batch_size = config.OBJECT_RECLAIMER_TRANSITION_SOURCE_BATCH_SIZE;
        const unreclaimed_transitions_sources = Array.isArray(objects) ?
            objects.filter(is_unreclaimed_transition_source) :
            await MDStore.instance().find_objects_with_transition_done_unreclaimed_source(batch_size);
        if (!unreclaimed_transitions_sources || !unreclaimed_transitions_sources.length) {
            dbg.log0('no unreclaimed transition source objects. nothing to do');
            return { had_work: false, had_errors: false };
        }

        let had_errors = false;
        dbg.log0('object_reclaimer: reclaiming transition source data for:',
            unreclaimed_transitions_sources.map(o => o.key).join(', '));

        await P.all(unreclaimed_transitions_sources.map(async obj => {
            try {
                // Skip if a restore appeared after the find (query already requires restore_status: null).
                if (obj.restore_status) {
                    dbg.log0('object_reclaimer: skip transition reclaim while restore_status set:',
                        obj.key);
                    return;
                }
                await map_deleter.delete_object_mappings_for_expired_restore_or_transition(obj);
                await MDStore.instance().update_object_by_id(
                    obj._id,
                    { 'transition_info.source_info.reclaimed': new Date() },
                );
            } catch (err) {
                dbg.error(`got error when reclaiming transition source data for object ${obj.key}:`, err);
                had_errors = true;
            }
        }));

        return { had_work: true, had_errors };
    }

    /**
     * @returns {nb.MessageQueueClient}
     */
    _queue() {
        return this.message_queue || message_queue_client.instance();
    }

    /**
     * @param {Date} stale_before
     */
    _reclaim_scanners(stale_before) {
        const md = MDStore.instance();
        const enqueue_options = { reclaim_enqueued_before: stale_before };
        return [
            {
                kind: 'deleted',
                find: () => md.find_unreclaimed_objects(config.OBJECT_RECLAIMER_BATCH_SIZE, enqueue_options),
            },
            {
                kind: 'expired_restore',
                find: () => md.find_expired_restore_objects(
                    config.OBJECT_RECLAIMER_EXPIRE_RESTORE_BATCH_SIZE,
                    new Date(),
                    enqueue_options,
                ),
            },
            {
                kind: 'transition_source',
                find: () => md.find_objects_with_transition_done_unreclaimed_source(
                    config.OBJECT_RECLAIMER_TRANSITION_SOURCE_BATCH_SIZE,
                    enqueue_options,
                ),
            },
        ];
    }

    /**
     * @param {nb.MessageQueueClient} queue
     * @returns {Promise<{ had_work: boolean, had_errors: boolean }>}
     */
    async _enqueue_reclaim_batches(queue) {
        let had_work = false;
        let had_errors = false;
        const queue_name = config.OBJECT_RECLAIMER_QUEUE_NAME;
        const tasks_per_message = Math.max(1, config.OBJECT_RECLAIMER_TASKS_PER_MESSAGE);
        const stale_before = new Date(Date.now() - config.OBJECT_RECLAIMER_ENQUEUE_STALE_MS);

        for (const scanner of this._reclaim_scanners(stale_before)) {
            if (await queue.size(queue_name) >= config.OBJECT_RECLAIMER_MAX_QUEUED_MESSAGES) break;
            try {
                const objects = await scanner.find();
                if (!objects || !objects.length) continue;
                const claimed_ids = await MDStore.instance().claim_objects_for_reclaim(
                    objects.map(obj => obj._id),
                    stale_before,
                );
                if (!claimed_ids.length) continue;
                const chunks = chunk_list(claimed_ids, tasks_per_message);
                let sent = 0;
                try {
                    for (const object_ids of chunks) {
                        await queue.enqueue(queue_name, { kind: scanner.kind, object_ids });
                        sent += 1;
                        had_work = true;
                    }
                } catch (err) {
                    const stranded = chunks.slice(sent).flat();
                    if (stranded.length) await this._release_reclaim_claim({ object_ids: stranded });
                    throw err;
                }
            } catch (err) {
                dbg.error(`object_reclaimer: failed to enqueue ${scanner.kind} batch`, err);
                had_errors = true;
            }
        }
        return { had_work, had_errors };
    }

    /**
     * @param {nb.MessageQueueClient} queue
     * @returns {Promise<{ had_work: boolean, had_errors: boolean }>}
     */
    async _consume_reclaim_batch(queue) {
        const message = await queue.dequeue(config.OBJECT_RECLAIMER_QUEUE_NAME);
        if (!message) return { had_work: false, had_errors: false };
        if (message_queue_client.is_terminal_message(message)) {
            await this._fail_message(queue, message, 'visibility timeout');
            return { had_work: true, had_errors: true };
        }
        const lease = message_queue_client.hold_lease(queue, message);
        try {
            const result = await this._process_reclaim_message(message.payload);
            if (result.had_errors) {
                await this._fail_message(queue, message, 'reclaim batch had errors');
                return result;
            }
            await queue.ack(message);
            // Drop the claim after the message is gone. Finished objects no longer
            // match the scans. Objects skipped inside the batch become eligible again.
            try {
                await this._release_reclaim_claim(message.payload);
            } catch (err) {
                dbg.error('object_reclaimer: failed to clear reclaim claim', err);
            }
            return { had_work: true, had_errors: false };
        } catch (err) {
            dbg.error('object_reclaimer: failed to process reclaim message', err);
            const reason = err instanceof Error ? err.message : String(err);
            await this._fail_message(queue, message, reason);
            return { had_work: true, had_errors: true };
        } finally {
            lease.stop();
        }
    }

    /**
     * @param {object} payload
     * @returns {Promise<{ had_work: boolean, had_errors: boolean }>}
     */
    async _process_reclaim_message(payload) {
        if (!is_reclaim_payload(payload)) {
            dbg.error('object_reclaimer: dropping invalid reclaim message', payload);
            return { had_work: false, had_errors: false };
        }
        const objects = await this._load_reclaim_objects(payload.object_ids);
        switch (payload.kind) {
            case 'deleted':
                return this.reclaim_deleted_objects(objects);
            case 'expired_restore':
                return this.reclaim_expired_restores(objects);
            case 'transition_source':
                return this.reclaim_transition_source_data(objects);
            default:
                dbg.error('object_reclaimer: unknown reclaim message kind', payload.kind);
                return { had_work: false, had_errors: false };
        }
    }

    /**
     * @param {string[]} object_ids
     * @returns {Promise<nb.ObjectMD[]>}
     */
    async _load_reclaim_objects(object_ids) {
        if (!object_ids.length) return [];
        const ids = object_ids.map(id => db_client.instance().parse_object_id(id));
        const objects = await MDStore.instance().find_objects_by_id(ids);
        return objects || [];
    }

    /**
     * @param {nb.MessageQueueClient} queue
     * @param {nb.QueueMessage} message
     * @param {string} [reason]
     */
    async _fail_message(queue, message, reason) {
        try {
            const res = await queue.nack(message, reason);
            if (res && res.dropped) await this._release_reclaim_claim(message.payload);
        } catch (err) {
            dbg.error('object_reclaimer: failed to nack reclaim message', err);
        }
    }

    /**
     * @param {object} payload
     */
    async _release_reclaim_claim(payload) {
        const object_ids = payload && /** @type {{ object_ids?: string[] }} */ (payload).object_ids;
        if (!Array.isArray(object_ids) || !object_ids.length) return;
        const ids = object_ids.map(id => db_client.instance().parse_object_id(id));
        await MDStore.instance().update_objects_by_ids(ids, undefined, { reclaim_enqueued_at: 1 });
    }

    /**
     * @param {Array<{ had_work: boolean, had_errors: boolean }>} results
     * @returns {number}
     */
    _next_delay(results) {
        if (results.some(r => r.had_errors)) {
            return config.OBJECT_RECLAIMER_ERROR_DELAY;
        }
        if (results.some(r => r.had_work)) {
            return config.OBJECT_RECLAIMER_BATCH_DELAY;
        }
        return config.OBJECT_RECLAIMER_EMPTY_DELAY;
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

    ////////////////////////
    // DEEP ARCHIVE UTILS //
    ////////////////////////

    /**
     * Per bucket, deletes archive keys via archive_api.
     * @param {{ [bucket_id: string]: object[] }} pending_archive_deletes_by_bucket
     * @returns {Promise<{ reclaimed_ids: object[], had_errors: boolean }>}
     */
    async _delete_archived_objects(pending_archive_deletes_by_bucket) {
        const system = system_store.data.systems[0];
        const auth_token = auth_server.make_auth_token({
            system_id: system._id,
            account_id: system.owner._id,
            role: 'admin',
        });
        const reclaimed_ids = [];
        let had_errors = false;

        await P.all(Object.entries(pending_archive_deletes_by_bucket).map(async ([bucket_id, objects]) => {
            try {
                const objects_to_delete = objects.map(obj => ({ obj_id: obj._id, key: obj.key }));
                const result = await this.client.archive.delete_archive_objects({ bucket_id, objects: objects_to_delete }, { auth_token });
                reclaimed_ids.push(...result.reclaimed_ids);
                had_errors = had_errors || result.had_errors;
            } catch (err) {
                dbg.error(`failed deleting archived objects for bucket ${bucket_id}:`, err);
                had_errors = true;
            }
        }));

        return { reclaimed_ids, had_errors };
    }

    /**
     * Abort incomplete archive MPUs. Failed aborts stay unreclaimed for retry.
     * @param {object[]} objects
     * @returns {Promise<{ reclaimed_ids: object[], had_errors: boolean }>}
     */
    async _abort_archive_multiparts(objects) {
        const system = system_store.data.systems[0];
        const auth_token = auth_server.make_auth_token({ system_id: system._id, account_id: system.owner._id, role: 'admin' });
        const reclaimed_ids = [];
        let had_errors = false;
        await P.map_with_concurrency(config.OBJECT_RECLAIMER_ABORT_CONCURRENCY, objects, async obj => {
            if (await this._abort_archive_multipart(obj, auth_token)) {
                reclaimed_ids.push(obj._id);
            } else {
                had_errors = true;
            }
        });
        return { reclaimed_ids, had_errors };
    }

    /**
     * Abort an incomplete archive MPU on the namespace resource.
     * @param {nb.ObjectMD} obj
     * @param {string} auth_token
     * @returns {Promise<boolean>} true if abort succeeded
     */
    async _abort_archive_multipart(obj, auth_token) {
        const upload_id = obj.target_data_info.upload_id;
        if (!upload_id) return true;
        try {
            await this.client.archive.abort_archive_multipart_upload({ bucket_id: obj.bucket, obj_id: obj._id, upload_id }, { auth_token });
            return true;
        } catch (err) {
            dbg.error(`object_reclaimer: failed to abort archive MPU for ${obj.key}`, { obj_id: obj._id, upload_id, storage_class: obj.storage_class }, err);
            return false;
        }
    }
}


exports.ObjectsReclaimer = ObjectsReclaimer;
exports.chunk_list = chunk_list;

/**
 * @param {string[]} items
 * @param {number} size
 * @returns {string[][]}
 */
function chunk_list(items, size) {
    const chunks = [];
    const step = Math.max(1, size || 1);
    for (let i = 0; i < items.length; i += step) {
        chunks.push(items.slice(i, i + step));
    }
    return chunks;
}

/**
 * @param {nb.ObjectMD} obj
 * @returns {boolean}
 */
function is_deleted_unreclaimed(obj) {
    return Boolean(obj && obj.deleted && !obj.reclaimed);
}

/**
 * @param {nb.ObjectMD} obj
 * @param {Date} now
 * @returns {boolean}
 */
function is_expired_restore(obj, now) {
    if (!obj || obj.deleted || obj.upload_started || !obj.restore_status) return false;
    if (obj.restore_status.ongoing || !obj.restore_status.expiry_time) return false;
    return new Date(obj.restore_status.expiry_time) <= now;
}

/**
 * @param {nb.ObjectMD} obj
 * @returns {boolean}
 */
function is_unreclaimed_transition_source(obj) {
    if (!obj || obj.deleted || obj.upload_started || obj.restore_status) return false;
    const info = obj.transition_info;
    if (!info || info.status !== CONSTANTS.ARCHIVE.TRANSITION_STATUS.DONE || !info.transition_end_ts) return false;
    if (!info.source_info || info.source_info.reclaimed) return false;
    return true;
}

/**
 * @typedef {object} ReclaimMessage
 * @property {'deleted' | 'expired_restore' | 'transition_source'} kind
 * @property {string[]} object_ids
 */

/**
 * @param {object} payload
 * @returns {payload is ReclaimMessage}
 */
function is_reclaim_payload(payload) {
    const body = /** @type {{ kind?: string, object_ids?: unknown }} */ (payload);
    return Boolean(
        body &&
        (body.kind === 'deleted' || body.kind === 'expired_restore' || body.kind === 'transition_source') &&
        Array.isArray(body.object_ids),
    );
}
