/* Copyright (C) 2016 NooBaa */
'use strict';

const config = require('../../../config');
const dbg = require('../../util/debug_module')(__filename);
const message_queue_client = require('../../util/message_queue_client');
const cloud_utils = require('../../util/cloud_utils');
const system_store = require('../system_services/system_store').get_instance();
const replication_utils = require('../utils/replication_utils');

/**
 * Store one replicate task. The scanner calls this instead of execute.
 * bg_task_executor.js dequeues the replicate queue and calls execute_payload.
 * @param {nb.MessageQueueClient | null | undefined} queue Scanner queue, or null for the process client.
 * @param {ScanReplicateTask | LogReplicateTask} payload JSON object. Nested dates are stored as strings.
 * @returns {Promise<string>} Id of the queued message.
 */
async function enqueue(queue, payload) {
    if (!payload || (payload.kind !== 'scan' && payload.kind !== 'log')) {
        throw new Error('replicate task kind is missing');
    }
    const client = queue || message_queue_client.instance();
    await client.connect();
    dbg.log1('replication_executor: enqueue', payload.kind, payload.replication_id, payload.rule_id);
    return client.enqueue(config.REPLICATION_QUEUE_NAME, payload);
}

/**
 * Run execute for one dequeued replicate task.
 * The scanner already stored the scan cursor, so the scan executor does not write it again.
 * @param {object} payload Claim payload. A scan or log replicate task.
 * @param {{ replication?: ReplicationScanner, log?: LogReplicationScanner }} scanners
 * @returns {Promise<void>}
 */
async function execute_payload(payload, scanners) {
    if (!payload || typeof payload !== 'object') {
        throw new Error('replicate task payload is missing');
    }
    const kind = /** @type {{ kind?: string }} */ (payload).kind;
    if (kind === 'scan') {
        await execute_scan(/** @type {ScanReplicateTask} */ (payload), scanners && scanners.replication);
        return;
    }
    if (kind === 'log') {
        await execute_log(/** @type {LogReplicateTask} */ (payload), scanners && scanners.log);
        return;
    }
    throw new Error(`unknown replicate task kind ${kind}`);
}

/**
 * @param {*} id
 * @returns {string}
 */
function id_string(id) {
    if (id === undefined || id === null) return '';
    return String(id);
}

exports.enqueue = enqueue;
exports.execute_payload = execute_payload;
exports.id_string = id_string;

/**
 * @param {ScanReplicateTask} payload
 * @param {ReplicationScanner} [scanner]
 * @returns {Promise<void>}
 */
async function execute_scan(payload, scanner) {
    if (!scanner) throw new Error('replication scanner is not configured');
    const { src_bucket, dst_bucket } = load_buckets(payload);
    const previous = scanner.commit_scan_status;
    scanner.commit_scan_status = false;
    try {
        await scanner.execute(
            src_bucket,
            dst_bucket,
            payload.keys_diff_map || {},
            payload.replication_id,
            payload.rule_id,
            payload.status || { last_cycle_start: Date.now() },
            {
                src_cont_token: payload.src_cont_token || '',
                dst_cont_token: payload.dst_cont_token || '',
            },
        );
    } finally {
        scanner.commit_scan_status = previous;
    }
}

/**
 * @param {LogReplicateTask} payload
 * @param {LogReplicationScanner} [scanner]
 * @returns {Promise<void>}
 */
async function execute_log(payload, scanner) {
    if (!scanner) throw new Error('log replication scanner is not configured');
    const { src_bucket, dst_bucket } = load_buckets(payload);
    ensure_log_connection(scanner);
    await scanner.execute(
        src_bucket,
        dst_bucket,
        {
            items: /** @type {nb.ReplicationLogCandidates} */ (payload.items || {}),
            // The scanner committed the log marker when it enqueued this task.
            done: async () => undefined,
        },
        Boolean(payload.sync_versions),
        Boolean(payload.sync_deletions),
        payload.rule_id,
        payload.replication_id,
    );
}

/**
 * @param {ScanReplicateTask | LogReplicateTask} payload
 * @returns {{ src_bucket: { name: nb.SensitiveString }, dst_bucket: { name: nb.SensitiveString } }}
 */
function load_buckets(payload) {
    const found = replication_utils.find_src_and_dst_buckets(
        payload.destination_bucket_id,
        payload.replication_id,
    );
    if (!found.src_bucket || !found.dst_bucket) {
        throw new Error(`replicate task buckets are missing for rule ${payload.rule_id}`);
    }
    return {
        src_bucket: /** @type {{ name: nb.SensitiveString }} */ (found.src_bucket),
        dst_bucket: /** @type {{ name: nb.SensitiveString }} */ (found.dst_bucket),
    };
}

/**
 * @param {LogReplicationScanner} scanner
 * @returns {void}
 */
function ensure_log_connection(scanner) {
    if (scanner.noobaa_connection) return;
    const system = system_store.data.systems && system_store.data.systems[0];
    if (!system) throw new Error('log replication executor has no system');
    scanner.noobaa_connection = cloud_utils.set_noobaa_s3_connection(system);
}

/**
 * @typedef {import('./replication_scanner').ReplicationScanner} ReplicationScanner
 * @typedef {import('./log_replication_scanner').LogReplicationScanner} LogReplicationScanner
 */

/**
 * @typedef {object} ScanReplicateTask
 * @property {'scan'} kind
 * @property {string} replication_id
 * @property {string} rule_id
 * @property {string} destination_bucket_id
 * @property {nb.BucketDiffKeysDiff | {}} keys_diff_map
 * @property {{ last_cycle_start: number }} status
 * @property {string} src_cont_token
 * @property {string} dst_cont_token
 */

/**
 * @typedef {object} LogReplicateTask
 * @property {'log'} kind
 * @property {string} replication_id
 * @property {string} rule_id
 * @property {string} destination_bucket_id
 * @property {object} items
 * @property {boolean} sync_versions
 * @property {boolean} sync_deletions
 */
