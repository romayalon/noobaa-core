/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const dbg = require('../../util/debug_module')(__filename);
const message_queue_client = require('../../util/message_queue_client');
// Shared queue registry - imports nothing from the executor, so the producer stays cycle-free.
const { BG_TASK_QUEUES } = require('../bg_services/bg_queues_utils');
const SensitiveString = require('../../util/sensitive_string');
const system_store = require('../system_services/system_store').get_instance();
const replication_store = require('../system_services/replication_store').instance();
const prom_reporting = require('../analytic_services/prometheus_reporting');
const auth_server = require('../common_services/auth_server');

const REPLICATION_TASK_TYPE = Object.freeze({
    SCAN: 'scan',
    // Log-based replication over the queue is not implemented yet - built on romy-log-replication-scanner-mq.
    LOG: 'log',
});

const PARTIAL_SINGLE_BUCKET_REPLICATION_DEFAULTS = {
    replication_id: '',
    last_cycle_rule_id: '',
    bucket_name: '',
    last_cycle_writes_num: 0,
    last_cycle_writes_size: 0,
    last_cycle_error_writes_num: 0,
    last_cycle_error_writes_size: 0,
    bucket_last_cycle_total_objects_num: 0,
    bucket_last_cycle_replicated_objects_num: 0,
    bucket_last_cycle_error_objects_num: 0,
};

//TODO: this function is not being used anymore, commenting out and keeping it as reference 
// function check_data_or_md_changed(src_info, dst_info) {
//     dbg.log1('replication_utils.check_data_or_md_changed:', src_info, dst_info);

//     // when object in dst bucket is more recent - we don't want to override it so we do not
//     // execute replication of object
//     const src_last_modified_recent = src_info.LastModified >= dst_info.LastModified;
//     if (!src_last_modified_recent) return false;

//     // data change - replicate the object
//     const data_change = src_info.ContentLength !== dst_info.ContentLength || src_info.ETag !== dst_info.ETag;
//     if (data_change) return true;

//     // md change - head objects and compare metadata, if metadata is different - copy object
//     if (!_.isEqual(src_info.Metadata, dst_info.Metadata)) return true;

//     // data and md is equal which means something else changed in src
//     // nothing to do 
//     return false;
// }

function get_rule_and_bucket_status(rule, src_cont_token, keys_diff_map, copy_res) {
    const { num_keys_to_copy, num_bytes_to_copy } = Object.entries(keys_diff_map).reduce(
        (acc, [key, value]) => {
            acc.num_keys_to_copy += value.length;
            acc.num_bytes_to_copy += value.reduce((key_bytes, obj) => key_bytes + obj.Size, 0);
            return acc;
        }, { num_keys_to_copy: 0, num_bytes_to_copy: 0 }
    );

    const num_keys_moved = (copy_res && copy_res.num_of_objects) || 0;
    const num_bytes_moved = (copy_res && copy_res.size_of_objects) || 0;

    const rule_status = {
        last_cycle_rule_id: rule,
        last_cycle_writes_num: num_keys_moved,
        last_cycle_writes_size: num_bytes_moved,
        last_cycle_error_writes_num: num_keys_to_copy - num_keys_moved,
        last_cycle_error_writes_size: num_bytes_to_copy - num_bytes_moved,
    };
    if (src_cont_token) rule_status.last_cycle_src_cont_token = src_cont_token;
    dbg.log1('get_rule_and_bucket_status:: rule_status: ', rule_status);

    const bucket_status = {
        bucket_last_cycle_total_objects_num: num_keys_to_copy,
        bucket_last_cycle_replicated_objects_num: num_keys_moved,
        bucket_last_cycle_error_objects_num: num_keys_to_copy - num_keys_moved,
    };
    dbg.log1('get_rule_and_bucket_status:: bucket_status: ', bucket_status);

    return {rule_status, bucket_status};
}

function update_replication_prom_report(bucket_name, replication_policy_id, rule_status, bucket_status) {
    const core_report = prom_reporting.get_core_report();
    const name = bucket_name && typeof bucket_name.unwrap === 'function' ? bucket_name.unwrap() : bucket_name;
    const last_cycle_status = _.defaults({
        ...rule_status,
        ...bucket_status,
        bucket_name: name,
        replication_id: replication_policy_id
    }, PARTIAL_SINGLE_BUCKET_REPLICATION_DEFAULTS);

    core_report.set_replication_status(last_cycle_status);
}

// used by scan-based replication (only) to update metrics when target is unreachable and diff cannot be computed
function report_failed_replication_cycle(bucket_name, replication_id, rule_id, total) {
    const core_report = prom_reporting.get_core_report();
    if (!core_report._metrics) return;
    const name = bucket_name instanceof SensitiveString ? bucket_name.unwrap() : bucket_name;
    // per replication_id metrics
    delete core_report._metrics.replication_status.hashMap[String(replication_id)];
    core_report._metrics.replication_status.set({ last_cycle_rule_id: rule_id, bucket_name: name, replication_id }, Date.now());
    core_report._metrics.replication_last_cycle_writes_size.set({ replication_id }, 0);
    core_report._metrics.replication_last_cycle_writes_num.set({ replication_id }, 0);
    core_report._metrics.replication_last_cycle_error_writes_size.set({ replication_id }, 0);
    core_report._metrics.replication_last_cycle_error_writes_num.inc({ replication_id }, 1);
    // per bucket metrics
    core_report._metrics.bucket_last_cycle_total_objects_num.set({ bucket_name: name }, total);
    core_report._metrics.bucket_last_cycle_replicated_objects_num.set({ bucket_name: name }, 0);
    core_report._metrics.bucket_last_cycle_error_objects_num.inc({ bucket_name: name }, 1);
    dbg.log0('report_failed_replication_cycle: updated error metrics bucket:', name,
        'replication_id:', replication_id, 'rule_id:', rule_id, 'total:', total);
}

function update_replication_target_status(replication_id, source_bucket, target_bucket, is_reachable) {
    if (!replication_id) return;
    const core_report = prom_reporting.get_core_report();
    const src_name = source_bucket instanceof SensitiveString ? source_bucket.unwrap() : source_bucket;
    const dst_name = target_bucket instanceof SensitiveString ? target_bucket.unwrap() : target_bucket;
    core_report.set_replication_target_status(String(replication_id), src_name, dst_name, is_reachable);
}

// clear target reachability metrics when the policy was deleted or has no source bucket (leftover series)
// orphan policy: replication_id not in replication DB, or no source bucket references it
function clear_replication_target_status_for_orphan_policy(replication_id) {
    if (!replication_id) return;
    const core_report = prom_reporting.get_core_report();
    core_report.clear_replication_target_status_by_replication_id(String(replication_id));
}

// reconcile_replication_target_status reconciles target reachability metrics at replication scan start
// drop stale replication_target_status series: one DB read, then clear gauge entries that no longer match active policies
async function reconcile_replication_target_status() {
    const hash_map = prom_reporting.get_core_report()._metrics?.replication_target_status?.hashMap;
    if (!hash_map) return;

    const keys = Object.keys(hash_map);
    if (!keys.some(k => hash_map[k]?.labels?.replication_id)) return;

    const replications = await replication_store.get_all_replication_configs();
    if (!replications?.length) return;

    const valid_label_keys = new Set();
    const clear_all_ids = new Set();

    // build allowed (replication_id|source|target) valid keys from DB and mark policies with no source for full clear
    for (const repl of replications) {
        const repl_id = String(repl._id);
        if (repl.deleted) {
            clear_all_ids.add(repl_id);
            continue;
        }
        const rules = repl.rules;
        if (!rules?.length) {
            clear_all_ids.add(repl_id);
            continue;
        }
        const { src_bucket } = find_src_and_dst_buckets(rules[0].destination_bucket, repl_id);
        if (!src_bucket) {
            clear_all_ids.add(repl_id);
            continue;
        }
        const src_name = bucket_name_for_metrics(src_bucket);
        // one valid series per rule destination for this policy
        for (const rule of rules) {
            const dst_name = await resolve_destination_bucket_name(rule.destination_bucket);
            valid_label_keys.add(`${repl_id}|${src_name}|${dst_name}`);
        }
    }

    // remove gauge samples that are orphaned, policy-less, or no longer match DB labels
    for (const key of keys) {
        const labels = hash_map[key]?.labels;
        if (!labels?.replication_id) continue;
        const repl_id = String(labels.replication_id);
        const label_key = `${repl_id}|${labels.source_bucket}|${labels.target_bucket}`;
        if (clear_all_ids.has(repl_id) || !valid_label_keys.has(label_key)) delete hash_map[key];
    }
}

// remove the `-deleting-<timestamp>` suffix noobaa appends while a bucket is being deleted
function strip_deleting_bucket_suffix(name) {
    const s = name instanceof SensitiveString ? name.unwrap() : name;
    if (!s) return '';
    const m = String(s).match(/^(.*)-deleting-\d+$/);
    return m ? m[1] : String(s);
}

// turn a bucket document into a plain string name suitable for metrics labels (unwrap + deleting rename)
function bucket_name_for_metrics(bucket) {
    if (!bucket?.name) return '';
    if (bucket.deleting) return strip_deleting_bucket_suffix(bucket.name);
    return bucket.name instanceof SensitiveString ? bucket.name.unwrap() : String(bucket.name);
}

// resolve a destination bucket id to a display name via system_store, then soft-deleted db rows
async function resolve_destination_bucket_name(dst_bucket_id) {
    const id = dst_bucket_id?.valueOf ? dst_bucket_id.valueOf() : dst_bucket_id;
    try {
        const bucket = system_store.data.get_by_id(id);
        if (bucket?.name) return bucket_name_for_metrics(bucket);

        const res = await system_store.data.get_by_id_include_deleted(id, 'buckets');
        if (res?.record?.name) return bucket_name_for_metrics(res.record);
        dbg.warn('resolve_destination_bucket_name: bucket not found for id:', id);
    } catch (err) {
        dbg.warn('resolve_destination_bucket_name failed:', id, err);
    }
    // worst-case: no row for this id (stale ref, e.g. bucket removed and recreated with a new _id); labels still need a non-empty string
    return String(id);
}

/**
 * @param {any} bucket_name
 * @param {string} key
 * @param {import('@aws-sdk/client-s3').S3} s3
 * @param {string} version_id
 */
async function get_object_md(bucket_name, key, s3, version_id) {
    if (bucket_name instanceof SensitiveString) bucket_name = bucket_name.unwrap();
    const params = {
        Bucket: bucket_name,
        Key: key,
        VersionId: version_id,
    };

    dbg.log1('get_object_md params:', params);
    try {
        const head = await s3.headObject(params);
        //for namespace s3 we are omitting the 'noobaa-namespace-s3-bucket' as it will be defer between buckets
        if (head?.Metadata) head.Metadata = _.omit(head.Metadata, 'noobaa-namespace-s3-bucket');
        dbg.log1('get_object_md: finished successfully', head);
        return head;
    } catch (err) {
        dbg.error('get_object_md: error.name: ', err?.name, ' error: ', err);
        if (err?.name === 'NotFound') return;
        throw err;
    }
}

function find_src_and_dst_buckets(dst_bucket_id, replication_id) {
    const ans = _.reduce(system_store.data.buckets, (acc, cur_bucket) => {
        dbg.log1('replication_utils.find_src_and_dst_buckets: ', cur_bucket._id, dst_bucket_id, cur_bucket.replication_policy_id, replication_id);
        if (cur_bucket._id.toString() === dst_bucket_id.toString()) acc.dst_bucket = cur_bucket;
        if (cur_bucket.replication_policy_id &&
            (cur_bucket.replication_policy_id.toString() === replication_id.toString())) {
            acc.src_bucket = cur_bucket;
        }
        return acc;
    }, { src_bucket: undefined, dst_bucket: undefined });

    return ans;
}

function get_copy_type() {
    // TODO: get copy type by src and dst buckets (for server side/other optimization)
    return 'MIX';
}

async function copy_objects(scanner_semaphore, client, copy_type, src_bucket_name, dst_bucket_name, keys_diff_map, replication_id) {
    const keys_length = Object.keys(keys_diff_map || {});
    if (!keys_length.length) {
        return { num_of_objects: 0, size_of_objects: 0 };
    }

    let res;
    try {
        res = await scanner_semaphore.surround_count(keys_length, //We will do key by key even when a key have more then one version
            async () => {
                try {
                    //calling copy_objects in the replication server
                    const res1 = await client.replication.copy_objects({
                        copy_type,
                        src_bucket_name,
                        dst_bucket_name,
                        keys_diff_map
                    }, {
                        auth_token: auth_server.make_auth_token({
                            system_id: system_store.data.systems[0]._id,
                            account_id: system_store.data.systems[0].owner._id,
                            role: 'admin'
                        })
                    });
                    return res1;
                } catch (err) {
                    // no need to do retries, eventually the object will be uploaded
                    // TODO: serious error codes with metrics (auth_failed, storage not exist etc)
                    dbg.error('replication_utils copy_objects: error: ', err, src_bucket_name, dst_bucket_name, keys_diff_map);
                }
            });
    } catch (err) {
        dbg.error('replication_utils copy_objects: semaphore error:', err, err.stack);
        // no need to handle semaphore errors, eventually the object will be uploaded
        return;
    }

    const moved = Number(res?.num_of_objects) || 0;
    if (replication_id) {
        if (moved > 0) {
            update_replication_target_status(replication_id, src_bucket_name, dst_bucket_name, true);
        } else {
            update_replication_target_status(replication_id, src_bucket_name, dst_bucket_name, false);
        }
    }
    return res ?? { num_of_objects: 0, size_of_objects: 0 };
}

//TODO: probably need to handle it also, getting an objects and not keys array
//      as delete_objects is not being called in the replication scanner, we will handle it later.
async function delete_objects(scanner_semaphore, client, bucket_name, keys) {
    if (!keys.length) return;
    try {
        const res = await scanner_semaphore.surround_count(keys.length,
            async () => {
                try {
                    const res1 = await client.replication.delete_objects({
                        bucket_name,
                        keys
                    }, {
                        auth_token: auth_server.make_auth_token({
                            system_id: system_store.data.systems[0]._id,
                            account_id: system_store.data.systems[0].owner._id,
                            role: 'admin'
                        })
                    });
                    return res1;
                } catch (err) {
                    dbg.error('replication_utils delete_objects: error: ', err, bucket_name, keys);
                }
            });
        return res;
    } catch (err) {
        dbg.error('replication_utils delete_objects: semaphore error:', err, err.stack);
    }
}


/**
 * Convert the stored format back to the flat API format.
 *
 * Stored shape:
 *   { aws_log_replication_info:   { logs_location: {...} } }
 *   { azure_log_replication_info: { prefix?: string } }
 *   { noobaa_log_replication_info:{ logs_location: {...} } }
 *
 * API shape (replication_policy#/log_replication_info):
 *   AWS   – { logs_location: {...} }  (endpoint_type optional)
 *   AZURE – { endpoint_type: 'AZURE', prefix?: string }
 *   NOOBAA– { endpoint_type: 'NOOBAA', logs_location: {...} }
 */
function _denormalize_log_replication_info(stored) {
    if (!stored) return undefined;

    if (stored.aws_log_replication_info) {
        // AWS: inner object already matches the API flat shape (no endpoint_type required)
        return stored.aws_log_replication_info;
    }
    if (stored.azure_log_replication_info) {
        return { ...stored.azure_log_replication_info, endpoint_type: 'AZURE' };
    }
    if (stored.noobaa_log_replication_info) {
        return { ...stored.noobaa_log_replication_info, endpoint_type: 'NOOBAA' };
    }
    // Empty / unrecognised wrapper – treat as absent so the reply stays schema-valid
    return undefined;
}

/**
 * @typedef {typeof REPLICATION_TASK_TYPE.SCAN | typeof REPLICATION_TASK_TYPE.LOG} ReplicationTaskType
 */

/**
 * Store one replicate task. The worker runs it instead of the scanner.
 * task_type is stored on the message. The payload stays the task body.
 * @param {nb.MessageQueueClient | null | undefined} mq_client Worker queue, or null for the process client.
 * @param {ReplicationTaskType} task_type
 * @param {{ replication_id: string, rule_id: string }} payload JSON object. Nested dates are stored as strings.
 * @returns {Promise<string>} Id of the queued message.
 * @throws {Error} when task_type or payload is missing, or the process has no queue
 */
async function enqueue_replication_task(mq_client, task_type, payload) {
    if (task_type !== REPLICATION_TASK_TYPE.SCAN && task_type !== REPLICATION_TASK_TYPE.LOG) {
        throw new Error('replicate task type is missing');
    }
    if (!payload) throw new Error('replicate task payload is missing');
    mq_client = mq_client || message_queue_client.instance();
    if (!mq_client) throw new Error('replicate task queue is not configured');
    const queue_name = BG_TASK_QUEUES.replication.queue;
    // connect applies the queue defaults from config; enqueue only inserts, so it needs no connect options.
    await mq_client.connect(queue_name);
    dbg.log1('replication: enqueue', task_type, payload.replication_id, payload.rule_id);
    return mq_client.enqueue(queue_name, payload, task_type);
}

/**
 * @param {*} id
 * @returns {string}
 */
function replication_id_string(id) {
    if (id === undefined || id === null) return '';
    return String(id);
}

/**
 * Load the source and destination buckets for a queued replicate task.
 * @param {{ destination_bucket_id: string, replication_id: string, rule_id: string }} payload
 * @returns {{ src_bucket: { name: nb.SensitiveString }, dst_bucket: { name: nb.SensitiveString } }}
 * @throws {Error} when either bucket is missing
 */
function load_replication_task_buckets(payload) {
    // Through exports so a test can replace the bucket lookup.
    const found = exports.find_src_and_dst_buckets(payload.destination_bucket_id, payload.replication_id);
    if (!found.src_bucket || !found.dst_bucket) {
        throw new Error(`replicate task buckets are missing for rule ${payload.rule_id}`);
    }
    return {
        src_bucket: /** @type {{ name: nb.SensitiveString }} */ (found.src_bucket),
        dst_bucket: /** @type {{ name: nb.SensitiveString }} */ (found.dst_bucket),
    };
}

const EXECUTOR_RULE_METRICS_LIMIT = 1000;

const RULE_METRIC_FIELDS = [
    'last_cycle_writes_num',
    'last_cycle_writes_size',
    'last_cycle_error_writes_num',
    'last_cycle_error_writes_size',
];

const BUCKET_METRIC_FIELDS = [
    'bucket_last_cycle_total_objects_num',
    'bucket_last_cycle_replicated_objects_num',
    'bucket_last_cycle_error_objects_num',
];

/**
 * Latest reports for one rule in the current scan cycle.
 * parts is keyed by report_id so a retry replaces that executor's numbers.
 * @typedef {object} ExecutorRuleMetrics
 * @property {number} last_cycle_start
 * @property {Map<string, { rule_status: object, bucket_status: object }>} parts
 */

/** @type {Map<string, ExecutorRuleMetrics>} */
const executor_rule_metrics = new Map();

/**
 * @typedef {object} ExecutorReplicationMetrics
 * @property {string} replication_id
 * @property {string} rule_id
 * @property {string | { unwrap: () => string }} bucket_name
 * @property {string} report_id
 * @property {number} last_cycle_start
 * @property {object} rule_status
 * @property {object} bucket_status
 */

/**
 * Send one finished chunk to the default background process.
 * That process owns the core report and adds this report into the rule sum.
 * @param {{ replication: { update_replication_metrics: (params: object, options: object) => Promise<void> } }} client
 * @param {ExecutorReplicationMetrics} entry
 * @returns {Promise<void>}
 */
async function send_replication_metrics(client, entry) {
    if (!client || !client.replication || typeof client.replication.update_replication_metrics !== 'function') {
        throw new Error('replication metrics rpc is not configured');
    }
    const bucket_name = entry.bucket_name && typeof entry.bucket_name.unwrap === 'function' ?
        entry.bucket_name.unwrap() : entry.bucket_name;
    await client.replication.update_replication_metrics({
        replication_id: String(entry.replication_id),
        rule_id: String(entry.rule_id),
        bucket_name: String(bucket_name),
        report_id: String(entry.report_id || ''),
        last_cycle_start: Number(entry.last_cycle_start) || 0,
        rule_status: entry.rule_status,
        bucket_status: entry.bucket_status,
    }, {
        auth_token: auth_server.make_auth_token({
            system_id: system_store.data.systems[0]._id,
            account_id: system_store.data.systems[0].owner._id,
            role: 'admin'
        })
    });
}

/**
 * Add one executor report to its rule and publish the sum.
 * A new last_cycle_start starts a new sum. The same report_id replaces the previous report.
 * @param {ExecutorReplicationMetrics} entry
 * @returns {void}
 */
function add_executor_replication_metrics(entry) {
    const replication_id = String(entry.replication_id || '');
    const rule_id = String(entry.rule_id || '');
    const report_id = String(entry.report_id || '');
    if (!replication_id || !rule_id || !report_id) {
        throw new Error('replication metrics report is missing its id');
    }
    const cycle = Number(entry.last_cycle_start) || 0;
    const id = `${replication_id}\0${rule_id}`;
    let rule = executor_rule_metrics.get(id);
    if (!rule || rule.last_cycle_start !== cycle) {
        if (!executor_rule_metrics.has(id) && executor_rule_metrics.size >= EXECUTOR_RULE_METRICS_LIMIT) {
            const oldest = executor_rule_metrics.keys().next().value;
            if (oldest !== undefined) executor_rule_metrics.delete(oldest);
        }
        rule = { last_cycle_start: cycle, parts: new Map() };
        executor_rule_metrics.set(id, rule);
    }
    rule.parts.set(report_id, {
        rule_status: entry.rule_status || {},
        bucket_status: entry.bucket_status || {},
    });
    const summed = sum_executor_rule_metrics(rule_id, rule.parts, entry.rule_status);
    const bucket_name = entry.bucket_name && typeof entry.bucket_name.unwrap === 'function' ?
        entry.bucket_name.unwrap() : entry.bucket_name;
    // Through exports so a test can replace the reporter without loading prometheus.
    exports.update_replication_prom_report(bucket_name, replication_id, summed.rule_status, summed.bucket_status);
}

/**
 * @param {string} rule_id
 * @param {Map<string, { rule_status: object, bucket_status: object }>} parts
 * @param {object} latest_rule_status
 * @returns {{ rule_status: object, bucket_status: object }}
 */
function sum_executor_rule_metrics(rule_id, parts, latest_rule_status) {
    /** @type {Record<string, number>} */
    const rule_counts = {};
    /** @type {Record<string, number>} */
    const bucket_status = {};
    for (const field of RULE_METRIC_FIELDS) rule_counts[field] = 0;
    for (const field of BUCKET_METRIC_FIELDS) bucket_status[field] = 0;
    for (const part of parts.values()) {
        for (const field of RULE_METRIC_FIELDS) {
            rule_counts[field] += metric_number(part.rule_status, field);
        }
        for (const field of BUCKET_METRIC_FIELDS) {
            bucket_status[field] += metric_number(part.bucket_status, field);
        }
    }
    const rule_status = {
        last_cycle_rule_id: rule_id,
        ...rule_counts,
    };
    if (latest_rule_status && latest_rule_status.last_cycle_src_cont_token) {
        rule_status.last_cycle_src_cont_token = latest_rule_status.last_cycle_src_cont_token;
    }
    return { rule_status, bucket_status };
}

/**
 * @param {object} status
 * @param {string} field
 * @returns {number}
 */
function metric_number(status, field) {
    const value = Number(status && status[field]);
    return Number.isFinite(value) ? value : 0;
}

// EXPORTS
exports.REPLICATION_TASK_TYPE = REPLICATION_TASK_TYPE;
exports.get_rule_and_bucket_status = get_rule_and_bucket_status;
exports.update_replication_prom_report = update_replication_prom_report;
exports.report_failed_replication_cycle = report_failed_replication_cycle;
exports.update_replication_target_status = update_replication_target_status;
exports.reconcile_replication_target_status = reconcile_replication_target_status;
exports.clear_replication_target_status_for_orphan_policy = clear_replication_target_status_for_orphan_policy;
exports.resolve_destination_bucket_name = resolve_destination_bucket_name;
exports.get_object_md = get_object_md;
exports.find_src_and_dst_buckets = find_src_and_dst_buckets;
exports.get_copy_type = get_copy_type;
exports.enqueue_replication_task = enqueue_replication_task;
exports.replication_id_string = replication_id_string;
exports.load_replication_task_buckets = load_replication_task_buckets;
exports.send_replication_metrics = send_replication_metrics;
exports.add_executor_replication_metrics = add_executor_replication_metrics;
exports.copy_objects = copy_objects;
exports.delete_objects = delete_objects;
exports._denormalize_log_replication_info = _denormalize_log_replication_info;
