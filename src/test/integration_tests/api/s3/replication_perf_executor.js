/* Copyright (C) 2026 NooBaa */
'use strict';

// Spawned by test_replication_perf.js. One process drains the replicate queue.
// The test points this process at the coretest database, root keys, and RPC port.

require('../../../../util/dotenv').load();
require('../../../../util/panic');
require('../../../../util/fips');

const config = require('../../../../../config');

const root_key_mount = process.env.REPLICATION_PERF_ROOT_KEY_MOUNT || '';
config.ROOT_KEY_MOUNT = root_key_mount;
config.POSTGRES_DEFAULT_MAX_CLIENTS = 2;
config.POSTGRES_MD_MAX_CLIENTS = 2;
config.BG_WORKERS_MODE = 'executor';
config.MESSAGE_QUEUE_TYPE = 'postgres';
config.SYSTEM_STORE_SOURCE = 'DB';
config.test_mode = true;
// run_task returns this delay. The scheduler treats 0 as "use the empty delay",
// so the batch delay stays at least 1ms. The empty delay is short so the first
// poll, before the scanner has enqueued anything, does not add a full second.
config.BG_EXECUTOR_BATCH_DELAY = delay_ms('REPLICATION_PERF_BATCH_DELAY_MS', 1);
config.BG_EXECUTOR_EMPTY_DELAY = delay_ms('REPLICATION_PERF_EMPTY_DELAY_MS', 20);

const db_client = require('../../../../util/db_client');
const server_rpc = require('../../../../server/server_rpc');

const db_name = process.env.REPLICATION_PERF_DB_NAME;
const rpc_address = process.env.REPLICATION_PERF_RPC_ADDRESS;
if (!db_name) throw new Error('replication_perf_executor: REPLICATION_PERF_DB_NAME is required');
if (!rpc_address) throw new Error('replication_perf_executor: REPLICATION_PERF_RPC_ADDRESS is required');
if (!root_key_mount) throw new Error('replication_perf_executor: REPLICATION_PERF_ROOT_KEY_MOUNT is required');

db_client.instance().set_db_name(db_name);
if (!server_rpc.rpc.router) throw new Error('replication_perf_executor: rpc router is missing');
const router = /** @type {Record<string, string>} */ (server_rpc.rpc.router);
router.default = rpc_address;
router.md = rpc_address;
router.bg = rpc_address;
router.hosted_agents = rpc_address;
router.master = rpc_address;

const bg_workers = require('../../../../server/bg_workers');

if (require.main === module) {
    main().catch(err => {
        console.error('replication_perf_executor:', err);
        process.exit(1);
    });
}

async function main() {
    await bg_workers.main();
    console.log('replication_perf_executor: ready');
}

/**
 * @param {string} name
 * @param {number} fallback
 * @returns {number}
 */
function delay_ms(name, fallback) {
    const value = Number(process.env[name]);
    if (!Number.isFinite(value) || value < 1) return fallback;
    return value;
}
