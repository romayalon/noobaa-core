/* Copyright (C) 2024 NooBaa */
'use strict';

// disabling init_rand_seed as it takes longer than the actual test execution
process.env.DISABLE_INIT_RANDOM_SEED = 'true';

// Jest module mocks must be hoisted before any require statements.

jest.mock('../../../../config', () => ({}));

jest.mock('../../../util/debug_module', () => () => ({
    set_module_level: () => undefined,
    log0: () => undefined,
    log1: () => undefined,
    log2: () => undefined,
    warn: () => undefined,
    error: () => undefined,
}));

const mock_make_changes = jest.fn().mockResolvedValue(undefined);
const mock_new_system_store_id = jest.fn().mockReturnValue('mock-nsr-object-id');

jest.mock('../../../server/system_services/system_store', () => ({
    get_instance: () => ({
        new_system_store_id: mock_new_system_store_id,
        make_changes: mock_make_changes,
        data: {
            namespace_resources: [],
            buckets: [],
            pools: [],
        },
        master_key_manager: {
            encrypt_sensitive_string_with_master_key_id: jest.fn().mockReturnValue('encrypted'),
        },
    }),
}));

jest.mock('../../../server/server_rpc', () => ({
    rpc: { new_client: jest.fn() },
    client: {
        bucket: { get_cloud_buckets: jest.fn() },
        stats: { get_partial_stats: jest.fn() },
    },
}));

jest.mock('../../../server/notifications/dispatcher', () => ({ instance: jest.fn() }));
jest.mock('../../../server/node_services/nodes_client', () => ({ instance: jest.fn() }));
jest.mock('../../../server/analytic_services/history_data_store', () => ({
    HistoryDataStore: { instance: jest.fn() },
}));
jest.mock('../../../server/analytic_services/io_stats_store', () => ({
    IoStatsStore: { instance: jest.fn() },
}));
jest.mock('../../../server/system_services/pool_controllers', () => ({}));
jest.mock('../../../server/kube-store.js', () => ({ KubeStore: { instance: jest.fn() } }));
jest.mock('../../../server/common_services/auth_server', () => ({
    make_auth_token: jest.fn().mockReturnValue('mock-token'),
}));
jest.mock('../../../util/cloud_utils', () => ({
    find_cloud_connection: jest.fn(),
    get_used_cloud_targets: jest.fn().mockReturnValue([]),
}));

const pool_server = require('../../../server/system_services/pool_server');

// ---------------------------------------------------------------------------
// get_namespace_resource_info – archive field exposure
// ---------------------------------------------------------------------------

describe('get_namespace_resource_info – archive field', () => {
    const base_nsr = () => ({
        _id: { toString: () => 'nsr-test-id' },
        name: 'test-nsr',
        access_mode: 'READ_WRITE',
        nsfs_config: { fs_root_path: '/data' },
        issues_report: [],
        system: {
            buckets_by_name: {},
            vector_buckets_by_name: {},
        },
    });

    it('includes archive:true when the NSR has archive set to true', () => {
        const nsr = { ...base_nsr(), archive: true };
        const info = pool_server.get_namespace_resource_info(nsr);
        expect(info.archive).toBe(true);
    });

    it('includes archive:false when the NSR has archive set to false', () => {
        const nsr = { ...base_nsr(), archive: false };
        const info = pool_server.get_namespace_resource_info(nsr);
        // archive:false is falsy but not undefined, so _.omitBy(_.isUndefined) preserves it
        expect(info.archive).toBe(false);
    });

    it('omits archive when the NSR does not have the archive field', () => {
        const nsr = { ...base_nsr() };
        const info = pool_server.get_namespace_resource_info(nsr);
        expect(info.archive).toBeUndefined();
    });
});

