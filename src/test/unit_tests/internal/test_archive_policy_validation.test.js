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

jest.mock('../../../server/system_services/system_store', () => ({
    get_instance: () => ({
        new_system_store_id: jest.fn().mockReturnValue('mock-id'),
        make_changes: jest.fn().mockResolvedValue(undefined),
        data: { namespace_resources: [], buckets: [], pools: [] },
        master_key_manager: {
            encrypt_sensitive_string_with_master_key_id: jest.fn().mockReturnValue('enc'),
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

jest.mock('../../../server/system_services/pool_server', () => ({}));
jest.mock('../../../server/system_services/tier_server', () => ({}));
jest.mock('../../../server/notifications/dispatcher', () => ({ instance: jest.fn() }));
jest.mock('../../../server/node_services/nodes_client', () => ({ instance: jest.fn() }));
jest.mock('../../../server/node_services/node_allocator', () => ({}));
jest.mock('../../../server/system_services/replication_store', () => ({ instance: jest.fn() }));
jest.mock('../../../server/bg_services/usage_aggregator', () => ({}));
jest.mock('../../../server/utils/chunk_config_utils', () => ({}));
jest.mock('../../../server/object_services/md_store', () => ({
    MDStore: { instance: jest.fn().mockReturnValue({}) },
}));
jest.mock('../../../server/analytic_services/bucket_stats_store', () => ({
    BucketStatsStore: { instance: jest.fn().mockReturnValue({}) },
}));
jest.mock('../../../util/NetStorageKit-Node-master/lib/netstorage', () => ({}));
jest.mock('../../../sdk/noobaa_s3_client/noobaa_s3_client', () => ({}));
jest.mock('../../../server/system_services/objects/quota', () => ({}));
jest.mock('../../../util/cloud_utils', () => ({
    find_cloud_connection: jest.fn(),
    get_used_cloud_targets: jest.fn().mockReturnValue([]),
}));

const bucket_server = require('../../../server/system_services/bucket_server');
const { RpcError } = require('../../../rpc');

// _resolve_archive_policy is exported from bucket_server for unit testing.
const resolve_archive_policy = bucket_server._resolve_archive_policy;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/**
 * Calls fn and returns the thrown error. Asserts that an error was thrown.
 */
function get_thrown(fn) {
    try {
        fn();
    } catch (err) {
        return err;
    }
    throw new Error('Expected function to throw but it did not');
}

function make_archive_req(resource_name, path_value, nsr_map = {}) {
    return {
        system: {
            _id: 'sys-id',
            namespace_resources_by_name: nsr_map,
        },
        rpc_params: {
            archive_policy: {
                deep_archive_resource: {
                    resource: resource_name,
                    path: path_value,
                },
            },
        },
    };
}

function make_nsr(id, archive_flag) {
    return { _id: id, name: 'nsr-' + id, archive: archive_flag };
}

// ---------------------------------------------------------------------------
// resolve_archive_policy – missing deep_archive_resource
// ---------------------------------------------------------------------------

describe('resolve_archive_policy – missing deep_archive_resource', () => {
    it('throws INVALID_ARCHIVE_POLICY when deep_archive_resource is absent', () => {
        const req = {
            system: { namespace_resources_by_name: {} },
            rpc_params: { archive_policy: {} },
        };
        const err = get_thrown(() => resolve_archive_policy(req));
        expect(err).toBeInstanceOf(RpcError);
        expect(err.rpc_code).toBe('INVALID_ARCHIVE_POLICY');
    });
});

// ---------------------------------------------------------------------------
// resolve_archive_policy – NSR lookup
// ---------------------------------------------------------------------------

describe('resolve_archive_policy – NSR not found', () => {
    it('throws INVALID_ARCHIVE_RESOURCE when the NSR does not exist in the system', () => {
        const req = make_archive_req('nonexistent-nsr', '/data', {});
        const err = get_thrown(() => resolve_archive_policy(req));
        expect(err).toBeInstanceOf(RpcError);
        expect(err.rpc_code).toBe('INVALID_ARCHIVE_RESOURCE');
        expect(err.message).toContain('nonexistent-nsr');
    });
});

// ---------------------------------------------------------------------------
// resolve_archive_policy – archive flag guard (new validation)
// ---------------------------------------------------------------------------

describe('resolve_archive_policy – archive flag guard', () => {
    it('throws INVALID_ARCHIVE_RESOURCE when NSR exists but archive is undefined', () => {
        const nsr = make_nsr('id-1', undefined);
        const req = make_archive_req('nsr-id-1', '/deep', { 'nsr-id-1': nsr });
        const err = get_thrown(() => resolve_archive_policy(req));
        expect(err).toBeInstanceOf(RpcError);
        expect(err.rpc_code).toBe('INVALID_ARCHIVE_RESOURCE');
        expect(err.message).toContain('archive:true');
    });

    it('throws INVALID_ARCHIVE_RESOURCE when NSR exists but archive is false', () => {
        const nsr = make_nsr('id-2', false);
        const req = make_archive_req('nsr-id-2', '/deep', { 'nsr-id-2': nsr });
        const err = get_thrown(() => resolve_archive_policy(req));
        expect(err).toBeInstanceOf(RpcError);
        expect(err.rpc_code).toBe('INVALID_ARCHIVE_RESOURCE');
        expect(err.message).toContain('archive:true');
    });

    it('throws INVALID_ARCHIVE_RESOURCE when NSR exists but archive is null', () => {
        const nsr = make_nsr('id-3', null);
        const req = make_archive_req('nsr-id-3', '/deep', { 'nsr-id-3': nsr });
        const err = get_thrown(() => resolve_archive_policy(req));
        expect(err).toBeInstanceOf(RpcError);
        expect(err.rpc_code).toBe('INVALID_ARCHIVE_RESOURCE');
        expect(err.message).toContain('archive:true');
    });
});

// ---------------------------------------------------------------------------
// resolve_archive_policy – happy path
// ---------------------------------------------------------------------------

describe('resolve_archive_policy – happy path', () => {
    it('returns resolved archive policy when NSR has archive:true', () => {
        const nsr = make_nsr('id-4', true);
        const req = make_archive_req('nsr-id-4', '/deep/path', { 'nsr-id-4': nsr });
        const result = resolve_archive_policy(req);
        expect(result).toEqual({
            deep_archive_resource: {
                resource: 'id-4',
                path: '/deep/path',
            },
        });
    });

    it('returns resolved archive policy with undefined path when path is omitted', () => {
        const nsr = make_nsr('id-5', true);
        const req = make_archive_req('nsr-id-5', undefined, { 'nsr-id-5': nsr });
        const result = resolve_archive_policy(req);
        expect(result).toEqual({
            deep_archive_resource: {
                resource: 'id-5',
                path: undefined,
            },
        });
    });

    it('uses the NSR _id (ObjectId) in the resolved policy, not the name', () => {
        const nsr = make_nsr('object-id-123', true);
        const req = make_archive_req('nsr-object-id-123', '/path', { 'nsr-object-id-123': nsr });
        const result = resolve_archive_policy(req);
        expect(result.deep_archive_resource.resource).toBe('object-id-123');
    });
});
