/* Copyright (C) 2026 NooBaa */
'use strict';

const deep_archive_utils = require('../../sdk/deep_archive_utils');
const NamespaceDeepArchive = require('../../sdk/namespace_deep_archive');
const s3_utils = require('../../endpoint/s3/s3_utils');
const { S3Error } = require('../../endpoint/s3/s3_errors');

const {
    XATTR_RESTORE_ONGOING,
    XATTR_RESTORE_EXPIRY,
    XATTR_RESTORE_DAYS,
    XATTR_STORAGE_CLASS,
    XATTR_RESTORE_ONGOING_DB,
    XATTR_RESTORE_DAYS_DB,
    merge_xattr,
    merge_db_xattr,
    parse_s3_restore_header,
    compute_restore_expiry,
    finalize_restore_xattr_patch,
    clear_restore_xattr_patch,
} = deep_archive_utils;

describe('deep_archive_utils', () => {
    it('parse_s3_restore_header detects ongoing restore', () => {
        expect(parse_s3_restore_header('ongoing-request="true"')).toEqual({ ongoing: true, ready: false });
        expect(parse_s3_restore_header(undefined)).toEqual({ ongoing: true, ready: false });
    });

    it('parse_s3_restore_header detects completed restore', () => {
        const header = 'ongoing-request="false", expiry-date="Fri, 23 Dec 2012 00:00:00 GMT"';
        expect(parse_s3_restore_header(header)).toEqual({ ongoing: false, ready: true });
    });

    it('compute_restore_expiry adds Days to now', () => {
        const now = new Date('2026-01-01T00:00:00.000Z');
        const expiry = compute_restore_expiry(3, now);
        expect(expiry.toISOString()).toBe('2026-01-04T00:00:00.000Z');
    });

    it('merge_xattr preserves existing keys', () => {
        const merged = merge_xattr(
            { [XATTR_STORAGE_CLASS]: 'DEEP_ARCHIVE', other: 'keep' },
            { [XATTR_RESTORE_ONGOING]: 'true' }
        );
        expect(merged[XATTR_STORAGE_CLASS]).toBe('DEEP_ARCHIVE');
        expect(merged.other).toBe('keep');
        expect(merged[XATTR_RESTORE_ONGOING]).toBe('true');
    });

    it('merge_db_xattr converts keys for MDStore', () => {
        const db = merge_db_xattr(
            { [XATTR_STORAGE_CLASS.replace(/\./g, '@')]: 'DEEP_ARCHIVE' },
            { [XATTR_RESTORE_ONGOING]: 'true' }
        );
        expect(db[XATTR_RESTORE_ONGOING_DB]).toBe('true');
        expect(db[XATTR_STORAGE_CLASS.replace(/\./g, '@')]).toBe('DEEP_ARCHIVE');
    });

    it('finalize_restore_xattr_patch sets expiry and clears ongoing/days', () => {
        const now = new Date('2026-01-01T00:00:00.000Z');
        const patch = finalize_restore_xattr_patch(2, now);
        expect(patch[XATTR_RESTORE_EXPIRY]).toBe('2026-01-03T00:00:00.000Z');
        expect(patch[XATTR_RESTORE_ONGOING]).toBe('');
        expect(patch[XATTR_RESTORE_DAYS]).toBe('');
    });

    it('clear_restore_xattr_patch clears all restore fields', () => {
        const patch = clear_restore_xattr_patch();
        expect(patch[XATTR_RESTORE_ONGOING]).toBe('');
        expect(patch[XATTR_RESTORE_EXPIRY]).toBe('');
        expect(patch[XATTR_RESTORE_DAYS]).toBe('');
    });
});

describe('NamespaceDeepArchive.restore_object', () => {
    function make_ns({ md, restoreObjectImpl } = {}) {
        const update_calls = [];
        const object_sdk = {
            rpc_client: {
                object: {
                    update_object_md: jest.fn(async params => {
                        update_calls.push(params);
                    }),
                },
            },
        };
        const namespace_nb = {
            read_object_md: jest.fn(async () => ({
                key: 'obj',
                bucket: 'bucket',
                storage_class: s3_utils.STORAGE_CLASS_DEEP_ARCHIVE,
                xattr: {
                    [XATTR_STORAGE_CLASS]: s3_utils.STORAGE_CLASS_DEEP_ARCHIVE,
                    ...(md?.xattr || {}),
                },
                ...md,
            })),
        };
        const deep_archive_ns = {
            bucket: 'archive-bucket',
            s3: {
                restoreObject: jest.fn(restoreObjectImpl || (async () => ({}))),
            },
        };
        const ns = new NamespaceDeepArchive({ deep_archive_ns, namespace_nb, stats: null });
        return { ns, object_sdk, update_calls, deep_archive_ns, namespace_nb };
    }

    it('starts restore with days xattr and merges storage-class', async () => {
        const { ns, object_sdk, update_calls, deep_archive_ns } = make_ns();
        const result = await ns.restore_object({ bucket: 'bucket', key: 'obj', days: 5 }, object_sdk);

        expect(result).toEqual({ accepted: true });
        expect(deep_archive_ns.s3.restoreObject).toHaveBeenCalledWith(expect.objectContaining({
            Bucket: 'archive-bucket',
            Key: 'obj',
            RestoreRequest: { Days: 5 },
        }));
        expect(update_calls).toHaveLength(1);
        expect(update_calls[0].xattr[XATTR_STORAGE_CLASS]).toBe(s3_utils.STORAGE_CLASS_DEEP_ARCHIVE);
        expect(update_calls[0].xattr[XATTR_RESTORE_ONGOING]).toBe('true');
        expect(update_calls[0].xattr[XATTR_RESTORE_DAYS]).toBe('5');
        expect(update_calls[0].xattr[XATTR_RESTORE_EXPIRY]).toBe('');
    });

    it('extends expiry when already restored', async () => {
        const future = new Date();
        future.setDate(future.getDate() + 2);
        const { ns, object_sdk, update_calls, deep_archive_ns } = make_ns({
            md: {
                xattr: {
                    [XATTR_STORAGE_CLASS]: s3_utils.STORAGE_CLASS_DEEP_ARCHIVE,
                    [XATTR_RESTORE_ONGOING]: '',
                    [XATTR_RESTORE_EXPIRY]: future.toISOString(),
                },
            },
        });

        const result = await ns.restore_object({ bucket: 'bucket', key: 'obj', days: 7 }, object_sdk);
        expect(result).toEqual({ accepted: false });
        expect(deep_archive_ns.s3.restoreObject).not.toHaveBeenCalled();
        expect(update_calls).toHaveLength(1);
        expect(update_calls[0].xattr[XATTR_STORAGE_CLASS]).toBe(s3_utils.STORAGE_CLASS_DEEP_ARCHIVE);
        expect(update_calls[0].xattr[XATTR_RESTORE_ONGOING]).toBe('');
        expect(update_calls[0].xattr[XATTR_RESTORE_EXPIRY]).toBeTruthy();
        expect(new Date(update_calls[0].xattr[XATTR_RESTORE_EXPIRY]).getTime()).toBeGreaterThan(Date.now());
    });

    it('throws RestoreAlreadyInProgress when ongoing', async () => {
        const { ns, object_sdk } = make_ns({
            md: {
                xattr: {
                    [XATTR_STORAGE_CLASS]: s3_utils.STORAGE_CLASS_DEEP_ARCHIVE,
                    [XATTR_RESTORE_ONGOING]: 'true',
                },
            },
        });
        await expect(ns.restore_object({ bucket: 'bucket', key: 'obj', days: 1 }, object_sdk))
            .rejects.toMatchObject({ code: S3Error.RestoreAlreadyInProgress.code });
    });

    it('rolls back xattrs when remote RestoreObject fails', async () => {
        const { ns, object_sdk, update_calls } = make_ns({
            restoreObjectImpl: async () => {
                throw new Error('remote failure');
            },
        });
        await expect(ns.restore_object({ bucket: 'bucket', key: 'obj', days: 3 }, object_sdk))
            .rejects.toThrow('remote failure');
        expect(update_calls).toHaveLength(2);
        // first sets ongoing, second rolls back
        expect(update_calls[0].xattr[XATTR_RESTORE_ONGOING]).toBe('true');
        expect(update_calls[1].xattr[XATTR_RESTORE_ONGOING]).toBe('');
        expect(update_calls[1].xattr[XATTR_RESTORE_DAYS]).toBe('');
        expect(update_calls[1].xattr[XATTR_STORAGE_CLASS]).toBe(s3_utils.STORAGE_CLASS_DEEP_ARCHIVE);
    });
});

describe('DeepArchiveRestoreWorker helpers via module behavior', () => {
    const { DeepArchiveRestoreWorker } = require('../../server/bg_services/deep_archive_restore_worker');
    const { MDStore } = require('../../server/object_services/md_store');
    const system_store = require('../../server/system_services/system_store').get_instance();
    const { Readable } = require('stream');

    afterEach(() => {
        jest.restoreAllMocks();
    });

    it('skips objects when archive restore is still ongoing', async () => {
        const worker = new DeepArchiveRestoreWorker({ name: 'test', client: {} });
        jest.spyOn(worker, '_can_run').mockReturnValue(true);
        jest.spyOn(MDStore, 'instance').mockReturnValue({
            find_objects_with_restore_ongoing: async () => ([{
                _id: 'id1',
                bucket: 'bid',
                key: 'obj',
                size: 4,
                xattr: { [XATTR_RESTORE_DAYS_DB]: '2' },
            }]),
            update_object_by_id: jest.fn(),
        });
        system_store.is_finished_initial_load = true;
        system_store.data = {
            systems: [{ _id: 'sys', owner: { _id: 'acc' } }],
            get_by_id: () => ({
                _id: 'bid',
                name: { unwrap: () => 'bucket' },
                archive_policy: {
                    deep_archive_resource: {
                        resource: {
                            _id: 'nsr',
                            connection: {
                                endpoint: 'http://archive',
                                target_bucket: 'ab',
                                access_key: { unwrap: () => 'ak' },
                                secret_key: { unwrap: () => 'sk' },
                            },
                            name: 'archive-nsr',
                        },
                    },
                },
            }),
        };

        const headObject = jest.fn(async () => ({ Restore: 'ongoing-request="true"' }));
        jest.spyOn(worker, '_create_archive_namespace').mockReturnValue({
            bucket: 'ab',
            s3: { headObject },
            read_object_stream: jest.fn(),
        });
        jest.spyOn(worker, '_create_object_sdk').mockReturnValue({});

        const delay = await worker.run_batch();
        expect(headObject).toHaveBeenCalled();
        expect(delay).toBeDefined();
        expect(MDStore.instance().update_object_by_id).not.toHaveBeenCalled();
    });

    it('copies data and finalizes xattrs when archive is ready', async () => {
        const worker = new DeepArchiveRestoreWorker({ name: 'test', client: {} });
        jest.spyOn(worker, '_can_run').mockReturnValue(true);
        const update_object_by_id = jest.fn();
        jest.spyOn(MDStore, 'instance').mockReturnValue({
            find_objects_with_restore_ongoing: async () => ([{
                _id: 'id1',
                bucket: 'bid',
                key: 'obj',
                size: 4,
                content_type: 'text/plain',
                xattr: {
                    [XATTR_RESTORE_ONGOING_DB]: 'true',
                    [XATTR_RESTORE_DAYS_DB]: '2',
                    [XATTR_STORAGE_CLASS.replace(/\./g, '@')]: 'DEEP_ARCHIVE',
                },
            }]),
            update_object_by_id,
        });
        system_store.is_finished_initial_load = true;
        system_store.data = {
            systems: [{ _id: 'sys', owner: { _id: 'acc' } }],
            get_by_id: () => ({
                _id: 'bid',
                name: { unwrap: () => 'bucket' },
                archive_policy: {
                    deep_archive_resource: {
                        resource: {
                            _id: 'nsr',
                            connection: {
                                endpoint: 'http://archive',
                                target_bucket: 'ab',
                                access_key: { unwrap: () => 'ak' },
                                secret_key: { unwrap: () => 'sk' },
                            },
                            name: 'archive-nsr',
                        },
                    },
                },
            }),
        };

        const upload_object = jest.fn(async () => ({}));
        jest.spyOn(worker, '_create_archive_namespace').mockReturnValue({
            bucket: 'ab',
            s3: {
                headObject: async () => ({
                    Restore: 'ongoing-request="false"',
                    ContentLength: 4,
                    ContentType: 'text/plain',
                    ETag: '"abcd"',
                }),
            },
            read_object_stream: async () => Readable.from([Buffer.from('data')]),
        });
        jest.spyOn(worker, '_create_object_sdk').mockReturnValue({});
        const NamespaceNB = require('../../sdk/namespace_nb');
        jest.spyOn(NamespaceNB.prototype, 'upload_object').mockImplementation(upload_object);

        const delay = await worker.run_batch();
        expect(upload_object).toHaveBeenCalledWith(
            expect.objectContaining({
                bucket: 'bucket',
                key: s3_utils.RESTORED_OBJECTS_DIR + 'obj',
                size: 4,
            }),
            expect.anything()
        );
        expect(update_object_by_id).toHaveBeenCalled();
        const set_xattr = update_object_by_id.mock.calls[0][1].xattr;
        expect(set_xattr[XATTR_RESTORE_ONGOING_DB]).toBe('');
        expect(set_xattr[XATTR_RESTORE_DAYS_DB]).toBe('');
        expect(set_xattr[XATTR_RESTORE_EXPIRY.replace(/\./g, '@')]).toBeTruthy();
        expect(delay).toBeDefined();
    });
});

describe('DeepArchiveRestoreExpiryWorker', () => {
    const { DeepArchiveRestoreExpiryWorker } = require('../../server/bg_services/deep_archive_restore_expiry_worker');
    const { MDStore } = require('../../server/object_services/md_store');

    afterEach(() => {
        jest.restoreAllMocks();
    });

    it('clears restore xattrs and sets data_expired on restored copy', async () => {
        const worker = new DeepArchiveRestoreExpiryWorker({ name: 'test', client: {} });
        jest.spyOn(worker, '_can_run').mockReturnValue(true);
        const update_object_by_id = jest.fn();
        jest.spyOn(MDStore, 'instance').mockReturnValue({
            find_objects_with_restore_expired: async () => ([{
                _id: 'arch1',
                bucket: 'bid',
                key: 'obj',
                xattr: {
                    [XATTR_RESTORE_EXPIRY.replace(/\./g, '@')]: '2020-01-01T00:00:00.000Z',
                    [XATTR_STORAGE_CLASS.replace(/\./g, '@')]: 'DEEP_ARCHIVE',
                },
            }]),
            find_object_latest: async () => ({ _id: 'rest1', key: s3_utils.RESTORED_OBJECTS_DIR + 'obj' }),
            update_object_by_id,
        });

        await worker.run_batch();
        expect(update_object_by_id).toHaveBeenCalledTimes(2);
        expect(update_object_by_id.mock.calls[0][0]).toBe('arch1');
        expect(update_object_by_id.mock.calls[0][1].xattr[XATTR_RESTORE_ONGOING_DB]).toBe('');
        expect(update_object_by_id.mock.calls[1][0]).toBe('rest1');
        expect(update_object_by_id.mock.calls[1][1].data_expired).toBeInstanceOf(Date);
    });
});

describe('ObjectsReclaimer data_expired path', () => {
    const { ObjectsReclaimer } = require('../../server/bg_services/objects_reclaimer');
    const { MDStore } = require('../../server/object_services/md_store');
    const map_deleter = require('../../server/object_services/map_deleter');

    afterEach(() => {
        jest.restoreAllMocks();
    });

    it('reclaims data_expired restored copies and resets parent restore xattrs', async () => {
        const reclaimer = new ObjectsReclaimer({ name: 'test', client: {} });
        jest.spyOn(reclaimer, '_can_run').mockReturnValue(true);
        const update_object_by_id = jest.fn();
        const update_objects_by_ids = jest.fn();
        jest.spyOn(MDStore, 'instance').mockReturnValue({
            find_unreclaimed_objects: async () => [],
            find_objects_with_data_expired: async () => ([{
                _id: 'rest1',
                bucket: 'bid',
                key: s3_utils.RESTORED_OBJECTS_DIR + 'obj',
                data_expired: new Date(),
            }]),
            find_object_latest: async () => ({
                _id: 'arch1',
                key: 'obj',
                xattr: {
                    [XATTR_RESTORE_EXPIRY.replace(/\./g, '@')]: '2020-01-01T00:00:00.000Z',
                },
            }),
            update_object_by_id,
            update_objects_by_ids,
        });
        jest.spyOn(map_deleter, 'delete_object_mappings').mockResolvedValue();

        await reclaimer.run_batch();
        expect(map_deleter.delete_object_mappings).toHaveBeenCalled();
        expect(update_object_by_id).toHaveBeenCalledWith('arch1', expect.objectContaining({
            xattr: expect.objectContaining({
                [XATTR_RESTORE_ONGOING_DB]: '',
            }),
        }));
        expect(update_objects_by_ids).toHaveBeenCalledWith(['rest1'], expect.objectContaining({
            reclaimed: expect.any(Date),
        }));
    });
});
