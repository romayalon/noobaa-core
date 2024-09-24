/* Copyright (C) 2016 NooBaa */
'use strict';

const path = require('path');
const P = require('../../../util/promise');
const fs_utils = require('../../../util/fs_utils');
const NamespaceFS = require('../../../sdk/namespace_fs');
const buffer_utils = require('../../../util/buffer_utils');
const { TMP_PATH } = require('../../system_tests/test_utils');
const { crypto_random_string } = require('../../../util/string_utils');
const endpoint_stats_collector = require('../../../sdk/endpoint_stats_collector');

function make_dummy_object_sdk(nsfs_config, uid, gid) {
    return {
        requesting_account: {
            nsfs_account_config: nsfs_config && {
                uid: uid || process.getuid(),
                gid: gid || process.getgid(),
                backend: '',
            }
        },
        abort_controller: new AbortController(),
        throw_if_aborted() {
            if (this.abort_controller.signal.aborted) throw new Error('request aborted signal');
        }
    };
}

const DUMMY_OBJECT_SDK = make_dummy_object_sdk(true);
describe('test versioning concurrency', () => {
    const tmp_fs_path = path.join(TMP_PATH, 'test_versioning_concurrency');

    const nsfs = new NamespaceFS({
        bucket_path: tmp_fs_path,
        bucket_id: '1',
        namespace_resource_id: undefined,
        access_mode: undefined,
        versioning: 'ENABLED',
        force_md5_etag: false,
        stats: endpoint_stats_collector.instance(),
    });

    beforeEach(async () => {
        await fs_utils.create_fresh_path(tmp_fs_path);
    });

    // afterEach(async () => {
    //     await fs_utils.folder_delete(tmp_fs_path);
    // });

    it('multiple puts of the same key', async () => {
        const bucket = 'bucket1';
        const key = 'key1';
        for (let i = 0; i < 5; i++) {
            const random_data = Buffer.from(String(i));
            const body = buffer_utils.buffer_to_read_stream(random_data);
            nsfs.upload_object({ bucket: bucket, key: key, source_stream: body }, DUMMY_OBJECT_SDK).catch(err => console.log('multiple puts of the same key error - ', err));
        }
        await P.delay(1000);
        const versions = await nsfs.list_object_versions({ bucket: bucket }, DUMMY_OBJECT_SDK);
        expect(versions.objects.length).toBe(5);
    });

    it('multiple delete version id and key', async () => {
        const bucket = 'bucket1';
        const key = 'key2';
        const versions_arr = [];
        // upload 5 versions of key2
        for (let i = 0; i < 5; i++) {
            const random_data = Buffer.from(String(i));
            const body = buffer_utils.buffer_to_read_stream(random_data);
            const res = await nsfs.upload_object({ bucket: bucket, key: key, source_stream: body }, DUMMY_OBJECT_SDK).catch(err => console.log('put error - ', err));
            versions_arr.push(res.etag);
        }
        const mid_version_id = versions_arr[3];
        const number_of_successful_operations = [];
        for (let i = 0; i < 15; i++) {
            nsfs.delete_object({ bucket: bucket, key: key, version_id: mid_version_id }, DUMMY_OBJECT_SDK)
                .then(res => number_of_successful_operations.push(res))
                .catch(err => console.log('delete the same key & version id error - ', err));
        }
        await P.delay(1000);
        expect(number_of_successful_operations.length).toBe(15);
    });

    // same as s3tests_boto3/functional/test_s3.py::test_versioning_concurrent_multi_object_delete, 
    // this test has a bug, it tries to create the bucket twice and fails
    // https://github.com/ceph/s3-tests/blob/master/s3tests_boto3/functional/test_s3.py#L1642
    // see - https://github.com/ceph/s3-tests/issues/588
    it('concurrent multi object delete', async () => {
        const bucket = 'bucket1';
        const concurrency_num = 10;
        const delete_objects_arr = [];
        for (let i = 0; i < concurrency_num; i++) {
            const key = `key${i}`;
            const random_data = Buffer.from(String(crypto_random_string(7)));
            const body = buffer_utils.buffer_to_read_stream(random_data);
            const res = await nsfs.upload_object({ bucket: bucket, key: key, source_stream: body }, DUMMY_OBJECT_SDK);
            delete_objects_arr.push({ key: key, version_id: res.version_id });
        }
        const versions = await nsfs.list_object_versions({ bucket: bucket }, DUMMY_OBJECT_SDK);

        for (const { key, version_id } of delete_objects_arr) {
            const found = versions.objects.find(object => object.key === key && object.version_id === version_id);
            expect(found).toBeDefined();
        }

        const delete_responses = [];
        const delete_errors = [];

        for (let i = 0; i < concurrency_num; i++) {
            nsfs.delete_multiple_objects({ bucket, objects: delete_objects_arr }, DUMMY_OBJECT_SDK)
                .then(res => delete_responses.push(res))
                .catch(err => delete_errors.push(err));
        }
        await P.delay(5000);
        expect(delete_responses.length).toBe(concurrency_num);
        for (const res of delete_responses) {
            expect(res.length).toBe(concurrency_num);
            for (const single_delete_res of res) {
                expect(single_delete_res.err_message).toBe(undefined);
            }
        }
        const list_res = await nsfs.list_objects({ bucket: bucket }, DUMMY_OBJECT_SDK);
        expect(list_res.objects.length).toBe(0);
    }, 8000);

    it('concurrent puts & delete latest objects', async () => {
        const bucket = 'bucket1';
        const key = 'key3';
        const upload_res_arr = [];
        const delete_res_arr = [];
        const delete_err_arr = [];
        const upload_err_arr = [];
        const initial_num_of_versions = 3;
        for (let i = 0; i < initial_num_of_versions; i++) {
            const random_data = Buffer.from(String(crypto_random_string(7)));
            const body = buffer_utils.buffer_to_read_stream(random_data);
            await nsfs.upload_object({ bucket: bucket, key: key, source_stream: body }, DUMMY_OBJECT_SDK);
        }
        const num_of_concurrency = 2;
        for (let i = 0; i < num_of_concurrency; i++) {
            const random_data = Buffer.from(String(crypto_random_string(7)));
            const body = buffer_utils.buffer_to_read_stream(random_data);
            nsfs.upload_object({ bucket: bucket, key: key, source_stream: body }, DUMMY_OBJECT_SDK)
                .then(res => {
                    upload_res_arr.push(res.etag);
                }).catch(err => {
                    upload_err_arr.push(err);
                });
            nsfs.delete_object({ bucket: bucket, key: key }, DUMMY_OBJECT_SDK)
                .then(res => {
                    delete_res_arr.push(res.created_version_id);
                }).catch(err => {
                    delete_err_arr.push(err);
                });

        }
        await P.delay(2000);
        expect(upload_res_arr).toHaveLength(num_of_concurrency);
        expect(delete_res_arr).toHaveLength(num_of_concurrency);
        expect(upload_err_arr).toHaveLength(0);
        expect(delete_err_arr).toHaveLength(0);
        const versions = await nsfs.list_object_versions({ bucket: bucket }, DUMMY_OBJECT_SDK);
        expect(versions.objects.length).toBe(initial_num_of_versions + 2 * num_of_concurrency);
        const num_of_delete_markers = (versions.objects.filter(version => version.delete_marker === true)).length;
        expect(num_of_delete_markers).toBe(num_of_concurrency);
        const num_of_latest_versions = (versions.objects.filter(version => version.is_latest === true)).length;
        expect(num_of_latest_versions).toBe(1);
    }, 6000);

    it('concurrent puts & delete objects by version id', async () => {
        const bucket = 'bucket1';
        const key = 'key4';
        const versions_to_delete = [];
        const upload_res_arr = [];
        const delete_res_arr = [];
        const delete_err_arr = [];
        const upload_err_arr = [];
        const initial_num_of_versions = 3;
        for (let i = 0; i < initial_num_of_versions; i++) {
            const random_data = Buffer.from(String(crypto_random_string(7)));
            const body = buffer_utils.buffer_to_read_stream(random_data);
            const res = await nsfs.upload_object({ bucket: bucket, key: key, source_stream: body }, DUMMY_OBJECT_SDK);
            versions_to_delete.push(res.version_id);
        }
        const num_of_concurrency = 3;
        for (let i = 0; i < num_of_concurrency; i++) {
            const random_data = Buffer.from(String(crypto_random_string(7)));
            const body = buffer_utils.buffer_to_read_stream(random_data);
            nsfs.upload_object({ bucket: bucket, key: key, source_stream: body }, DUMMY_OBJECT_SDK)
                .then(res => {
                    upload_res_arr.push(res.etag);
                }).catch(err => {
                    upload_err_arr.push(err);
                });
            nsfs.delete_object({ bucket: bucket, key: key, version_id: versions_to_delete[i] }, DUMMY_OBJECT_SDK)
                .then(res => {
                    delete_res_arr.push(res.deleted_version_id);
                }).catch(err => {
                    delete_err_arr.push(err);
                });

        }
        await P.delay(2000);
        expect(upload_res_arr).toHaveLength(num_of_concurrency);
        expect(delete_res_arr).toHaveLength(num_of_concurrency);
        expect(upload_err_arr).toHaveLength(0);
        expect(delete_err_arr).toHaveLength(0);
        const versions = await nsfs.list_object_versions({ bucket: bucket }, DUMMY_OBJECT_SDK);
        expect(versions.objects.length).toBe(num_of_concurrency);
        const num_of_delete_markers = (versions.objects.filter(version => version.delete_marker === true)).length;
        expect(num_of_delete_markers).toBe(0);
        const num_of_latest_versions = (versions.objects.filter(version => version.is_latest === true)).length;
        expect(num_of_latest_versions).toBe(1);
    }, 6000);

    it('concurrent delete objects by version id/latest', async () => {
        const bucket = 'bucket1';
        const key = 'key5';
        const versions_to_delete = [];
        const delete_ver_res_arr = [];
        const delete_ver_err_arr = [];
        const delete_res_arr = [];
        const delete_err_arr = [];
        const initial_num_of_versions = 1;
        for (let i = 0; i < initial_num_of_versions; i++) {
            const random_data = Buffer.from(String(crypto_random_string(7)));
            const body = buffer_utils.buffer_to_read_stream(random_data);
            const res = await nsfs.upload_object({ bucket: bucket, key: key, source_stream: body }, DUMMY_OBJECT_SDK);
            versions_to_delete.push(res.version_id);
        }
        const num_of_concurrency = initial_num_of_versions;
        for (let i = 0; i < num_of_concurrency; i++) {
            console.log('ROMY versions_to_delete[num_of_concurrency - i - 1]', versions_to_delete[num_of_concurrency - i - 1]);
            nsfs.delete_object({ bucket: bucket, key: key, version_id: versions_to_delete[num_of_concurrency - i - 1] }, DUMMY_OBJECT_SDK)
                .then(res => {
                    delete_ver_res_arr.push(res.deleted_version_id);
                }).catch(err => {
                    delete_ver_err_arr.push(err);
                });
            nsfs.delete_object({ bucket: bucket, key: key }, DUMMY_OBJECT_SDK)
                .then(res => {
                    delete_res_arr.push(res);
                }).catch(err => {
                    delete_err_arr.push(err);
                });

        }
        await P.delay(5000);
        expect(delete_ver_res_arr).toHaveLength(num_of_concurrency);
        expect(delete_res_arr).toHaveLength(num_of_concurrency);
        expect(delete_ver_err_arr).toHaveLength(0);
        expect(delete_err_arr).toHaveLength(0);
        const versions = await nsfs.list_object_versions({ bucket: bucket }, DUMMY_OBJECT_SDK);
        console.log('ROMY versions', versions);

        expect(versions.objects.length).toBe(num_of_concurrency);
        const num_of_delete_markers = (versions.objects.filter(version => version.delete_marker === true)).length;
        expect(num_of_delete_markers).toBe(num_of_concurrency);
        const num_of_latest_versions = (versions.objects.filter(version => version.is_latest === true)).length;
        expect(num_of_latest_versions).toBe(1);
    }, 6000);

    it('nested key - concurrent delete multiple objects', async () => {
        const bucket = 'bucket1';
        const key = 'dir2/key2';
        const concurrency_num = 10;
        const upload_res_arr = [];
        const delete_res_arr = [];
        const delete_err_arr = [];

        for (let i = 0; i < concurrency_num; i++) {
            const random_data = Buffer.from(String(crypto_random_string(7)));
            const body = buffer_utils.buffer_to_read_stream(random_data);
            const res = await nsfs.upload_object({ bucket, key, source_stream: body }, DUMMY_OBJECT_SDK);
            upload_res_arr.push({ key, version_id: res.version_id });
        }
        const versions = await nsfs.list_object_versions({ bucket: bucket }, DUMMY_OBJECT_SDK);
        for (const { version_id } of upload_res_arr) {
            const found = versions.objects.find(object => object.key === key && object.version_id === version_id);
            expect(found).toBeDefined();
        }

        for (let i = 0; i < concurrency_num; i++) {
            nsfs.delete_multiple_objects({ bucket, objects: upload_res_arr }, DUMMY_OBJECT_SDK)
                .then(res => delete_res_arr.push(res))
                .catch(err => delete_err_arr.push(err));
        }

        await P.delay(2000);
        expect(delete_res_arr.length).toBe(concurrency_num);
        for (const res of delete_res_arr) {
            expect(res.length).toBe(concurrency_num);
            for (const single_delete_res of res) {
                expect(single_delete_res.err_message).toBe(undefined);
            }
        }
        const list_res = await nsfs.list_objects({ bucket: bucket }, DUMMY_OBJECT_SDK);
        expect(list_res.objects.length).toBe(0);
    }, 8000);


    it('nested key - concurrent puts & deletes', async () => {
        const bucket = 'bucket1';
        const key = 'dir3/key3';
        const upload_res_arr = [];
        const delete_res_arr = [];
        const delete_err_arr = [];
        const upload_err_arr = [];
        for (let i = 0; i < 5; i++) {
            const random_data = Buffer.from(String(crypto_random_string(7)));
            const body = buffer_utils.buffer_to_read_stream(random_data);
            nsfs.upload_object({ bucket: bucket, key: key, source_stream: body }, DUMMY_OBJECT_SDK)
                .then(res => {
                    upload_res_arr.push(res.etag);
                    nsfs.delete_object({ bucket: bucket, key: key, version_id: res.version_id }, DUMMY_OBJECT_SDK)
                        .then(delete_res => delete_res_arr.push(delete_res))
                        .catch(err => delete_err_arr.push(err));
                }).catch(err => {
                    upload_err_arr.push(err);
                });
        }
        await P.delay(3000);
        expect(upload_res_arr).toHaveLength(5);
        expect(upload_err_arr).toHaveLength(0);
        expect(delete_res_arr).toHaveLength(5);
        expect(delete_err_arr).toHaveLength(0);
    }, 6000);

    it('concurrent puts & list versions', async () => {
        const bucket = 'bucket1';
        const versions_arr = [];
        const upload_res_arr = [];
        const list_res_arr = [];
        const list_err_arr = [];
        const upload_err_arr = [];
        const initial_num_of_versions = 20;
        const initial_num_of_objects = 20;

        for (let i = 0; i < initial_num_of_objects; i++) {
            const key = 'key_put' + i;
            for (let j = 0; j < initial_num_of_versions; j++) {
                const random_data = Buffer.from(String(crypto_random_string(7)));
                const body = buffer_utils.buffer_to_read_stream(random_data);
                const res = await nsfs.upload_object({ bucket: bucket, key: key, source_stream: body }, DUMMY_OBJECT_SDK);
                versions_arr.push(res.version_id);
            }
        }
        const num_of_concurrency = 20;
        for (let i = 0; i < num_of_concurrency; i++) {
            const key = 'key_put' + i;
            const random_data = Buffer.from(String(crypto_random_string(7)));
            const body = buffer_utils.buffer_to_read_stream(random_data);
            nsfs.upload_object({ bucket: bucket, key: key, source_stream: body }, DUMMY_OBJECT_SDK)
                .then(res => {
                    upload_res_arr.push(res.etag);
                }).catch(err => {
                    upload_err_arr.push(err);
                });
            nsfs.list_object_versions({ bucket: bucket }, DUMMY_OBJECT_SDK)
                .then(res => {
                    list_res_arr.push(res);
                }).catch(err => {
                    list_err_arr.push(err);
                });

        }
        await P.delay(2000);
        expect(upload_res_arr).toHaveLength(num_of_concurrency);
        expect(list_res_arr).toHaveLength(num_of_concurrency);
        expect(upload_err_arr).toHaveLength(0);
        expect(list_err_arr).toHaveLength(0);
        const versions = await nsfs.list_object_versions({ bucket: bucket }, DUMMY_OBJECT_SDK);
        expect(versions.objects.length).toBe(initial_num_of_objects * initial_num_of_versions + num_of_concurrency);
        const num_of_delete_markers = (versions.objects.filter(version => version.delete_marker === true)).length;
        expect(num_of_delete_markers).toBe(0);
        const num_of_latest_versions = (versions.objects.filter(version => version.is_latest === true)).length;
        expect(num_of_latest_versions).toBe(initial_num_of_objects);
    }, 6000);
});
