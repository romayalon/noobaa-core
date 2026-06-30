/* Copyright (C) 2016 NooBaa */
'use strict';

const BucketSpaceNB = require('../../../../sdk/bucketspace_nb');
const s3_utils = require('../../../../endpoint/s3/s3_utils');

function create_mock_rpc_client(bucket_response) {
    return {
        bucket: {
            read_bucket: async () => bucket_response,
        },
    };
}

describe('BucketSpaceNB', () => {
    describe('read_bucket', () => {
        it('should return supported_storage_classes with GLACIER when bucket has archive_policy', async () => {
            const bucket_response = {
                name: 'test-bucket',
                archive_policy: {
                    deep_archive_resource: {
                        resource: 'my-archive-nsr',
                        path: '/archive',
                    }
                }
            };
            const bs = new BucketSpaceNB({
                rpc_client: create_mock_rpc_client(bucket_response),
            });
            const result = await bs.read_bucket({ name: 'test-bucket' });
            expect(result.supported_storage_classes).toEqual([
                s3_utils.STORAGE_CLASS_STANDARD,
                s3_utils.STORAGE_CLASS_GLACIER,
                s3_utils.STORAGE_CLASS_DEEP_ARCHIVE,
            ]);
        });

        it('should not set supported_storage_classes when bucket has no archive_policy', async () => {
            const bucket_response = {
                name: 'test-bucket',
            };
            const bs = new BucketSpaceNB({
                rpc_client: create_mock_rpc_client(bucket_response),
            });
            const result = await bs.read_bucket({ name: 'test-bucket' });
            expect(result.supported_storage_classes).toBeUndefined();
        });
    });
});
