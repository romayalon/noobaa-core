/* Copyright (C) 2016 NooBaa */
'use strict';

// disabling init_rand_seed as it takes longer than the actual test execution
process.env.DISABLE_INIT_RANDOM_SEED = 'true';
const { convert_expiry_rule_to_gpfs_ilm_policy, convert_filter_to_gpfs_ilm_policy } = require('../../../manage_nsfs/nc_lifecycle');

const new_umask = process.env.NOOBAA_ENDPOINT_UMASK || 0o000;
const old_umask = process.umask(new_umask);
console.log('test_nc_lifecycle_cli: replacing old umask: ', old_umask.toString(8), 'with new umask: ', new_umask.toString(8));

describe('convert_expiry_rule_to_gpfs_ilm_policy unit tests', () => {
    const days = 3;
    const lifecycle_rule_base = {
        id: 'abort mpu and expire all objects after 3 days',
        status: 'Enabled',
        filter: { 'prefix': '' },
        abort_incomplete_multipart_upload: {
            days_after_initiation: days
        }
    };
    it('convert_expiry_rule_to_gpfs_ilm_policy - expiry days', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, expiration: { days: days } };
        const ilm_policy = convert_expiry_rule_to_gpfs_ilm_policy(lifecycle_rule);
        expect(ilm_policy).toBe(get_expected_ilm_expiry_days(days));
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - expiry date', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, expiration: { date: Date.now() } };
        const ilm_policy = convert_expiry_rule_to_gpfs_ilm_policy(lifecycle_rule);
        expect(ilm_policy).toBe('');
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - no expiry', () => {
        const lifecycle_rule = lifecycle_rule_base;
        const ilm_policy = convert_expiry_rule_to_gpfs_ilm_policy(lifecycle_rule);
        expect(ilm_policy).toBe('');
    });
});

describe('convert_expiry_rule_to_gpfs_ilm_policy unit tests', () => {
    const bucket_path = 'mock_bucket_path';
    const prefix = 'mock_prefix';
    const days = 3;
    const tags = [{ key: 'key1', value: 'val1' }, { key: 'key3', value: 'val4' }];
    const object_size_greater_than = 5;
    const object_size_less_than = 10;
    const mock_bucket_json = { _id: 'mock_bucket_id', name: 'mock_bucket_name', path: bucket_path };
    const lifecycle_rule_base = {
        id: 'abort mpu and expire all objects after 3 days',
        status: 'Enabled',
        abort_incomplete_multipart_upload: {
            days_after_initiation: days
        },
        expiration: { days: days }
    };

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter empty', () => {
        const lifecycle_rule = lifecycle_rule_base;
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        expect(ilm_policy).toBe('');
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - inline prefix', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, prefix };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        expect(ilm_policy).toBe(get_expected_ilm_prefix(bucket_path, prefix));
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with prefix', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { prefix } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        expect(ilm_policy).toBe(get_expected_ilm_prefix(bucket_path, prefix));
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with size gt', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { object_size_greater_than } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        expect(ilm_policy).toBe(get_expected_ilm_size_greater_than(object_size_greater_than));
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with size lt', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { object_size_less_than } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        expect(ilm_policy).toBe(get_expected_ilm_size_less_than(object_size_less_than));
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with tags', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { tags } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        expect(ilm_policy).toBe(get_expected_ilm_tags(tags));
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with prefix + size gt', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { prefix, object_size_greater_than } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        const expected_ilm_filter = get_expected_ilm_prefix(bucket_path, prefix) +
            get_expected_ilm_size_greater_than(object_size_greater_than);
        expect(ilm_policy).toBe(expected_ilm_filter);
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with prefix + size lt', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { prefix, object_size_less_than } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        const expected_ilm_filter = get_expected_ilm_prefix(bucket_path, prefix) + get_expected_ilm_size_less_than(object_size_less_than);
        expect(ilm_policy).toBe(expected_ilm_filter);
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with prefix + tags', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { prefix, tags } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        const expected_ilm_filter = get_expected_ilm_prefix(bucket_path, prefix) + get_expected_ilm_tags(tags);
        expect(ilm_policy).toBe(expected_ilm_filter);
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with size gt + size lt', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { object_size_less_than, object_size_greater_than } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        const expected_ilm_filter = get_expected_ilm_size_greater_than(object_size_greater_than) +
            get_expected_ilm_size_less_than(object_size_less_than);
        expect(ilm_policy).toBe(expected_ilm_filter);
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with size gt + tags', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { object_size_greater_than, tags } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        const expected_ilm_filter = get_expected_ilm_size_greater_than(object_size_greater_than) +
            get_expected_ilm_tags(tags);
        expect(ilm_policy).toBe(expected_ilm_filter);
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with size lt + tags', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { object_size_less_than, tags } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        const expected_ilm_filter = get_expected_ilm_size_less_than(object_size_less_than) +
            get_expected_ilm_tags(tags);
        expect(ilm_policy).toBe(expected_ilm_filter);
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with prefix + size gt + size lt', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { prefix, object_size_greater_than, object_size_less_than } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        const expected_ilm_filter = get_expected_ilm_prefix(bucket_path, prefix) +
            get_expected_ilm_size_greater_than(object_size_greater_than) +
            get_expected_ilm_size_less_than(object_size_less_than);
        expect(ilm_policy).toBe(expected_ilm_filter);
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with prefix + size lt + tags', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { prefix, object_size_less_than, tags } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        const expected_ilm_filter = get_expected_ilm_prefix(bucket_path, prefix) +
            get_expected_ilm_size_less_than(object_size_less_than) +
            get_expected_ilm_tags(tags);
        expect(ilm_policy).toBe(expected_ilm_filter);
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with size gt + size lt + tags', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { object_size_greater_than, object_size_less_than, tags } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        const expected_ilm_filter = get_expected_ilm_size_greater_than(object_size_greater_than) +
            get_expected_ilm_size_less_than(object_size_less_than) +
            get_expected_ilm_tags(tags);
        expect(ilm_policy).toBe(expected_ilm_filter);
    });

    it('convert_expiry_rule_to_gpfs_ilm_policy - filter with prefix + size gt + size lt + tags', () => {
        const lifecycle_rule = { ...lifecycle_rule_base, filter: { prefix, object_size_greater_than, object_size_less_than, tags } };
        const ilm_policy = convert_filter_to_gpfs_ilm_policy(lifecycle_rule, mock_bucket_json);
        const expected_ilm_filter = get_expected_ilm_prefix(bucket_path, prefix) +
            get_expected_ilm_size_greater_than(object_size_greater_than) +
            get_expected_ilm_size_less_than(object_size_less_than) +
            get_expected_ilm_tags(tags);
        expect(ilm_policy).toBe(expected_ilm_filter);
    });
});

/**
 * get_expected_ilm_expiry_days returns the expected ilm policy of expiry days
 * @param {number} days 
 * @returns {String}
 */
function get_expected_ilm_expiry_days(days) {
    return `AND mod_age > ${days}\n`;
}

function get_expected_ilm_prefix(bucket_path, prefix) {
    return `AND PATH_NAME LIKE ${bucket_path}/${prefix}%\n`;
}

function get_expected_ilm_size_greater_than(size) {
    return `AND FILE_SIZE > ${size}\n`;
}

function get_expected_ilm_size_less_than(size) {
    return `AND FILE_SIZE < ${size}\n`;
}

function get_expected_ilm_tags(tags) {
    return tags.map(tag => `AND XATTR('user.noobaa.tag.${tag.key}') LIKE ${tag.value}\n`).join('');
}
