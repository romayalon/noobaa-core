/* Copyright (C) 2016 NooBaa */
'use strict';

const NoobaaEvent = require('./nc_events_utils').NoobaaEvent;

/**
 * @typedef {{
 *      code?: string, 
 *      message: string, 
 *      http_code: number,
 * }} NooBaaCLIErrorSpec
 */

class NooBaaCLIError extends Error {

    /**
     * @param {NooBaaCLIErrorSpec} error_spec 
     */
    constructor({ code, message, http_code }) {
        super(message); // sets this.message
        this.code = code;
        this.http_code = http_code;
    }

    to_string(detail) {
        const json = {
            error: {
                code: this.code,
                message: this.message,
                detail: detail
            }
        };
        return JSON.stringify(json, null, 2);
    }
}

// See NooBaa CLI error codes docs - TODO: add docs

////////////////////////
//// GENERAL ERRORS ////
////////////////////////


NooBaaCLIError.InternalError = Object.freeze({
    code: 'InternalError',
    message: 'The server encountered an internal error. Please retry the request',
    http_code: 500,
});

NooBaaCLIError.InvalidRequest = Object.freeze({
    code: 'InvalidRequest',
    message: 'The request is invalid',
    http_code: 400,
});

NooBaaCLIError.NotImplemented = Object.freeze({
    code: 'NotImplemented',
    message: 'functionality is not implemented.',
    http_code: 501,
});

NooBaaCLIError.InvalidAction = Object.freeze({
    code: 'InvalidAction',
    message: 'Invalid action, available actions are add, status, update, delete, list',
    http_code: 400,
});

NooBaaCLIError.InvalidArgument = Object.freeze({
    code: 'InvalidArgument',
    message: 'Invalid argument',
    http_code: 400,
});

NooBaaCLIError.InvalidArgumentType = Object.freeze({
    code: 'InvalidArgumentType',
    message: 'Invalid argument type',
    http_code: 400,
});

NooBaaCLIError.InvalidType = Object.freeze({
    code: 'InvalidType',
    message: 'Invalid type, available types are account, bucket or whitelist',
    http_code: 400,
});

NooBaaCLIError.MissingConfigDirPath = Object.freeze({
    code: 'MissingConfigDirPath',
    message: 'Config dir path should not be empty',
    http_code: 400,
});

NooBaaCLIError.InvalidSchema = Object.freeze({
    code: 'InvalidSchema',
    message: 'Schema invalid, please use required properties',
    http_code: 400,
});

NooBaaCLIError.InvalidFilePath = Object.freeze({
    code: 'InvalidFilePath',
    message: 'Invalid file path',
    http_code: 400,
});

NooBaaCLIError.InvalidJSONFile = Object.freeze({
    code: 'InvalidJSONFile',
    message: 'Invalid JSON file',
    http_code: 400,
});

NooBaaCLIError.MissingUpdateProperty = Object.freeze({
    code: 'MissingUpdateProperty',
    message: 'Should have at least one property to update',
    http_code: 400,
});

//////////////////////////////
//// IP WHITE LIST ERRORS ////
//////////////////////////////

NooBaaCLIError.MissingWhiteListIPFlag = Object.freeze({
    code: 'MissingWhiteListIPFlag',
    message: 'Whitelist ips are mandatory, please use the --ips flag',
    http_code: 400,
});

NooBaaCLIError.InvalidWhiteListIPFormat = Object.freeze({
    code: 'InvalidWhiteListIPFormat',
    message: 'Whitelist IP body format is invalid',
    http_code: 400,
});

NooBaaCLIError.WhiteListIPUpdateFailed = Object.freeze({
    code: 'WhiteListIPUpdateFailed',
    message: 'Whitelist ip update failed',
    http_code: 500,
});

////////////////////////
//// ACCOUNT ERRORS ////
////////////////////////

NooBaaCLIError.AccessDenied = Object.freeze({
    code: 'AccessDenied',
    message: 'Account has no permissions to access the bucket',
    http_code: 403,
});

NooBaaCLIError.NoSuchAccountAccessKey = Object.freeze({
    code: 'NoSuchAccountAccessKey',
    message: 'Account does not exist - access key',
    http_code: 404,
});

NooBaaCLIError.NoSuchAccountName = Object.freeze({
    code: 'NoSuchAccountName',
    message: 'Account does not exist - name',
    http_code: 404,
});

NooBaaCLIError.AccountAccessKeyAlreadyExists = Object.freeze({
    code: 'AccountAccessKeyAlreadyExists',
    message: 'Account already exists - access_key',
    http_code: 409,
});

NooBaaCLIError.AccountNameAlreadyExists = Object.freeze({
    code: 'AccountNameAlreadyExists',
    message: 'Account already exists - name',
    http_code: 409,
});

NooBaaCLIError.AccountDeleteForbiddenHasBuckets = Object.freeze({
    code: 'AccountDeleteForbiddenHasBuckets',
    message: 'Cannot delete account that is owner of buckets. ' +
        'You must delete all buckets before deleting the account',
    http_code: 403,
});


//////////////////////////////////
//// ACCOUNT ARGUMENTS ERRORS ////
//////////////////////////////////

NooBaaCLIError.MissingAccountSecretKeyFlag = Object.freeze({
    code: 'MissingAccountSecretKeyFlag',
    message: 'Account secret key is mandatory, please use the --secret_key flag or --regenerate on update',
    http_code: 400,
});

NooBaaCLIError.MissingAccountAccessKeyFlag = Object.freeze({
    code: 'MissingAccountAccessKeyFlag',
    message: 'Account access key is mandatory, please use the --access_key flag or --regenerate on update on update',
    http_code: 400,
});

NooBaaCLIError.AccountSecretKeyFlagComplexity = Object.freeze({
    code: 'AccountSecretKeyFlagComplexity',
    message: 'Account secret length must be 40, and must contain uppercase, lowercase, numbers and symbols',
    http_code: 400,
});

NooBaaCLIError.AccountAccessKeyFlagComplexity = Object.freeze({
    code: 'AccountAccessKeyFlagComplexity',
    message: 'Account access key length must be 20, and must contain uppercase and numbers',
    http_code: 400,
});

NooBaaCLIError.MissingAccountNameFlag = Object.freeze({
    code: 'MissingAccountNameFlag',
    message: 'Account name is mandatory, please use the --name flag',
    http_code: 400,
});

NooBaaCLIError.MissingIdentifier = Object.freeze({
    code: 'MissingIdentifier',
    message: 'Account identifier is mandatory, please use the --access_key or --name flag',
    http_code: 400,
});

NooBaaCLIError.InvalidAccountNSFSConfig = Object.freeze({
    code: 'InvalidAccountNSFSConfig',
    message: 'Account config should not be empty, should contain UID, GID or user',
    http_code: 400,
});

NooBaaCLIError.MissingAccountNSFSConfigUID = Object.freeze({
    code: 'MissingAccountNSFSConfigUID',
    message: 'Account config should include UID',
    http_code: 400,
});

NooBaaCLIError.MissingAccountNSFSConfigGID = Object.freeze({
    code: 'MissingAccountNSFSConfigGID',
    message: 'Account config should include GID',
    http_code: 400,
});

NooBaaCLIError.InvalidAccountNewBucketsPath = Object.freeze({
    code: 'InvalidAccountNewBucketsPath',
    message: 'Account\'s new_buckets_path should be a valid and existing directory path',
    http_code: 400,
});

NooBaaCLIError.InvalidBooleanValue = Object.freeze({
    code: 'InvalidBooleanValue',
    message: 'supported values are true and false',
    http_code: 400,
});

NooBaaCLIError.InvalidNewNameAccountIdentifier = Object.freeze({
    code: 'InvalidNewNameAccountIdentifier',
    message: 'Account new_name can not be used on add command, please remove the --new_name flag',
    http_code: 400,
});

NooBaaCLIError.InvalidNewAccessKeyIdentifier = Object.freeze({
    code: 'InvalidNewAccessKeyIdentifier',
    message: 'Account new_access_key can not be used on add command, please remove the --new_access_key flag',
    http_code: 400,
});

NooBaaCLIError.InaccessibleAccountNewBucketsPath = Object.freeze({
    code: 'InaccessibleAccountNewBucketsPath',
    message: 'Account should have read & write access to the specified new_buckets_path',
    http_code: 400,
});

NooBaaCLIError.InvalidAccountDistinguishedName = Object.freeze({
    code: 'InvalidAccountDistinguishedName',
    message: 'Account distinguished name was not found',
    http_code: 400,
});

NooBaaCLIError.InvalidGlacierOperation = Object.freeze({
    code: 'InvalidGlacierOperation',
    message: 'only "migrate", "restore" and "expiry" subcommands are supported',
    http_code: 400,
});


////////////////////////
//// BUCKET ERRORS /////
////////////////////////

NooBaaCLIError.NoSuchBucket = Object.freeze({
    code: 'NoSuchBucket',
    message: 'Bucket does not exist',
    http_code: 404,
});

NooBaaCLIError.InvalidBucketName = Object.freeze({
    code: 'InvalidBucketName',
    message: 'The specified bucket name is not valid.',
    http_code: 400,
});

NooBaaCLIError.InvalidStoragePath = Object.freeze({
    code: 'InvalidStoragePath',
    message: 'The specified bucket storage path is not valid.',
    http_code: 400,
});

NooBaaCLIError.BucketAlreadyExists = Object.freeze({
    code: 'BucketAlreadyExists',
    message: 'The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.',
    http_code: 409,
});

NooBaaCLIError.BucketSetForbiddenNoBucketOwner = Object.freeze({
    code: 'BucketSetForbiddenNoBucketOwner',
    message: 'The bucket owner you set for the bucket does not exist. ' +
        'Please set the bucket owner from existing account',
    http_code: 403,
});

NooBaaCLIError.BucketCreationNotAllowed = Object.freeze({
    code: 'BucketCreationNotAllowed',
    message: 'Not allowed to create new buckets',
    http_code: 403,
});

NooBaaCLIError.BucketDeleteForbiddenHasObjects = Object.freeze({
    code: 'BucketDeleteForbiddenHasObjects',
    message: 'Cannot delete non-empty bucket. ' +
    'You must delete all object before deleting the bucket or use --force flag',
    http_code: 403,
});

/////////////////////////////////
//// BUCKET ARGUMENTS ERRORS ////
/////////////////////////////////


NooBaaCLIError.MissingBucketNameFlag = Object.freeze({
    code: 'MissingBucketNameFlag',
    message: 'Bucket name is mandatory, please use the --name flag',
    http_code: 400,
});

NooBaaCLIError.MissingBucketOwnerFlag = Object.freeze({
    code: 'MissingBucketOwnerFlag',
    message: 'Bucket owner (account name) is mandatory, please use the --owner flag',
    http_code: 400,
});

NooBaaCLIError.MissingBucketPathFlag = Object.freeze({
    code: 'MissingBucketPathFlag',
    message: 'Bucket path is mandatory, please use the --path flag',
    http_code: 400,
});

NooBaaCLIError.InvalidNewNameBucketIdentifier = Object.freeze({
    code: 'InvalidNewNameBucketIdentifier',
    message: 'Bucket new_name can not be used on add command, please remove the --new_name flag',
    http_code: 400,
});

NooBaaCLIError.InvalidFSBackend = Object.freeze({
    code: 'InvalidFSBackend',
    message: 'FS backend supported types are GPFS, CEPH_FS, NFSv4 default is POSIX',
    http_code: 400,
});

NooBaaCLIError.MalformedPolicy = Object.freeze({
    code: 'MalformedPolicy',
    message: 'Invalid bucket policy',
    http_code: 400,
});

NooBaaCLIError.InaccessibleStoragePath = Object.freeze({
    code: 'InaccessibleStoragePath',
    message: 'Bucket owner should have read & write access to the specified bucket storage path',
    http_code: 400,
});

NooBaaCLIError.BucketNotEmpty = Object.freeze({
    code: 'BucketNotEmpty',
    message: 'The bucket you tried to delete is not empty. You must delete all versions in the bucket',
    http_code: 400,
});

NooBaaCLIError.FS_ERRORS_TO_NOOBAA_CLI_ERRORS = Object.freeze({
    EACCES: NooBaaCLIError.AccessDenied,
    EPERM: NooBaaCLIError.AccessDenied,
    EINVAL: NooBaaCLIError.InvalidRequest,
    NOT_IMPLEMENTED: NooBaaCLIError.NotImplemented,
    INTERNAL_ERROR: NooBaaCLIError.InternalError,
    // ENOENT: NooBaaCLIError.NoSuchBucket,
    NOT_EMPTY: NooBaaCLIError.BucketNotEmpty,
    MALFORMED_POLICY: NooBaaCLIError.MalformedPolicy,
    // EEXIST: NooBaaCLIError.BucketAlreadyExists,
});

NooBaaCLIError.RPC_ERROR_TO_NOOBAA_CLI_ERROR = Object.freeze({
    INVALID_SCHEMA: NooBaaCLIError.InvalidSchema,
    NO_SUCH_USER: NooBaaCLIError.InvalidAccountDistinguishedName
});

const NSFS_CLI_ERROR_EVENT_MAP = {
    WhiteListIPUpdateFailed: NoobaaEvent.WHITELIST_UPDATED_FAILED,
    AccessDenied: NoobaaEvent.UNAUTHORIZED,
    AccountAccessKeyAlreadyExists: NoobaaEvent.ACCOUNT_ALREADY_EXISTS,
    AccountNameAlreadyExists: NoobaaEvent.ACCOUNT_ALREADY_EXISTS,
    AccountDeleteForbiddenHasBuckets: NoobaaEvent.ACCOUNT_DELETE_FORBIDDEN,
    BucketAlreadyExists: NoobaaEvent.BUCKET_ALREADY_EXISTS,
    BucketSetForbiddenNoBucketOwner: NoobaaEvent.UNAUTHORIZED,
};

exports.NooBaaCLIError = NooBaaCLIError;
exports.NSFS_CLI_ERROR_EVENT_MAP = NSFS_CLI_ERROR_EVENT_MAP;
