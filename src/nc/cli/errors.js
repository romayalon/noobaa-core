/* Copyright (C) 2016 NooBaa */
'use strict';

const NoobaaEvent = require('../events_utils').NoobaaEvent;

/**
 * @typedef {{
 *      code?: string, 
 *      message: string, 
 *      http_code: number,
 *      detail: string,
 *      cause?: Error  
 * }} CLIErrorSpec
 */

class CLIError extends Error {

    /**
     * @param {CLIErrorSpec} error_spec 
     */
    constructor({ code, message, http_code, detail, cause }) {
        super(message, { cause });
        this.code = code;
        this.http_code = http_code;
        this.detail = detail;
    }

    to_string() {
        const json = {
            error: {
                code: this.code,
                message: this.message,
                detail: this.detail,
                cause: this.cause?.stack || this.cause?.message
            }
        };
        return JSON.stringify(json, null, 2);
    }
}

// See NooBaa CLI error codes docs - TODO: add docs

////////////////////////
//// GENERAL ERRORS ////
////////////////////////


CLIError.InternalError = Object.freeze({
    code: 'InternalError',
    message: 'The server encountered an internal error. Please retry the request',
    http_code: 500,
});

CLIError.InvalidRequest = Object.freeze({
    code: 'InvalidRequest',
    message: 'The request is invalid',
    http_code: 400,
});

CLIError.NotImplemented = Object.freeze({
    code: 'NotImplemented',
    message: 'functionality is not implemented.',
    http_code: 501,
});

CLIError.InvalidAction = Object.freeze({
    code: 'InvalidAction',
    message: 'Invalid action, available actions are add, status, update, delete, list',
    http_code: 400,
});

CLIError.InvalidArgument = Object.freeze({
    code: 'InvalidArgument',
    message: 'Invalid argument',
    http_code: 400,
});

CLIError.InvalidArgumentType = Object.freeze({
    code: 'InvalidArgumentType',
    message: 'Invalid argument type',
    http_code: 400,
});

CLIError.InvalidType = Object.freeze({
    code: 'InvalidType',
    message: 'Invalid type, available types are account, bucket or whitelist',
    http_code: 400,
});

CLIError.MissingConfigDirPath = Object.freeze({
    code: 'MissingConfigDirPath',
    message: 'Config dir path should not be empty',
    http_code: 400,
});

CLIError.InvalidSchema = Object.freeze({
    code: 'InvalidSchema',
    message: 'Schema invalid, please use required properties',
    http_code: 400,
});

CLIError.InvalidFilePath = Object.freeze({
    code: 'InvalidFilePath',
    message: 'Invalid file path',
    http_code: 400,
});

CLIError.InvalidJSONFile = Object.freeze({
    code: 'InvalidJSONFile',
    message: 'Invalid JSON file',
    http_code: 400,
});

CLIError.MissingUpdateProperty = Object.freeze({
    code: 'MissingUpdateProperty',
    message: 'Should have at least one property to update',
    http_code: 400,
});

CLIError.InvalidFlagsCombination = Object.freeze({
    code: 'InvalidFlagsCombination',
    message: 'The flags combination that you used is invalid',
    http_code: 400,
});

CLIError.InvalidAccountName = Object.freeze({
    code: 'InvalidAccountName',
    message: 'Account name is invalid',
    http_code: 400,
});

//////////////////////////////
//// IP WHITE LIST ERRORS ////
//////////////////////////////

CLIError.MissingWhiteListIPFlag = Object.freeze({
    code: 'MissingWhiteListIPFlag',
    message: 'Whitelist ips are mandatory, please use the --ips flag',
    http_code: 400,
});

CLIError.InvalidWhiteListIPFormat = Object.freeze({
    code: 'InvalidWhiteListIPFormat',
    message: 'Whitelist IP body format is invalid',
    http_code: 400,
});

CLIError.WhiteListIPUpdateFailed = Object.freeze({
    code: 'WhiteListIPUpdateFailed',
    message: 'Whitelist ip update failed',
    http_code: 500,
});

CLIError.InvalidMasterKey = Object.freeze({
    code: 'InvalidMasterKey',
    message: 'Master key manager had issues loading master key, can not decrypt/encrypt secrets.',
    http_code: 500,
});

////////////////////////
//// ACCOUNT ERRORS ////
////////////////////////

CLIError.AccessDenied = Object.freeze({
    code: 'AccessDenied',
    message: 'Account has no permissions to access the bucket',
    http_code: 403,
});

CLIError.NoSuchAccountAccessKey = Object.freeze({
    code: 'NoSuchAccountAccessKey',
    message: 'Account does not exist - access key',
    http_code: 404,
});

CLIError.NoSuchAccountName = Object.freeze({
    code: 'NoSuchAccountName',
    message: 'Account does not exist - name',
    http_code: 404,
});

CLIError.AccountAccessKeyAlreadyExists = Object.freeze({
    code: 'AccountAccessKeyAlreadyExists',
    message: 'Account already exists - access_key',
    http_code: 409,
});

CLIError.AccountNameAlreadyExists = Object.freeze({
    code: 'AccountNameAlreadyExists',
    message: 'Account already exists - name',
    http_code: 409,
});

CLIError.AccountDeleteForbiddenHasBuckets = Object.freeze({
    code: 'AccountDeleteForbiddenHasBuckets',
    message: 'Cannot delete account that is owner of buckets. ' +
        'You must delete all buckets before deleting the account',
    http_code: 403,
});

CLIError.AccountCannotCreateRootAccountsRequesterIAMUser = Object.freeze({
    code: 'AccountCannotCreateRootAccounts',
    message: 'Cannot update account to have iam_operate_on_root_account. ' +
        'You must use root account for this action',
    http_code: 409,
});

CLIError.AccountCannotBeRootAccountsManager = Object.freeze({
    code: 'AccountCannotBeRootAccountsManager',
    message: 'Cannot update account to have iam_operate_on_root_account. ' +
        'You must delete all IAM accounts before update or ' +
        'use root accounts that does not owns any IAM accounts',
    http_code: 409,
});

//////////////////////////////////
//// ACCOUNT ARGUMENTS ERRORS ////
//////////////////////////////////

CLIError.MissingAccountSecretKeyFlag = Object.freeze({
    code: 'MissingAccountSecretKeyFlag',
    message: 'Account secret key is mandatory, please use the --secret_key flag or --regenerate on update',
    http_code: 400,
});

CLIError.MissingAccountAccessKeyFlag = Object.freeze({
    code: 'MissingAccountAccessKeyFlag',
    message: 'Account access key is mandatory, please use the --access_key flag or --regenerate on update on update',
    http_code: 400,
});

CLIError.InvalidAccountSecretKeyFlag = Object.freeze({
    code: 'InvalidAccountSecretKeyFlag',
    message: 'Account secret length must be 40, and must contain only alpha-numeric chars, "+", "/"',
    http_code: 400,
});

CLIError.InvalidAccountAccessKeyFlag = Object.freeze({
    code: 'InvalidAccountAccessKeyFlag',
    message: 'Account access key length must be 20, and must contain only alpha-numeric chars',
    http_code: 400,
});

CLIError.MissingAccountNameFlag = Object.freeze({
    code: 'MissingAccountNameFlag',
    message: 'Account name is mandatory, please use the --name flag',
    http_code: 400,
});

CLIError.MissingIdentifier = Object.freeze({
    code: 'MissingIdentifier',
    message: 'Account identifier is mandatory, please use the --access_key or --name flag',
    http_code: 400,
});

CLIError.InvalidAccountNSFSConfig = Object.freeze({
    code: 'InvalidAccountNSFSConfig',
    message: 'Account config should not be empty, should contain UID, GID or user',
    http_code: 400,
});

CLIError.MissingAccountNSFSConfigUID = Object.freeze({
    code: 'MissingAccountNSFSConfigUID',
    message: 'Account config should include UID',
    http_code: 400,
});

CLIError.MissingAccountNSFSConfigGID = Object.freeze({
    code: 'MissingAccountNSFSConfigGID',
    message: 'Account config should include GID',
    http_code: 400,
});

CLIError.InvalidAccountNewBucketsPath = Object.freeze({
    code: 'InvalidAccountNewBucketsPath',
    message: 'Account\'s new_buckets_path should be a valid and existing directory path',
    http_code: 400,
});

CLIError.InvalidBooleanValue = Object.freeze({
    code: 'InvalidBooleanValue',
    message: 'supported values are true and false',
    http_code: 400,
});

CLIError.InaccessibleAccountNewBucketsPath = Object.freeze({
    code: 'InaccessibleAccountNewBucketsPath',
    message: 'Account should have read & write access to the specified new_buckets_path',
    http_code: 400,
});

CLIError.InvalidAccountDistinguishedName = Object.freeze({
    code: 'InvalidAccountDistinguishedName',
    message: 'Account distinguished name was not found',
    http_code: 400,
});
CLIError.InvalidGlacierOperation = Object.freeze({
    code: 'InvalidGlacierOperation',
    message: 'only "migrate", "restore" and "expiry" subcommands are supported',
    http_code: 400,
});


////////////////////////
//// BUCKET ERRORS /////
////////////////////////

CLIError.NoSuchBucket = Object.freeze({
    code: 'NoSuchBucket',
    message: 'Bucket does not exist',
    http_code: 404,
});

CLIError.InvalidBucketName = Object.freeze({
    code: 'InvalidBucketName',
    message: 'The specified bucket name is not valid.',
    http_code: 400,
});

CLIError.InvalidStoragePath = Object.freeze({
    code: 'InvalidStoragePath',
    message: 'The specified bucket storage path is not valid.',
    http_code: 400,
});

CLIError.BucketAlreadyExists = Object.freeze({
    code: 'BucketAlreadyExists',
    message: 'The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.',
    http_code: 409,
});

CLIError.BucketSetForbiddenNoBucketOwner = Object.freeze({
    code: 'BucketSetForbiddenNoBucketOwner',
    message: 'The bucket owner you set for the bucket does not exist. ' +
        'Please set the bucket owner from existing account',
    http_code: 403,
});

CLIError.BucketCreationNotAllowed = Object.freeze({
    code: 'BucketCreationNotAllowed',
    message: 'Not allowed to create new buckets',
    http_code: 403,
});

CLIError.BucketDeleteForbiddenHasObjects = Object.freeze({
    code: 'BucketDeleteForbiddenHasObjects',
    message: 'Cannot delete non-empty bucket. ' +
    'You must delete all object before deleting the bucket or use --force flag',
    http_code: 403,
});

/////////////////////////////////
//// BUCKET ARGUMENTS ERRORS ////
/////////////////////////////////


CLIError.MissingBucketNameFlag = Object.freeze({
    code: 'MissingBucketNameFlag',
    message: 'Bucket name is mandatory, please use the --name flag',
    http_code: 400,
});

CLIError.MissingBucketOwnerFlag = Object.freeze({
    code: 'MissingBucketOwnerFlag',
    message: 'Bucket owner (account name) is mandatory, please use the --owner flag',
    http_code: 400,
});

CLIError.MissingBucketPathFlag = Object.freeze({
    code: 'MissingBucketPathFlag',
    message: 'Bucket path is mandatory, please use the --path flag',
    http_code: 400,
});

CLIError.InvalidFSBackend = Object.freeze({
    code: 'InvalidFSBackend',
    message: 'FS backend supported types are GPFS, CEPH_FS, NFSv4 default is POSIX',
    http_code: 400,
});

CLIError.MalformedPolicy = Object.freeze({
    code: 'MalformedPolicy',
    message: 'Invalid bucket policy',
    http_code: 400,
});

CLIError.InaccessibleStoragePath = Object.freeze({
    code: 'InaccessibleStoragePath',
    message: 'Bucket owner should have read & write access to the specified bucket storage path',
    http_code: 400,
});

CLIError.BucketNotEmpty = Object.freeze({
    code: 'BucketNotEmpty',
    message: 'The bucket you tried to delete is not empty. You must delete all versions in the bucket',
    http_code: 400,
});

CLIError.FS_ERRORS_TO_CLI = Object.freeze({
    EACCES: CLIError.AccessDenied,
    EPERM: CLIError.AccessDenied,
    EINVAL: CLIError.InvalidRequest,
    NOT_IMPLEMENTED: CLIError.NotImplemented,
    INTERNAL_ERROR: CLIError.InternalError,
    // ENOENT: CLIError.NoSuchBucket,
    NOT_EMPTY: CLIError.BucketNotEmpty,
    MALFORMED_POLICY: CLIError.MalformedPolicy,
    // EEXIST: CLIError.BucketAlreadyExists,
});

CLIError.RPC_ERROR_TO_CLI = Object.freeze({
    INVALID_SCHEMA: CLIError.InvalidSchema,
    NO_SUCH_USER: CLIError.InvalidAccountDistinguishedName,
    INVALID_MASTER_KEY: CLIError.InvalidMasterKey,
    INVALID_BUCKET_NAME: CLIError.InvalidBucketName
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

exports.CLIError = CLIError;
exports.NSFS_CLI_ERROR_EVENT_MAP = NSFS_CLI_ERROR_EVENT_MAP;
