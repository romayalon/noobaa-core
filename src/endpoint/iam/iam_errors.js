/* Copyright (C) 2024 NooBaa */
'use strict';
const xml_utils = require('../../util/xml_utils');

const error_type_enum = {
    SENDER: 'Sender',
    RECEIVER: 'Receiver',
    UNKNOWM: 'Unknown',
};

// https://docs.aws.amazon.com/IAM/latest/APIReference/CommonErrors.html
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_ErrorDetails.html
/**
 * @typedef {{
 *      code: string, 
 *      message: string, 
 *      http_code?: number,
 *      type?: string,
 * }} IamErrorSpec
 */

class IamError extends Error {

    /**
     * @param {IamErrorSpec} error_spec
     */
    constructor({ code, message, http_code, type }) {
        super(message); // sets this.message
        this.code = code;
        this.http_code = http_code;
        this.type = type;
    }

    reply(request_id) {
        const xml = {
            ErrorResponse: {
                Error: {
                    Type: this.type,
                    Code: this.code,
                    Message: this.message,
                },
                RequestId: request_id || '',
            }
        };
        return xml_utils.encode_xml(xml);
    }

}

IamError.AccessDeniedException = Object.freeze({
    code: 'AccessDeniedException',
    message: 'You do not have sufficient access to perform this action.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.IncompleteSignature = Object.freeze({
    code: 'IncompleteSignature',
    message: 'The request signature does not conform to AWS standards.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.InternalFailure = Object.freeze({
    code: 'InternalFailure',
    message: 'The request processing has failed because of an unknown error, exception or failure.',
    http_code: 500,
    type: error_type_enum.RECEIVER,
});
IamError.InvalidAction = Object.freeze({
    code: 'InvalidAction',
    message: 'The action or operation requested is invalid. Verify that the action is typed correctly.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.InvalidClientTokenId = Object.freeze({
    code: 'InvalidClientTokenId',
    message: 'The X.509 certificate or AWS access key ID provided does not exist in our records.',
    http_code: 403,
    type: error_type_enum.SENDER,
});
IamError.NotAuthorized = Object.freeze({
    code: 'NotAuthorized',
    message: 'You do not have permission to perform this action.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.OptInRequired = Object.freeze({
    code: 'OptInRequired',
    message: 'The AWS access key ID needs a subscription for the service.',
    http_code: 403,
    type: error_type_enum.SENDER,
});
IamError.RequestExpired = Object.freeze({
    code: 'RequestExpired',
    message: 'The request reached the service more than 15 minutes after the date stamp on the request or more than 15 minutes after the request expiration date (such as for pre-signed URLs), or the date stamp on the request is more than 15 minutes in the future.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.ServiceUnavailable = Object.freeze({
    code: 'ServiceUnavailable',
    message: 'The request has failed due to a temporary failure of the server.',
    http_code: 503,
    type: error_type_enum.RECEIVER,
});
IamError.ThrottlingException = Object.freeze({
    code: 'ThrottlingException',
    message: 'The request was denied due to request throttling.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.ValidationError = Object.freeze({
    code: 'ValidationError',
    message: 'The input fails to satisfy the constraints specified by an AWS service.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
// internal error (not appears in the IAM error list)
IamError.NotImplemented = Object.freeze({
    code: 'NotImplemented',
    message: 'A header you provided implies functionality that is not implemented.',
    http_code: 501,
    type: error_type_enum.RECEIVER,
});

// These errors were copied from IAM APIs errors
// CreateUser errors https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateUser.html#API_CreateUser_Errors
// DeleteUser errors https://docs.aws.amazon.com/IAM/latest/APIReference/API_DeleteUser.html#API_DeleteUser_Errors
// GetUser    errors https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetUser.html#API_GetUser_Errors
// UpdateUser errors https://docs.aws.amazon.com/IAM/latest/APIReference/API_UpdateUser.html#API_UpdateUser_Errors
// ListUsers  errors https://docs.aws.amazon.com/IAM/latest/APIReference/API_ListUsers.html
IamError.ConcurrentModification = Object.freeze({
    code: 'EntityAlreadyExists',
    message: 'The request was rejected because multiple requests to change this object were submitted simultaneously. Wait a few minutes and submit your request again.',
    http_code: 409,
    type: error_type_enum.SENDER,
});
IamError.EntityAlreadyExists = Object.freeze({
    code: 'EntityAlreadyExists',
    message: 'The request was rejected because it attempted to create a resource that already exists.',
    http_code: 409,
    type: error_type_enum.SENDER,
});
IamError.InvalidInput = Object.freeze({
    code: 'EntityAlreadyExists',
    message: 'The request was rejected because an invalid or out-of-range value was supplied for an input parameter.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.LimitExceeded = Object.freeze({
    code: 'EntityAlreadyExists',
    message: 'The request was rejected because it attempted to create resources beyond the current AWS account limits. The error message describes the limit exceeded.',
    http_code: 409,
    type: error_type_enum.SENDER,
});
IamError.NoSuchEntity = Object.freeze({
    code: 'NoSuchEntity',
    message: 'The request was rejected because it referenced a resource entity that does not exist. The error message describes the resource.',
    http_code: 404,
    type: error_type_enum.SENDER,
});
IamError.ServiceFailure = Object.freeze({
    code: 'ServiceFailure',
    message: 'The request processing has failed because of an unknown error, exception or failure.',
    http_code: 500,
    type: error_type_enum.RECEIVER,
});
IamError.DeleteConflict = Object.freeze({
    code: 'DeleteConflict',
    message: 'The request was rejected because it attempted to delete a resource that has attached subordinate entities. The error message describes these entities.',
    http_code: 409,
    type: error_type_enum.SENDER,
});
IamError.EntityTemporarilyUnmodifiable = Object.freeze({
    code: 'EntityTemporarilyUnmodifiable',
    message: 'The request was rejected because it referenced an entity that is temporarily unmodifiable, such as a user name that was deleted and then recreated. The error indicates that the request is likely to succeed if you try again after waiting several minutes. The error message describes the entity.',
    http_code: 409,
    type: error_type_enum.SENDER,
});
// These errors were actually send after performing IAM actions
IamError.AccessDenied = Object.freeze({
    code: 'AccessDenied',
    message: 'user is not authorized to perform action on resource',
    http_code: 400,
    type: error_type_enum.SENDER,
});


// These errors were copied from STS errors
// TODO - can be deleted after verifying we will not use them
IamError.InvalidParameterCombination = Object.freeze({
    code: 'InvalidParameterCombination',
    message: 'Parameters that must not be used together were used together.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.InvalidParameterValue = Object.freeze({
    code: 'InvalidParameterValue',
    message: 'An invalid or out-of-range value was supplied for the input parameter.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.InvalidQueryParameter = Object.freeze({
    code: 'InvalidQueryParameter',
    message: 'The AWS query string is malformed or does not adhere to AWS standards.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.MalformedQueryString = Object.freeze({
    code: 'MalformedQueryString',
    message: 'The query string contains a syntax error.',
    http_code: 404,
    type: error_type_enum.SENDER,
});
IamError.MissingAction = Object.freeze({
    code: 'MissingAction',
    message: 'The request is missing an action or a required parameter.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.MissingAuthenticationToken = Object.freeze({
    code: 'MissingAuthenticationToken',
    message: 'The request must contain either a valid (registered) AWS access key ID or X.509 certificate.',
    http_code: 403,
    type: error_type_enum.SENDER,
});
IamError.MissingParameter = Object.freeze({
    code: 'MissingParameter',
    message: 'A required parameter for the specified action is not supplied.',
    http_code: 400,
    type: error_type_enum.SENDER,
});
IamError.ExpiredToken = Object.freeze({
    code: 'ExpiredToken',
    message: 'The security token included in the request is expired',
    http_code: 400,
    type: error_type_enum.SENDER,
});

// EXPORTS
exports.IamError = IamError;
