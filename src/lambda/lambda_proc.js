/* Copyright (C) 2016 NooBaa */
'use strict';

const path = require('path');

try {
    process.on('uncaughtException', fail);
    process.on('unhandledRejection', fail);

    process.once('message', msg => {

        console.log('lambda_proc: received message', msg);
        const handler_arg = msg.config.handler;
        const handler_split = handler_arg.split('.', 2);
        const module_name = handler_split[0] + '.js';
        const export_name = handler_split[1];
        const module_exports = require(path.resolve(module_name)); // eslint-disable-line global-require
        const handler = module_exports[export_name];

        if (typeof(handler) !== 'function') {
            fail(new Error(`Lambda handler not a function ${handler_arg}`));
        }

        // http://docs.aws.amazon.com/lambda/latest/dg/nodejs-prog-model-context.html
        const func_context = {
            callbackWaitsForEmptyEventLoop: true,
            functionName: '',
            functionVersion: '',
            invokedFunctionArn: '',
            memoryLimitInMB: 0,
            awsRequestId: '',
            logGroupName: '',
            logStreamName: '',
            identity: null,
            clientContext: null,
            getRemainingTimeInMillis: () => 60 * 1000, // TODO calculate timeout
        };

        const func_callback = (err, reply) => {
            if (err) {
                console.log('lambda_proc: callback', err);
                if (func_context.callbackWaitsForEmptyEventLoop) {
                    process.on('beforeExit', () => fail(err));
                } else {
                    fail(err);
                }
                return;
            }
            // console.log('lambda_proc: callback reply', reply);
            if (func_context.callbackWaitsForEmptyEventLoop) {
                process.on('beforeExit', () => success(reply));
            } else {
                success(reply);
            }
        };

        handler(msg.event, func_context, func_callback);
    });

} catch (err) {
    fail(err);
}

function fail(err) {
    console.error('lambda_proc: fail', err);
    process.send({
        error: {
            message: err.message,
            code: err.code,
            stack: err.stack,
        }
    }, () => process.exit(1));
}

function success(result) {
    // console.log('lambda_proc: success', result);
    process.send({
        result: result
    }, () => process.exit(1));
}
