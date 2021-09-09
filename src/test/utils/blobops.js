/* Copyright (C) 2016 NooBaa */
'use strict';

const azure_storage = require('../../util/azure_storage_wrap');
const RandStream = require('../../util/rand_stream');


const {
    AZURE_STORAGE_ACCOUNT_NAME,
    AZURE_STORAGE_ACCOUNT_KEY
} = process.env;

const AzureDefaultConnection = {
    name: 'AZUREConnection',
    endpoint: `https://${AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,
    endpoint_type: "AZURE",
    identity: AZURE_STORAGE_ACCOUNT_NAME,
    secret: AZURE_STORAGE_ACCOUNT_KEY
};

const blobService = new azure_storage.BlobServiceClient(
    AzureDefaultConnection.endpoint,
    new azure_storage.StorageSharedKeyCredential(AzureDefaultConnection.identity, AzureDefaultConnection.secret)
);

async function uploadRandomFileDirectlyToAzure(container, file_name, size, err_handler) {
    const message = `Uploading random file ${file_name} to azure container ${container}`;
    console.log(message);
    const streamFile = new RandStream(size, {
        highWaterMark: 1024 * 1024,
    });


    // TODO: need to check for each option that it works
    // const options = {
    //     storeBlobContentMD5: true,
    //     useTransactionalMD5: true,
    //     transactionalContentMD5: true
    // };
    try {
        const container_client = blobService.getContainerClient(container);
        const block_blob_client = container_client.getBlockBlobClient(file_name);
        const { new_stream, md5_buf } = await azure_storage.calc_body_md5(streamFile);
        await block_blob_client.uploadStream(new_stream, size, undefined, {
            blobHTTPHeaders: {
                blobContentMD5: md5_buf
            }
        });
    } catch (err) {
        _handle_error(err, message, err_handler);
    }
}

async function getPropertyBlob(container, file_name, err_handler) {
    const message = `Getting md5 for ${file_name} directly from azure container: ${container}`;
    console.log(message);
    try {
        const container_client = blobService.getContainerClient(container);
        const blob_client = container_client.getBlobClient(file_name).getBlockBlobClient();
        const blobProperties = await blob_client.getProperties();
        console.log(JSON.stringify(blobProperties));
        return {
            md5: blobProperties.contentMD5,
            size: blobProperties.contentLength
        };
    } catch (err) {
        _handle_error(err, message, err_handler);
    }
}

async function getListFilesAzure(bucket, err_handler) {
    const message = `Getting list of files from azure container for ${bucket}`;
    console.log(message);
    try {
        const container_client = blobService.getContainerClient(bucket);
        let iterator = container_client.listBlobsFlat().byPage({ maxPageSize: 1000 });
        let response = (await iterator.next()).value;
        return response.segment.blobItems.map(blob => blob.name);
    } catch (err) {
        _handle_error(err, message, err_handler);
    }
}

function _handle_error(err, message, err_handler) {
    console.error(`Failed ${message}`);
    if (err_handler) {
        err_handler(message, err);
    } else {
        throw err;
    }
}

exports.AzureDefaultConnection = AzureDefaultConnection;
exports.uploadRandomFileDirectlyToAzure = uploadRandomFileDirectlyToAzure;
exports.getPropertyBlob = getPropertyBlob;
exports.getListFilesAzure = getListFilesAzure;
