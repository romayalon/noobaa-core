/* Copyright (C) 2016 NooBaa */
'use strict';
var crypto = require("crypto");
const stream = require('stream');
const azure_storage = require("@azure/storage-blob");
// needed only for enerateBlockIdPrefix() and get_block_id() functions
const old_azure_storage = require('azure-storage');


function new_md5_stream() {
    const md5_stream = new stream.Transform({
        transform(buf, encoding, next) {
            this.md5.update(buf);
            this.size += buf.length;
            this.push(buf);
            next();
        }
    });
    md5_stream.md5 = crypto.createHash('md5');
    md5_stream.size = 0;
    return md5_stream;
}


azure_storage.get_container_client = (blob_service, container) => blob_service.getContainerClient(container);

azure_storage.get_blob_client = (container_client, blob) => container_client.getBlobClient(blob).getBlockBlobClient();

azure_storage.calc_body_md5 = async stream_file => {
    const md5_stream = new_md5_stream();
    const new_stream = stream_file.pipe(md5_stream);
    await new Promise((resolve, reject) => {
        new_stream.once('error', reject);
        new_stream.once('finish', resolve);
    });
    const final_md5 = md5_stream.md5.digest('hex');

    const md5_buf = Buffer.from(final_md5, 'hex');
    return { new_stream, md5_buf };
};

// create old lib blob service - needed for functions that do not exist in the new lib
// needed only for generateBlockIdPrefix() and get_block_id() functions
azure_storage.get_old_blob_service_conn_string = conn_string => {
    old_azure_storage.createBlobService(conn_string);
};
azure_storage.get_old_blob_service_creds = (account, pass, endpoint) => {
    old_azure_storage.createBlobService(account, pass, endpoint);
};
// these 2 functions are using the old blob service since there is no matching functions in the new lib
azure_storage.generate_block_id_prefix = old_blob_service => old_blob_service.generateBlockIdPrefix();
azure_storage.get_block_id = (old_blob_service, block_id_prefix, part_num) => old_blob_service.getBlockId(block_id_prefix, part_num);

module.exports = azure_storage;
