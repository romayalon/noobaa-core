/* Copyright (C) 2023 NooBaa */
'use strict';

const dbg = require('../../util/debug_module')(__filename);
dbg.set_process_name('analyze_network');


async function main(argv) {
    try {
        dbg.log0('starting to analyze network');
        const deployment_type = argv.deployment_type;
        if (deployment_type === 'nc') await test_nc_network();
        else await test_network();
    } catch (err) {
        process.exit(1);
    }
    process.exit(0);
}

const ANALYZE_FUNCTION_BY_SERVICE_TYPE = {
    S3: analyze_s3,
    STS: analyze_sts,
    IAM: analyze_iam,
    DB: analyze_db,
    MGMT: analyze_mgmt,
    METRICS: analyze_metrics
};

/**
 * 
 */
async function test_nc_network() {
    await analyze_forks();
    const services_info = await nc_prepare_service_info();
    const nc_enabled_services = ['S3', 'METRICS']; // IAM, STS
    for (const service of nc_enabled_services) {
        await analyze_service(service, services_info[service]);
    }
}

/**
 * 
 */
async function test_network(services_info) {
    const conatinerized_enabled_services = ['S3', 'STS', 'MGMT', 'DB', 'METRICS']; // IAM
    for (const service of conatinerized_enabled_services) {
        await analyze_service(service, services_info[service]);
    }
}

/**
 * analyze_service
 */
async function analyze_service(service_type, service_info) {
    await nslookup_service(service_info);
    await ping_service(service_info); // ping DNS
    await ping_service(service_info); // ping IP
    await curl_service(service_info);
    const analyze_service_closure = ANALYZE_FUNCTION_BY_SERVICE_TYPE[service_type];
    await analyze_service_closure(service_info);
}


///////////////////////////////////
//          GENERAL HELPERS      //
///////////////////////////////////


async function ping_service(service_info) {

}

async function nslookup_service(service_info) {

}

async function curl_service(service_info) {

}

async function analyze_s3(service_info) {

}

async function analyze_sts(service_info) {

}

async function analyze_iam(service_info) {
    // nice to have
}

async function analyze_db(service_info) {

}

async function analyze_mgmt(service_info) {

}

async function analyze_metrics(service_info) {

}

/////////////////////////////////
///           NC               //
/////////////////////////////////

async function analyze_forks() {

}

async function nc_prepare_service_info() {

}


if (require.main === module) {
    main();
}
