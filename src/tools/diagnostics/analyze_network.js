/* Copyright (C) 2023 NooBaa */
'use strict';

const dbg = require('../../util/debug_module')(__filename);
dbg.set_process_name('analyze_network');


async function main() {
    try {
        dbg.log0('starting to analyze network');
        if (nc_deployment) await test_nc_network();
        else await test_network();
    } catch (err) {
        process.exit(1);
    }
    process.exit(0);
}

if (require.main === module) {
    main();
}
