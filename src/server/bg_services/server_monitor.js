/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const dbg = require('../../util/debug_module')(__filename);
const os_utils = require('../../util/os_utils');
const system_store = require('../system_services/system_store').get_instance();

async function background_worker() {
    if (!system_store.is_finished_initial_load) {
        dbg.log0('SERVER_MONITOR: waiting for system store to load');
        return;
    }
    if (!system_store.data.systems[0]) {
        dbg.log0('SERVER_MONITOR: system does not exist, skipping');
        return;
    }

    dbg.log0('SERVER_MONITOR: BEGIN');
    await system_store.refresh();
    try {
        await refresh_system_address();
    } catch (err) {
        dbg.error('Trying to discover address changes failed', err);
    }
    dbg.log0('SERVER_MONITOR: END');
}

/**
 * @param {nb.ID} [system_id]
 */
async function refresh_system_address(system_id) {
    const [system] = system_store.data.systems;
    const id = system_id || (system && system._id);
    if (!id) {
        return;
    }

    const system_address = (process.env.CONTAINER_PLATFORM === 'KUBERNETES') ?
        await os_utils.discover_k8s_services() : [];

    // This works because the lists are always sorted, see discover_k8s_services().
    const curr_address = system && system.system_address;
    if (curr_address && _.isEqual(curr_address, system_address)) {
        return;
    }

    await system_store.make_changes({
        update: {
            systems: [{
                _id: id,
                $set: { system_address }
            }]
        }
    });
}

exports.background_worker = background_worker;
exports.refresh_system_address = refresh_system_address;
