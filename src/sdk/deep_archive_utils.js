/* Copyright (C) 2026 NooBaa */
'use strict';

/**
 * Shared Deep Archive restore xattr keys and helpers.
 * Object MD stores xattr keys with '.' replaced by '@' (see object_server.update_object_md).
 */

const XATTR_RESTORE_ONGOING = 'noobaa-deep-archive.restore.ongoing';
const XATTR_RESTORE_EXPIRY = 'noobaa-deep-archive.restore.expiry';
const XATTR_RESTORE_DAYS = 'noobaa-deep-archive.restore.days';
const XATTR_STORAGE_CLASS = 'noobaa-deep-archive.storage-class';

/** DB-safe form of xattr keys (dots → @). */
const XATTR_RESTORE_ONGOING_DB = XATTR_RESTORE_ONGOING.replace(/\./g, '@');
const XATTR_RESTORE_EXPIRY_DB = XATTR_RESTORE_EXPIRY.replace(/\./g, '@');
const XATTR_RESTORE_DAYS_DB = XATTR_RESTORE_DAYS.replace(/\./g, '@');

/**
 * Convert API-style xattr keys (with dots) to DB keys (with @).
 * @param {Record<string, string>} xattr
 * @returns {Record<string, string>}
 */
function xattr_to_db(xattr) {
    const out = {};
    for (const [k, v] of Object.entries(xattr || {})) {
        out[k.replace(/\./g, '@')] = v;
    }
    return out;
}

/**
 * Convert DB-style xattr keys (with @) to API keys (with dots).
 * @param {Record<string, string>} xattr
 * @returns {Record<string, string>}
 */
function xattr_from_db(xattr) {
    const out = {};
    for (const [k, v] of Object.entries(xattr || {})) {
        out[k.replace(/@/g, '.')] = v;
    }
    return out;
}

/**
 * Merge restore-related updates into existing object xattr without wiping other keys.
 * @param {Record<string, string>|undefined} existing_xattr API-style keys (dots)
 * @param {Record<string, string>} updates
 * @returns {Record<string, string>}
 */
function merge_xattr(existing_xattr, updates) {
    return Object.assign({}, existing_xattr || {}, updates);
}

/**
 * Apply an API-style xattr patch onto a DB xattr object and return DB-keyed result.
 * @param {Record<string, string>|undefined} db_xattr
 * @param {Record<string, string>} api_patch
 * @returns {Record<string, string>}
 */
function merge_db_xattr(db_xattr, api_patch) {
    return xattr_to_db(merge_xattr(xattr_from_db(db_xattr), api_patch));
}

/**
 * Parse S3 Restore response header from HeadObject.
 * Examples:
 *   ongoing-request="true"
 *   ongoing-request="false", expiry-date="Fri, 23 Dec 2012 00:00:00 GMT"
 * @param {string|undefined} restore_header
 * @returns {{ ongoing: boolean, ready: boolean }}
 */
function parse_s3_restore_header(restore_header) {
    if (!restore_header) {
        return { ongoing: true, ready: false };
    }
    const ongoing = /ongoing-request\s*=\s*"true"/i.test(restore_header);
    return { ongoing, ready: !ongoing };
}

/**
 * Compute restore expiry timestamp from request Days relative to now.
 * @param {number|string} days
 * @param {Date} [now]
 * @returns {Date}
 */
function compute_restore_expiry(days, now = new Date()) {
    const expiry = new Date(now.getTime());
    expiry.setDate(expiry.getDate() + Number(days));
    return expiry;
}

/**
 * Build xattr patch that clears restore state (API-style keys).
 * Empty strings clear values consistently with existing restore_object usage.
 * @returns {Record<string, string>}
 */
function clear_restore_xattr_patch() {
    return {
        [XATTR_RESTORE_ONGOING]: '',
        [XATTR_RESTORE_EXPIRY]: '',
        [XATTR_RESTORE_DAYS]: '',
    };
}

/**
 * Build xattr patch that finalizes a successful restore copy.
 * Sets expiry before clearing ongoing (NSFS-style safety ordering in a single merge).
 * @param {number|string} days
 * @param {Date} [now]
 * @returns {Record<string, string>}
 */
function finalize_restore_xattr_patch(days, now = new Date()) {
    return {
        [XATTR_RESTORE_EXPIRY]: compute_restore_expiry(days, now).toISOString(),
        [XATTR_RESTORE_ONGOING]: '',
        [XATTR_RESTORE_DAYS]: '',
    };
}

exports.XATTR_RESTORE_ONGOING = XATTR_RESTORE_ONGOING;
exports.XATTR_RESTORE_EXPIRY = XATTR_RESTORE_EXPIRY;
exports.XATTR_RESTORE_DAYS = XATTR_RESTORE_DAYS;
exports.XATTR_STORAGE_CLASS = XATTR_STORAGE_CLASS;
exports.XATTR_RESTORE_ONGOING_DB = XATTR_RESTORE_ONGOING_DB;
exports.XATTR_RESTORE_EXPIRY_DB = XATTR_RESTORE_EXPIRY_DB;
exports.XATTR_RESTORE_DAYS_DB = XATTR_RESTORE_DAYS_DB;
exports.xattr_to_db = xattr_to_db;
exports.xattr_from_db = xattr_from_db;
exports.merge_xattr = merge_xattr;
exports.merge_db_xattr = merge_db_xattr;
exports.parse_s3_restore_header = parse_s3_restore_header;
exports.compute_restore_expiry = compute_restore_expiry;
exports.clear_restore_xattr_patch = clear_restore_xattr_patch;
exports.finalize_restore_xattr_patch = finalize_restore_xattr_patch;
