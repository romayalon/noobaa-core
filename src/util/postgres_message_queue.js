/* Copyright (C) 2016 NooBaa */
/** @typedef {typeof import('../sdk/nb')} nb */
'use strict';

const crypto = require('crypto');
const dbg = require('./debug_module')(__filename);
const config = require('../../config');
const db_client = require('./db_client');
const { MessageQueueClient } = require('./message_queue_client');
const message_queue_schema = require('./message_queue_schema');
const message_queue_indexes = require('./message_queue_indexes');

const TABLE = 'nb_message_queue';

/** @type {any} */
let queue_table;

/**
 * Persistent queue stored in the NooBaa Postgres database.
 * dequeue uses FOR UPDATE SKIP LOCKED so concurrent workers each take a
 * different message. Each claim gets a lock token that ack and nack must
 * present, so a worker whose visibility lock expired cannot complete the
 * next claim. extend slides that lock forward. An expired claim that already
 * used its attempts is returned with terminal set, and nack records it with
 * dead_at so it leaves the live index.
 *
 * @extends {MessageQueueClient}
 */
class PostgresMessageQueue extends MessageQueueClient {

    /** @type {PostgresMessageQueue | undefined} */
    static _instance;

    constructor() {
        super();
        this._ready = false;
        /** @type {Promise<void> | null} */
        this._connect_promise = null;
    }

    static instance() {
        PostgresMessageQueue._instance = PostgresMessageQueue._instance || new PostgresMessageQueue();
        return PostgresMessageQueue._instance;
    }

    async connect() {
        if (this._ready) return;
        if (!this._connect_promise) {
            this._connect_promise = this._connect().catch(err => {
                this._connect_promise = null;
                throw err;
            });
        }
        await this._connect_promise;
    }

    /**
     * @param {string} queue
     * @param {object} payload
     * @returns {Promise<string>}
     */
    async enqueue(queue, payload) {
        this._assert_queue(queue);
        const body = this._normalize_payload(payload);
        await this.connect();
        const _id = db_client.instance().new_object_id();
        const now = new Date();
        await this._table().insertOne({
            _id,
            queue_name: queue,
            payload: body,
            enqueued_at: now,
            visible_at: now,
            attempts: 0,
        });
        return String(_id);
    }

    /**
     * @param {string} queue
     * @returns {Promise<nb.QueueMessage | null>}
     */
    async dequeue(queue) {
        this._assert_queue(queue);
        await this.connect();
        const token = crypto.randomBytes(16).toString('hex');
        const res = await this._query(
            `WITH candidate AS (
                 SELECT _id, (data->>'attempts')::int AS attempts
                 FROM ${TABLE}
                 WHERE data->>'queue_name' = $1
                   AND data->'dead_at' IS NULL
                   AND (data->>'visible_at')::timestamptz <= now()
                   AND (
                     data->'locked_until' IS NULL
                     OR (data->>'locked_until')::timestamptz < now()
                   )
                 ORDER BY _id
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1
             )
             UPDATE ${TABLE} AS q
             SET data = q.data || jsonb_build_object(
                 'lock_token', $3::text,
                 'locked_until', now() + ($2::text || ' milliseconds')::interval,
                 'attempts', CASE
                     WHEN candidate.attempts >= $4 THEN candidate.attempts
                     ELSE candidate.attempts + 1 END
             )
             FROM candidate
             WHERE q._id = candidate._id
             RETURNING q._id, q.data, (candidate.attempts >= $4) AS terminal`,
            [queue, String(config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS), token, config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS],
        );
        const row = res.rows[0];
        if (!row) return null;
        const data = row.data;
        return {
            id: String(row._id).trim(),
            queue,
            payload: data.payload,
            attempts: Number(data.attempts),
            lock_token: token,
            terminal: Boolean(row.terminal),
        };
    }

    /**
     * @param {string} queue
     * @returns {Promise<number>}
     */
    async size(queue) {
        this._assert_queue(queue);
        await this.connect();
        const res = await this._query(
            `SELECT count(*)::int AS size FROM ${TABLE}
             WHERE data->>'queue_name' = $1 AND data->'dead_at' IS NULL`,
            [queue],
        );
        return Number(res.rows[0].size);
    }

    /**
     * @param {nb.QueueMessage} message
     * @returns {Promise<void>}
     */
    async ack(message) {
        const token = this._lock_token(message);
        await this.connect();
        const res = await this._query(
            `DELETE FROM ${TABLE}
             WHERE _id = $1 AND data->>'queue_name' = $2
               AND data->>'lock_token' = $3 AND data->'dead_at' IS NULL`,
            [message.id, message.queue, token],
        );
        if (!res.rowCount) {
            throw new Error('postgres message queue lost the lock on the message');
        }
    }

    /**
     * @param {nb.QueueMessage} message
     * @param {string} [reason]
     * @returns {Promise<nb.MessageQueueNackResult>}
     */
    async nack(message, reason) {
        const token = this._lock_token(message);
        await this.connect();
        if (message.attempts >= config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS) {
            dbg.warn('postgres message queue dropping message after max attempts', message.id, message.queue, reason);
            const res = await this._query(
                `UPDATE ${TABLE}
                 SET data = (data - 'locked_until' - 'lock_token') || jsonb_build_object(
                     'dead_at', now(),
                     'last_error', $4::text
                 )
                 WHERE _id = $1 AND data->>'queue_name' = $2
                   AND data->>'lock_token' = $3 AND data->'dead_at' IS NULL`,
                [message.id, message.queue, token, reason || ''],
            );
            if (!res.rowCount) {
                throw new Error('postgres message queue lost the lock on the message');
            }
            return { dropped: true };
        }
        const delay_ms = message.attempts * config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS;
        const res = await this._query(
            `UPDATE ${TABLE}
             SET data = (data - 'locked_until' - 'lock_token') || jsonb_build_object(
                 'last_error', $4::text,
                 'visible_at', now() + ($3::text || ' milliseconds')::interval
             )
             WHERE _id = $1 AND data->>'queue_name' = $2
               AND data->>'lock_token' = $5 AND data->'dead_at' IS NULL`,
            [message.id, message.queue, String(delay_ms), reason || '', token],
        );
        if (!res.rowCount) {
            throw new Error('postgres message queue lost the lock on the message');
        }
        return { dropped: false };
    }

    /**
     * @param {nb.QueueMessage} message
     * @returns {Promise<boolean>}
     */
    async extend(message) {
        const token = this._lock_token(message);
        await this.connect();
        const res = await this._query(
            `UPDATE ${TABLE}
             SET data = data || jsonb_build_object(
                 'locked_until', now() + ($3::text || ' milliseconds')::interval
             )
             WHERE _id = $1 AND data->>'queue_name' = $2
               AND data->>'lock_token' = $4 AND data->'dead_at' IS NULL`,
            [message.id, message.queue, String(config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS), token],
        );
        return Boolean(res.rowCount);
    }

    async _connect() {
        const db = db_client.instance();
        if (!db.is_connected()) await db.connect();
        await this._drop_legacy_table();
        const table = this._table();
        // define_collection starts create and swallows its error. Wait for that,
        // then create again so a failure here still fails connect.
        if (table.init_promise) await table.init_promise;
        await table._create_table(table.get_pool());
        await this._query(
            `DELETE FROM ${TABLE}
             WHERE data->'dead_at' IS NOT NULL
               AND (data->>'dead_at')::timestamptz < now() - interval '7 days'`,
        );
        this._ready = true;
        dbg.log0('postgres message queue ready');
    }

    /**
     * The first version of this table used typed columns. define_collection
     * will not replace an existing table, so drop that shape once.
     */
    async _drop_legacy_table() {
        const res = await this._query(
            `SELECT 1 FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = $1
               AND column_name = 'id'`,
            [TABLE],
        );
        if (!res.rows.length) return;
        dbg.log0('postgres message queue dropping legacy typed table', TABLE);
        await this._query(`DROP TABLE ${TABLE}`);
    }

    _table() {
        const db = db_client.instance();
        if (!queue_table) {
            queue_table = db.define_collection({
                name: TABLE,
                schema: message_queue_schema,
                db_indexes: message_queue_indexes,
            });
        }
        return queue_table;
    }

    /**
     * @param {string} text
     * @param {any[]} [values]
     */
    _query(text, values) {
        return db_client.instance().executeSQL(text, values || []);
    }

    /**
     * @param {nb.QueueMessage} message
     * @returns {string}
     */
    _lock_token(message) {
        if (!message || !message.lock_token) {
            throw new Error('postgres message queue message has no lock token');
        }
        return message.lock_token;
    }

}

exports.PostgresMessageQueue = PostgresMessageQueue;
exports.instance = PostgresMessageQueue.instance;
