/* Copyright (C) 2016 NooBaa */
/** @typedef {typeof import('../sdk/nb')} nb */
'use strict';

const crypto = require('crypto');
const dbg = require('./debug_module')(__filename);
const config = require('../../config');
const db_client = require('./db_client');
const { MessageQueueClient } = require('./message_queue_client');
const schema = require('./message_queue_schema');

const TABLE = schema.TABLE;

/**
 * Persistent queue stored in the NooBaa Postgres database.
 * dequeue uses FOR UPDATE SKIP LOCKED so concurrent workers each take a
 * different message. Each claim gets a lock token that ack and nack must
 * present, so a worker whose visibility lock expired cannot complete the
 * next claim. extend slides that lock forward. An expired claim that already
 * used its attempts is returned with terminal set, and nack records it with
 * dead_at so it leaves the ready index.
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
        const res = await this._query(
            `INSERT INTO ${TABLE} (queue_name, payload)
             VALUES ($1, $2::jsonb)
             RETURNING id`,
            [queue, JSON.stringify(body)],
        );
        return String(res.rows[0].id);
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
                 SELECT id, attempts
                 FROM ${TABLE}
                 WHERE queue_name = $1
                   AND dead_at IS NULL
                   AND visible_at <= now()
                   AND (locked_until IS NULL OR locked_until < now())
                 ORDER BY id
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1
             )
             UPDATE ${TABLE} AS q
             SET lock_token = $3,
                 locked_until = now() + ($2::text || ' milliseconds')::interval,
                 attempts = CASE
                     WHEN candidate.attempts >= $4 THEN q.attempts
                     ELSE q.attempts + 1 END
             FROM candidate
             WHERE q.id = candidate.id
             RETURNING q.id, q.payload, q.attempts, (candidate.attempts >= $4) AS terminal`,
            [queue, String(config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS), token, config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS],
        );
        const row = res.rows[0];
        if (!row) return null;
        return {
            id: String(row.id),
            queue,
            payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload,
            attempts: Number(row.attempts),
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
             WHERE queue_name = $1 AND dead_at IS NULL`,
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
             WHERE id = $1::bigint AND queue_name = $2 AND lock_token = $3 AND dead_at IS NULL`,
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
                 SET dead_at = now(),
                     last_error = $4,
                     locked_until = NULL,
                     lock_token = NULL
                 WHERE id = $1::bigint AND queue_name = $2 AND lock_token = $3 AND dead_at IS NULL`,
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
             SET locked_until = NULL,
                 lock_token = NULL,
                 last_error = $4,
                 visible_at = now() + ($3::text || ' milliseconds')::interval
             WHERE id = $1::bigint AND queue_name = $2 AND lock_token = $5 AND dead_at IS NULL`,
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
             SET locked_until = now() + ($3::text || ' milliseconds')::interval
             WHERE id = $1::bigint AND queue_name = $2 AND lock_token = $4 AND dead_at IS NULL`,
            [message.id, message.queue, String(config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS), token],
        );
        return Boolean(res.rowCount);
    }

    async _connect() {
        const db = db_client.instance();
        if (!db.is_connected()) await db.connect();
        await this._query(schema.CREATE_TABLE);
        for (const statement of schema.ADD_COLUMNS) {
            await this._query(statement);
        }
        await this._query(schema.LIVE_INDEX);
        await this._query(schema.DROP_READY_INDEX);
        await this._query(schema.DELETE_EXPIRED_DEAD);
        this._ready = true;
        dbg.log0('postgres message queue ready');
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
