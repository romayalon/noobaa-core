/* Copyright (C) 2016 NooBaa */
/** @typedef {typeof import('../sdk/nb')} nb */
'use strict';

const crypto = require('crypto');
const { Pool } = require('pg');

const dbg = require('./debug_module')(__filename);
const config = require('../../config');
const db_client = require('./db_client');
const { MessageQueueClient } = require('./message_queue_client');
const { makeWorkerUtils } = require('graphile-worker');

// graphile-worker 0.16 keeps runnable jobs in this schema. addJob is the
// supported enqueue API. completeJob deletes by id, so ack and nack use a
// locked_by token that dequeue mints for this claim alone.
const GRAPHILE_SCHEMA = 'graphile_worker';

/**
 * Message queue backed by graphile-worker on the NooBaa Postgres database.
 * The queue name is the graphile task identifier. Jobs are not given a
 * graphile queue_name, because that name runs jobs one at a time.
 *
 * @extends {MessageQueueClient}
 */
class GraphileMessageQueue extends MessageQueueClient {

    /** @type {GraphileMessageQueue | undefined} */
    static _instance;

    constructor() {
        super();
        this._ready = false;
        /** @type {Promise<void> | null} */
        this._connect_promise = null;
        /** @type {import('pg').Pool | null} */
        this._pool = null;
        /** @type {Awaited<ReturnType<typeof makeWorkerUtils>> | null} */
        this._utils = null;
    }

    static instance() {
        GraphileMessageQueue._instance = GraphileMessageQueue._instance || new GraphileMessageQueue();
        return GraphileMessageQueue._instance;
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

    async disconnect() {
        const utils = this._utils;
        const pool = this._pool;
        this._utils = null;
        this._pool = null;
        this._ready = false;
        this._connect_promise = null;
        try {
            if (utils) await utils.release();
        } finally {
            if (pool) await pool.end();
        }
    }

    /**
     * @param {string} queue
     * @param {object} payload
     * @returns {Promise<string>}
     */
    async enqueue(queue, payload) {
        this._assert_queue(queue);
        const body = this._normalize_payload(payload);
        const utils = await this._utils_ready();
        const job = await utils.addJob(queue, body, {
            maxAttempts: Math.max(1, config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS),
        });
        return String(job.id);
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
                SELECT jobs.id, jobs.attempts, jobs.max_attempts
                FROM ${GRAPHILE_SCHEMA}._private_jobs AS jobs
                WHERE jobs.task_id = (
                    SELECT id FROM ${GRAPHILE_SCHEMA}._private_tasks WHERE identifier = $2::text
                )
                  AND jobs.job_queue_id IS NULL
                  AND jobs.run_at <= now()
                  AND (
                    jobs.locked_at IS NULL
                    OR jobs.locked_at < now() - ($3::text || ' milliseconds')::interval
                  )
                ORDER BY jobs.priority ASC, jobs.run_at ASC, jobs.id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE ${GRAPHILE_SCHEMA}._private_jobs AS jobs
            SET attempts = CASE
                    WHEN candidate.attempts >= candidate.max_attempts THEN jobs.attempts
                    ELSE jobs.attempts + 1 END,
                locked_by = $1::text,
                locked_at = now(),
                updated_at = now()
            FROM candidate
            WHERE jobs.id = candidate.id
            RETURNING jobs.id, jobs.payload, jobs.attempts,
                (candidate.attempts >= candidate.max_attempts) AS terminal`,
            [token, queue, String(config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS)],
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
            `SELECT count(*)::int AS size
             FROM ${GRAPHILE_SCHEMA}._private_jobs AS jobs
             JOIN ${GRAPHILE_SCHEMA}._private_tasks AS tasks ON tasks.id = jobs.task_id
             WHERE tasks.identifier = $1::text`,
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
            `DELETE FROM ${GRAPHILE_SCHEMA}._private_jobs
             WHERE id = $1::bigint AND locked_by = $2::text`,
            [message.id, token],
        );
        if (!res.rowCount) {
            throw new Error('graphile message queue lost the lock on the message');
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
            dbg.warn('graphile message queue dropping message after max attempts', message.id, message.queue, reason);
            const res = await this._query(
                `DELETE FROM ${GRAPHILE_SCHEMA}._private_jobs
                 WHERE id = $1::bigint AND locked_by = $2::text`,
                [message.id, token],
            );
            if (!res.rowCount) {
                throw new Error('graphile message queue lost the lock on the message');
            }
            return { dropped: true };
        }
        const delay_ms = message.attempts * config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS;
        const res = await this._query(
            `UPDATE ${GRAPHILE_SCHEMA}._private_jobs
             SET last_error = $3::text,
                 run_at = now() + ($4::text || ' milliseconds')::interval,
                 locked_by = NULL,
                 locked_at = NULL,
                 updated_at = now()
             WHERE id = $1::bigint AND locked_by = $2::text`,
            [message.id, token, reason || 'nack', String(delay_ms)],
        );
        if (!res.rowCount) {
            throw new Error('graphile message queue lost the lock on the message');
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
            `UPDATE ${GRAPHILE_SCHEMA}._private_jobs
             SET locked_at = now(),
                 updated_at = now()
             WHERE id = $1::bigint AND locked_by = $2::text`,
            [message.id, token],
        );
        return Boolean(res.rowCount);
    }

    async _connect() {
        const db = db_client.instance();
        if (!db.is_connected()) await db.connect();
        const pool_params = /** @type {{ new_pool_params?: import('pg').PoolConfig }} */ (db).new_pool_params;
        if (!pool_params) throw new Error('graphile message queue requires the postgres db client');
        this._pool = new Pool({
            ...pool_params,
            max: config.MESSAGE_QUEUE_GRAPHILE_POOL_MAX,
        });
        this._utils = await makeWorkerUtils({
            pgPool: this._pool,
            noPreparedStatements: true,
        });
        await this._utils.migrate();
        this._ready = true;
        dbg.log0('graphile message queue ready');
    }

    async _utils_ready() {
        await this.connect();
        if (!this._utils) throw new Error('graphile message queue is not connected');
        return this._utils;
    }

    /**
     * @param {string} text
     * @param {any[]} [values]
     */
    async _query(text, values) {
        const utils = await this._utils_ready();
        return utils.withPgClient(client => client.query(text, values));
    }

    /**
     * @param {nb.QueueMessage} message
     * @returns {string}
     */
    _lock_token(message) {
        if (!message || !message.lock_token) {
            throw new Error('graphile message queue message has no lock token');
        }
        return message.lock_token;
    }

}

exports.GraphileMessageQueue = GraphileMessageQueue;
exports.instance = GraphileMessageQueue.instance;
