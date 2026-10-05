/* Copyright (C) 2016 NooBaa */
/** @typedef {typeof import('../sdk/nb')} nb */
'use strict';

const { PgBoss } = require('pg-boss');

const dbg = require('./debug_module')(__filename);
const config = require('../../config');
const db_client = require('./db_client');
const { MessageQueueClient } = require('./message_queue_client');

// pg-boss installs its own tables in this schema. Jobs live in nb_pgboss.job.
// fetch() is dequeue: the first claim leaves retry_count at 0, and every later
// claim increments it, so attempts reported to callers are retry_count + 1.
const SCHEMA = 'nb_pgboss';

/**
 * Message queue backed by pg-boss on the NooBaa Postgres database.
 * enqueue sends a job, dequeue fetches one, ack deletes it, and nack either
 * deletes it or fails it back onto the queue. ack and nack pass the fetched
 * retry_count so a late settle cannot delete a newer claim of the same id.
 *
 * @extends {MessageQueueClient}
 */
class PgBossMessageQueue extends MessageQueueClient {

    /** @type {PgBossMessageQueue | undefined} */
    static _instance;

    constructor() {
        super();
        this._ready = false;
        /** @type {Promise<void> | null} */
        this._connect_promise = null;
        /** @type {import('pg-boss').PgBoss | null} */
        this._boss = null;
        /** @type {Set<string>} */
        this._queues = new Set();
    }

    static instance() {
        PgBossMessageQueue._instance = PgBossMessageQueue._instance || new PgBossMessageQueue();
        return PgBossMessageQueue._instance;
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
        const boss = this._boss;
        this._boss = null;
        this._ready = false;
        this._connect_promise = null;
        this._queues.clear();
        if (boss) await boss.stop({ graceful: false });
    }

    /**
     * @param {string} queue
     * @param {object} payload
     * @returns {Promise<string>}
     */
    async enqueue(queue, payload) {
        this._assert_queue(queue);
        const body = this._normalize_payload(payload);
        const boss = await this._boss_ready();
        await this._ensure_queue(queue);
        const id = await boss.send(queue, body, job_options());
        if (!id) throw new Error('pg-boss message queue did not accept the message');
        return id;
    }

    /**
     * @param {string} queue
     * @returns {Promise<nb.QueueMessage | null>}
     */
    async dequeue(queue) {
        this._assert_queue(queue);
        const boss = await this._boss_ready();
        if (!await this._has_queue(queue)) return null;
        const [job] = await boss.fetch(queue);
        if (!job) return null;
        const payload = typeof job.data === 'string' ? JSON.parse(job.data) : job.data;
        const retry_count = Number(job.retryCount);
        return {
            id: job.id,
            queue,
            payload,
            attempts: retry_count + 1,
            retry_count,
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
             FROM ${SCHEMA}.job
             WHERE name = $1
               AND state::text IN ('created', 'retry', 'active')`,
            [queue],
        );
        return Number(res.rows[0].size);
    }

    /**
     * @param {nb.QueueMessage} message
     * @returns {Promise<void>}
     */
    async ack(message) {
        const boss = await this._boss_ready();
        const deleted = await boss.deleteJob(message.queue, claimed_job(message));
        if (!affected_count(deleted)) throw new Error('pg-boss message queue could not ack the message');
    }

    /**
     * @param {nb.QueueMessage} message
     * @param {string} [reason]
     * @returns {Promise<nb.MessageQueueNackResult>}
     */
    async nack(message, reason) {
        const boss = await this._boss_ready();
        if (message.attempts >= config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS) {
            dbg.warn('pg-boss message queue dropping message after max attempts', message.id, message.queue, reason);
            const deleted = await boss.deleteJob(message.queue, claimed_job(message));
            if (!affected_count(deleted)) throw new Error('pg-boss message queue could not drop the message');
            return { dropped: true };
        }
        const delay_ms = message.attempts * config.MESSAGE_QUEUE_DEFAULT_RETRY_DELAY_MS;
        const output = { message: reason || 'nack' };
        if (delay_ms <= 0) {
            const failed = await boss.fail(message.queue, claimed_job(message), output);
            if (!affected_count(failed)) throw new Error('pg-boss message queue could not nack the message');
            return { dropped: false };
        }
        await this._nack_later(message, output, delay_ms);
        return { dropped: false };
    }

    /**
     * pg-boss expires an active job from started_on + expireInSeconds. touch()
     * only moves heartbeat_on, so the lease slides by rewriting started_on
     * for this attempt.
     * @param {nb.QueueMessage} message
     * @returns {Promise<boolean>}
     */
    async extend(message) {
        const job = claimed_job(message);
        await this.connect();
        const res = await this._query(
            `UPDATE ${SCHEMA}.job
             SET started_on = now(),
                 heartbeat_on = now()
             WHERE name = $1
               AND id = $2::uuid
               AND state = 'active'
               AND (id::text || ':' || retry_count::text) = $3`,
            [message.queue, job.id, `${job.id}:${job.retryCount}`],
        );
        return Boolean(res.rowCount);
    }

    /**
     * Fail the job and push its start time forward in one transaction, so another
     * worker cannot fetch it before the retry delay is stored.
     * @param {nb.QueueMessage} message
     * @param {object} output
     * @param {number} delay_ms
     */
    async _nack_later(message, output, delay_ms) {
        const boss = this._boss;
        if (!boss) throw new Error('pg-boss message queue is not connected');
        const db = boss.getDb();
        if (typeof db.beginTransaction !== 'function') {
            throw new Error('pg-boss message queue cannot delay a nack without a transaction');
        }
        const tx = await db.beginTransaction();
        try {
            const failed = await boss.fail(message.queue, claimed_job(message), output, { db: tx.db });
            if (!affected_count(failed)) throw new Error('pg-boss message queue could not nack the message');
            const updated = await boss.update({
                name: message.queue,
                options: {
                    id: message.id,
                    startAfter: new Date(Date.now() + delay_ms),
                    db: tx.db,
                },
            });
            if (!updated.updated) throw new Error('pg-boss message queue could not delay the message');
            await tx.commit();
        } catch (err) {
            await tx.rollback();
            throw err;
        }
    }

    async _connect() {
        const db = db_client.instance();
        if (!db.is_connected()) await db.connect();
        const pool_params = /** @type {{ new_pool_params?: import('pg').PoolConfig }} */ (db).new_pool_params;
        if (!pool_params) throw new Error('pg-boss message queue requires the postgres db client');
        const boss = new PgBoss({
            host: pool_params.host,
            port: pool_params.port,
            user: pool_params.user,
            password: pool_params.password,
            database: pool_params.database,
            ssl: pool_params.ssl,
            max: config.MESSAGE_QUEUE_PGBOSS_POOL_MAX,
            schema: SCHEMA,
            application_name: 'noobaa-pgboss',
            schedule: false,
        });
        boss.on('error', err => {
            dbg.error('pg-boss message queue error', err);
        });
        this._boss = boss;
        try {
            await boss.start();
        } catch (err) {
            this._boss = null;
            await boss.stop({ graceful: false }).catch(stop_err => {
                dbg.error('pg-boss message queue stop after failed start', stop_err);
            });
            throw err;
        }
        this._ready = true;
        dbg.log0('pg-boss message queue ready');
    }

    async _boss_ready() {
        await this.connect();
        if (!this._boss) throw new Error('pg-boss message queue is not connected');
        return this._boss;
    }

    /**
     * @param {string} queue
     */
    async _ensure_queue(queue) {
        if (this._queues.has(queue)) return;
        const boss = await this._boss_ready();
        await boss.createQueue(queue, job_options());
        this._queues.add(queue);
    }

    /**
     * @param {string} queue
     * @returns {Promise<boolean>}
     */
    async _has_queue(queue) {
        if (this._queues.has(queue)) return true;
        const boss = await this._boss_ready();
        const existing = await boss.getQueue(queue);
        if (!existing) return false;
        this._queues.add(queue);
        return true;
    }

    /**
     * @param {string} text
     * @param {any[]} [values]
     */
    async _query(text, values) {
        const boss = await this._boss_ready();
        return boss.getDb().executeSql(text, values);
    }

}

/**
 * Retry and visibility settings copied onto each queue and job.
 * @returns {import('pg-boss').SendOptions}
 */
function job_options() {
    return {
        retryLimit: retry_limit(),
        retryDelay: 0,
        retryBackoff: false,
        expireInSeconds: expire_seconds(),
    };
}

function retry_limit() {
    return Math.max(1, config.MESSAGE_QUEUE_DEFAULT_MAX_ATTEMPTS);
}

function expire_seconds() {
    const seconds = Math.ceil(config.MESSAGE_QUEUE_DEFAULT_VISIBILITY_MS / 1000);
    return Math.min(24 * 60 * 60, Math.max(1, seconds));
}

/**
 * deleteJob and fail resolve { affected }, which the published CommandResponse
 * type leaves out.
 * @param {import('pg-boss').CommandResponse} response
 */
function affected_count(response) {
    const count = /** @type {{ affected?: number }} */ (/** @type {unknown} */ (response)).affected;
    return Number(count) || 0;
}

/**
 * The attempt fetch returned. deleteJob and fail fence on this pair, so a
 * later claim of the same id is left alone.
 * @param {nb.QueueMessage} message
 * @returns {import('pg-boss').JobAttempt}
 */
function claimed_job(message) {
    const retry_count = message && message.retry_count;
    if (!message || !message.id || typeof retry_count !== 'number' || !Number.isInteger(retry_count) || retry_count < 0) {
        throw new Error('pg-boss message queue message has no attempt');
    }
    return { id: message.id, retryCount: retry_count };
}

exports.PgBossMessageQueue = PgBossMessageQueue;
exports.instance = PgBossMessageQueue.instance;
