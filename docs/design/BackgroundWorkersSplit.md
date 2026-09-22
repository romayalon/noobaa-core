# Background Workers Isolation and Scaling — Detailed Design

High-level architecture (pods, producer-consumer, CRD, phasing):
[BackgroundWorkers.md](./BackgroundWorkers.md)

This document is the **internal** companion: worker inventory, RPC coupling, volumes, configuration, and how the producer-consumer pattern is implemented (including the message queue).

**Jira:**

* [RHSTOR-8954](https://redhat.atlassian.net/browse/RHSTOR-8954) — Extract BG out of core
* [RHSTOR-9447](https://redhat.atlassian.net/browse/RHSTOR-9447) — Change NooBaa's background workers to a sharded model

**Related:** [Background Worker Producer-Consumer Message Queue](./BackgroundWorkerMessageQueue.md)

---

### Table of Contents

* [Introduction](#introduction)
* [Glossary](#glossary)
* [Goals](#goals)
* [In Scope](#in-scope)
* [Out of Scope](#out-of-scope)
* [Feature Technical Details](#feature-technical-details)
  * [Current state](#current-state)
  * [Motivation](#motivation)
  * [Proposed architecture](#proposed-architecture)
  * [Phased rollout](#phased-rollout)
  * [Worker inventory and placement](#worker-inventory-and-placement)
  * [Workers that need extra changes](#workers-that-need-extra-changes)
  * [Queue choice](#queue-choice)
  * [Producer / consumer split](#producer--consumer-split)
  * [Scaling both producer and consumer](#scaling-both-producer-and-consumer)
  * [Configuration](#configuration)
  * [Performance](#performance)
  * [Scalability](#scalability)
  * [Availability / High Availability](#availability--high-availability)
  * [Concurrency / Race Conditions](#concurrency--race-conditions)
  * [Tradeoffs](#tradeoffs)
  * [DB schema changes](#db-schema-changes)
  * [API changes](#api-changes)
  * [CRD changes](#crd-changes)
  * [Documentation requirements](#documentation-requirements)
* [Affected Components](#affected-components)
* [Limitations](#limitations)
* [Dependencies](#dependencies)
* [Effort Estimation](#effort-estimation)
* [Open Questions](#open-questions)

---

## Introduction

Today the `noobaa-core` pod holds three responsibilities:

1. **Web Server** — configuration, stats, node monitor, mapper, management and MD RPC.
2. **Hosted agents** — service-based BackingStores and config.
3. **Background workers** — `bg_workers.js` via `Background_Scheduler`.

Those workers cover replication, archive, lifecycle (expiry and transition / migration), reclaimers, stats, DB cleaner, scrubber, and more. Several of them are heavy on DB or I/O. They **compete for CPU, memory, and Postgres** with core and with each other. When they are enabled, they slow I/O on one hand and cannot progress at a reasonable pace on the other.

On top of that, each worker is a **named singleton loop**: a cycle, a batch size, then sleep. That shape cannot add capacity when expiry, migration, replication, or a bulk delete of millions of objects needs more throughput.

The product architecture is in [BackgroundWorkers.md](./BackgroundWorkers.md). This document covers **both** tickets at implementation level: **which workers move**, **which talk to core**, **which split onto producer-consumer**, **how producer and consumer scale**, and **how work is handed off** (queue comparison in [BackgroundWorkerMessageQueue.md](./BackgroundWorkerMessageQueue.md)).

---

## Glossary

| Term | Meaning |
|------|---------|
| **Core / web server** | `web_server.js`: configuration, stats, node monitor, mapper, management RPC, object MD API, system_store publisher. |
| **Hosted agents** | `hosted_agents` process in the core pod. Not moved by this design. |
| **BG workers process** | `bg_workers.js`: registers every background worker with `Background_Scheduler`. |
| **BG pod** | New Deployment that owns running BG workers after RHSTOR-8954. One replica until the scale-up ticket splits it. |
| **Producer** | Discovers object-handling work and **enqueues** it. Can be scaled by **sharding** discovery (bucket / rule / key-range), not by running the same scan twice. |
| **Consumer** | Pops queue messages and **executes** them (delete, copy, transition, reclaim). Scaled as competing consumers. |
| **Object-handling workers** | Lifecycle expiry, lifecycle/archive **migration** (transition), replication (and log replication), objects reclaimer / bulk delete. These are the scale-up ticket’s first users. |
| **Singleton worker** | Stats, aggregators, key rotator, namespace monitor, scrubber **server**, and similar. They move to the BG pod but are **not** split onto the queue in the first scale-up phase. |
| **Task weight** | Relative share of a consumer’s effort for a named task (lifecycle vs replication vs reclaim, …). |
| **BG RPC** | APIs registered on the BG process: `scrubber_api`, `replication_api`, `archive_api`. Routed to `rpc.router.bg` (default port `SSL_PORT+2`). |
| **MD RPC** | `object_api`, served by the web server (`rpc.router.md`). |
| **Mgmt RPC** | Account / system / pool / bucket APIs, served by the web server (`rpc.router.default`). |
| **Queue** | Persistent producer-consumer handoff. See the [queue design](./BackgroundWorkerMessageQueue.md). |
| **Migration** | Object data movement driven by lifecycle transition / archive / tier spill — not cluster migration. |
| **NC** | Non-containerized NooBaa. Not this design. |

---

## Goals

**RHSTOR-8954**

* `noobaa-core` **no longer runs** BG workers.
* A **new pod** takes ownership of running the BG workers.
* Heavy BG flows (replication, archive, lifecycle, cleaner, …) get **dedicated CPU/memory** so they do not starve core I/O, and so they can be sized to make progress.
* Leave the door open for later auto-scale (not implemented in 8954).

**Scale-up ticket**

* Break the “one worker, one cycle, one batch” limit for **object-handling** flows: expiry, migration, replication, and large delete bursts.
* **First phase:** introduce **producer + consumer + queue** so **both** components can be scaled up.
* Expose producer/consumer replica counts and **per-task weights** on the NooBaa CR.

**Shared**

* Inventory every worker in `bg_workers.js`: who moves, who talks to core, who can later scale.
* Keep singleton scanners from double-running the **same** partition of work.

---

## In Scope

**RHSTOR-8954**

* Containerized NooBaa (operator / ODF).
* Core image and supervisor: stop `bg_workers` in the core pod.
* New BG Deployment + Service; independent `resources`.
* Cross-pod RPC: core → BG (`scrubber` / `replication` / `archive`) and BG → core (`object`, `pool`, `system`, `redirector`).
* Metrics that today are proxied from `web_server` to `localhost:7002`.
* Documentation: resource consumption, pod deployments, architecture behavior.

**Scale-up ticket (first phase: producer / consumer / queue)**

* Pluggable queue and dummy producer/consumer (see [queue design](./BackgroundWorkerMessageQueue.md)).
* Split object-handling workers onto produce + consume:
  * lifecycle **expiry**
  * lifecycle / archive **migration** (transition)
  * **replication** and log replication
  * **objects reclaimer** / bulk delete (millions of deletes)
* Scale **producer** (sharded discovery) and **consumer** (competing execute).
* CRD for producer/consumer replicas and task weights.
* DB cleaner is in the same “heavy / batch cycle” family; include it in the split set if it is needed for delete-burst responsiveness. Final order TBD.

**Workers that move with 8954 but are not queue-split in the first scale-up phase** still belong in this design (placement and RPC coupling), so the extract does not strand them.

---

## Out of Scope

* NC (`DB_TYPE=none` / nsfs-only). BG stays in-process there.
* Moving **hosted agents** out of core.
* Kubernetes **HPA / auto-scale** of BG pods (8954 makes it discussable; it is not the first scale-up phase).
* Exactly-once processing (at-least-once + idempotent consumers).
* Replacing `Background_Scheduler` as the producer tick.
* Kafka / AMQ Streams as the BG queue.
* Product UI / CLI for DLQ redrive (SQL/CLI is enough at first).
* Rewriting replication/lifecycle **algorithms** (smarter paging, intra-copy parallelism). Extra producers/consumers are the first lever.
* Changing S3 API behavior.

---

## Feature Technical Details

### Current state

```
                    noobaa-core Deployment (replicas: 1, or 2 with core HA)
  ┌──────────────────────────────────────────────────────────────────────┐
  │  supervisord                                                         │
  │    1. web_server.js   (config, stats, node monitor, mapper, MD/mgmt) │
  │    2. hosted_agents   (BackingStore / NamespaceStore agents)         │
  │    3. bg_workers.js   (replication, archive, lifecycle, reclaim, …)  │
  │         localhost RPC  (scrubber / replication / archive)            │
  │         metrics proxy  /metrics/bg_workers → :7002                   │
  └──────────────────────────────────────────────────────────────────────┘
         │  shared pod cgroup (CPU / RAM)
         │  shared PostgreSQL with S3 MD
         ▼
  Each worker: cycle → batch of N objects → sleep. Cannot add workers
  for expiry / migration / replication / bulk delete.
```

`web_server` already treats BG as a **separate RPC domain**
(`scrubber_api` / `replication_api` / `archive_api` → `router.bg`). The
processes are already split; only the **pod, cgroup, and Service** are not.

`BG_NODE_OPTIONS` vs `WEB_NODE_OPTIONS` can tune heap. They cannot give the
web server CPU that a BG worker is using, and they cannot add a second
replication or expiry consumer.

### Motivation

From **RHSTOR-8954**:

| Symptom | Why it happens today |
|---------|----------------------|
| Core I/O and management latency while BG is busy | Same pod: core, hosted agents, and BG share a cgroup. |
| Heavy workers (replication, archive, lifecycle, DB cleaner) starve each other and core | One process, one event loop, one Postgres. |
| Cannot give BG dedicated RAM/CPU | One `coreResources` knob sizes everything. |
| Core HA duplicates BG | Standby/leader core would clone workers that must not double-scan. |
| Auto-scale is not discussable | BG is not a separate Deployment. |

From the **scale-up ticket**:

| Symptom | Why it happens today |
|---------|----------------------|
| Expiry / migration / replication cannot go faster under load | One named worker, one cycle, one batch size. |
| Millions of deletes (lifecycle or reclaim) stay unresponsive | Same serial batch loop. |
| Cannot add capacity for one flow without rewriting that worker | No queue; produce and consume are the same `run_batch()`. |

**BG-internal issues** (same process, even after 8954 until the queue exists):

* One Node event loop: a CPU-heavy batch stalls every other worker.
* Shared `pg` pool.
* Implicit singleton: markers (`scrubber`, agent block verifier/reclaimer),
  `last_check` (DB cleaner), replication “least recently updated rule”.
  Two replicas of today’s `bg_workers` would double-scan and race.

8954 removes BG from core’s cgroup. The scale-up ticket removes the
single-cycle bottleneck for object work.

### Proposed architecture

**After RHSTOR-8954 (extract — still one BG replica, current scheduler):**

```
  noobaa-core Deployment                         noobaa-bg-workers (replicas: 1)
  ┌─────────────────────────┐   mgmt / md RPC    ┌──────────────────────────┐
  │ web_server              │◄──────────────────►│ bg_workers.js            │
  │ hosted_agents           │   BG RPC           │ all current workers      │
  └─────────────────────────┘   (Service)        └──────────────────────────┘
         ▲
         │ S3
  noobaa-endpoint
```

Core no longer runs BG. BG has its own `resources`. Behavior of each worker
is unchanged.

**After scale-up ticket phase 1 (producer + consumer + queue; both scalable):**

```
  noobaa-core                         noobaa-bg-producer (N, sharded)
  ┌─────────────────┐                 ┌──────────────────────────────┐
  │ web_server      │  mgmt/md RPC    │ singleton workers            │
  │ hosted_agents   │◄───────────────►│ + object-work producers      │
  └─────────────────┘  BG RPC         │   (discover + enqueue)       │
                                      └──────────────┬───────────────┘
                                                     │ produce
                                                     ▼
                                          PostgreSQL queue (see MQ doc)
                                                     │ consume
                                                     ▼
                                      noobaa-bg-consumer (M, competing)
                                      ┌──────────────────────────────┐
                                      │ expiry / migration /         │
                                      │ replication / reclaim …      │
                                      │ (weights on the CR)          │
                                      └──────────────────────────────┘
```

Two Deployments (producer + consumer) are the scale-up ticket’s first phase,
**not** a requirement to close 8954. Recommendation: ship 8954 as **one** BG
Deployment; add the consumer Deployment when the queue and first split worker
land.

### Phased rollout

| Phase | Ticket | What ships | What the customer gets |
|-------|--------|------------|------------------------|
| **1. Extract** | RHSTOR-8954 | Operator: BG Deployment + Service. Core stops `bg_workers`. RPC addresses, metrics, volumes, docs. | Dedicated BG resources. Core I/O isolated from BG CPU. Prerequisite for scale. |
| **2. Queue + dummy** | Scale-up (first phase) | Pluggable queue + dummy producer/consumer. | Proof of produce/consume, retry, DLQ, crash recovery. |
| **3. Split object-handling workers** | Scale-up | Expiry, migration, replication, objects reclaimer (and cleaner if in scope) onto the queue. **Producer and consumer Deployments, both scalable.** | Higher throughput for those flows; replicaCount on CR. |
| **4. CRD weights** | Scale-up | Per-task weights so a consumer pool can bias expiry vs replication vs reclaim. | Steer capacity without a Deployment per task. |
| **Later** | Follow-up | HPA / auto-scale; per-task consumer Deployments if weights are not enough isolation. | Opt-in auto-scale (the discussion 8954 enables). |

Phase 1 does **not** require a queue. Phases 2–4 do.

### Worker inventory and placement

Every worker registered in `run_master_workers()` / `main()` in
`src/server/bg_workers.js`.

**Placement keys:**

* **BG** = move to the BG pod in RHSTOR-8954.
* **P→C** = scale-up ticket: producer discovers, consumer executes.
* **Stay-core** = should not move without a rewrite.

**Scale keys:**

* **singleton** = one logical scanner (or one RPC server). Extra **unsharded** replicas are unsafe.
* **scale-out** = first users of producer/consumer/queue.

| Worker | 8954 | Scale-up | Scale | What it does | Primary I/O |
|--------|------|----------|-------|--------------|-------------|
| `cluster_heartbeat_writer` | **Stay-core (TBD)** | Stay-core | singleton | Writes **this process** OS/CPU/RAM/disk into cluster info; runs `server_monitor` | Heartbeat of the **web server** is wrong if this runs on the BG pod. |
| `system_server_stats_aggregator` | BG | BG | singleton | Phone-home + Prometheus system stats | **Mgmt RPC** (`node`, `host`, `pool`, `stats`, `system`) |
| `statistics_collector` | BG | BG | singleton | `system.read_system` | **Mgmt RPC** |
| `namespace_monitor` | BG | BG | singleton | Probe NS resources; update issues | **Mgmt RPC** `pool.update_*`; cloud SDKs |
| `replication scanner` | BG | **P→C** | **scale-out** | Diff src/dst, copy keys | **BG RPC** `replication.copy_objects`; S3 to endpoints |
| `log replication scanner` | BG | **P→C** | **scale-out** | Log-based replication | Same pattern as replication |
| `Bucket Log Uploader` | BG | BG | singleton | Upload access logs | Local **`/log/noobaa_bucket_logs/`** + S3. Volume must move with the pod. |
| `md_aggregator` | BG | BG | singleton | Object/chunk/block size windows | Postgres MD. Cursor on `global_last_update`. |
| `usage_aggregator` | BG | BG | singleton | Bandwidth / account usage | Postgres |
| `scrubber` | BG | BG (execute could P→C later) | singleton scanner | Iterate chunks, `MapBuilder` | Postgres + agents. **Also an RPC server** used by core. |
| `mirror_writer` | BG | BG | singleton | Async mirror of new chunks | **BG RPC** `scrubber.build_chunks` |
| `bucket_reclaimer` | BG | P→C candidate | singleton-ish | Delete objects in deleting buckets | **MD RPC** `object.delete_multiple_objects_unordered` |
| `object_reclaimer` | BG | **P→C** | **scale-out** | Reclaim deleted / expired restore / transition source; **bulk delete** | MD store, `map_deleter`, **BG RPC** `archive.*` |
| `tier_ttf_worker` | BG | P→C later (migration) | singleton first | Time-to-fill spill | **BG RPC** `scrubber.build_chunks` |
| `tier_spillover_worker` | BG | P→C later (migration) | singleton first | Spillback | **BG RPC** `scrubber.build_chunks` |
| `tiering_ttl_worker` | BG | P→C later (migration) | singleton first | TTL move | **BG RPC** `scrubber.build_chunks` |
| `dedup_indexer` | BG | BG | singleton | Dedup index | Postgres |
| `db_cleaner` | BG | **P→C** candidate | **scale-out** | Permanently remove old deleted docs | Postgres. **MD-latency risk.** Helps delete-burst cleanup. |
| `agent_blocks_verifier` | BG | BG | singleton | Verify blocks on agents | **block_store RPC** via n2n; MD |
| `agent_blocks_reclaimer` | BG | BG | singleton | Delete reclaimed blocks on nodes | MD + node RPCs |
| `lifecycle` | BG | **P→C** | **scale-out** | **Expiry**, MPU abort, **migration** (transition) | **MD RPC** `object.*`; **BG RPC** `archive.*`; `system_store.make_changes` |
| `key rotator` | BG | BG | singleton | Re-encrypt master key | `system_store.make_changes` → **redirector on core** |
| `Notificator` | BG | BG | singleton | Drain notification log files | **`NOTIFICATION_LOG_DIR`** |
| `restore_worker` | BG | P→C candidate | scale-out later | Complete ongoing restores | MD, ObjectIO, `archive_server` |

**8954 move set:** every worker except `cluster_heartbeat_writer` (see [Open Questions](#open-questions)).

**Scale-up first split set (object-handling):**

1. lifecycle **expiry**
2. lifecycle / archive **migration** (transition)
3. **replication** scanner
4. **log replication** scanner
5. **objects reclaimer** (bulk delete / reclaim)

Tier spill / TTF / TTL are also “migration” in the product sense; they can
follow once `scrubber.build_chunks` work is a queue job rather than a
core-facing RPC. Restore completion is a good second wave (already named in
the [queue design](./BackgroundWorkerMessageQueue.md)).

**Do not load-balance the scrubber RPC server** across consumer replicas.
Core (`map_server`, `nodes_monitor`) calls it. Keep that server on a
stable BG address (producer Deployment, or the 8954 single BG Service).

### Workers that need extra changes

These do **not** “just run on another pod”. They talk to the web server,
serve BG RPC, or use pod-local files. **RHSTOR-8954 must fix the routing
and volumes** even if those workers stay as in-process cycles until the
scale-up ticket splits them.

#### Shared: `system_store` (every worker)

BG workers call `system_store.wait_for_load()` then read cached config.
`make_changes()` publishes via `redirector.publish_to_cluster` on the **web
server**. `_register_for_changes` uses `redirector.register_to_cluster`.

**Required for extract:** BG pods must have `rpc.router.default` / `master`
pointing at the **core Service**, not at themselves. Today loopback works
because all processes share `localhost`.

#### Core → BG RPC (must keep working after extract)

Registered only on `bg_workers.js` (`register_bg_services`):

| API | Callers **outside** `bg_workers` |
|-----|----------------------------------|
| `scrubber_api` | `map_server.js`, `map_reader.js`, `nodes_monitor.js` (web_server / hosted agents) |
| `replication_api` | Replication scanners via `client.replication.copy_objects` (same process today) |
| `archive_api` | lifecycle, objects reclaimer, restore_worker |

**Required:** a Kubernetes Service for the BG RPC port (`SSL_PORT+2`) and
router addresses (`BG_ADDR` / address list) so **core and endpoints** still
reach scrubber/archive after BG leaves localhost.

When producer and consumer are two Deployments: **producer** (or a dedicated
headless Service with replica 1 for RPC) keeps `scrubber_api` /
`archive_api` / `replication_api` until those execute paths are consumers.
Do not round-robin BG RPC across scaled consumer pods.

#### BG → MD / mgmt RPC (web server)

| Worker | Calls | Route |
|--------|-------|--------|
| `lifecycle` | `object.delete_*`, `object.update_transition_info`, `object.unset_transition_in_progress` | `object_api` → **md** |
| `bucket_reclaimer` | `object.delete_multiple_objects_unordered` | md |
| `namespace_monitor` | `pool.update_issues_report`, `pool.update_last_monitoring` | **default** |
| `stats_aggregator` | `node.list_nodes`, `host.list_hosts`, `pool.read_*`, `stats.*` | default / master |
| `statistics_collector` | `system.read_system` | default |
| `replication*` | `replication.copy_objects` / `delete_objects` | **bg** (self today) |

These keep working if the router points at core (and at the BG Service for
`replication_api`). After extract, **latency, NetworkPolicy, and TLS** are
real.

#### Local disk (must move volume or stay on core)

| Worker | Path | Risk |
|--------|------|------|
| `Bucket Log Uploader` | `/log/noobaa_bucket_logs/` | EmptyDir on core is **lost** if the worker moves without the volume. |
| `Notificator` | `NOTIFICATION_LOG_DIR` | Endpoints write files; BG drains them. PVC must follow the BG pod **or** these workers stay with the files. |

#### Process-local identity

| Worker | Issue |
|--------|-------|
| `cluster_heartbeat_writer` | Heartbeat is **this OS**. On a BG pod it reports the BG node as if it were core. **Keep on `web_server`.** |
| `server_monitor` (from cluster_hb) | `cluster_server.check_cluster_status` — conceptually core. |

#### In-process vs RPC inconsistency

`archive_server.js` documents: call in-process, not via `rpc_client.archive`.
`restore_worker` mixes ObjectIO + `archive_server` require and
`rpc_client.archive.check_archive_restore_status`. After extract, **archive
execution stays on the process that registered `archive_api`**. After the
scale-up split, migration/expiry consumers call archive as a **queue
handler** (or RPC to the producer) — not a second uncoordinated in-process
server.

#### n2n / agents

`agent_blocks_verifier` calls `server_rpc.client.block_store.verify_blocks`.
`ServerRpc` registers an n2n proxy through `client.node.n2n_proxy`. Node API
is on **master** (web_server). BG pods need that router entry and the
existing internal auth token.

### Queue choice

Full comparison: [BackgroundWorkerMessageQueue.md](./BackgroundWorkerMessageQueue.md).

Needed for the **scale-up ticket**, not for 8954.

**Decision to ratify (recommended default):**

| Option | Role |
|--------|------|
| **Graphile Worker** | **Default.** npm job queue on existing Postgres. `addJob` + task handlers. Upstream owns queue-engine CVEs (we bump npm). No extra broker pod. |
| Homegrown `SKIP LOCKED` | Only if we need caller-side `pop()` / `ack()` / `nack()`, a ~30s visibility timeout, or a dedicated DLQ table. **More work.** |
| PGMQ | Lab only. Vendoring `pgmq.sql` makes us the PGMQ CVE owner. |
| RabbitMQ / NATS | Not default. Extra StatefulSet, no RH product image. |

Handler-based consume (`produce` + `run_consumer(queue, handler)`) matches
Graphile and still allows a later broker. Task **weights** map to per-queue
concurrency, not to wall-clock “50% of the time”.

### Producer / consumer split

Object-handling workers **stop doing find+execute in one `run_batch()`**.

| Today (one cycle, one batch) | After split |
|------------------------------|-------------|
| Lifecycle expiry: query matching keys **and** delete | Producer: jobs `{ bucket, rule, prefix/token or object ids }`. Consumer: expire that batch. |
| Lifecycle / archive migration: find transition candidates **and** archive | Producer: enqueue object ids + target class. Consumer: transition. |
| Replication: list+diff **and** `copy_objects` | Producer: `{ replication_id, rule_id, keys_diff_map }` or a page. Consumer: copy. |
| Objects reclaimer / bulk delete: `find_unreclaimed_objects` **and** mapping/archive delete | Producer: enqueue object ids. Consumer: reclaim. |
| DB cleaner (if included): `find_deleted_*` **and** `db_delete_*` | Producer: enqueue id lists. Consumer: hard-delete. Keep `md_aggregator` watermark on the producer. |

**Idempotency is mandatory.** Consumers are at-least-once. Lifecycle delete,
reclaim, and replication copy must tolerate duplicates (they largely already
do). Prefer a `dedupe_key` on produce (object id + operation) so scaled
producers do not flood the queue.

Singleton workers (stats, aggregators, key rotator, namespace monitor,
scrubber **server**) stay on the producer Deployment as today’s scheduler
loops. They are not why we scale.

### Scaling both producer and consumer

The scale-up ticket’s first phase requires that **both** components can
scale. That is not “run today’s scanner N times”.

**Consumer (M replicas)** — competing consumers on the queue. Standard.
`SKIP LOCKED` / Graphile claim. Throughput ≈ min(queue depth, M ×
concurrency × handler speed).

**Producer (N replicas)** — shard **discovery**, then enqueue:

| Approach | How | Use for |
|----------|-----|---------|
| **Hash / range by bucket** | `hash(bucket_id) % N == ordinal` (or consistent hashing via a lease table) | Lifecycle expiry/migration, bucket reclaimer |
| **Hash by replication policy / rule** | One producer owns a subset of rules | Replication, log replication |
| **SKIP LOCKED scan leases** | Rows `{ worker, partition, cursor }`; producers claim a partition | Shared object-id iteration (reclaimer, cleaner) |
| **Queue as the only work set** | Producer inserts candidate ids with `ON CONFLICT` / Graphile job key | Safety net when two producers overlap |

Kubernetes `ordinal` (StatefulSet) or a small lease in Postgres gives each
producer a stable shard. ReplicaCount changes require rebalancing
(re-hash or steal leases). **Do not** use a Deployment of N identical
scanners with no shard key.

Producer scale helps when **discovery** is the bottleneck (listing millions
of keys, many buckets with lifecycle). Consumer scale helps when **execute**
is the bottleneck (copy, transition, delete). Customers with bulk delete or
replication of large buckets typically need **both**.

Replica **1** of each remains the default. Scale is opt-in on the CR.

### Configuration

#### `config.js` / env (core image)

RHSTOR-8954:

```javascript
// Core pod: do not start bg_workers.js at all (supervisor / entrypoint).
// BG pod: today's run_master_workers().
```

Scale-up ticket:

```javascript
config.BG_WORKER_ROLE = process.env.BG_WORKER_ROLE || 'all';
// 'all'     — 8954 single BG pod (current loops)
// 'producer'— singleton workers + enqueue object-handling work
// 'consumer'— queue handlers only

config.BG_PRODUCER_ORDINAL = Number(process.env.BG_PRODUCER_ORDINAL) || 0;
config.BG_PRODUCER_COUNT = Number(process.env.BG_PRODUCER_COUNT) || 1;
```

Queue knobs: [queue design](./BackgroundWorkerMessageQueue.md)
(`MQ_TYPE`, `MQ_MAX_ATTEMPTS`, dummy worker flags).

Task weights — **relative dequeue / concurrency**, not wall-clock slicing:

```javascript
config.BG_CONSUMER_WEIGHTS = parse_weights(process.env.BG_CONSUMER_WEIGHTS);
// e.g. "lifecycle=50,replication=30,objects_reclaimer=20"
```

Wall-clock “50% of time” is a poor control: one long copy job blows the
budget. Weights as **job slots** (Graphile concurrency per task, or
weighted pop among non-empty queues) are implementable and map to the CR.

#### Supervisor / image

8954 core pod: `webserver` + `hosted_agents` only.

8954 BG pod: `bg_workers` only.

Scale-up: same image; `BG_WORKER_ROLE` selects producer vs consumer
entrypoint so we do not duplicate RPC/db/`system_store` init.

### Performance

* **8954 win:** web_server CPU/heap no longer shared with BG. MD Postgres
  is still shared — cleaner and aggregators can still hurt S3. Batch sizes
  and cycles stay configurable.
* **Scale-up win:** expiry / migration / replication / bulk delete add
  consumers (and sharded producers) instead of shrinking `run_batch` delay.
* **Extra hop after extract:** lifecycle `object.*` RPCs become pod-to-pod.
  Expect extra milliseconds per call; workers already batch.
* **Do not enqueue on the S3 PUT path.** Queue traffic is background scans.
* Graphile / homegrown queues add writes on the **same** Postgres. Object
  jobs are low tens–thousands of messages/min at first, not a bus. If
  replication of billions of keys enqueues one job per object, re-evaluate
  payload size (page of keys per job, not one key per job). See queue doc
  Performance.

### Scalability

* **Vertical (8954):** `resources` on the BG Deployment. Immediate answer
  to “give BG RAM without giving it to core.”
* **Horizontal producers (scale-up):** `replicaCount` + shard key. Default 1.
* **Horizontal consumers (scale-up):** competing consumers. Default 1 until
  the first worker is split; then default can stay 1 with opt-in N.
* **Weights** steer a shared consumer pool. Alternative: a consumer
  Deployment per task (`noobaa-bg-lifecycle`, …). Start with **one consumer
  Deployment + weights**. Split further only if noisy-neighbor among tasks
  remains.
* **HPA** is a later discussion, enabled by having distinct Deployments.

### Availability / High Availability

* **8954:** one BG replica. Pod kill pauses all BG work until restart. Core
  stays up. Core HA no longer clones BG — operator must **not** start
  `bg_workers` on standby core.
* **Scale-up:** producer shards; losing one producer pauses **that shard’s
  discovery** only. Consumers are cattle; crash → visibility timeout /
  Graphile lock → retry.
* Postgres down: workers already cannot run. Queue on Postgres has the same
  fate (see queue doc).
* BG Service down: **core `map_server` / `nodes_monitor` cannot call
  scrubber**. BG readiness must include BG RPC listen.

### Concurrency / Race Conditions

* **Unsharded duplicate producers** of the same worker are unsafe (markers,
  “least recently updated rule”).
* **Sharded producers** must have a disjoint partition function; `dedupe_key`
  is the backstop.
* **Consumers** must use the queue’s claim.
* **DB cleaner vs md_aggregator:** cleaner already waits until aggregator
  passes `DB_CLEANER_BACK_TIME`. Keep that watermark on the producer.
* **Replication status / cont tokens:** one writer per rule (the producer
  that owns that shard). Consumers report copy results; avoid lost updates
  on `replication_store`.
* **Lifecycle last_sync:** producer writes after a page is enqueued or after
  a generation is fully acked. TBD per implementation ticket.

### Tradeoffs

| Choice | Gain | Cost |
|--------|------|------|
| 8954 extract, no queue | Dedicated BG resources; smaller core HA story; docs/pod model | Still one BG event loop; still one cycle/batch per worker |
| Producer + consumer + queue (scale-up phase 1) | Both can scale; expiry/migration/replication/delete throughput | Queue on MD Postgres; at-least-once; worker splits |
| Two Deployments from day one of 8954 | CR shape matches end state | Empty consumer before it pays off |
| Graphile default | Least queue engineering; npm CVE bumps | Handler consume; 4h crash lock unless tuned; ESM |
| Homegrown queue | Exact pop/ack/DLQ/VT | We own a queue forever |
| Weights on one consumer pool | Simple CR | Noisy neighbor among consumer tasks |
| Consumer Deployment per task | Hard isolation | Many Deployments |
| Leave heartbeat on core | Correct cluster health | BG pod uses its own liveness, not `cluster_hb` |
| Page-of-keys jobs vs one-object jobs | Queue stays small at replication scale | Consumer batches must stay idempotent |

**Recommendation:**

1. **8954:** one BG Deployment, all current workers, dedicated resources.
2. **Scale-up phase 1:** Graphile (unless the queue doc decision is reversed),
   dummy worker, then split object-handling workers; **producer and consumer
   Deployments, both scalable via CR replicaCount**.
3. Weights on one consumer pool; per-task Deployments only if needed.
4. HPA later.

### DB schema changes

* **8954:** none.
* **Scale-up:** Graphile `graphile_worker` schema, or homegrown `mq_*`
  tables. Optional scan-lease table for producer sharding. See
  [queue design](./BackgroundWorkerMessageQueue.md).
* Worker splits should **not** add new product collections if the queue
  payload can hold ids/tokens. Replication/lifecycle status stays in
  existing stores.

### API changes

* No S3 API change.
* RPC **routing** changes (addresses), not schemas, in 8954.
* Optional later: admin RPC to inspect/redrive DLQ.

### CRD changes

Operator (noobaa-operator). Names are a proposal.

**RHSTOR-8954 minimum:**

```yaml
apiVersion: noobaa.io/v1alpha1
kind: NooBaa
spec:
  # web_server + hosted_agents only
  coreResources:
    requests: { cpu: "1", memory: 4Gi }

  backgroundWorkers:
    resources:
      requests: { cpu: "1", memory: 4Gi }
      limits:   { cpu: "2", memory: 8Gi }
    replicaCount: 1   # must stay 1 until scale-up sharding exists
```

`coreResources` no longer sizes BG.

**Scale-up ticket** (extends the same CR):

```yaml
spec:
  backgroundWorkers:
    producer:
      replicaCount: 1   # opt-in >1 when sharding is implemented
      resources: { requests: { cpu: "1", memory: 4Gi } }
    consumer:
      replicaCount: 2
      resources: { requests: { cpu: "2", memory: 4Gi } }
      # relative effort; 0 = this pool will not run that task
      weights:
        lifecycle: 50          # expiry + migration jobs
        replication: 30
        objectsReclaimer: 20
```

8954 can ship `backgroundWorkers.resources` and later nest it under
`producer` without breaking customers (operator conversion).

Metrics: stop `web_server` proxy of `localhost:7002`. Scrape the BG
Service (`/metrics/bg_workers` or a dedicated port) from ServiceMonitor.
After the split, scrape producer and consumer separately.

### Documentation requirements

From RHSTOR-8954:

* Update **resource consumption** docs: core vs BG (and later producer vs
  consumer) requests/limits; that `coreResources` does not include BG.
* Update **pod deployments**: `noobaa-core` (web_server + hosted_agents),
  `noobaa-bg-workers` (then `noobaa-bg-producer` / `noobaa-bg-consumer`).
* Document behavior in the **architecture** document
  ([BackgroundWorkers.md](./BackgroundWorkers.md)). Operator/ODF architecture
  docs must describe the new topology, RPC between core and BG, and that BG
  replicaCount > 1 is unsafe until sharding exists.

---

## Affected Components

| Component | Ticket | Change |
|-----------|--------|--------|
| `src/deploy/NVA_build/noobaa_supervisor.conf` | 8954 | Core: drop `bg_workers`. BG-only entrypoint. |
| `src/server/bg_workers.js` | both | 8954: unchanged loops on BG pod. Scale-up: `BG_WORKER_ROLE` producer/consumer. |
| `src/server/web_server.js` | 8954 | Metrics proxy; maybe take over cluster heartbeat. |
| RPC router / operator env | 8954 | `BG_ADDR`, core Service DNS for md/mgmt from BG pods. |
| `noobaa-operator` | both | 8954: BG Deployment, Service, PDB, metrics, CR `resources`. Scale-up: producer/consumer Deployments, replicaCount, weights. |
| Queue (`src/util/mq/`) | scale-up | See queue design. |
| Lifecycle, replication, objects reclaimer, … | scale-up | Split `run_batch` into produce vs consume. |
| Tests | both | Coretest in-process. Integration: remote BG RPC; then queue concurrency. |
| Docs (resources, topology, architecture) | 8954 | Required by the ticket. |
| NC | — | None. |

---

## Limitations

* **8954** does not fix **Postgres** contention (cleaner, aggregators).
* **8954** still has **one BG event loop** and one cycle/batch per worker.
* Unsharded BG `replicaCount` > 1 is **unsafe** until producer sharding.
* At-least-once consumers: duplicate expiry/replication/delete possible.
* Graphile default: crash lock and DLQ shape per queue doc.
* Bucket logs and notifications need a **volume story** or they break on
  extract.
* NC is unchanged.
* Core `build_chunks` still depends on BG RPC availability.
* Queue on the MD database can itself become a bottleneck at “one job per
  object” for billions of keys — use paged jobs.

---

## Dependencies

| Dependency | Ticket | Notes |
|------------|--------|--------|
| noobaa-operator Deployment/Service/CRD | 8954, scale-up | Hard dependency for product |
| Existing PostgreSQL 15 | both | Unchanged |
| Core HA (RHSTOR-7491) | 8954 | Extract makes HA cleaner; never start BG on standby core |
| Queue library (Graphile or homegrown) | scale-up | [queue design](./BackgroundWorkerMessageQueue.md) |
| Shared volumes for logs/notifications | 8954 | If those workers move |

Jira lists **N/A** for 8954 product dependencies; operator work is still
required to create the pod.

---

## Effort Estimation

| Work item | Size | Ticket |
|-----------|------|--------|
| Operator: BG Deployment + Service + router env + metrics | M (1–1.5w) | 8954 |
| Image/supervisor: BG-only vs core-only | S (2–3d) | 8954 |
| Heartbeat / metrics / volume follow-ups | S–M | 8954 |
| Integration test: core RPC → remote BG scrubber | S–M | 8954 |
| Docs: resources, pods, architecture | S | 8954 |
| Queue + dummy workers | ~1–2w | scale-up |
| Producer sharding (ordinal / leases) | M | scale-up |
| Split lifecycle (expiry + migration) | M–L | scale-up |
| Split replication + log replication | M–L | scale-up |
| Split objects reclaimer (bulk delete) | M | scale-up |
| Split DB cleaner (watermark + batch ids) | M | scale-up (if included) |
| CRD replicaCount + weights | M | scale-up |

**Do not wait for the queue to extract the pod.** 8954 is independently
valuable. The scale-up ticket’s **first** deliverable is producer +
consumer + queue, then the object-handling splits.

---

## Open Questions

Product-level questions (two Deployments in 8954, heartbeat, split order, weights, cleaner, work-item size, volumes) are in [BackgroundWorkers.md](./BackgroundWorkers.md).

Implementation:

1. **Queue default:** Graphile vs homegrown. Recommendation: **Graphile**.
   Ratify before 9447 phase 1. See [queue design](./BackgroundWorkerMessageQueue.md).
2. **Who serves BG RPC** when consumers exist? Recommendation: **producer
   (or replica-1 RPC Service) only**.
3. **Producer scale mechanism:** bucket hash vs Postgres scan leases vs
   StatefulSet ordinal. Need one before `producer.replicaCount` > 1.
4. **Core HA + 8954:** confirm operator never starts `bg_workers` on
   standby core pods.
5. **NetworkPolicy / mTLS** between core ↔ BG. Today localhost.
6. **Consumer entrypoint:** same `bg_workers.js` + `BG_WORKER_ROLE`
   (recommended) vs `bg_consumers.js`.
7. **`dedupe_key` on produce** from the first real split (recommended once
   producers can scale) vs after dummy shows duplicates.






