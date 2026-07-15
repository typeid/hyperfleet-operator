# DynamoDB Status Distribution

How DynamoDB status updates reach the right controller.

## Overview

```mermaid
flowchart LR
    subgraph "hyperfleet-operator"
        CC[ClusterController]
        NC[NodePoolController]
        MC[ManifestController]
        ER[EventRouter]
        SM["Stream Manager\n(1 watcher per MC table)"]
    end

    subgraph "DynamoDB (per MC)"
        ST["status-readdesires\nstatus-applydesires\nstatus-deletedesires"]
    end

    KA[kube-applier-aws]

    KA -->|writes status| ST
    ST -->|DynamoDB Stream| SM
    SM -->|"Dispatch(docID)"| ER
    ER -->|GenericEvent| CC
    ER -->|GenericEvent| NC
    ER -->|GenericEvent| MC
```

## Event flow

1. **kube-applier-aws** applies/deletes/reads resources on a management cluster and writes the result to the MC's DynamoDB status tables.

2. **Stream Manager** runs one `Watcher` goroutine per MC per status table suffix. Each watcher polls the DynamoDB Stream (every 1s), extracts the `documentID` from each INSERT/MODIFY event, and calls `EventRouter.Dispatch(docID)`. DynamoDB Streams has no push mechanism — "tailing" a stream means polling `GetRecords` in a loop.

3. **EventRouter** is a shared in-memory index mapping `documentID → {channel, CR key}`. On dispatch, it looks up the document ID and sends a `GenericEvent` into the target controller's `StatusEvents` channel (non-blocking — drops if full).

4. **Controller** receives the `GenericEvent` via `WatchesRawSource(source.Channel(...))` in `SetupWithManager`, which enqueues a reconcile for the CR. The reconcile calls `GetDesireStatus` to read the current status from DynamoDB with a consistent read.

## Registration

Controllers register their document IDs with the EventRouter during reconciliation, after upserting desires:

```go
r.EventRouter.Register(docID, EventTarget{
    Channel: r.StatusEvents,
    Key:     req.NamespacedName,
})
```

On deletion, controllers deregister to stop receiving events:

```go
r.EventRouter.Deregister(docID)
```

Each controller type has its own `StatusEvents` channel (buffered, capacity 256). All controllers share one `EventRouter` instance.

## Replica limit

The operator is currently limited to **2 replicas** due to DynamoDB Streams constraints. Each stream shard can only be read by a limited number of consumers, and the stream watcher runs on every replica — scaling beyond 2 replicas risks throttling or missed events on the stream path.

## Shard tracking

DynamoDB splits a stream into shards that rotate over time — a parent shard closes and one or more child shards take over. The watcher tracks shards by ID and handles rotation automatically:

- On startup, it adopts all open shards with `TRIM_HORIZON` (replay from the beginning of each shard).
- When a shard closes, it triggers immediate discovery to adopt child shards.
- Expired iterators are refreshed using the last processed sequence number.
- If the stream ARN changes (table recreated), all shard state is reset.

Controllers reconcile everything from DynamoDB on startup anyway, so the stream only needs to catch events that happen after that initial sync.

## Reliability

The stream is a low-latency optimization, not the consistency guarantee. Events can be missed in edge cases:

- **Channel full**: `EventRouter.Dispatch` is non-blocking. If a controller's channel (capacity 256) is full, the event is dropped.
- **Registration race**: A status update can arrive on the stream before the controller has registered its document ID with EventRouter.
- **Data trimmed**: If the watcher falls >24h behind, DynamoDB discards old records and the watcher skips ahead.

None of these cause permanent state loss. Every successful reconcile returns `RequeueAfter: 5m` as a safety net — the controller re-reads status directly from DynamoDB. Active waiting states (no placement yet, delete pending) use `RequeueAfter: 5s`. So a missed stream event delays the reaction by at most 5 minutes, it doesn't lose state.

## Writing and reading specs

Use `UpsertApplyDesire`, `UpsertDeleteDesire`, or `UpsertReadDesire` to write specs. The DynamoDB client keeps an in-memory hash cache per desire — if the spec hasn't changed, the write is skipped entirely (no DynamoDB call). Use `DeleteDesireSpec` to remove a spec row (always remove ApplyDesires before writing DeleteDesires).

Use `GetApplyDesireStatus` / `GetDeleteDesireStatus` / `GetReadDesireStatus` for consistent reads. Use `CheckApplyDesireStatuses` / `CheckDeleteDesireStatuses` to check whether kube-applier has processed your specs — these compare `ObservedDesireUpdateTime` against the spec's `updateTime` to ignore stale statuses.
