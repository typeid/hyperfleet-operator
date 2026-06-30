# DynamoDB Read/Write Strategy

How controllers interact with DynamoDB. Follow these patterns when writing a new controller.

```mermaid
sequenceDiagram
    box hyperfleet-operator
        participant C as Controller
        participant W as Stream Watcher
    end
    participant Specs as specs tables
    participant KA as kube-applier-aws
    participant Status as status tables

    C->>Specs: UpsertDesire (hash check, skip if unchanged)
    KA->>Specs: poll for specs
    KA->>KA: apply/delete/read on MC
    KA->>Status: write result
    Status-->>W: DynamoDB Stream event (~2s)
    W-->>C: GenericEvent via EventRouter
    C->>Status: GetDesireStatus (consistent read)
    C->>C: update CR status on fleet-db
```

## Writing specs

Use `UpsertApplyDesire`, `UpsertDeleteDesire`, or `UpsertReadDesire`. All three hash the spec and skip the write if unchanged, preserving the existing `updateTime`. Always use `UpsertResult.UpdateTime` when building `DesireStatusEntry`, since staleness gating compares it against the status timestamp.

## Reading status

Use `GetApplyDesireStatus`, `GetDeleteDesireStatus`, or `GetReadDesireStatus`. These do strongly consistent reads.

Use `CheckApplyDesireStatuses` or `CheckDeleteDesireStatuses` to check whether kube-applier has processed your specs. These compare `ObservedDesireUpdateTime` against the spec's `updateTime` to ignore stale statuses.

## Removing specs

Use `DeleteDesireSpec` to remove a spec row. Always remove ApplyDesire specs before writing DeleteDesires, otherwise kube-applier may re-apply a resource you're trying to delete.

## Receiving status updates

Register your document IDs with the `EventRouter` during reconciliation. The stream watcher picks up status changes and sends a `GenericEvent` to your controller's `StatusEvents` channel, triggering a reconcile. See existing controllers' `SetupWithManager` for how to wire this up.

Set a `RequeueAfter` as a fallback in case stream events are missed.
