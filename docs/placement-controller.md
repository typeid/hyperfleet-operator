# Placement Controller

## Purpose

Watches Cluster CRs. When a new Cluster appears without a Placement, creates one by selecting a management cluster. Sets owner reference so the Placement is garbage-collected with its Cluster. Skips Clusters that are being deleted.

## MC Selection

The controller reads ManagementCluster CRs from PostgreSQL via the pgruntime cache (see [Architecture — MC Registry](architecture.md#management-cluster-registry)). It picks the first available MC from the list. A placement strategy is planned but not yet implemented — see the `TODO` in `selectManagementCluster`.

## Reconcile Flow

```mermaid
flowchart TD
    A[Watch: Cluster CR or Placement CR] --> B{Cluster being deleted?}
    B -->|Yes| C[No-op]
    B -->|No| D{Placement exists?}
    D -->|Yes| E[Ensure status.phase = Bound]
    D -->|No| F[Select MC from ManagementCluster CRs]
    F --> G[Create Placement CR in same namespace]
    G --> H[Set ownerReference → Cluster]
    H --> E
    E --> I[Set Cluster.status.PlacementRef]
```

## Watches

The controller watches two resource types:

- **Cluster CRs** — primary watch, reconciles when a Cluster is created or updated
- **Placement CRs** — secondary watch via `handler.EnqueueRequestsFromMapFunc`, which maps a Placement back to its parent Cluster via `spec.clusterRef` and triggers reconciliation

## Notes

- Cluster and Placement are namespace-scoped under the customer's AWS account ID. The Placement is created in the same namespace as the Cluster.
- Owner references ensure Placements are garbage-collected when the Cluster CR is deleted. The Cluster controller also explicitly deletes the Placement during its deletion flow as a safety measure.
- ManagementCluster is cluster-scoped and declared as an `UnshardedGVK`, so every operator pod sees all ManagementClusters through the standard manager client (see [Bucket Sharding](bucket-sharding.md)).
