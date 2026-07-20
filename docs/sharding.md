# Namespace-Hash Sharding

The hyperfleet operator uses namespace-hash sharding to horizontally scale
controllers across multiple replicas. Each operator replica owns a subset of
namespaces determined by `abs(hashtext(namespace)::bigint) % replicaCount`, so it only
reconciles resources whose namespace hashes to its shard.

## How It Works

### Shard Assignment

The pgruntime cache partitions its List/Watch streams using PostgreSQL's
`hashtext()` function:

```
shard = abs(hashtext(namespace)::bigint) % replicaCount
```

Hashing on namespace gives **cluster-level affinity**: all resources for the same
cluster (Cluster, NodePools, Manifests) land in the same shard, because they
share a namespace (the cluster ID). This means one operator replica handles a
cluster and all its child resources -- no cross-pod coordination needed.

The sharding is configured via `pgruntime.ShardConfig`:

```go
pgruntime.ShardConfig{
    Mod:   replicaCount,
    Owned: []int{ordinal},
    UnshardedGVKs: []schema.GroupVersionKind{
        v1alpha1.SchemeGroupVersion.WithKind("ManagementCluster"),
    },
}
```

- **Mod**: the modulus (total number of shards, equal to `replicaCount`)
- **Owned**: the shard indices this replica owns (typically just `[ordinal]`)
- **UnshardedGVKs**: GVKs exempt from sharding (every replica sees them)

### Operator Replica Shard Assignment

The operator runs as a **StatefulSet**. Each pod derives its shard from two
values:

- **Ordinal**: parsed from the pod hostname (e.g., `hyperfleet-operator-2` -> 2)
- **REPLICA_COUNT**: total number of replicas (env var, set by Helm to match `replicaCount`)

Each pod owns the shard equal to its ordinal:

```
my_shard = ordinal
```

Example with 4 replicas:

| Pod   | Shard | Reconciles namespaces where       |
| ----- | ----- | --------------------------------- |
| Pod-0 | 0     | `abs(hashtext(namespace)::bigint) % 4 == 0`    |
| Pod-1 | 1     | `abs(hashtext(namespace)::bigint) % 4 == 1`    |
| Pod-2 | 2     | `abs(hashtext(namespace)::bigint) % 4 == 2`    |
| Pod-3 | 3     | `abs(hashtext(namespace)::bigint) % 4 == 3`    |

### ManagementCluster Visibility

ManagementCluster is **cluster-scoped** and declared in `UnshardedGVKs`. Unsharded
GVKs bypass the shard filter entirely, so every pod sees all ManagementClusters
through the standard manager client. This means the PlacementReconciler on every
pod automatically has access to the full MC registry -- no separate client needed.

### Platform API

The API uses `pgruntime.NewClient()` which is never sharded. It sees all data
regardless of shard configuration. No shard-related configuration is needed for
the API.

## Configuration

### Environment Variables

| Variable        | Default | Description                                 |
| --------------- | ------- | ------------------------------------------- |
| `REPLICA_COUNT` | `1`     | Number of operator replicas (= shard count) |
| `POSTGRES_DSN`  | --      | PostgreSQL connection string (required)     |

### Helm Values (Operator)

```yaml
replicaCount: 4
```

The chart automatically sets `REPLICA_COUNT` to match `replicaCount`.

## Scaling

To scale from 2 to 4 operator replicas:

1. Update `replicaCount: 4` in the operator Helm values
2. The chart sets `REPLICA_COUNT=4` on every pod
3. Each pod re-derives its shard on startup (rolling restart)

No per-pod configuration, no manual shard assignment.

### Constraints

- Changing `replicaCount` requires a rolling restart of the StatefulSet so each
  pod picks up the new modulus. During the rollout, some namespaces may
  temporarily be handled by two pods or none, which is safe because pgruntime
  uses fenced writes.
- The direct client (used by `pgruntime.NewClient()`) is never sharded and
  always sees all data. Only the cache/watch layer is partitioned.
