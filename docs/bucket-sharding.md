# Bucket Sharding

The hyperfleet operator uses bucket sharding to horizontally scale controllers
across multiple replicas. Each resource in the database is assigned a `bucket_id`
on creation, and each operator replica owns a slice of buckets — it only
reconciles resources in its assigned buckets.

## How It Works

### Bucket Assignment

Resources are assigned to buckets using an FNV-1a hash of the namespace:

```
bucket_id = fnv32a(namespace) % bucket_count
```

Hashing on namespace gives **cluster-level affinity**: all resources for the same
cluster (Cluster, NodePools, Manifests) land in the same bucket, because they
share a namespace (the cluster ID). This means one operator replica handles a
cluster and all its child resources — no cross-pod coordination needed.

The assigner lives in the `pgruntime` package:

```go
pgruntime.NewBucketAssigner(bucketCount)
```

### Operator Replica Bucket Slicing

The operator runs as a **StatefulSet**. Each pod derives its bucket slice from
three values:

- **Ordinal**: parsed from the pod hostname (e.g., `hyperfleet-operator-2` → 2)
- **BUCKET_COUNT**: total number of buckets (env var)
- **REPLICA_COUNT**: total number of replicas (env var, set by Helm to match `replicaCount`)

Each pod computes:

```
buckets_per_replica = BUCKET_COUNT / REPLICA_COUNT
my_start = ordinal * buckets_per_replica
my_buckets = [my_start .. my_start + buckets_per_replica - 1]
```

Example with 32 buckets:

| Replicas | Pod-0 | Pod-1 | Pod-2 | Pod-3    |
| -------- | ----- | ----- | ----- | -------- |
| 2        | 0–15  | 16–31 | —     | —        |
| 4        | 0–7   | 8–15  | 16–23 | 24–31    |
| 8        | 0–3   | 4–7   | 8–11  | 12–15... |

### ManagementCluster Visibility

ManagementCluster is **cluster-scoped** and declared in `UnshardedGVKs`. Unsharded
GVKs are assigned to sentinel bucket `-1`, which every pod watches regardless of
its bucket slice. This means the PlacementReconciler on every pod automatically
sees all ManagementClusters through the standard manager client — no separate
client is needed.

### Platform API

The API has unrestricted access to all buckets — any request can land on any API
pod. It uses `AllBuckets(bucketCount)` for reads and
`NewBucketAssigner(bucketCount)` to stamp `bucket_id` on resource creation. The
API does not partition work by bucket.

## Configuration

### Environment Variables

| Variable        | Default | Description                                 |
| --------------- | ------- | ------------------------------------------- |
| `BUCKET_COUNT`  | `1`     | Total number of buckets                     |
| `REPLICA_COUNT` | `1`     | Number of operator replicas (operator only) |
| `POSTGRES_DSN`  | —       | PostgreSQL connection string (required)     |

### Helm Values (Operator)

```yaml
replicaCount: 2

hyperfleetdb:
  bucketCount: 32
```

The chart automatically sets `REPLICA_COUNT` to match `replicaCount` and
`BUCKET_COUNT` to match `hyperfleetdb.bucketCount`.

### Helm Values (Platform API)

```yaml
platformApi:
  hyperfleetdb:
    bucketCount: 32
```

## Scaling

To scale from 2 to 4 operator replicas:

1. Update `replicaCount: 4` in the operator Helm values
2. The chart sets `REPLICA_COUNT=4` on every pod
3. Each pod re-derives its bucket slice on startup

No per-pod configuration, no manual bucket assignment.

### Constraints

- **`BUCKET_COUNT` must be divisible by `replicaCount`**. 32 divides cleanly by
  1, 2, 4, 8, 16, 32.
- **API and operator must use the same `BUCKET_COUNT`**. The API stamps
  `bucket_id` on creation; the operator reads by bucket. Mismatched counts cause
  resources to be invisible to the wrong replica.
- **`BUCKET_COUNT` should not change** once resources exist. Changing it would
  reassign existing resources to different buckets on next write, but the
  database `bucket_id` column retains the original assignment. Use the initial
  count for the lifetime of the deployment.
