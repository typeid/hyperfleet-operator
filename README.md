# hyperfleet-operator

Kubernetes-native cluster lifecycle controller for ROSA HCP. Watches Custom Resources on fleet-db (EKS) and writes DynamoDB desires that [kube-applier-aws](https://github.com/openshift-online/kube-applier-aws) applies to management clusters.

See [docs/architecture.md](docs/architecture.md) for the full design and per-controller documentation.

## Prerequisites

- Go 1.24+
- Container runtime (podman or docker) — for e2e tests with DynamoDB Local
- `kubectl` and access to a Kubernetes cluster (for deployment)

## Quick Start

```sh
make help          # list all targets
make test          # unit tests (envtest)
make test-e2e      # e2e tests (envtest + DynamoDB Local container)
make manifests     # regenerate CRDs from Go types
make build         # build the operator binary
```

## Infrastructure Access

The operator requires two AWS resources at runtime:

### fleet-db (EKS)

The operator connects to fleet-db's kube-apiserver to watch/update CRDs. Authentication uses [EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html) — the operator's ServiceAccount is annotated with an IAM role ARN that has `eks:DescribeCluster` permission on the fleet-db cluster, and the role is registered as an EKS access entry with Kubernetes RBAC permissions for the `hyperfleet.io` API group.

### DynamoDB

The operator reads and writes DynamoDB tables using the same IAM role. See [docs/quickstart.md](docs/quickstart.md#dynamodb) for the full permissions table and table setup.

## Contributing

Run `make help` for all available targets. Run `make test` before pushing changes. See [docs/architecture.md](docs/architecture.md) for design context and the individual controller docs for flow details.

## License

Copyright 2026. Licensed under the Apache License, Version 2.0.
