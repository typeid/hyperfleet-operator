# hyperfleet-operator

Cluster lifecycle controller for ROSA HCP. Watches Custom Resources backed by PostgreSQL (via [pgruntime](https://github.com/jmelis/postgres-controller-backend)) and writes DynamoDB desires that [kube-applier-aws](https://github.com/openshift-online/kube-applier-aws) applies to management clusters.

See [docs/architecture.md](docs/architecture.md) for the full design and per-controller documentation.

## Prerequisites

- Go 1.24+
- Container runtime (podman or docker) — for integration tests (Postgres + DynamoDB Local)

## Quick Start

```sh
make help              # list all targets
make test              # unit tests
make test-integration  # integration tests (Postgres + DynamoDB Local containers)
make manifests         # regenerate CRDs from Go types
make build             # build the operator binary
```

## Infrastructure Access

The operator requires two AWS resources at runtime:

### PostgreSQL

The operator stores all Custom Resources in PostgreSQL via pgruntime. The connection string is provided via the `POSTGRES_DSN` environment variable.

### DynamoDB

The operator reads and writes DynamoDB tables using IAM credentials. See [docs/quickstart.md](docs/quickstart.md#dynamodb) for the full permissions table and table setup.

## Contributing

Run `make help` for all available targets. Run `make test` before pushing changes. See [docs/architecture.md](docs/architecture.md) for design context and the individual controller docs for flow details.

## License

Copyright 2026. Licensed under the Apache License, Version 2.0.
