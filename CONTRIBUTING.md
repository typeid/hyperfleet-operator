# Contributing to hyperfleet-operator

## Prerequisites

- Go 1.24+
- [controller-gen](https://book.kubebuilder.io/reference/controller-gen) (installed via `make controller-gen`)
- [podman](https://podman.io/) or docker for building images
- Container runtime (podman or docker) for Postgres + DynamoDB Local in integration tests

## Development Workflow

```bash
# Generate CRDs and deepcopy
make manifests generate

# Run unit tests
make test

# Build the binary
make build

# Build the container image
make docker-build IMG=quay.io/youruser/hyperfleet-operator:dev

# Run integration tests (Postgres + DynamoDB Local containers)
make test-integration
```

## Adding a New CRD Field

1. Edit the type in `api/v1alpha1/`
2. Run `make manifests generate` to regenerate CRDs and deepcopy
3. Update the relevant controller and manifest generation code
4. Add tests

## Code Style

- Follow standard Go conventions
- No comments unless the _why_ is non-obvious
- Use `go fmt` and `go vet` (run automatically by `make test`)
