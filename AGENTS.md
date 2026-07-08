# hyperfleet-operator - AI Agent Guide

## Project Structure

```
cmd/main.go                    Manager entry (registers controllers)
api/<version>/*_types.go       CRD schemas (+kubebuilder markers)
api/<version>/zz_generated.*   Auto-generated (DO NOT EDIT)
internal/controller/*          Reconciliation logic
config/crd/bases/*             Generated CRDs (DO NOT EDIT)
charts/hyperfleet-operator/    Helm chart for deployment
test/                          Integration tests (Postgres + DynamoDB Local)
Makefile                       Build/test commands
```

## Critical Rules

### Never Edit These (Auto-Generated)
- `config/crd/bases/*.yaml` - from `make manifests`
- `**/zz_generated.*.go` - from `make generate`

### Storage Backend
The operator uses **pgruntime** (`postgres-controller-backend`) as its storage backend — all controller-runtime interfaces (`client.Client`, `cache.Cache`, `manager.Manager`) are backed by PostgreSQL, not a Kubernetes API server. There is no envtest, no etcd, no kube-apiserver.

## After Making Changes

**After editing `*_types.go` or markers:**
```
make manifests  # Regenerate CRDs from markers
make generate   # Regenerate DeepCopy methods
```

**After editing `*.go` files:**
```
make lint-fix   # Auto-fix code style
make test       # Run unit tests
```

## Testing & Development

```bash
make test              # Run unit tests
make test-integration  # Integration tests (Postgres + DynamoDB Local containers)
make build             # Build the operator binary
```

Tests use **Ginkgo + Gomega** (BDD style). Check `test/suite_test.go` for integration test setup and `internal/controller/suite_test.go` for unit test setup.

## Deployment

```bash
# 1. Regenerate manifests
make manifests generate

# 2. Build & push
export IMG=<registry>/<project>:tag
make docker-build docker-push IMG=$IMG

# 3. Deploy via Helm
helm install hyperfleet-operator charts/hyperfleet-operator \
  --namespace hyperfleet-operator --create-namespace \
  --set awsRegion=us-east-1 \
  --set baseDomain=example.com
```

### API Design

**Key markers for** `api/<version>/*_types.go`:

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"

// On fields:
// +kubebuilder:validation:Required
// +kubebuilder:validation:Minimum=1
// +kubebuilder:validation:MaxLength=100
// +kubebuilder:validation:Pattern="^[a-z]+$"
// +kubebuilder:default="value"
```

- **Use** `metav1.Condition` for status (not custom string fields)
- **Use predefined types**: `metav1.Time` instead of `string` for dates
- **Follow K8s API conventions**: Standard field names (`spec`, `status`, `metadata`)

### Controller Design

**Implementation rules:**
- **Idempotent reconciliation**: Safe to run multiple times
- **Re-fetch before updates**: `r.Get(ctx, req.NamespacedName, obj)` before `r.Update` to avoid conflicts
- **Structured logging**: `log := log.FromContext(ctx); log.Info("msg", "key", val)`
- **Finalizers**: Clean up external resources (DynamoDB desires)

### Logging

**Follow Kubernetes logging message style guidelines:**

- Start from a capital letter
- Do not end the message with a period
- Active voice: subject present (`"Deployment could not create Pod"`) or omitted (`"Could not create Pod"`)
- Past tense: `"Could not delete Pod"` not `"Cannot delete Pod"`
- Specify object type: `"Deleted Pod"` not `"Deleted"`
- Balanced key-value pairs

```go
log.Info("Starting reconciliation")
log.Info("Created Deployment", "name", deploy.Name)
log.Error(err, "Failed to create Pod", "name", name)
```

## References

- **controller-runtime**: https://github.com/kubernetes-sigs/controller-runtime
- **controller-tools**: https://github.com/kubernetes-sigs/controller-tools
- **postgres-controller-backend**: https://github.com/jmelis/postgres-controller-backend
