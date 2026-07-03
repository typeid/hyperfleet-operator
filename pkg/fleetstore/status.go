package fleetstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StatusWriter implements client.SubResourceWriter for the status sub-resource.
// Single-writer semantics (A3): whole-document replacement, no CAS.
type StatusWriter struct {
	pool   *pgxpool.Pool
	client *Client
	logger *slog.Logger
}

// Update replaces the status of an object. No CAS — single-writer per A3.
// Client-side deep-equal skip before IO; trigger suppresses no-ops at DB level.
func (sw *StatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	kind, err := KindFor(obj)
	if err != nil {
		return err
	}

	ns := obj.GetNamespace()
	if IsGlobal(kind) {
		ns = GlobalNamespace
	}

	newStatus, err := extractStatus(kind, obj)
	if err != nil {
		return err
	}

	// Client-side deep-equal skip: read current status and compare.
	current, err := sw.client.directRead(ctx, kind, ns, obj.GetName())
	if err != nil {
		return err
	}

	if jsonEqual(current.Status, newStatus) {
		StatusNoop.Inc()
		return nil
	}

	tag, err := sw.pool.Exec(ctx, `
		UPDATE resources SET status = $1
		WHERE kind=$2 AND namespace=$3 AND name=$4 AND deleted_at IS NULL`,
		newStatus, kind, ns, obj.GetName(),
	)
	if err != nil {
		return fmt.Errorf("status update: %w", err)
	}

	if tag.RowsAffected() == 0 {
		// Trigger-suppressed no-op, tombstoned, or absent.
		return sw.disambiguateStatusZero(ctx, kind, ns, obj.GetName())
	}

	return sw.client.directGet(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()}, obj)
}

// Patch implements Status().Patch via read-modify-write.
func (sw *StatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	kind, err := KindFor(obj)
	if err != nil {
		return err
	}

	ns := obj.GetNamespace()
	if IsGlobal(kind) {
		ns = GlobalNamespace
	}

	current, err := sw.client.directRead(ctx, kind, ns, obj.GetName())
	if err != nil {
		return err
	}

	decoded, err := Decode(current)
	if err != nil {
		return fmt.Errorf("decode for patch: %w", err)
	}

	patchData, err := patch.Data(decoded)
	if err != nil {
		return fmt.Errorf("compute patch: %w", err)
	}

	if err := json.Unmarshal(patchData, decoded); err != nil {
		return fmt.Errorf("apply patch: %w", err)
	}

	return sw.Update(ctx, decoded)
}

// Create is not supported for status sub-resource.
func (sw *StatusWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return ErrUnsupported
}

// Apply is not supported for status sub-resource.
func (sw *StatusWriter) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return ErrUnsupported
}

func (sw *StatusWriter) disambiguateStatusZero(ctx context.Context, kind, ns, name string) error {
	var deletedAt *string
	err := sw.pool.QueryRow(ctx, `
		SELECT deleted_at::text FROM resources
		WHERE kind=$1 AND namespace=$2 AND name=$3`,
		kind, ns, name,
	).Scan(&deletedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound(kind, name)
		}
		return fmt.Errorf("disambiguate status: %w", err)
	}
	if deletedAt != nil {
		return notFound(kind, name)
	}
	// Row is live, trigger suppressed — it was a no-op. Success.
	StatusNoop.Inc()
	return nil
}

func extractStatus(kind string, obj client.Object) (json.RawMessage, error) {
	_, status, err := marshalSpecStatus(kind, obj)
	if err != nil {
		return nil, fmt.Errorf("extract status: %w", err)
	}
	return status, nil
}

func jsonEqual(a, b json.RawMessage) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	var va, vb interface{}
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	return reflect.DeepEqual(va, vb)
}
