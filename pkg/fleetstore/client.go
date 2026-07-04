package fleetstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Client implements client.Client backed by Postgres.
// Reads delegate to the cache when synced; writes go to Postgres.
type Client struct {
	pool   *pgxpool.Pool
	cache  *Cache
	logger *slog.Logger
}

// NewClient creates a new FleetStore client.
func NewClient(pool *pgxpool.Pool, cache *Cache, logger *slog.Logger) *Client {
	return &Client{pool: pool, cache: cache, logger: logger}
}

// NewDirectClient creates a client that reads directly from Postgres (no cache).
func NewDirectClient(pool *pgxpool.Pool, logger *slog.Logger) *Client {
	return &Client{pool: pool, logger: logger}
}

// Get retrieves an object by key. Reads from cache if synced, otherwise direct.
func (c *Client) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.cache != nil && c.cache.synced.Load() {
		return c.cache.Get(ctx, key, obj)
	}
	return c.directGet(ctx, key, obj)
}

// List lists objects of the given type. Reads from cache if synced.
func (c *Client) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.cache != nil && c.cache.synced.Load() {
		return c.cache.List(ctx, list, opts...)
	}
	return c.directList(ctx, list, opts...)
}

// Create implements the POST verb per §6.
// Uses savepoint to prevent phantom global_seq bumps on AlreadyExists.
func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	start := time.Now()
	defer func() { WriteLatency.WithLabelValues("create").Observe(time.Since(start).Seconds()) }()

	row, err := Encode(obj)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	row.UID = types.UID(uuid.New().String())

	if !IsGlobal(row.Kind) && row.AWSAccountID == nil {
		return fmt.Errorf("aws_account_id is required for namespaced kind %s", row.Kind)
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "SAVEPOINT create_attempt")
	if err != nil {
		return fmt.Errorf("savepoint: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO resources (kind, namespace, name, uid,
			labels, annotations, owner_refs, finalizers, spec,
			aws_account_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		row.Kind, row.Namespace, row.Name, row.UID,
		row.Labels, row.Annotations, row.OwnerRefs, row.Finalizers, row.Spec,
		row.AWSAccountID,
	)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// unique_violation — try tombstone revival
			_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT create_attempt")
			if err != nil {
				return fmt.Errorf("rollback savepoint: %w", err)
			}

			tag, err := tx.Exec(ctx, `
				UPDATE resources SET
					uid = $4, labels = $5, annotations = $6,
					owner_refs = $7, finalizers = $8, spec = $9,
					status = '{"conditions": []}',
					generation = 1, created_at = now(),
					deleted_at = NULL, deletion_timestamp = NULL,
					aws_account_id = $10
				WHERE kind=$1 AND namespace=$2 AND name=$3
					AND deleted_at IS NOT NULL`,
				row.Kind, row.Namespace, row.Name,
				row.UID, row.Labels, row.Annotations,
				row.OwnerRefs, row.Finalizers, row.Spec,
				row.AWSAccountID,
			)
			if err != nil {
				return fmt.Errorf("tombstone revival: %w", err)
			}
			if tag.RowsAffected() == 0 {
				tx.Rollback(ctx)
				return alreadyExists(row.Kind, row.Name)
			}
		} else {
			return fmt.Errorf("insert: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// Read back the created object to populate seq/timestamps.
	return c.directGet(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()}, obj)
}

// Update implements the UPDATE verb per §6 — CAS on seq (resourceVersion).
func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	start := time.Now()
	defer func() { WriteLatency.WithLabelValues("update").Observe(time.Since(start).Seconds()) }()

	row, err := Encode(obj)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	expectedSeq, err := SeqFromResourceVersion(obj.GetResourceVersion())
	if err != nil {
		return fmt.Errorf("parse resourceVersion: %w", err)
	}

	// Finalization completion: deleting object with empty finalizers → tombstone.
	if obj.GetDeletionTimestamp() != nil && len(obj.GetFinalizers()) == 0 {
		return c.tombstoneWithCAS(ctx, row.Kind, row.Namespace, row.Name, expectedSeq)
	}

	tag, err := c.pool.Exec(ctx, `
		UPDATE resources SET
			spec = $1, labels = $2, annotations = $3,
			finalizers = $4, owner_refs = $5,
			generation = generation + CASE WHEN spec IS DISTINCT FROM $1 THEN 1 ELSE 0 END
		WHERE kind=$6 AND namespace=$7 AND name=$8
			AND seq = $9 AND deleted_at IS NULL`,
		row.Spec, row.Labels, row.Annotations,
		row.Finalizers, row.OwnerRefs,
		row.Kind, row.Namespace, row.Name,
		expectedSeq,
	)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	if tag.RowsAffected() == 0 {
		// Distinguish trigger-suppressed no-op from real CAS conflict:
		// if seq still matches, the trigger suppressed an identical write.
		var currentSeq int64
		err := c.pool.QueryRow(ctx, `
			SELECT seq FROM resources
			WHERE kind=$1 AND namespace=$2 AND name=$3 AND deleted_at IS NULL`,
			row.Kind, row.Namespace, row.Name,
		).Scan(&currentSeq)
		if err == nil && currentSeq == expectedSeq {
			return nil
		}
		WriteConflicts.WithLabelValues("update").Inc()
		return c.disambiguateUpdateFailure(ctx, row.Kind, row.Namespace, row.Name)
	}

	return c.directGet(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()}, obj)
}

// Delete implements two-phase delete per §6.
func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	start := time.Now()
	defer func() { WriteLatency.WithLabelValues("delete").Observe(time.Since(start).Seconds()) }()

	kind, err := KindFor(obj)
	if err != nil {
		return err
	}

	ns := obj.GetNamespace()
	if IsGlobal(kind) {
		ns = GlobalNamespace
	}
	name := obj.GetName()

	// Read current state to check finalizers.
	current, err := c.directRead(ctx, kind, ns, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return err
		}
		return fmt.Errorf("read for delete: %w", err)
	}

	if len(current.Finalizers) > 0 {
		if current.DeletionTimestamp == nil {
			// Phase 1: set deletion_timestamp.
			tag, err := c.pool.Exec(ctx, `
				UPDATE resources SET deletion_timestamp = now()
				WHERE kind=$1 AND namespace=$2 AND name=$3
					AND deletion_timestamp IS NULL AND deleted_at IS NULL`,
				kind, ns, name,
			)
			if err != nil {
				return fmt.Errorf("delete phase 1: %w", err)
			}
			if tag.RowsAffected() == 0 {
				// Already has deletion_timestamp or already tombstoned — idempotent success.
				return nil
			}
		}
		// Finalizers still present — wait for controllers to clear them.
		return nil
	}

	// Phase 2: tombstone (no finalizers).
	return c.tombstone(ctx, kind, ns, name)
}

func (c *Client) tombstone(ctx context.Context, kind, ns, name string) error {
	tag, err := c.pool.Exec(ctx, `
		UPDATE resources SET
			deleted_at = now(),
			deletion_timestamp = COALESCE(deletion_timestamp, now())
		WHERE kind=$1 AND namespace=$2 AND name=$3 AND deleted_at IS NULL
			AND finalizers = '{}'`,
		kind, ns, name,
	)
	if err != nil {
		return fmt.Errorf("tombstone: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return c.disambiguateTombstoneFailure(ctx, kind, ns, name)
	}
	return nil
}

func (c *Client) disambiguateTombstoneFailure(ctx context.Context, kind, ns, name string) error {
	row, err := c.directRead(ctx, kind, ns, name)
	if err != nil {
		return err
	}
	if len(row.Finalizers) > 0 {
		return conflict(kind, name)
	}
	return notFound(kind, name)
}

func (c *Client) tombstoneWithCAS(ctx context.Context, kind, ns, name string, expectedSeq int64) error {
	tag, err := c.pool.Exec(ctx, `
		UPDATE resources SET
			deleted_at = now(),
			deletion_timestamp = COALESCE(deletion_timestamp, now())
		WHERE kind=$1 AND namespace=$2 AND name=$3
			AND seq = $4 AND deleted_at IS NULL`,
		kind, ns, name, expectedSeq,
	)
	if err != nil {
		return fmt.Errorf("tombstone: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return c.disambiguateUpdateFailure(ctx, kind, ns, name)
	}
	return nil
}

// DeleteAllOf is not supported.
func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	return ErrUnsupported
}

// Apply is not supported.
func (c *Client) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	return ErrUnsupported
}

// Patch is not supported (use Update).
func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	return ErrUnsupported
}

// Status returns the status writer.
func (c *Client) Status() client.SubResourceWriter {
	return &StatusWriter{pool: c.pool, client: c, logger: c.logger}
}

// SubResource returns a stub for unsupported sub-resources.
func (c *Client) SubResource(subResource string) client.SubResourceClient {
	if subResource == "status" {
		return &subResourceClient{writer: c.Status()}
	}
	return &unsupportedSubResource{}
}

// Scheme returns nil — FleetStore uses a static type registry, not a runtime.Scheme.
func (c *Client) Scheme() *runtime.Scheme {
	return nil
}

// RESTMapper returns nil — FleetStore uses a static type registry.
func (c *Client) RESTMapper() meta.RESTMapper {
	return nil
}

// GroupVersionKindFor returns the GVK for the given object.
func (c *Client) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, ErrUnsupported
}

// IsObjectNamespaced returns whether the object is namespaced.
func (c *Client) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	co, ok := obj.(client.Object)
	if !ok {
		return false, fmt.Errorf("object %T does not implement client.Object", obj)
	}
	kind, err := KindFor(co)
	if err != nil {
		return false, err
	}
	return !IsGlobal(kind), nil
}

// --- Direct reads (uncached) ---

func (c *Client) directGet(ctx context.Context, key client.ObjectKey, into client.Object) error {
	kind, err := KindFor(into)
	if err != nil {
		return err
	}

	ns := key.Namespace
	if IsGlobal(kind) {
		ns = GlobalNamespace
	}

	row, err := c.directRead(ctx, kind, ns, key.Name)
	if err != nil {
		return err
	}

	decoded, err := Decode(row)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	return copyInto(decoded, into)
}

func (c *Client) directRead(ctx context.Context, kind, ns, name string) (*ResourceRow, error) {
	row := &ResourceRow{}
	err := c.pool.QueryRow(ctx, `
		SELECT kind, namespace, name, uid, generation,
			labels, annotations, owner_refs, finalizers,
			spec, status, created_at, deletion_timestamp,
			seq, aws_account_id, updated_at, deleted_at
		FROM resources
		WHERE kind=$1 AND namespace=$2 AND name=$3`,
		kind, ns, name,
	).Scan(
		&row.Kind, &row.Namespace, &row.Name, &row.UID, &row.Generation,
		&row.Labels, &row.Annotations, &row.OwnerRefs, &row.Finalizers,
		&row.Spec, &row.Status, &row.CreatedAt, &row.DeletionTimestamp,
		&row.Seq, &row.AWSAccountID, &row.UpdatedAt, &row.DeletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, notFound(kind, name)
		}
		return nil, fmt.Errorf("query: %w", err)
	}

	if row.DeletedAt != nil {
		return nil, notFound(kind, name)
	}

	return row, nil
}

func (c *Client) directList(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	kind, err := KindForList(list)
	if err != nil {
		return err
	}

	listOpts := client.ListOptions{}
	for _, o := range opts {
		o.ApplyToList(&listOpts)
	}

	query := `SELECT kind, namespace, name, uid, generation,
		labels, annotations, owner_refs, finalizers,
		spec, status, created_at, deletion_timestamp,
		seq, aws_account_id, updated_at, deleted_at
		FROM resources WHERE kind=$1 AND deleted_at IS NULL`
	args := []any{kind}

	if listOpts.Namespace != "" {
		ns := listOpts.Namespace
		if IsGlobal(kind) {
			ns = GlobalNamespace
		}
		query += fmt.Sprintf(" AND namespace=$%d", len(args)+1)
		args = append(args, ns)
	}

	query += " ORDER BY seq"

	rows, err := c.pool.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("list query: %w", err)
	}
	defer rows.Close()

	return scanIntoList(rows, kind, list, listOpts)
}

func (c *Client) disambiguateUpdateFailure(ctx context.Context, kind, ns, name string) error {
	row := &ResourceRow{}
	err := c.pool.QueryRow(ctx, `
		SELECT kind, deleted_at FROM resources
		WHERE kind=$1 AND namespace=$2 AND name=$3`,
		kind, ns, name,
	).Scan(&row.Kind, &row.DeletedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound(kind, name)
		}
		return fmt.Errorf("disambiguate: %w", err)
	}
	if row.DeletedAt != nil {
		return notFound(kind, name)
	}
	return conflict(kind, name)
}

// scanIntoList scans query rows into a typed list, applying label selectors client-side.
func scanIntoList(rows pgx.Rows, kind string, list client.ObjectList, opts client.ListOptions) error {
	var items []client.Object
	for rows.Next() {
		row := &ResourceRow{}
		if err := rows.Scan(
			&row.Kind, &row.Namespace, &row.Name, &row.UID, &row.Generation,
			&row.Labels, &row.Annotations, &row.OwnerRefs, &row.Finalizers,
			&row.Spec, &row.Status, &row.CreatedAt, &row.DeletionTimestamp,
			&row.Seq, &row.AWSAccountID, &row.UpdatedAt, &row.DeletedAt,
		); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		obj, err := Decode(row)
		if err != nil {
			return fmt.Errorf("decode: %w", err)
		}

		if opts.LabelSelector != nil && !opts.LabelSelector.Matches(labelsFromObject(obj)) {
			continue
		}

		items = append(items, obj)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}

	return setListItems(kind, list, items)
}

func labelsFromObject(obj client.Object) labels.Set {
	l := obj.GetLabels()
	if l == nil {
		return labels.Set{}
	}
	return labels.Set(l)
}

func setListItems(_ string, list client.ObjectList, items []client.Object) error {
	lv := reflect.ValueOf(list).Elem()
	itemsField := lv.FieldByName("Items")

	newSlice := reflect.MakeSlice(itemsField.Type(), 0, len(items))
	for _, item := range items {
		newSlice = reflect.Append(newSlice, reflect.ValueOf(item).Elem())
	}
	itemsField.Set(newSlice)
	return nil
}

func copyInto(src, dst client.Object) error {
	srcJSON, err := json.Marshal(src)
	if err != nil {
		return fmt.Errorf("marshal src: %w", err)
	}
	return json.Unmarshal(srcJSON, dst)
}

type subResourceClient struct {
	writer client.SubResourceWriter
}

func (s *subResourceClient) Get(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceGetOption) error {
	return ErrUnsupported
}

func (s *subResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return ErrUnsupported
}

func (s *subResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return s.writer.Update(ctx, obj, opts...)
}

func (s *subResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return s.writer.Patch(ctx, obj, patch, opts...)
}

func (s *subResourceClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return s.writer.Apply(ctx, obj, opts...)
}

type unsupportedSubResource struct{}

func (u *unsupportedSubResource) Get(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceGetOption) error {
	return ErrUnsupported
}
func (u *unsupportedSubResource) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return ErrUnsupported
}
func (u *unsupportedSubResource) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return ErrUnsupported
}
func (u *unsupportedSubResource) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return ErrUnsupported
}
func (u *unsupportedSubResource) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return ErrUnsupported
}
