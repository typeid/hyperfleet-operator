package fleetstore

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EventType mirrors watch.EventType.
type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"
	EventBookmark EventType = "BOOKMARK"
)

// Event represents a change observed by the watch protocol.
type Event struct {
	Type   EventType
	Object *ResourceRow
	Cursor int64
}

// WatchConfig holds configuration for the poll loop.
type WatchConfig struct {
	PollIdle time.Duration
	PollMin  time.Duration
}

// DefaultWatchConfig returns the default watch configuration.
func DefaultWatchConfig() WatchConfig {
	return WatchConfig{
		PollIdle: 5 * time.Second,
		PollMin:  50 * time.Millisecond,
	}
}

// Watcher runs the seq-poll + LISTEN doorbell loop per §7.
type Watcher struct {
	pool    *pgxpool.Pool
	cfg     WatchConfig
	logger  *slog.Logger

	mu      sync.RWMutex
	cursor  int64
	stores  map[string]*InformerStore

	doorbell chan struct{}
}

// NewWatcher creates a new watcher.
func NewWatcher(pool *pgxpool.Pool, cfg WatchConfig, logger *slog.Logger) *Watcher {
	return &Watcher{
		pool:     pool,
		cfg:      cfg,
		logger:   logger,
		stores:   make(map[string]*InformerStore),
		doorbell: make(chan struct{}, 1),
	}
}

// RegisterStore registers an informer store for a kind.
func (w *Watcher) RegisterStore(kind string, store *InformerStore) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stores[kind] = store
}

// Run executes the LIST-then-WATCH startup and poll loop.
func (w *Watcher) Run(ctx context.Context) error {
	if err := w.initialList(ctx); err != nil {
		return fmt.Errorf("initial list: %w", err)
	}

	go w.listenLoop(ctx)
	w.pollLoop(ctx)
	return nil
}

// initialList implements §7.2 — LIST-then-WATCH startup.
func (w *Watcher) initialList(ctx context.Context) error {
	rows, err := w.pool.Query(ctx,
		`SELECT kind, namespace, name, uid, generation,
			labels, annotations, owner_refs, finalizers,
			spec, status, created_at, deletion_timestamp,
			seq, aws_account_id, updated_at, deleted_at
		FROM resources ORDER BY seq`)
	if err != nil {
		return fmt.Errorf("list query: %w", err)
	}
	defer rows.Close()

	var maxSeq int64
	for rows.Next() {
		row, err := scanResourceRow(rows)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		if row.Seq > maxSeq {
			maxSeq = row.Seq
		}
		if row.DeletedAt != nil {
			continue
		}

		w.mu.RLock()
		store, ok := w.stores[row.Kind]
		w.mu.RUnlock()
		if ok {
			store.Apply(row, false)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}

	w.mu.Lock()
	w.cursor = maxSeq
	w.mu.Unlock()

	// Mark all stores synced.
	w.mu.RLock()
	for _, store := range w.stores {
		store.MarkSynced()
	}
	w.mu.RUnlock()

	w.logger.Info("initial list complete", "cursor", maxSeq)
	return nil
}

// listenLoop runs the LISTEN doorbell on a dedicated connection.
func (w *Watcher) listenLoop(ctx context.Context) {
	for {
		if err := w.listenOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("LISTEN connection error, reconnecting", "error", err)
			time.Sleep(time.Second)
		}
	}
}

func (w *Watcher) listenOnce(ctx context.Context) error {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire listen conn: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, "LISTEN fleetstore")
	if err != nil {
		return fmt.Errorf("LISTEN: %w", err)
	}

	for {
		_, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		// Ring the doorbell (non-blocking).
		select {
		case w.doorbell <- struct{}{}:
		default:
		}
	}
}

// pollLoop implements §7.3 — poll on doorbell OR pollIdle timeout.
func (w *Watcher) pollLoop(ctx context.Context) {
	timer := time.NewTimer(w.cfg.PollIdle)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.doorbell:
			// Debounce: wait pollMin to batch write bursts.
			time.Sleep(w.cfg.PollMin)
		case <-timer.C:
		}

		if err := w.poll(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("poll error, retrying", "error", err)
		}

		timer.Reset(w.cfg.PollIdle)
	}
}

func (w *Watcher) poll(ctx context.Context) error {
	w.mu.RLock()
	cursor := w.cursor
	w.mu.RUnlock()

	rows, err := w.pool.Query(ctx,
		`SELECT kind, namespace, name, uid, generation,
			labels, annotations, owner_refs, finalizers,
			spec, status, created_at, deletion_timestamp,
			seq, aws_account_id, updated_at, deleted_at
		FROM resources WHERE seq > $1 ORDER BY seq`, cursor)
	if err != nil {
		return fmt.Errorf("poll query: %w", err)
	}
	defer rows.Close()

	var maxSeq int64
	var count int
	for rows.Next() {
		row, err := scanResourceRow(rows)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		if row.Seq > maxSeq {
			maxSeq = row.Seq
		}
		count++

		w.mu.RLock()
		store, ok := w.stores[row.Kind]
		w.mu.RUnlock()
		if ok {
			store.Apply(row, false)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}

	if count > 0 {
		w.mu.Lock()
		w.cursor = maxSeq
		w.mu.Unlock()
	}

	return nil
}

// Cursor returns the current cursor position.
func (w *Watcher) Cursor() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cursor
}

func scanResourceRow(rows pgx.Rows) (*ResourceRow, error) {
	row := &ResourceRow{}
	if err := rows.Scan(
		&row.Kind, &row.Namespace, &row.Name, &row.UID, &row.Generation,
		&row.Labels, &row.Annotations, &row.OwnerRefs, &row.Finalizers,
		&row.Spec, &row.Status, &row.CreatedAt, &row.DeletionTimestamp,
		&row.Seq, &row.AWSAccountID, &row.UpdatedAt, &row.DeletedAt,
	); err != nil {
		return nil, err
	}
	return row, nil
}
