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

// WatchConfig holds configuration for the watcher.
type WatchConfig struct {
	PollIdle time.Duration // fallback poll interval when no NOTIFY arrives
}

// DefaultWatchConfig returns the default watch configuration.
func DefaultWatchConfig() WatchConfig {
	return WatchConfig{
		PollIdle: 5 * time.Second,
	}
}

// Watcher runs a LISTEN/NOTIFY-driven poll loop to keep InformerStores up to
// date. A Postgres NOTIFY on channel "fleetstore" triggers an immediate poll;
// the PollIdle timer acts as a fallback in case a notification is missed.
type Watcher struct {
	pool   *pgxpool.Pool
	cfg    WatchConfig
	logger *slog.Logger

	mu     sync.RWMutex
	cursor int64
	stores map[string]*InformerStore

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

// Run executes the LIST-then-WATCH startup, starts the LISTEN loop, and
// enters the poll loop.
func (w *Watcher) Run(ctx context.Context) error {
	if err := w.initialList(ctx); err != nil {
		return fmt.Errorf("initial list: %w", err)
	}

	go w.listenLoop(ctx)
	w.pollLoop(ctx)
	return nil
}

// initialList implements LIST-then-WATCH startup.
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

// listenLoop maintains a LISTEN connection and rings the doorbell on each
// notification. It reconnects automatically on error.
func (w *Watcher) listenLoop(ctx context.Context) {
	for ctx.Err() == nil {
		if err := w.listenOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("LISTEN connection lost, reconnecting", "error", err)
			ListenReconnects.Inc()
			time.Sleep(time.Second)
		}
	}
}

func (w *Watcher) listenOnce(ctx context.Context) error {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN fleetstore"); err != nil {
		return fmt.Errorf("LISTEN: %w", err)
	}

	for {
		_, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		// Non-blocking send: collapse multiple notifications into one poll.
		select {
		case w.doorbell <- struct{}{}:
		default:
		}
	}
}

// pollLoop waits for either a doorbell ring or the fallback timer, then polls.
func (w *Watcher) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.PollIdle)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.doorbell:
		case <-ticker.C:
		}

		if err := w.poll(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("poll error, retrying", "error", err)
		}
	}
}

func (w *Watcher) poll(ctx context.Context) error {
	start := time.Now()

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
	var latestUpdatedAt time.Time
	for rows.Next() {
		row, err := scanResourceRow(rows)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		if row.Seq > maxSeq {
			maxSeq = row.Seq
			latestUpdatedAt = row.UpdatedAt
		}
		count++

		WatchEventLag.Observe(time.Since(row.UpdatedAt).Seconds())

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

	PollLatency.Observe(time.Since(start).Seconds())
	PollRows.Add(float64(count))

	if count > 0 {
		CursorLag.Set(time.Since(latestUpdatedAt).Seconds())
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
