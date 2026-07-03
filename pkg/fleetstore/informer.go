package fleetstore

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"

	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type storeEntry struct {
	obj client.Object
	seq int64
}

// InformerStore is a per-kind in-memory store fed by the watch protocol.
// Implements versionGuardedApply (§7.4) and handler fan-out.
type InformerStore struct {
	kind   string
	logger *slog.Logger

	mu    sync.RWMutex
	items map[string]storeEntry // key: "namespace/name"

	synced     atomic.Bool
	maxSeenSeq sync.Map // key → max seq seen for freshness floor

	handlers      []registeredHandler
	cacheHandlers []toolscache.ResourceEventHandler
	handlerMu     sync.RWMutex
}

type registeredHandler struct {
	handler    handler.EventHandler
	queue      workqueue.TypedRateLimitingInterface[reconcile.Request]
	predicates []predicate.Predicate
}

// NewInformerStore creates a new informer store for the given kind.
func NewInformerStore(kind string, logger *slog.Logger) *InformerStore {
	return &InformerStore{
		kind:   kind,
		logger: logger,
		items:  make(map[string]storeEntry),
	}
}

// MarkSynced marks this store as having completed its initial LIST.
func (s *InformerStore) MarkSynced() {
	s.synced.Store(true)
}

// HasSynced returns true if the initial LIST has completed.
func (s *InformerStore) HasSynced() bool {
	return s.synced.Load()
}

// AddEventHandler registers a handler for events from this store.
func (s *InformerStore) AddEventHandler(h handler.EventHandler, queue workqueue.TypedRateLimitingInterface[reconcile.Request], preds ...predicate.Predicate) {
	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()
	s.handlers = append(s.handlers, registeredHandler{
		handler:    h,
		queue:      queue,
		predicates: preds,
	})
}

// AddCacheHandler registers a client-go ResourceEventHandler for events from this store.
func (s *InformerStore) AddCacheHandler(h toolscache.ResourceEventHandler) {
	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()
	s.cacheHandlers = append(s.cacheHandlers, h)
}

// Get retrieves an object from the store.
func (s *InformerStore) Get(key string) (client.Object, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.items[key]
	if !ok {
		return nil, false
	}
	return entry.obj.DeepCopyObject().(client.Object), true
}

// List returns all objects in the store, optionally filtered by namespace.
func (s *InformerStore) List(namespace string) []client.Object {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]client.Object, 0, len(s.items))
	for _, entry := range s.items {
		if namespace != "" && entry.obj.GetNamespace() != namespace {
			continue
		}
		result = append(result, entry.obj.DeepCopyObject().(client.Object))
	}
	return result
}

// Apply implements versionGuardedApply (§7.4).
// isAudit indicates this is a full re-list (enables rule 1: absent → Deleted).
func (s *InformerStore) Apply(row *ResourceRow, isAudit bool) {
	key := storeKey(row.Namespace, row.Name)

	// Update maxSeenSeq for freshness floor.
	s.updateMaxSeenSeq(key, row.Seq)

	s.mu.Lock()
	existing, exists := s.items[key]

	// Rule 2: tombstone — emit Deleted if stored, then remove.
	if row.DeletedAt != nil {
		if exists {
			old := existing.obj
			delete(s.items, key)
			s.mu.Unlock()
			s.emitDeleted(old)
			return
		}
		s.mu.Unlock()
		return
	}

	if exists {
		// Rule 3: uid mismatch — recreation of the same name.
		if string(row.UID) != string(existing.obj.GetUID()) {
			old := existing.obj
			obj, err := Decode(row)
			if err != nil {
				s.mu.Unlock()
				s.logger.Error("decode failed in Apply (uid mismatch)", "kind", s.kind, "key", key, "error", err)
				return
			}
			s.items[key] = storeEntry{obj: obj, seq: row.Seq}
			s.mu.Unlock()
			s.emitDeleted(old)
			s.emitAdded(obj)
			return
		}

		// Rule 4: seq regression — drop.
		if row.Seq <= existing.seq {
			s.mu.Unlock()
			return
		}

		// Rule 5: store first, then emit Updated.
		old := existing.obj
		obj, err := Decode(row)
		if err != nil {
			s.mu.Unlock()
			s.logger.Error("decode failed in Apply (update)", "kind", s.kind, "key", key, "error", err)
			return
		}
		s.items[key] = storeEntry{obj: obj, seq: row.Seq}
		s.mu.Unlock()
		s.emitUpdated(old, obj)
		return
	}

	// Not in store — store then emit Added.
	obj, err := Decode(row)
	if err != nil {
		s.mu.Unlock()
		s.logger.Error("decode failed in Apply (add)", "kind", s.kind, "key", key, "error", err)
		return
	}
	s.items[key] = storeEntry{obj: obj, seq: row.Seq}
	s.mu.Unlock()
	s.emitAdded(obj)
}

// AuditDiff computes keys present in the store but absent from a full re-list result.
// Only flags keys whose stored seq <= auditMaxSeq; keys with higher seq were added
// after the audit snapshot and must not be evicted.
func (s *InformerStore) AuditDiff(listedKeys map[string]bool, auditMaxSeq int64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var missing []string
	for key, entry := range s.items {
		if !listedKeys[key] && entry.seq <= auditMaxSeq {
			missing = append(missing, key)
		}
	}
	return missing
}

// RemoveKey removes a key from the store and emits Deleted (for audit corrections).
func (s *InformerStore) RemoveKey(key string) {
	s.mu.Lock()
	entry, exists := s.items[key]
	if !exists {
		s.mu.Unlock()
		return
	}
	delete(s.items, key)
	s.mu.Unlock()
	s.emitDeleted(entry.obj)
}

// SeqForKey returns the stored seq for a key, for freshness floor checks.
func (s *InformerStore) SeqForKey(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.items[key]
	if !ok {
		return 0
	}
	return entry.seq
}

// MaxSeenSeq returns the highest seq ever seen for a key (freshness floor).
func (s *InformerStore) MaxSeenSeq(key string) int64 {
	v, ok := s.maxSeenSeq.Load(key)
	if !ok {
		return 0
	}
	return v.(int64)
}

func (s *InformerStore) updateMaxSeenSeq(key string, seq int64) {
	for {
		existing, loaded := s.maxSeenSeq.LoadOrStore(key, seq)
		if !loaded {
			return
		}
		cur := existing.(int64)
		if cur >= seq {
			return
		}
		if s.maxSeenSeq.CompareAndSwap(key, existing, seq) {
			return
		}
	}
}

func (s *InformerStore) emitAdded(obj client.Object) {
	ctx := context.Background()
	s.handlerMu.RLock()
	defer s.handlerMu.RUnlock()
	for _, h := range s.handlers {
		s.safeCall(func() { h.handler.Create(ctx, event.CreateEvent{Object: obj}, h.queue) })
	}
	for _, h := range s.cacheHandlers {
		s.safeCall(func() { h.OnAdd(obj, false) })
	}
}

func (s *InformerStore) emitUpdated(old, new client.Object) {
	ctx := context.Background()
	s.handlerMu.RLock()
	defer s.handlerMu.RUnlock()
	for _, h := range s.handlers {
		s.safeCall(func() { h.handler.Update(ctx, event.UpdateEvent{ObjectOld: old, ObjectNew: new}, h.queue) })
	}
	for _, h := range s.cacheHandlers {
		s.safeCall(func() { h.OnUpdate(old, new) })
	}
}

func (s *InformerStore) emitDeleted(obj client.Object) {
	ctx := context.Background()
	s.handlerMu.RLock()
	defer s.handlerMu.RUnlock()
	for _, h := range s.handlers {
		s.safeCall(func() { h.handler.Delete(ctx, event.DeleteEvent{Object: obj}, h.queue) })
	}
	for _, h := range s.cacheHandlers {
		s.safeCall(func() { h.OnDelete(obj) })
	}
}

func (s *InformerStore) safeCall(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in event handler", "kind", s.kind, "panic", r)
		}
	}()
	fn()
}

func storeKey(namespace, name string) string {
	return namespace + "/" + name
}

// FreshnessCheck returns true if the cache entry for the given key has a seq
// >= the max seen seq. If false, the caller should requeue.
func (s *InformerStore) FreshnessCheck(key string) bool {
	maxSeen := s.MaxSeenSeq(key)
	if maxSeen == 0 {
		return true
	}
	current := s.SeqForKey(key)
	fresh := current >= maxSeen
	if !fresh {
		FreshnessFloorHits.Inc()
	}
	return fresh
}

func seqFromObject(obj client.Object) int64 {
	rv := obj.GetResourceVersion()
	if rv == "" {
		return 0
	}
	seq, err := strconv.ParseInt(rv, 10, 64)
	if err != nil {
		return 0
	}
	return seq
}

// Ensure InformerStore has the storeKey function available for external use.
func StoreKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}
