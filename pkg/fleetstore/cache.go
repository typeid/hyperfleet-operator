package fleetstore

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Cache implements cache.Cache over informer stores.
type Cache struct {
	stores map[string]*InformerStore
	synced atomic.Bool
}

// NewCache creates a new cache backed by informer stores.
func NewCache(stores map[string]*InformerStore) *Cache {
	return &Cache{stores: stores}
}

// Get retrieves an object from the cache.
func (c *Cache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	kind, err := KindFor(obj)
	if err != nil {
		return err
	}

	store, ok := c.stores[kind]
	if !ok {
		return fmt.Errorf("no informer for kind %s", kind)
	}

	ns := key.Namespace
	if IsGlobal(kind) {
		ns = GlobalNamespace
	}
	sk := storeKey(ns, key.Name)

	cached, found := store.Get(sk)
	if !found {
		return notFound(kind, key.Name)
	}

	return copyInto(cached, obj)
}

// List lists objects from the cache with optional namespace and label filtering.
func (c *Cache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	kind, err := KindForList(list)
	if err != nil {
		return err
	}

	store, ok := c.stores[kind]
	if !ok {
		return fmt.Errorf("no informer for kind %s", kind)
	}

	listOpts := client.ListOptions{}
	for _, o := range opts {
		o.ApplyToList(&listOpts)
	}

	ns := listOpts.Namespace
	if IsGlobal(kind) && ns == "" {
		ns = ""
	} else if IsGlobal(kind) {
		ns = ""
	}

	items := store.List(ns)

	// Apply label selector client-side.
	if listOpts.LabelSelector != nil {
		filtered := items[:0]
		for _, item := range items {
			if listOpts.LabelSelector.Matches(labelsFromObject(item)) {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}

	return setListItems(kind, list, items)
}

// GetInformer returns the informer for the given object.
func (c *Cache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	kind, err := KindFor(obj)
	if err != nil {
		return nil, err
	}
	store, ok := c.stores[kind]
	if !ok {
		return nil, fmt.Errorf("no informer for kind %s", kind)
	}
	return &informerAdapter{store: store}, nil
}

// GetInformerForKind returns the informer for the given GVK.
func (c *Cache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...cache.InformerGetOption) (cache.Informer, error) {
	kind := gvk.Kind
	store, ok := c.stores[kind]
	if !ok {
		return nil, fmt.Errorf("no informer for kind %s", kind)
	}
	return &informerAdapter{store: store}, nil
}

// RemoveInformer is a no-op — informers are static.
func (c *Cache) RemoveInformer(ctx context.Context, obj client.Object) error {
	return nil
}

// Start is a no-op — the watcher drives the cache.
func (c *Cache) Start(ctx context.Context) error {
	return nil
}

// WaitForCacheSync blocks until all registered stores are synced or the context is cancelled.
func (c *Cache) WaitForCacheSync(ctx context.Context) bool {
	for {
		allSynced := true
		for _, store := range c.stores {
			if !store.HasSynced() {
				allSynced = false
				break
			}
		}
		if allSynced {
			c.synced.Store(true)
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// IndexField is not supported.
func (c *Cache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	return nil
}

// informerAdapter adapts InformerStore to the cache.Informer interface.
type informerAdapter struct {
	store *InformerStore
}

func (i *informerAdapter) AddEventHandler(handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	i.store.AddCacheHandler(handler)
	return &syncedRegistration{store: i.store}, nil
}

func (i *informerAdapter) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, resyncPeriod time.Duration) (toolscache.ResourceEventHandlerRegistration, error) {
	i.store.AddCacheHandler(handler)
	return &syncedRegistration{store: i.store}, nil
}

func (i *informerAdapter) AddEventHandlerWithOptions(handler toolscache.ResourceEventHandler, options toolscache.HandlerOptions) (toolscache.ResourceEventHandlerRegistration, error) {
	i.store.AddCacheHandler(handler)
	return &syncedRegistration{store: i.store}, nil
}

func (i *informerAdapter) RemoveEventHandler(handle toolscache.ResourceEventHandlerRegistration) error {
	return nil
}

func (i *informerAdapter) AddIndexers(indexers toolscache.Indexers) error {
	return nil
}

func (i *informerAdapter) HasSynced() bool {
	return i.store.HasSynced()
}

func (i *informerAdapter) HasSyncedChecker() toolscache.DoneChecker {
	return nil
}

func (i *informerAdapter) IsStopped() bool {
	return false
}

// syncedRegistration implements toolscache.ResourceEventHandlerRegistration.
type syncedRegistration struct {
	store *InformerStore
}

func (r *syncedRegistration) HasSynced() bool                          { return r.store.HasSynced() }
func (r *syncedRegistration) HasSyncedChecker() toolscache.DoneChecker { return nil }

// Scheme and RESTMapper return nil — not used by FleetStore.
func (c *Cache) Scheme() *runtime.Scheme     { return nil }
func (c *Cache) RESTMapper() meta.RESTMapper { return nil }
func (c *Cache) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, nil
}
func (c *Cache) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return false, nil
}
