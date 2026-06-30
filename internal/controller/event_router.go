package controller

import (
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// EventTarget pairs a channel for GenericEvents with the CR identity
// that should be reconciled when a DynamoDB status stream event arrives.
type EventTarget struct {
	Channel chan<- event.GenericEvent
	Key     types.NamespacedName
}

// EventRouter maps DynamoDB document IDs to controller event channels.
type EventRouter struct {
	mu    sync.RWMutex
	index map[string]EventTarget
}

func NewEventRouter() *EventRouter {
	return &EventRouter{index: make(map[string]EventTarget)}
}

func (r *EventRouter) Register(docID string, target EventTarget) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.index[docID] = target
}

func (r *EventRouter) Deregister(docID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.index, docID)
}

func (r *EventRouter) Lookup(docID string) (EventTarget, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.index[docID]
	return t, ok
}

// Dispatch sends a GenericEvent to the target registered for docID.
// Returns false if no target is registered.
func (r *EventRouter) Dispatch(docID string) bool {
	target, ok := r.Lookup(docID)
	if !ok {
		return false
	}
	target.Channel <- event.GenericEvent{
		Object: &metav1.PartialObjectMetadata{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: target.Key.Namespace,
				Name:      target.Key.Name,
			},
		},
	}
	return true
}
