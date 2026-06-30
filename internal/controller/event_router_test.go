package controller

import (
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestDispatch_SendsEvent(t *testing.T) {
	er := NewEventRouter()
	ch := make(chan event.GenericEvent, 1)
	key := types.NamespacedName{Namespace: "ns", Name: "test-cr"}
	er.Register("doc-1", EventTarget{Channel: ch, Key: key})

	if !er.Dispatch("doc-1") {
		t.Fatal("Dispatch returned false for registered docID")
	}

	select {
	case evt := <-ch:
		if evt.Object.GetNamespace() != "ns" || evt.Object.GetName() != "test-cr" {
			t.Errorf("event has wrong identity: %s/%s", evt.Object.GetNamespace(), evt.Object.GetName())
		}
	default:
		t.Fatal("expected event on channel")
	}
}

func TestDispatch_UnregisteredReturnsFalse(t *testing.T) {
	er := NewEventRouter()
	if er.Dispatch("unknown") {
		t.Fatal("Dispatch returned true for unregistered docID")
	}
}

func TestDispatch_DropsWhenChannelFull(t *testing.T) {
	er := NewEventRouter()
	ch := make(chan event.GenericEvent, 1)
	key := types.NamespacedName{Namespace: "ns", Name: "test-cr"}
	er.Register("doc-1", EventTarget{Channel: ch, Key: key})

	// Fill the channel.
	er.Dispatch("doc-1")

	// Second dispatch should not block — it drops the event.
	if !er.Dispatch("doc-1") {
		t.Fatal("Dispatch returned false even though docID is registered")
	}

	// Only one event should be in the channel.
	<-ch
	select {
	case <-ch:
		t.Fatal("expected channel to be empty after draining one event")
	default:
	}
}

func TestDeregister_RemovesEntry(t *testing.T) {
	er := NewEventRouter()
	ch := make(chan event.GenericEvent, 1)
	er.Register("doc-1", EventTarget{Channel: ch, Key: types.NamespacedName{Name: "cr"}})

	er.Deregister("doc-1")

	if er.Dispatch("doc-1") {
		t.Fatal("Dispatch should return false after Deregister")
	}
	if _, ok := er.Lookup("doc-1"); ok {
		t.Fatal("Lookup should return false after Deregister")
	}
}
