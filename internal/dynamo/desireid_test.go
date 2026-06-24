package dynamo

import (
	"testing"
)

func TestNewDocumentIDDeterministic(t *testing.T) {
	id1 := NewDocumentID("hyperfleet-operator", "", "v1", "namespaces", "", "clusters-abc12345")
	id2 := NewDocumentID("hyperfleet-operator", "", "v1", "namespaces", "", "clusters-abc12345")
	if id1 != id2 {
		t.Errorf("same inputs produced different IDs: %s != %s", id1, id2)
	}
}

func TestNewDocumentIDDiffersOnInput(t *testing.T) {
	id1 := NewDocumentID("hyperfleet-operator", "", "v1", "namespaces", "", "clusters-abc")
	id2 := NewDocumentID("hyperfleet-operator", "", "v1", "namespaces", "", "clusters-xyz")
	if id1 == id2 {
		t.Errorf("different inputs produced same ID: %s", id1)
	}
}

func TestNewDocumentIDDiffersOnTaskKey(t *testing.T) {
	id1 := NewDocumentID("hyperfleet-operator", "", "v1", "namespaces", "", "clusters-abc")
	id2 := NewDocumentID("hyperfleet-operator-read", "", "v1", "namespaces", "", "clusters-abc")
	if id1 == id2 {
		t.Errorf("different task keys produced same ID: %s", id1)
	}
}

func TestNewDocumentIDFormat(t *testing.T) {
	id := NewDocumentID("hyperfleet-operator", "", "v1", "namespaces", "", "clusters-abc")
	if len(id) != 36 {
		t.Errorf("expected UUID format (36 chars), got %d chars: %s", len(id), id)
	}
	if id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		t.Errorf("not a valid UUID format: %s", id)
	}
}
