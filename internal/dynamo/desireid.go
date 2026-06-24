package dynamo

import (
	"fmt"

	"github.com/google/uuid"
)

// NamespaceUUID is the fixed UUID v5 namespace for kube-applier desire
// document IDs. Shared between the operator and kube-applier-aws.
// Changing this value invalidates all existing document IDs.
var NamespaceUUID = uuid.MustParse("a3f1b2c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c")

// NewDocumentID computes a deterministic UUID v5 for a desire document.
// Same inputs always produce the same UUID for natural idempotency.
func NewDocumentID(taskKey, group, version, resource, namespace, name string) string {
	input := fmt.Sprintf("%s/%s/%s/%s/%s/%s", taskKey, group, version, resource, namespace, name)
	return uuid.NewSHA1(NamespaceUUID, []byte(input)).String()
}
