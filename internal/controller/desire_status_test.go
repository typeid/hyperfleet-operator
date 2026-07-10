package controller

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/typeid/hyperfleet-operator/internal/dynamo"
)

var _ = Describe("CheckApplyDesireStatuses", func() {
	ctx := context.Background()

	entries := func(docIDs ...string) []DesireStatusEntry {
		var out []DesireStatusEntry
		for _, id := range docIDs {
			out = append(out, DesireStatusEntry{DocID: id, Resource: "configmaps", Name: id})
		}
		return out
	}

	It("should return AllSynced when all desires report Successful=True", func() {
		fd := &fakeDynamo{
			applyStatuses: map[string]*dynamo.ApplyDesireStatus{
				"doc-a": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionTrue, Reason: "NoErrors"}}, ObservedDesireUpdateTime: time.Now()},
				"doc-b": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionTrue, Reason: "NoErrors"}}, ObservedDesireUpdateTime: time.Now()},
			},
		}
		cond := CheckApplyDesireStatuses(ctx, fd, "status-prefix", entries("doc-a", "doc-b"), 1)
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal("AllSynced"))
		Expect(cond.Message).To(ContainSubstring("2/2"))
	})

	It("should return AwaitingSync when some desires have no status yet", func() {
		fd := &fakeDynamo{
			applyStatuses: map[string]*dynamo.ApplyDesireStatus{
				"doc-a": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionTrue, Reason: "NoErrors"}}, ObservedDesireUpdateTime: time.Now()},
			},
		}
		cond := CheckApplyDesireStatuses(ctx, fd, "status-prefix", entries("doc-a", "doc-b"), 1)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("AwaitingSync"))
		Expect(cond.Message).To(ContainSubstring("1/2"))
	})

	It("should return SyncFailed when a desire reports Successful=False", func() {
		fd := &fakeDynamo{
			applyStatuses: map[string]*dynamo.ApplyDesireStatus{
				"doc-a": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionTrue, Reason: "NoErrors"}}, ObservedDesireUpdateTime: time.Now()},
				"doc-b": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionFalse, Reason: "KubeAPIError", Message: "server-side apply: NodePool is invalid"}}, ObservedDesireUpdateTime: time.Now()},
			},
		}
		cond := CheckApplyDesireStatuses(ctx, fd, "status-prefix", entries("doc-a", "doc-b"), 1)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("SyncFailed"))
		Expect(cond.Message).To(ContainSubstring("KubeAPIError"))
		Expect(cond.Message).To(ContainSubstring("server-side apply: NodePool is invalid"))
	})

	It("should return SyncFailed when DynamoDB returns a non-NotFound error", func() {
		fd := &fakeDynamo{
			applyStatuses: map[string]*dynamo.ApplyDesireStatus{
				"doc-a": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionTrue, Reason: "NoErrors"}}, ObservedDesireUpdateTime: time.Now()},
			},
			applyStatusErrors: map[string]error{
				"doc-b": fmt.Errorf("dynamodb throttle"),
			},
		}
		cond := CheckApplyDesireStatuses(ctx, fd, "status-prefix", entries("doc-a", "doc-b"), 1)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("SyncFailed"))
		Expect(cond.Message).To(ContainSubstring("dynamodb throttle"))
	})

	It("should skip stale statuses whose ObservedDesireUpdateTime predates the spec write", func() {
		staleTime := time.Now().Add(-10 * time.Minute)
		freshTime := time.Now()
		fd := &fakeDynamo{
			applyStatuses: map[string]*dynamo.ApplyDesireStatus{
				"doc-a": {
					Conditions:               []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionTrue, Reason: "NoErrors"}},
					ObservedDesireUpdateTime: staleTime,
				},
			},
		}
		e := []DesireStatusEntry{{DocID: "doc-a", Resource: "configmaps", Name: "doc-a", DesireUpdateTime: freshTime}}
		cond := CheckApplyDesireStatuses(ctx, fd, "status-prefix", e, 1)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("AwaitingSync"))
	})

	It("should set ObservedGeneration on the returned condition", func() {
		fd := &fakeDynamo{}
		cond := CheckApplyDesireStatuses(ctx, fd, "status-prefix", entries("doc-a"), 42)
		Expect(cond.ObservedGeneration).To(Equal(int64(42)))
	})
})

var _ = Describe("CheckDeleteDesireStatuses", func() {
	ctx := context.Background()

	entries := func(docIDs ...string) []DesireStatusEntry {
		var out []DesireStatusEntry
		for _, id := range docIDs {
			out = append(out, DesireStatusEntry{DocID: id, Resource: "namespaces", Name: id})
		}
		return out
	}

	It("should return AllSynced when all deletes report Successful=True", func() {
		fd := &fakeDynamo{
			deleteStatuses: map[string]*dynamo.DeleteDesireStatus{
				"doc-a": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionTrue, Reason: "NoErrors"}}},
				"doc-b": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionTrue, Reason: "NoErrors"}}},
			},
		}
		cond := CheckDeleteDesireStatuses(ctx, fd, "status-prefix", entries("doc-a", "doc-b"), 1)
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal("AllSynced"))
		Expect(cond.Message).To(ContainSubstring("2/2"))
	})

	It("should return AwaitingDeletion when a delete reports Successful=False", func() {
		fd := &fakeDynamo{
			deleteStatuses: map[string]*dynamo.DeleteDesireStatus{
				"doc-a": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionTrue, Reason: "NoErrors"}}},
				"doc-b": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionFalse, Reason: "WaitingForDeletion"}}},
			},
		}
		cond := CheckDeleteDesireStatuses(ctx, fd, "status-prefix", entries("doc-a", "doc-b"), 1)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("AwaitingDeletion"))
		Expect(cond.Message).To(ContainSubstring("WaitingForDeletion"))
	})

	It("should return StatusCheckFailed when DynamoDB returns a non-NotFound error", func() {
		fd := &fakeDynamo{
			deleteStatuses: map[string]*dynamo.DeleteDesireStatus{
				"doc-a": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionTrue, Reason: "NoErrors"}}},
			},
			deleteStatusErrors: map[string]error{
				"doc-b": fmt.Errorf("dynamodb throttle"),
			},
		}
		cond := CheckDeleteDesireStatuses(ctx, fd, "status-prefix", entries("doc-a", "doc-b"), 1)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("StatusCheckFailed"))
		Expect(cond.Message).To(ContainSubstring("dynamodb throttle"))
	})

	It("should return AwaitingDeletion when status is not yet available (ErrNotFound)", func() {
		fd := &fakeDynamo{
			deleteStatuses: map[string]*dynamo.DeleteDesireStatus{
				"doc-a": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionTrue, Reason: "NoErrors"}}},
			},
		}
		cond := CheckDeleteDesireStatuses(ctx, fd, "status-prefix", entries("doc-a", "doc-b"), 1)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("AwaitingDeletion"))
	})

	It("should prioritize errors over pending deletions", func() {
		fd := &fakeDynamo{
			deleteStatuses: map[string]*dynamo.DeleteDesireStatus{
				"doc-b": {Conditions: []metav1.Condition{{Type: dynamo.DesireConditionSuccessful, Status: metav1.ConditionFalse, Reason: "WaitingForDeletion"}}},
			},
			deleteStatusErrors: map[string]error{
				"doc-a": fmt.Errorf("dynamodb throttle"),
			},
		}
		cond := CheckDeleteDesireStatuses(ctx, fd, "status-prefix", entries("doc-a", "doc-b"), 1)
		Expect(cond.Reason).To(Equal("StatusCheckFailed"))
	})
})
