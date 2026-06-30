package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/typeid/hyperfleet-operator/internal/dynamo"
)

const ConditionSynced = "Synced"

// DesireStatusEntry identifies a single desire to check.
type DesireStatusEntry struct {
	DocID            string
	Resource         string
	Name             string
	DesireUpdateTime time.Time
}

// CheckApplyDesireStatuses queries GetApplyDesireStatus for each entry and
// returns an aggregate Synced condition reflecting whether kube-applier has
// successfully applied all desires.
func CheckApplyDesireStatuses(
	ctx context.Context,
	dc dynamo.DesireClient,
	statusPrefix string,
	entries []DesireStatusEntry,
	generation int64,
) metav1.Condition {
	total := len(entries)

	type result struct {
		status *dynamo.ApplyDesireStatus
		err    error
	}
	results := make([]result, total)
	var wg sync.WaitGroup
	for i, e := range entries {
		wg.Add(1)
		go func(idx int, docID string) {
			defer wg.Done()
			s, err := dc.GetApplyDesireStatus(ctx, statusPrefix, docID)
			results[idx] = result{status: s, err: err}
		}(i, e.DocID)
	}
	wg.Wait()

	synced := 0
	var failedMessages []string
	for i, e := range entries {
		r := results[i]
		if r.err != nil {
			if !errors.Is(r.err, dynamo.ErrNotFound) {
				failedMessages = append(failedMessages, fmt.Sprintf("%s/%s: %v", e.Resource, e.Name, r.err))
			}
			continue
		}

		if !e.DesireUpdateTime.IsZero() && r.status.ObservedDesireUpdateTime.Before(e.DesireUpdateTime) {
			continue
		}

		cond := meta.FindStatusCondition(r.status.Conditions, dynamo.DesireConditionSuccessful)
		if cond == nil {
			continue
		}

		if cond.Status == metav1.ConditionTrue {
			synced++
		} else {
			failedMessages = append(failedMessages, fmt.Sprintf("%s/%s: %s", e.Resource, e.Name, cond.Reason))
		}
	}

	switch {
	case len(failedMessages) > 0:
		return metav1.Condition{
			Type:               ConditionSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "SyncFailed",
			Message:            fmt.Sprintf("%d/%d synced; failing: %s", synced, total, strings.Join(failedMessages, "; ")),
			ObservedGeneration: generation,
		}
	case synced == total:
		return metav1.Condition{
			Type:               ConditionSynced,
			Status:             metav1.ConditionTrue,
			Reason:             "AllSynced",
			Message:            fmt.Sprintf("%d/%d desires applied successfully", synced, total),
			ObservedGeneration: generation,
		}
	default:
		return metav1.Condition{
			Type:               ConditionSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "AwaitingSync",
			Message:            fmt.Sprintf("%d/%d synced, waiting for kube-applier to process remaining desires", synced, total),
			ObservedGeneration: generation,
		}
	}
}

// CheckDeleteDesireStatuses queries GetDeleteDesireStatus for each entry and
// returns an aggregate Synced condition reflecting whether kube-applier has
// successfully deleted all targeted resources.
func CheckDeleteDesireStatuses(
	ctx context.Context,
	dc dynamo.DesireClient,
	statusPrefix string,
	entries []DesireStatusEntry,
	generation int64,
) metav1.Condition {
	total := len(entries)

	type result struct {
		status *dynamo.DeleteDesireStatus
		err    error
	}
	results := make([]result, total)
	var wg sync.WaitGroup
	for i, e := range entries {
		wg.Add(1)
		go func(idx int, docID string) {
			defer wg.Done()
			s, err := dc.GetDeleteDesireStatus(ctx, statusPrefix, docID)
			results[idx] = result{status: s, err: err}
		}(i, e.DocID)
	}
	wg.Wait()

	deleted := 0
	var errorMessages []string
	var pendingMessages []string

	for i, e := range entries {
		r := results[i]
		if r.err != nil {
			if !errors.Is(r.err, dynamo.ErrNotFound) {
				errorMessages = append(errorMessages, fmt.Sprintf("%s/%s: %v", e.Resource, e.Name, r.err))
			}
			continue
		}

		cond := meta.FindStatusCondition(r.status.Conditions, dynamo.DesireConditionSuccessful)
		if cond == nil {
			continue
		}

		if cond.Status == metav1.ConditionTrue {
			deleted++
		} else {
			pendingMessages = append(pendingMessages, fmt.Sprintf("%s/%s: %s", e.Resource, e.Name, cond.Reason))
		}
	}

	switch {
	case len(errorMessages) > 0:
		return metav1.Condition{
			Type:               ConditionSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "StatusCheckFailed",
			Message:            fmt.Sprintf("%d/%d deleted; errors: %s", deleted, total, strings.Join(errorMessages, "; ")),
			ObservedGeneration: generation,
		}
	case len(pendingMessages) > 0:
		return metav1.Condition{
			Type:               ConditionSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "AwaitingDeletion",
			Message:            fmt.Sprintf("%d/%d deleted; pending: %s", deleted, total, strings.Join(pendingMessages, "; ")),
			ObservedGeneration: generation,
		}
	case deleted == total:
		return metav1.Condition{
			Type:               ConditionSynced,
			Status:             metav1.ConditionTrue,
			Reason:             "AllSynced",
			Message:            fmt.Sprintf("%d/%d resources deleted successfully", deleted, total),
			ObservedGeneration: generation,
		}
	default:
		return metav1.Condition{
			Type:               ConditionSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "AwaitingDeletion",
			Message:            fmt.Sprintf("%d/%d deleted, waiting for kube-applier to process remaining deletions", deleted, total),
			ObservedGeneration: generation,
		}
	}
}
