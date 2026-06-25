/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sync"

	"github.com/typeid/hyperfleet-operator/internal/dynamo"
)

// fakeDynamo records DynamoDB calls made by controllers.
type fakeDynamo struct {
	mu          sync.Mutex
	applyCount  int
	deleteCount int
	readCount   int
	applies     []*dynamo.ApplyDesire
	deletes     []*dynamo.DeleteDesire
	reads       []*dynamo.ReadDesire

	// Set readStatus to return a specific status from GetReadDesireStatus.
	readStatus *dynamo.ReadDesireStatus
	// Set applyStatus to return a specific status from GetApplyDesireStatus.
	applyStatus *dynamo.ApplyDesireStatus
	// Set deleteStatus to return a specific status from GetDeleteDesireStatus.
	deleteStatus *dynamo.DeleteDesireStatus
	// deletedSpecs tracks calls to DeleteDesireSpec (suffix/docID).
	deletedSpecs []string
	// Set applyErr to make PutApplyDesire return an error.
	applyErr error
	// Set deleteErr to make PutDeleteDesire return an error.
	deleteErr error
}

var _ dynamo.DesireClient = (*fakeDynamo)(nil)

func (f *fakeDynamo) PutApplyDesire(_ context.Context, _ string, desire *dynamo.ApplyDesire) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.applyErr != nil {
		return f.applyErr
	}
	f.applyCount++
	f.applies = append(f.applies, desire)
	return nil
}

func (f *fakeDynamo) PutDeleteDesire(_ context.Context, _ string, desire *dynamo.DeleteDesire) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleteCount++
	f.deletes = append(f.deletes, desire)
	return nil
}

func (f *fakeDynamo) PutReadDesire(_ context.Context, _ string, desire *dynamo.ReadDesire) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readCount++
	f.reads = append(f.reads, desire)
	return nil
}

func (f *fakeDynamo) GetApplyDesireStatus(_ context.Context, _, _ string) (*dynamo.ApplyDesireStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.applyStatus != nil {
		return f.applyStatus, nil
	}
	return nil, fmt.Errorf("%w: fake", dynamo.ErrNotFound)
}

func (f *fakeDynamo) GetDeleteDesireStatus(_ context.Context, _, _ string) (*dynamo.DeleteDesireStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteStatus != nil {
		return f.deleteStatus, nil
	}
	return nil, fmt.Errorf("%w: fake", dynamo.ErrNotFound)
}

func (f *fakeDynamo) GetReadDesireStatus(_ context.Context, _, _ string) (*dynamo.ReadDesireStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readStatus != nil {
		return f.readStatus, nil
	}
	return nil, fmt.Errorf("%w: fake", dynamo.ErrNotFound)
}

func (f *fakeDynamo) DeleteDesireSpec(_ context.Context, _, suffix, docID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedSpecs = append(f.deletedSpecs, suffix+"/"+docID)
	return nil
}
