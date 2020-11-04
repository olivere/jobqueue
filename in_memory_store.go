// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package jobqueue

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryStore is a simple in-memory store implementation.
// It implements the Store interface. Do not use in production.
type InMemoryStore struct {
	mu   sync.Mutex
	jobs map[string]Job
}

// NewInMemoryStore creates a new InMemoryStore.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		jobs: make(map[string]Job),
	}
}

// Start the store.
func (st *InMemoryStore) Start(_ StartupBehaviour) error {
	return nil
}

// Create adds a new job.
func (st *InMemoryStore) Create(ctx context.Context, job *Job) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.jobs[job.ID] = *job
	return nil
}

// Delete removes the job.
func (st *InMemoryStore) Delete(ctx context.Context, job *Job) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.jobs, job.ID)
	return nil
}

// Update updates the job.
func (st *InMemoryStore) Update(ctx context.Context, job *Job) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.jobs[job.ID] = *job
	return nil
}

// Next picks the next job to execute.
func (st *InMemoryStore) Next() (*Job, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	var next *Job
	for _, job := range st.jobs {
		if job.State == Waiting {
			if next == nil || job.Rank > next.Rank || job.Priority > next.Priority {
				dup := job
				next = &dup
			}
		}
	}
	return next, nil
}

// Stats returns statistics about the jobs in the store.
func (st *InMemoryStore) Stats(ctx context.Context, req *StatsRequest) (*Stats, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	stats := &Stats{}
	for _, job := range st.jobs {
		if req.Topic != "" && job.Topic != req.Topic {
			continue
		}
		if req.CorrelationGroup != "" && job.CorrelationGroup != req.CorrelationGroup {
			continue
		}
		switch job.State {
		default:
			return nil, fmt.Errorf("found unknown state %v", job.State)
		case Waiting:
			stats.Waiting++
		case Working:
			stats.Working++
		case Succeeded:
			stats.Succeeded++
		case Failed:
			stats.Failed++
		}
	}
	return stats, nil
}

// Lookup returns the job with the specified identifier (or ErrNotFound).
func (st *InMemoryStore) Lookup(ctx context.Context, id string) (*Job, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	job, found := st.jobs[id]
	if !found {
		return nil, ErrNotFound
	}
	dup := job
	return &dup, nil
}

// LookupByCorrelationID returns the details of jobs by their correlation identifier.
// If no such job could be found, an empty array is returned.
func (st *InMemoryStore) LookupByCorrelationID(ctx context.Context, correlationID string) ([]*Job, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	var result []*Job
	for _, job := range st.jobs {
		if job.CorrelationID == correlationID {
			dup := job
			result = append(result, &dup)
		}
	}
	return result, nil
}

// List finds matching jobs.
func (st *InMemoryStore) List(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	rsp := &ListResponse{}
	i := -1
	for _, job := range st.jobs {
		var skip bool
		if req.Topic != "" && req.Topic != job.Topic {
			skip = true
		}
		if req.State != "" && job.State != req.State {
			skip = true
		}
		if req.Offset > 0 && req.Offset < i {
			skip = true
			rsp.Total++
		}
		if req.Limit > 0 && i > req.Limit {
			skip = true
			rsp.Total++
		}
		if !skip {
			dup := job
			rsp.Jobs = append(rsp.Jobs, &dup)
		}
		i++
	}
	return rsp, nil
}
