// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package jobqueue

import (
	"fmt"
	"sync"
)

// InMemoryStore is a simple in-memory store implementation.
// It implements the Store interface. Do not use in production.
type InMemoryStore struct {
	mu   sync.Mutex
	jobs map[string]*Job
}

// NewInMemoryStore creates a new InMemoryStore.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		jobs: make(map[string]*Job),
	}
}

// Start the store.
func (st *InMemoryStore) Start() error {
	return nil
}

// Create adds a new job.
func (st *InMemoryStore) Create(job *Job) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.jobs[job.ID] = job
	return nil
}

// Delete removes the job.
func (st *InMemoryStore) Delete(job *Job) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.jobs, job.ID)
	return nil
}

// Update updates the job.
func (st *InMemoryStore) Update(job *Job) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.jobs[job.ID] = job
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
				next = job
			}
		}
	}
	return next, nil
}

// Stats returns statistics about the jobs in the store.
func (st *InMemoryStore) Stats() (*Stats, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	stats := &Stats{}
	for _, job := range st.jobs {
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
func (st *InMemoryStore) Lookup(id string) (*Job, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	job, found := st.jobs[id]
	if !found {
		return nil, ErrNotFound
	}
	return job, nil
}

// LookupByCorrelationID returns the job with the specified correlation identifier (or ErrNotFound).
func (st *InMemoryStore) LookupByCorrelationID(correlationID string) (*Job, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, job := range st.jobs {
		if job.CorrelationID == correlationID {
			return job, nil
		}
	}
	return nil, ErrNotFound
}

// List finds matching jobs.
func (st *InMemoryStore) List(req *ListRequest) (*ListResponse, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	rsp := &ListResponse{}
	i := -1
	for _, job := range st.jobs {
		var skip bool
		if req.Offset > 0 && req.Offset < i {
			skip = true
		}
		if req.State != "" && job.State != req.State {
			skip = true
			rsp.Total++
		}
		if req.Limit > 0 && i > req.Limit {
			skip = true
		}
		if !skip {
			rsp.Jobs = append(rsp.Jobs, job)
		}
		i++
	}
	return rsp, nil
}
