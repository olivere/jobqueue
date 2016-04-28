// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package jobqueue

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/satori/go.uuid"
)

const (
	defaultConcurrency = 5
)

func nop() {}

// Manager schedules job executing. Create a new manager via New.
type Manager struct {
	logger  Logger
	st      Store // persistent storage
	backoff BackoffFunc

	mu          sync.Mutex           // guards the following block
	tm          map[string]Processor // maps topic to processor
	concurrency int                  // number of parallel workers
	working     int                  // number of busy workers
	started     bool
	workers     []*worker
	stopSched   chan struct{} // stop signal for scheduler
	workersWg   sync.WaitGroup
	jobc        chan *Job

	testManagerStarted   func() // testing hook
	testManagerStopped   func() // testing hook
	testSchedulerStarted func() // testing hook
	testSchedulerStopped func() // testing hook
	testJobAdded         func() // testing hook
	testJobScheduled     func() // testing hook
	testJobStarted       func() // testing hook
	testJobRetry         func() // testing hook
	testJobFailed        func() // testing hook
	testJobSucceeded     func() // testing hook
}

// New creates a new manager. Pass options to Manager to configure it.
func New(options ...ManagerOption) *Manager {
	m := &Manager{
		logger:               stdLogger{},
		st:                   NewInMemoryStore(),
		backoff:              exponentialBackoff,
		tm:                   make(map[string]Processor),
		concurrency:          defaultConcurrency,
		testManagerStarted:   nop,
		testManagerStopped:   nop,
		testSchedulerStarted: nop,
		testSchedulerStopped: nop,
		testJobAdded:         nop,
		testJobScheduled:     nop,
		testJobStarted:       nop,
		testJobRetry:         nop,
		testJobFailed:        nop,
		testJobSucceeded:     nop,
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}

// -- Configuration --

// ManagerOption is the signature of an options provider.
type ManagerOption func(*Manager)

// SetLogger specifies the logger to use when e.g. reporting errors.
func SetLogger(logger Logger) ManagerOption {
	return func(m *Manager) {
		m.logger = logger
	}
}

// SetStore specifies the backing Store implementation for the manager.
func SetStore(store Store) ManagerOption {
	return func(m *Manager) {
		m.st = store
	}
}

// SetBackoffFunc specifies the backoff function that returns the time span
// between retries of failed jobs. Exponential backoff is used by default.
func SetBackoffFunc(fn BackoffFunc) ManagerOption {
	return func(m *Manager) {
		if fn != nil {
			m.backoff = fn
		} else {
			m.backoff = exponentialBackoff
		}
	}
}

// SetConcurrency sets the maximum number of workers that will be run at
// the same time. Concurrency must be greater or equal to 1 and is 5 by
// default.
func SetConcurrency(n int) ManagerOption {
	return func(m *Manager) {
		if n <= 1 {
			m.concurrency = 1
		}
		m.concurrency = n
	}
}

// Register registers a topic and the associated processor for jobs with
// that topic.
func (m *Manager) Register(topic string, p Processor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, found := m.tm[topic]; found {
		return fmt.Errorf("jobqueue: topic %s already registered", topic)
	}
	m.tm[topic] = p
	return nil
}

// -- Start and Stop --

// Start runs the manager. Use Stop, Close, or CloseWithTimeout to stop it.
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return errors.New("jobqueue: manager already started")
	}

	// Initialize Store
	err := m.st.Start()
	if err != nil {
		return err
	}

	m.jobc = make(chan *Job, m.concurrency)
	m.workers = make([]*worker, m.concurrency)
	for i := 0; i < m.concurrency; i++ {
		m.workersWg.Add(1)
		m.workers[i] = newWorker(m, m.jobc)
	}

	m.stopSched = make(chan struct{})
	go m.schedule()

	m.started = true

	m.testManagerStarted() // testing hook

	return nil
}

// Stop stops the manager. It waits for working jobs to finish.
func (m *Manager) Stop() error {
	return m.Close()
}

// Close is an alias to Stop. It stops the manager and waits for working
// jobs to finish.
func (m *Manager) Close() error {
	return m.CloseWithTimeout(-1 * time.Second)
}

// CloseWithTimeout stops the manager. It waits for the specified timeout,
// then closes down, even if there are still jobs working. If the timeout
// is negative, the manager waits forever for all working jobs to end.
func (m *Manager) CloseWithTimeout(timeout time.Duration) error {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	// Stop accepting new jobs
	m.stopSched <- struct{}{}
	<-m.stopSched
	close(m.stopSched)
	close(m.jobc)

	// Wait for all workers to complete?
	if timeout.Nanoseconds() < 0 {
		// Yes: Wait forever
		m.workersWg.Wait()
		m.testManagerStopped() // testing hook
		return nil
	}

	// Wait with timeout
	complete := make(chan struct{}, 1)
	go func() {
		// Stop workers
		m.workersWg.Wait()
		close(complete)
	}()
	var err error
	select {
	case <-complete: // Completed in time
	case <-time.After(timeout):
		err = errors.New("jobqueue: close timed out")
	}

	m.mu.Lock()
	m.started = false
	m.mu.Unlock()
	m.testManagerStopped() // testing hook
	return err
}

// -- Add --

// Add gives the manager a new job to execute. If Add returns nil, the caller
// can be sure the job is stored in the backing store. It will be picked up
// by the scheduler at a later time.
func (m *Manager) Add(job *Job) error {
	if job.Topic == "" {
		return errors.New("jobqueue: no topic specified")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, found := m.tm[job.Topic]
	if !found {
		return fmt.Errorf("jobqueue: topic %s not registered", job.Topic)
	}
	job.ID = uuid.NewV4().String()
	job.State = Waiting
	job.Retry = 0
	job.Priority = -time.Now().UnixNano()
	job.Created = time.Now().UnixNano()
	err := m.st.Create(job)
	if err != nil {
		return err
	}
	m.testJobAdded() // testing hook
	return nil
}

// -- Stats, Lookup and List --

// Stats returns current statistics about the job queue.
func (m *Manager) Stats() (*Stats, error) {
	return m.st.Stats()
}

// Lookup returns the job with the specified identifer.
// If no such job exists, ErrNotFound is returned.
func (m *Manager) Lookup(id string) (*Job, error) {
	return m.st.Lookup(id)
}

// List returns all jobs matching the parameters in the request.
func (m *Manager) List(request *ListRequest) (*ListResponse, error) {
	return m.st.List(request)
}

// -- Scheduler --

// schedule periodically picks up waiting jobs and passes them to idle workers.
func (m *Manager) schedule() {
	m.testSchedulerStarted()       // testing hook
	defer m.testSchedulerStopped() // testing hook

	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			// Fill up available worker slots with jobs
			for {
				m.mu.Lock()
				concurrency := m.concurrency
				working := m.working
				m.mu.Unlock()
				if working >= concurrency {
					// All workers busy
					break
				}
				job, err := m.st.Next()
				if err == ErrNotFound {
					break
				}
				if err != nil {
					m.logger.Printf("jobqueue: error picking next job to schedule: %v", err)
					break
				}
				if job == nil {
					break
				}
				m.mu.Lock()
				m.working++
				job.State = Working
				job.Started = time.Now().UnixNano()
				err = m.st.Update(job)
				if err != nil {
					m.mu.Unlock()
					m.logger.Printf("jobqueue: error updating job: %v", err)
					break
				}
				m.mu.Unlock()
				m.testJobScheduled()
				m.jobc <- job
			}
		case <-m.stopSched:
			m.stopSched <- struct{}{}
			return
		}
	}
}
