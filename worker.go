package jobqueue

import (
	"fmt"
	"time"
)

// worker is a single instance processing jobs.
type worker struct {
	m    *Manager
	jobc <-chan *Job
}

// newWorker creates a new worker. It spins up a new goroutine that waits
// on jobc for new jobs to process.
func newWorker(m *Manager, jobc <-chan *Job) *worker {
	w := &worker{m: m, jobc: jobc}
	go w.run()
	return w
}

// run is the main goroutine in the worker. It listens for new jobs, then
// calls process.
func (w *worker) run() {
	defer w.m.workersWg.Done()
	for {
		select {
		case job, more := <-w.m.jobc:
			if !more {
				// jobc has been closed
				return
			}
			err := w.process(job)
			if err != nil {
				w.m.logger.Printf("jobqueue: job %v failed: %v", job.ID, err)
			}
		}
	}
}

// process runs a single job.
func (w *worker) process(job *Job) error {
	defer func() {
		w.m.mu.Lock()
		w.m.working--
		w.m.mu.Unlock()
	}()

	// Find the topic
	w.m.mu.Lock()
	p, found := w.m.tm[job.Topic]
	w.m.mu.Unlock()
	if !found {
		return fmt.Errorf("no processor found for topic %s", job.Topic)
	}

	w.m.testJobStarted() // testing hook

	// Execute the job
	err := p(job.Args...)
	if err != nil {
		w.m.logger.Printf("jobqueue: Job %v failed with: %v", job.ID, err)

		if job.Retry >= job.MaxRetry {
			// Failed
			w.m.testJobFailed() // testing hook
			job.State = Failed
			job.Completed = time.Now().UnixNano()
			err = w.m.st.Update(job)
			if err != nil {
				return err
			}
			return nil
		}

		// Retry
		w.m.testJobRetry() // testing hook
		job.Priority = -time.Now().Add(w.m.backoff(job.Retry)).UnixNano()
		job.State = Waiting
		job.Retry++
		err = w.m.st.Update(job)
		if err != nil {
			return err
		}
		return nil
	}

	// Successfully executed the job
	job.State = Succeeded
	job.Completed = time.Now().UnixNano()
	err = w.m.st.Update(job)
	if err != nil {
		return err
	}
	w.m.testJobSucceeded()
	return nil
}
