// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type stringLogger struct {
	Lines []string
}

func (l *stringLogger) Printf(format string, v ...interface{}) {
	l.Lines = append(l.Lines, fmt.Sprintf(format, v...))
}

func TestManagerDefaults(t *testing.T) {
	m := New()
	if m.st == nil {
		t.Fatal("Store is nil")
	}
	if have, want := len(m.concurrency), 1; have != want {
		t.Fatalf("len(concurrency) = %v, want %v", have, want)
	}
	if have, want := m.concurrency[0], defaultConcurrency; have != want {
		t.Fatalf("concurrency = %v, want %v", have, want)
	}
	if have, want := m.started, false; have != want {
		t.Fatalf("concurrency = %t, want %t", have, want)
	}
	if have, want := 0, len(m.workers); have != want {
		t.Fatalf("len(workers) = %d, want %d", have, want)
	}
}

func TestManagerRegisterDuplicateTopic(t *testing.T) {
	m := New()
	f := func(args ...interface{}) error { return nil }
	err := m.Register("topic", f)
	if err != nil {
		t.Fatalf("Register failed with %v", err)
	}
	err = m.Register("topic", f)
	if err == nil {
		t.Fatalf("expected Register to fail")
	}
}

func TestManagerStartStop(t *testing.T) {
	m := New()
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	m.testManagerStarted = func() { started <- struct{}{} }
	m.testManagerStopped = func() { stopped <- struct{}{} }

	err := m.Start()
	if err != nil {
		t.Fatalf("Start failed with %v", err)
	}
	select {
	case <-started:
	case <-time.After(1 * time.Second):
		t.Fatal("Start timed out")
	}

	err = m.Stop()
	if err != nil {
		t.Fatalf("Stop failed with %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(1 * time.Second):
		t.Fatal("Stop timed out")
	}
}

// TestJobSuccess is the green case where a job is called and it is
// processed without problems.
func TestJobSuccess(t *testing.T) {
	scheduled := make(chan struct{}, 1)
	started := make(chan struct{}, 1)
	succeeded := make(chan struct{}, 1)
	jobDone := make(chan struct{}, 1)

	m := New()
	m.testJobScheduled = func() { scheduled <- struct{}{} }
	m.testJobStarted = func() { started <- struct{}{} }
	m.testJobSucceeded = func() { succeeded <- struct{}{} }

	f := func(args ...interface{}) error {
		if len(args) != 1 {
			return fmt.Errorf("expected len(args) == 1, have %d", len(args))
		}
		s, ok := args[0].(string)
		if !ok {
			return fmt.Errorf("expected type of 1st arg == string, have %T", args[0])
		}
		if have, want := s, "Hello"; have != want {
			return fmt.Errorf("expected 1st arg = %q, have %q", want, have)
		}
		jobDone <- struct{}{}
		return nil
	}
	err := m.Register("topic", f)
	if err != nil {
		t.Fatalf("Register failed with %v", err)
	}
	err = m.Start()
	if err != nil {
		t.Fatalf("Start failed with %v", err)
	}
	job := &Job{Topic: "topic", Args: []interface{}{"Hello"}}
	err = m.Add(context.Background(), job)
	if err != nil {
		t.Fatalf("Add failed with %v", err)
	}
	if job.ID == "" {
		t.Fatalf("Job ID = %q", job.ID)
	}
	timeout := 2 * time.Second
	select {
	case <-scheduled:
	case <-time.After(timeout):
		t.Fatal("Scheduler timed out")
	}
	select {
	case <-started:
	case <-time.After(timeout):
		t.Fatal("Job Start timed out")
	}
	select {
	case <-jobDone:
	case <-time.After(timeout):
		t.Fatal("Processor func timed out")
	}
	select {
	case <-succeeded:
	case <-time.After(timeout):
		t.Fatal("Job Completion timed out")
	}
}

// TestJobFailure will try to process a job that fails. We check that it
// will end up in the Failed state.
func TestJobFailure(t *testing.T) {
	scheduled := make(chan struct{}, 1)
	started := make(chan struct{}, 1)
	failed := make(chan struct{}, 1)
	jobDone := make(chan struct{}, 1)

	l := &stringLogger{}
	m := New(SetLogger(l))
	m.testJobScheduled = func() { scheduled <- struct{}{} }
	m.testJobStarted = func() { started <- struct{}{} }
	m.testJobFailed = func() { failed <- struct{}{} }

	f := func(args ...interface{}) error {
		if len(args) != 1 {
			return fmt.Errorf("expected len(args) == 1, have %d", len(args))
		}
		s, ok := args[0].(string)
		if !ok {
			return fmt.Errorf("expected type of 1st arg == string, have %T", args[0])
		}
		if have, want := s, "Hello"; have != want {
			return fmt.Errorf("expected 1st arg = %q, have %q", want, have)
		}
		jobDone <- struct{}{}
		return errors.New("failed job")
	}
	err := m.Register("topic", f)
	if err != nil {
		t.Fatalf("Register failed with %v", err)
	}
	err = m.Start()
	if err != nil {
		t.Fatalf("Start failed with %v", err)
	}
	job := &Job{Topic: "topic", Args: []interface{}{"Hello"}}
	err = m.Add(context.Background(), job)
	if err != nil {
		t.Fatalf("Add failed with %v", err)
	}
	if job.ID == "" {
		t.Fatalf("Job ID = %q", job.ID)
	}
	timeout := 2 * time.Second
	select {
	case <-scheduled:
	case <-time.After(timeout):
		t.Fatal("Scheduler timed out")
	}
	select {
	case <-started:
	case <-time.After(timeout):
		t.Fatal("Job Start timed out")
	}
	select {
	case <-jobDone:
	case <-time.After(timeout):
		t.Fatal("Processor func timed out")
	}
	select {
	case <-failed:
	case <-time.After(timeout):
		t.Fatal("Job failure timed out")
	}
	if have, want := len(l.Lines), 1; have != want {
		t.Fatal("expected lines written to Logger")
	}
}

// TestJobSuccessAfterRetry will schedule a job that will fail on the 1st
// call, but succeed on the 2nd. We check that the retry invoked and it
// will succeed after the 2nd run.
func TestJobSuccessAfterRetry(t *testing.T) {
	scheduled := make(chan struct{}, 1)
	started := make(chan struct{}, 1)
	succeeded := make(chan struct{}, 1)
	retry := make(chan struct{}, 1)
	jobDone := make(chan struct{}, 2)

	l := &stringLogger{}
	m := New(SetLogger(l))
	m.testJobScheduled = func() { scheduled <- struct{}{} }
	m.testJobStarted = func() { started <- struct{}{} }
	m.testJobRetry = func() { retry <- struct{}{} }
	m.testJobSucceeded = func() { succeeded <- struct{}{} }

	var call int
	f := func(args ...interface{}) error {
		call++
		jobDone <- struct{}{}
		// only fail on first call
		if call == 1 {
			return errors.New("failed job on 1st call")
		}
		return nil
	}
	err := m.Register("topic", f)
	if err != nil {
		t.Fatalf("Register failed with %v", err)
	}
	err = m.Start()
	if err != nil {
		t.Fatalf("Start failed with %v", err)
	}
	job := &Job{Topic: "topic", MaxRetry: 1, Args: []interface{}{"Hello"}}
	err = m.Add(context.Background(), job)
	if err != nil {
		t.Fatalf("Add failed with %v", err)
	}
	if job.ID == "" {
		t.Fatalf("Job ID = %q", job.ID)
	}
	timeout := 2 * time.Second
	select {
	case <-scheduled:
	case <-time.After(timeout):
		t.Fatal("Scheduler timed out")
	}
	select {
	case <-started:
	case <-time.After(timeout):
		t.Fatal("Job Start timed out")
	}
	select {
	case <-jobDone:
	case <-time.After(timeout):
		t.Fatal("Processor func timed out")
	}
	select {
	case <-retry:
	case <-time.After(timeout):
		t.Fatal("Job retry timed out")
	}
	select {
	case <-started:
	case <-time.After(timeout):
		t.Fatal("Job Start timed out")
	}
	select {
	case <-jobDone:
	case <-time.After(timeout):
		t.Fatal("Processor func timed out")
	}
	select {
	case <-succeeded:
	case <-time.After(timeout):
		t.Fatal("Job success timed out")
	}
	if have, want := len(l.Lines), 1; have != want {
		t.Fatal("expected lines written to Logger")
	}
}
