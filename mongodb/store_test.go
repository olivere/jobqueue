package mongodb

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/globalsign/mgo"

	"github.com/olivere/jobqueue"
)

const (
	testDBURL = "mongodb://localhost/jobqueue_test"
)

func TestMain(m *testing.M) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	uri, err := url.Parse(testDBURL)
	if err != nil {
		panic(fmt.Sprintf("unable to parse connection string %q: %v", testDBURL, err))
	}
	if uri.Path == "" || uri.Path == "/" {
		panic(fmt.Sprintf("no database specified in connection string %q", testDBURL))
	}
	dbname := strings.TrimLeft(uri.Path, "/") // uri.Path[1:]

	session, err := mgo.DialWithTimeout(testDBURL, 15*time.Second)
	if err != nil {
		panic(fmt.Sprintf("unable to connect to %q: %v", testDBURL, err))
	}
	defer session.Close()

	code := m.Run()

	err = session.DB(dbname).DropDatabase()
	if err != nil {
		panic(fmt.Sprintf("unable to drop database in connection string %q: %v", testDBURL, err))
	}

	os.Exit(code)
}

func TestMongoDBNewStore(t *testing.T) {
	_, err := NewStore(testDBURL)
	if err != nil {
		t.Fatalf("NewStore returned %v", err)
	}
}

// TestJobSuccess is the green case where a job is called and it is
// processed without problems.
func TestMongoDBJobSuccess(t *testing.T) {
	jobDone := make(chan struct{}, 1)

	st, err := NewStore(testDBURL)
	if err != nil {
		t.Fatalf("NewStore returned %v", err)
	}

	m := jobqueue.New(jobqueue.SetStore(st))

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
	err = m.Register("topic", f)
	if err != nil {
		t.Fatalf("Register failed with %v", err)
	}
	err = m.Start()
	if err != nil {
		t.Fatalf("Start failed with %v", err)
	}
	job := &jobqueue.Job{Topic: "topic", Args: []interface{}{"Hello"}}
	err = m.Add(context.Background(), job)
	if err != nil {
		t.Fatalf("Add failed with %v", err)
	}
	if job.ID == "" {
		t.Fatalf("Job ID = %q", job.ID)
	}
	timeout := 2 * time.Second
	select {
	case <-jobDone:
	case <-time.After(timeout):
		t.Fatal("Processor func timed out")
	}
}
