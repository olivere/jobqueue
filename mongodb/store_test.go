package mongodb

import (
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/globalsign/mgo"

	"github.com/olivere/jobqueue"
)

const (
	testDBURL = "mongodb://localhost/jobqueue_e2e"
)

func isTravis() bool {
	return os.Getenv("TRAVIS") != ""
}

func travisGoVersion() string {
	return os.Getenv("TRAVIS_GO_VERSION")
}

// dropDatabase drops the database specified in the dburl connection string.
func dropDatabase(t *testing.T, dburl string) {
	uri, err := url.Parse(dburl)
	if err != nil {
		t.Fatal(err)
	}
	if uri.Path == "" || uri.Path == "/" {
		t.Fatalf("no database specified in %q", dburl)
	}
	dbname := uri.Path[1:]

	session, err := mgo.DialWithTimeout(dburl, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	err = session.DB(dbname).DropDatabase()
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewStore(t *testing.T) {
	if !isTravis() {
		t.Skip("skipping integration test; it will only run on travis")
		return
	}

	defer dropDatabase(t, testDBURL)

	_, err := NewStore(testDBURL)
	if err != nil {
		t.Fatalf("NewStore returned %v", err)
	}
}

// TestJobSuccess is the green case where a job is called and it is
// processed without problems.
func TestJobSuccess(t *testing.T) {
	if !isTravis() {
		t.Skip("skipping integration test; it will only run on travis")
		return
	}

	jobDone := make(chan struct{}, 1)

	st, err := NewStore(testDBURL)
	if err != nil {
		t.Fatalf("NewStore returned %v", err)
	}
	defer dropDatabase(t, testDBURL)

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
	err = m.Add(job)
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
