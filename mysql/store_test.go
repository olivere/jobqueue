package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/olivere/jobqueue"
)

const (
	testDBURL = "root@tcp(127.0.0.1:3306)/jobqueue_test?loc=UTC&parseTime=true"
)

func TestMain(m *testing.M) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cfg, err := mysql.ParseDSN(testDBURL)
	if err != nil {
		panic(fmt.Sprintf("unable to parse connection string %q: %v", testDBURL, err))
	}
	dbname := cfg.DBName
	if dbname == "" {
		panic(fmt.Sprintf("no database specified in connection string %q", testDBURL))
	}
	// Connect without DB name
	cfg.DBName = ""
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		panic(fmt.Sprintf("unable to open connection string %q: %v", cfg.FormatDSN(), err))
	}
	defer db.Close()

	// Create database
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbname))
	if err != nil {
		panic(fmt.Sprintf("unable to create database %q from connection string %q: %v", dbname, testDBURL, err))
	}

	code := m.Run()

	// Drop database
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbname))
	if err != nil {
		panic(fmt.Sprintf("unable to drop database %q from connection string %q: %v", dbname, testDBURL, err))
	}

	os.Exit(code)
}

func TestMySQLNewStore(t *testing.T) {
	_, err := NewStore(testDBURL, SetDebug(true))
	if err != nil {
		t.Fatalf("NewStore returned %v", err)
	}
}

// TestJobSuccess is the green case where a job is called and it is
// processed without problems.
func TestMySQLJobSuccess(t *testing.T) {
	jobDone := make(chan struct{}, 1)

	st, err := NewStore(testDBURL, SetDebug(true))
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
