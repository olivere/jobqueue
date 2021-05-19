# Jobqueue

Jobqueue manages running and scheduling jobs (think Sidekiq or Resque).

[![Test](https://github.com/olivere/jobqueue/actions/workflows/test.yaml/badge.svg)](https://github.com/olivere/jobqueue/actions/workflows/test.yaml)
[![Docs](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://pkg.go.dev/github.com/olivere/jobqueue)
[![License](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/olivere/jobqueue/master/LICENSE)

## Prerequisites

You can choose between
[MySQL](https://travis-ci.org/olivere/jobqueue/master/mysql)
and
[MongoDB](https://travis-ci.org/olivere/jobqueue/master/mongodb)
as a backend for persistent storage.

## Getting started

Get the repository with `go get github.com/olivere/jobqueue`.

Example:

```go
import (
	"github.com/olivere/jobqueue"
	"github.com/olivere/jobqueue/mysql"
)

// Create a MySQL-based persistent backend.
store, err := mysql.NewStore("root@tcp(127.0.0.1:3306)/jobqueue_e2e?loc=UTC&parseTime=true")
if err != nil {
	panic(err)
}

// Create a manager with the MySQL store and 10 concurrent workers.
m := jobqueue.New(
	jobqueue.SetStore(store),
	jobqueue.SetConcurrency(10),
)

// Register one or more topics and their processor
m.Register("clicks", func(args ...interface{}) error {
	// Handle "clicks" topic
})

// Start the manager
err := m.Start()
if err != nil {
	panic(err)
}

// Add a job: It'll be added to the store and processed eventually.
err = m.Add(&jobqueue.Add{Topic: "clicks", Args: []interface{}{640, 480}})
if err != nil {
	panic(err)
}

...

// Stop the manager, either via Stop/Close (which stops after all workers
// are finished) or CloseWithTimeout (which gracefully waits for a specified
// time span)
err = m.CloseWithTimeout(15 * time.Second) // wait for 15 seconds before forced stop
if err != nil {
	panic(err)
}
```

See the tests for more details on using jobqueue.

## Tests and Web UI

Ensure the tests succeed with `go test`. You may have to install dependencies.

You can run a simulation of a real worker like so:

```sh
cd e2e
go run main.go
```

Play with the options: `go run e2e/main.go -h`.

Then open a second console and watch the worker doing its job:

```sh
cd ui
go run main.go
```

Then open your web browser at [http://127.0.0.1:12345](http://127.0.0.1:12345).

![Screenshot](https://raw.githubusercontent.com/olivere/jobqueue/master/doc/screenshot1.png)

# License

MIT License. See [LICENSE](https://olivere.mit-license.org/) file for details.
