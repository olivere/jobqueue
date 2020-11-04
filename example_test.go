package jobqueue_test

import (
	"context"
	"fmt"
	"time"

	"github.com/olivere/jobqueue"
)

func ExampleManager() {
	// Create a new manager with 10 concurrent workers for rank 0 and 2 for rank 1
	m := jobqueue.New(
		jobqueue.SetConcurrency(0, 10),
		jobqueue.SetConcurrency(1, 2),
	)

	// Register the processor for topic "crawl"
	jobDone := make(chan struct{}, 1)
	err := m.Register("crawl", func(job *jobqueue.Job) error {
		url, _ := job.Args[0].(string)
		fmt.Printf("Crawl %s\n", url)
		jobDone <- struct{}{}
		return nil
	})
	if err != nil {
		fmt.Println("Register failed")
		return
	}

	// Start the manager
	err = m.Start()
	if err != nil {
		fmt.Println("Start failed")
		return
	}
	fmt.Println("Started")

	// Add a new crawler job
	job := &jobqueue.Job{Topic: "crawl", Args: []interface{}{"https://alt-f4.de"}}
	err = m.Add(context.Background(), job)
	if err != nil {
		fmt.Println("Add failed")
		return
	}
	fmt.Println("Job added")

	// Wait for the crawler job to complete
	select {
	case <-jobDone:
	case <-time.After(5 * time.Second):
		fmt.Println("Job timed out")
		return
	}

	// Stop/Close the manager
	err = m.Stop()
	if err != nil {
		fmt.Println("Stop failed")
		return
	}
	fmt.Println("Stopped")

	// Output:
	// Started
	// Job added
	// Crawl https://alt-f4.de
	// Stopped
}
