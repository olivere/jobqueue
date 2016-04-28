// Package jobqueue manages running and scheduling jobs.
//
// Applications using jobqueue first create a Manager. One manager handles
// one or more topics. There is one processor per topic. Applications need
// to register topics and their processors before starting the manager.
//
// Once started, the manager initializes the list of workers that will
// work on the actual jobs. At the beginning, all workers are idle.
//
// The manager has a Store to implement persistent storage. By default, an
// in memory store is used. There is a MySQL-based persistent store in
// the "mysql" package.
//
// New jobs are added to the manager via the Add method. The manager asks
// the store to create the job.
//
// A scheduler inside manager periodically asks the Store for jobs in the
// Waiting state. The scheduler will tell idle workers to handle those jobs.
// The number of concurrent jobs can be specified via the manager option
// SetConcurrency.
//
// A job in jobqueue has always in one of these four states: Waiting (to be
// executed), Working (currently busy working on a job), Succeeded (completed
// successfully), and Failed (failed to complete successfully even after
// retrying).
//
// A job can be configured to be retried. To do so, specify the MaxRetry
// field in Job. Only if the number of retries exceeds the MaxRetry value,
// the job gets marked as failed. Otherwise, it gets put back into Waiting
// state and rescheduled (after an some backoff time). The backoff function
// is exponential by default (see backoff.go). However, one can specify a
// custom backoff function by the manager option SetBackoffFunc.
//
// If the manager crashes and gets restarted, the Store gets started via the
// Start method. This gives the store implementation a chance to do cleanup.
// E.g. the MySQL-based store implementation moves all jobs still marked as
// Working into the Failed state. Notice that you are responsible to prevent
// that two concurrent managers try to access the same database!
package jobqueue
