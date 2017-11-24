// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package jobqueue

const (
	// Waiting for executing.
	Waiting string = "waiting"
	// Working is the state for currently executing jobs.
	Working string = "working"
	// Succeeded without errors.
	Succeeded string = "succeeded"
	// Failed even after retries.
	Failed string = "failed"
)

// Job is a task that needs to be executed.
type Job struct {
	ID               string        `json:"id"`        // internal identifier
	Topic            string        `json:"topic"`     // topic to find the correct processor
	State            string        `json:"state"`     // current state
	Args             []interface{} `json:"args"`      // arguments to pass to processor
	Rank             int           `json:"rank"`      // jobs with higher ranks get executed earlier
	Priority         int64         `json:"prio"`      // priority (highest gets executed first)
	Retry            int           `json:"retry"`     // current number of retries
	MaxRetry         int           `json:"maxretry"`  // maximum number of retries
	CorrelationGroup string        `json:"cgroup"`    // external group
	CorrelationID    string        `json:"cid"`       // external identifier
	Created          int64         `json:"created"`   // time when Add was called (in UnixNano)
	Updated          int64         `json:"updated"`   // time when the job was last updated (in UnixNano)
	Started          int64         `json:"started"`   // time when the job was started (in UnixNano)
	Completed        int64         `json:"completed"` // time when job reached either state Succeeded or Failed (in UnixNano)
}
