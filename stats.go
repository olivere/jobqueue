// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package jobqueue

// Stats returns statistics about the job queue.
type Stats struct {
	Waiting   int `json:"waiting"`   // number of jobs waiting to be executed
	Working   int `json:"working"`   // number of jobs currently being executed
	Succeeded int `json:"succeeded"` // number of successfully completed jobs
	Failed    int `json:"failed"`    // number of failed jobs (even after retries)
}
