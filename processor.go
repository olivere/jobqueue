// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package jobqueue

// Processor is responsible to process a job for a certain topic.
type Processor func(...interface{}) error
