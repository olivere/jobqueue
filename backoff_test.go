// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package jobqueue

import (
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	tests := []struct {
		Expected time.Duration
	}{
		{time.Duration(0 * time.Millisecond)},
		{time.Duration(10 * time.Millisecond)},
		{time.Duration(100 * time.Millisecond)},
		{time.Duration(1000 * time.Millisecond)},
		{time.Duration(10000 * time.Millisecond)},
		{time.Duration(100000 * time.Millisecond)},
	}

	for i, test := range tests {
		if want, have := test.Expected, exponentialBackoff(i); want != have {
			t.Fatalf("want %v, have %v", want, have)
		}
	}
}
