// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package jobqueue

import "log"

// Logger defines an interface that implementers can use to redirect
// logging into their own application.
type Logger interface {
	Printf(format string, v ...interface{})
}

// stdLogger implements the Logger interface by wrapping the Go log package.
type stdLogger struct{}

func (stdLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}
