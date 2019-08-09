package internal

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

// IsNotFound returns true if the given error indicates that a record
// could not be found.
func IsNotFound(err error) bool {
	return err == sql.ErrNoRows
}

// IsDup returns true if the given error indicates that we found
// a duplicate record.
func IsDup(err error) bool {
	me, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}
	return me.Number == 1062 // Duplicate key error
}

// IsDeadlock returns true if the given error indicates that we
// found a deadlock.
func IsDeadlock(err error) bool {
	me, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}
	// Error 1213: Deadlock found when trying to get lock; try restarting transaction
	return me.Number == 1213
}
