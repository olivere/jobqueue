// +build windows

package internal_test

import "database/sql"

func connect() (*sql.DB, error) {
	return nil, errSkipTest
}
