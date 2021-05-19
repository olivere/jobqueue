// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris

package internal_test

import (
	"database/sql"

	_ "modernc.org/sqlite"
)

func connect() (*sql.DB, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(createPersonTableSQL); err != nil {
		return nil, err
	}
	return db, nil
}
