// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris

package internal_test

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

func connect() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(createPersonTableSQL); err != nil {
		return nil, err
	}
	return db, nil
}
