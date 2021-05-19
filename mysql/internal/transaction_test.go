package internal_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"

	"github.com/olivere/jobqueue/mysql/internal"
)

var (
	errSkipTest = errors.New("skip test")
)

type Person struct {
	ID   int64
	Name string
}

const (
	createPersonTableSQL = `CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, name TEXT NOT NULL);`
)

func newBackoff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 500 * time.Millisecond
	b.MaxInterval = 1 * time.Second
	b.MaxElapsedTime = 5 * time.Second
	return b
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestRunInTx(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		testRunInTxOK(t)
	})
	t.Run("Deadlock", func(t *testing.T) {
		testRunInTxWithDeadlock(t)
	})
	t.Run("ErrorInFn", func(t *testing.T) {
		testRunInTxErrorInFn(t)
	})
	t.Run("PanicInFn", func(t *testing.T) {
		testRunInTxPanicInFn(t)
	})
	t.Run("Retryable", func(t *testing.T) {
		testRunInTxRetryable(t, newBackoff())
	})
}

func testRunInTxOK(t *testing.T) {
	db, err := connect()
	if err == errSkipTest {
		t.SkipNow()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	alice := &Person{Name: "Alice"}
	bob := &Person{Name: "Bob"}
	err = internal.RunInTx(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
		// Alice
		res, err := tx.Exec(`INSERT INTO people (name) VALUES (?)`, alice.Name)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		alice.ID = id

		// Bob
		res, err = tx.Exec(`INSERT INTO people (name) VALUES (?)`, bob.Name)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return err
		}
		bob.ID = id

		// Done
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if alice.ID <= 0 {
		t.Fatal("expected Alice.ID > 0")
	}
	if bob.ID <= 0 {
		t.Fatal("expected Bob.ID > 0")
	}
	var count int64
	err = db.QueryRow(`SELECT COUNT(*) FROM people`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := int64(2), count; want != have {
		t.Fatalf("expected %d rows, got %d", want, have)
	}
}

func testRunInTxWithDeadlock(t *testing.T) {
	db, err := connect()
	if err == errSkipTest {
		t.SkipNow()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	alice := &Person{Name: "Alice"}
	bob := &Person{Name: "Bob"}
	var deadlocks int
	err = internal.RunInTx(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
		// Alice
		res, err := tx.Exec(`INSERT INTO people (name) VALUES (?)`, alice.Name)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		alice.ID = id

		// Bob
		res, err = tx.Exec(`INSERT INTO people (name) VALUES (?)`, bob.Name)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return err
		}
		bob.ID = id

		// Done
		deadlocks++
		if deadlocks < 3 {
			return &mysql.MySQLError{
				Number:  1213,
				Message: fmt.Sprintf("Deadlock found when trying to get lock; try restarting transaction (#%d)", deadlocks),
			}
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if want, have := "Error 1213: Deadlock found when trying to get lock; try restarting transaction (#1)", err.Error(); want != have {
		t.Fatalf("expected error %q, got %q", want, have)
	}
	if alice.ID == 0 {
		t.Fatalf("expected Alice.ID == 0, got %d", alice.ID)
	}
	if bob.ID == 0 {
		t.Fatalf("expected Bob.ID == 0, got %d", bob.ID)
	}
	var count int64
	err = db.QueryRow(`SELECT COUNT(*) FROM people`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := int64(0), count; want != have {
		t.Fatalf("expected %d rows, got %d", want, have)
	}
}

func testRunInTxErrorInFn(t *testing.T) {
	db, err := connect()
	if err == errSkipTest {
		t.SkipNow()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	alice := &Person{Name: "Alice"}
	bob := &Person{Name: "Bob"}
	err = internal.RunInTx(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
		// Alice
		res, err := tx.Exec(`INSERT INTO people (name) VALUES (?)`, alice.Name)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		alice.ID = id

		// Bob
		res, err = tx.Exec(`INSERT INTO people (name) VALUES (?)`, bob.Name)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return err
		}
		bob.ID = id

		// Kaboom
		return errors.New("kaboom")
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if want, have := "kaboom", err.Error(); want != have {
		t.Fatalf("expected error %q, got %q", want, have)
	}
	if alice.ID == 0 {
		t.Fatalf("expected Alice.ID == 0, got %d", alice.ID)
	}
	if bob.ID == 0 {
		t.Fatalf("expected Bob.ID == 0, got %d", bob.ID)
	}
	var count int64
	err = db.QueryRow(`SELECT COUNT(*) FROM people`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := int64(0), count; want != have {
		t.Fatalf("expected %d rows, got %d", want, have)
	}
}

func testRunInTxPanicInFn(t *testing.T) {
	db, err := connect()
	if err == errSkipTest {
		t.SkipNow()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	alice := &Person{Name: "Alice"}
	bob := &Person{Name: "Bob"}
	err = internal.RunInTx(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
		// Alice
		res, err := tx.Exec(`INSERT INTO people (name) VALUES (?)`, alice.Name)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		alice.ID = id

		// Bob
		res, err = tx.Exec(`INSERT INTO people (name) VALUES (?)`, bob.Name)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return err
		}
		bob.ID = id

		// Kaboom
		panic("kaboom")
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if want, have := "kaboom", err.Error(); want != have {
		t.Fatalf("expected error %q, got %q", want, have)
	}
	if alice.ID == 0 {
		t.Fatalf("expected Alice.ID == 0, got %d", alice.ID)
	}
	if bob.ID == 0 {
		t.Fatalf("expected Bob.ID == 0, got %d", bob.ID)
	}
	var count int64
	err = db.QueryRow(`SELECT COUNT(*) FROM people`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := int64(0), count; want != have {
		t.Fatalf("expected %d rows, got %d", want, have)
	}
}

func TestRunInTxWithRetry(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		testRunInTxWithRetryOK(t, newBackoff())
	})
	t.Run("DeadlockRetry", func(t *testing.T) {
		testRunInTxWithRetryAndDeadlock(t, newBackoff())
	})
	t.Run("ErrorInFn", func(t *testing.T) {
		testRunInTxWithRetryAndErrorInFn(t, newBackoff())
	})
	t.Run("PanicInFn", func(t *testing.T) {
		testRunInTxWithRetryAndPanicInFn(t, newBackoff())
	})
}

func testRunInTxWithRetryOK(t *testing.T, b backoff.BackOff) {
	db, err := connect()
	if err == errSkipTest {
		t.SkipNow()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	alice := &Person{Name: "Alice"}
	bob := &Person{Name: "Bob"}
	err = internal.RunInTxWithRetryBackoff(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
		// Alice
		res, err := tx.Exec(`INSERT INTO people (name) VALUES (?)`, alice.Name)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		alice.ID = id

		// Bob
		res, err = tx.Exec(`INSERT INTO people (name) VALUES (?)`, bob.Name)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return err
		}
		bob.ID = id

		// Done
		return nil
	}, nil, b)
	if err != nil {
		t.Fatal(err)
	}
	if alice.ID <= 0 {
		t.Fatal("expected Alice.ID > 0")
	}
	if bob.ID <= 0 {
		t.Fatal("expected Bob.ID > 0")
	}
	var count int64
	err = db.QueryRow(`SELECT COUNT(*) FROM people`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := int64(2), count; want != have {
		t.Fatalf("expected %d rows, got %d", want, have)
	}
}

func testRunInTxWithRetryAndErrorInFn(t *testing.T, b backoff.BackOff) {
	db, err := connect()
	if err == errSkipTest {
		t.SkipNow()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	alice := &Person{Name: "Alice"}
	bob := &Person{Name: "Bob"}
	err = internal.RunInTxWithRetryBackoff(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
		// Alice
		res, err := tx.Exec(`INSERT INTO people (name) VALUES (?)`, alice.Name)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		alice.ID = id

		// Bob
		res, err = tx.Exec(`INSERT INTO people (name) VALUES (?)`, bob.Name)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return err
		}
		bob.ID = id

		// Kaboom
		return errors.New("kaboom")
	}, nil, b)
	if err == nil {
		t.Fatal("expected an error")
	}
	if want, have := "kaboom", err.Error(); want != have {
		t.Fatalf("expected error %q, got %q", want, have)
	}
	if alice.ID == 0 {
		t.Fatalf("expected Alice.ID == 0, got %d", alice.ID)
	}
	if bob.ID == 0 {
		t.Fatalf("expected Bob.ID == 0, got %d", bob.ID)
	}
	var count int64
	err = db.QueryRow(`SELECT COUNT(*) FROM people`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := int64(0), count; want != have {
		t.Fatalf("expected %d rows, got %d", want, have)
	}
}

func testRunInTxWithRetryAndPanicInFn(t *testing.T, b backoff.BackOff) {
	db, err := connect()
	if err == errSkipTest {
		t.SkipNow()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	alice := &Person{Name: "Alice"}
	bob := &Person{Name: "Bob"}
	err = internal.RunInTxWithRetryBackoff(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
		// Alice
		res, err := tx.Exec(`INSERT INTO people (name) VALUES (?)`, alice.Name)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		alice.ID = id

		// Bob
		res, err = tx.Exec(`INSERT INTO people (name) VALUES (?)`, bob.Name)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return err
		}
		bob.ID = id

		// Kaboom
		panic("kaboom")
	}, nil, b)
	if err == nil {
		t.Fatal("expected an error")
	}
	if want, have := "kaboom", err.Error(); want != have {
		t.Fatalf("expected error %q, got %q", want, have)
	}
	if alice.ID == 0 {
		t.Fatalf("expected Alice.ID == 0, got %d", alice.ID)
	}
	if bob.ID == 0 {
		t.Fatalf("expected Bob.ID == 0, got %d", bob.ID)
	}
	var count int64
	err = db.QueryRow(`SELECT COUNT(*) FROM people`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := int64(0), count; want != have {
		t.Fatalf("expected %d rows, got %d", want, have)
	}
}

func testRunInTxWithRetryAndDeadlock(t *testing.T, b backoff.BackOff) {
	db, err := connect()
	if err == errSkipTest {
		t.SkipNow()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	alice := &Person{Name: "Alice"}
	bob := &Person{Name: "Bob"}
	var deadlocks int
	err = internal.RunInTxWithRetryBackoff(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
		// Alice
		res, err := tx.Exec(`INSERT INTO people (name) VALUES (?)`, alice.Name)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		alice.ID = id

		// Bob
		res, err = tx.Exec(`INSERT INTO people (name) VALUES (?)`, bob.Name)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return err
		}
		bob.ID = id

		// Done
		deadlocks++
		if deadlocks < 3 {
			return &mysql.MySQLError{
				Number:  1213,
				Message: fmt.Sprintf("Deadlock found when trying to get lock; try restarting transaction (#%d)", deadlocks),
			}
		}
		return nil
	}, nil, b)
	if err != nil {
		t.Fatal(err)
	}
	if alice.ID <= 0 {
		t.Fatal("expected Alice.ID > 0")
	}
	if bob.ID <= 0 {
		t.Fatal("expected Bob.ID > 0")
	}
	var count int64
	err = db.QueryRow(`SELECT COUNT(*) FROM people`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := int64(2), count; want != have {
		t.Fatalf("expected %d rows, got %d", want, have)
	}
}

func testRunInTxRetryable(t *testing.T, b backoff.BackOff) {
	db, err := connect()
	if err == errSkipTest {
		t.SkipNow()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var retries int
	errDoNotRetry := errors.New("no retry")
	err = internal.RunInTxWithRetryBackoff(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
		// After 3 tries, we'll pass errDoNotRetry, which should stop the loop
		retries++
		if retries == 3 {
			return errDoNotRetry
		}
		return errors.New("retry")
	}, func(err error) bool {
		return err != errDoNotRetry
	}, b)
	if err != errDoNotRetry {
		t.Fatalf("expected errDoNotRetry, got %v", err)
	}
	if want, have := 3, retries; want != have {
		t.Fatalf("expected %d retries, got %d", want, have)
	}
}
