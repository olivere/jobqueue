package internal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
)

// Run runs fn with the given database connection.
// Run recovers from panics, e.g. in fn.
func Run(ctx context.Context, db *sql.DB, fn func(context.Context) error) (err error) {
	defer func() {
		if rerr := recover(); rerr != nil {
			err = fmt.Errorf("%v", rerr)
		}
	}()
	return fn(ctx)
}

func RunWithRetry(ctx context.Context, db *sql.DB, fn func(context.Context) error, retryable func(error) bool) (err error) {
	return RunWithRetryBackoff(
		ctx,
		db,
		fn,
		retryable,
		backoff.NewExponentialBackOff(), // use defaults
	)
}

// RunWithRetryBackoff is like RunWithRetry but with configurable backoff.
func RunWithRetryBackoff(ctx context.Context, db *sql.DB, fn func(context.Context) error, retryable func(error) bool, b backoff.BackOff) (err error) {
	b.Reset()
	for {
		if err = Run(ctx, db, fn); err == nil {
			return nil
		}
		if retryable != nil && !retryable(err) {
			return err
		}
		delay := b.NextBackOff()
		if delay == backoff.Stop {
			return err
		}
		time.Sleep(delay)
	}
}

// RunInTx runs fn in a database transaction.
// The context ctx is passed to fn, as well as the newly created
// transaction. If fn fails, it is repeated several times before
// giving up, with exponential backoff.
//
// There are a few rules that fn must respect:
//
// 1. fn must use the passed tx reference for all database calls.
// 2. fn must not commit or rollback the transaction: Run will do that.
// 3. fn must be idempotent, i.e. it may be called several times
//    without side effects.
//
// If fn returns nil, RunInTx commits the transaction, returning
// the Commit and a nil error if it succeeds.
//
// If fn returns a non-nil value, RunInTx rolls back the
// transaction and will return the reported error from fn.
//
// RunInTx also recovers from panics, e.g. in fn.
func RunInTx(ctx context.Context, db *sql.DB, fn func(context.Context, *sql.Tx) error) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if rerr := recover(); rerr != nil {
			err = fmt.Errorf("%v", rerr)
			_ = tx.Rollback()
		}
	}()
	if err = fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	return err
}

// RunInTxWithRetry is like RunInTx but will retry
// several times with exponential backoff. In that case, fn must also
// be idempotent, i.e. it may be called several times without side effects.
func RunInTxWithRetry(ctx context.Context, db *sql.DB, fn func(context.Context, *sql.Tx) error, retryable func(error) bool) (err error) {
	return RunInTxWithRetryBackoff(
		ctx,
		db,
		fn,
		retryable,
		backoff.NewExponentialBackOff(), // use defaults
	)
}

// RunInTxWithRetryBackoff is like RunInTxWithRetry but with configurable
// backoff.
func RunInTxWithRetryBackoff(ctx context.Context, db *sql.DB, fn func(context.Context, *sql.Tx) error, retryable func(error) bool, b backoff.BackOff) (err error) {
	b.Reset()
	for {
		if err = RunInTx(ctx, db, fn); err == nil {
			return nil
		}
		if retryable != nil && !retryable(err) {
			return err
		}
		delay := b.NextBackOff()
		if delay == backoff.Stop {
			return err
		}
		time.Sleep(delay)
	}
}
