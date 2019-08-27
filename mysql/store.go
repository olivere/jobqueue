package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"

	"github.com/olivere/jobqueue"
	"github.com/olivere/jobqueue/mysql/internal"
)

const (
	mysqlSchema = `CREATE TABLE IF NOT EXISTS jobqueue_jobs (
id varchar(36) primary key,
topic varchar(255),
state varchar(30),
args text,
priority bigint,
retry integer,
max_retry integer,
correlation_id varchar(255),
created bigint,
started bigint,
completed bigint,
last_mod bigint,
index ix_jobs_topic (topic),
index ix_jobs_state (state),
index ix_jobs_priority (priority),
index ix_jobs_correlation_id (correlation_id),
index ix_jobs_created (created),
index ix_jobs_started (started),
index ix_jobs_completed (completed),
index ix_jobs_last_mod (last_mod));`

	// add rank column and index on (rank, priority)
	mysqlUpdate001 = `ALTER TABLE jobqueue_jobs ADD rank INT NOT NULL DEFAULT '0', ADD INDEX ix_jobs_rank_priority (rank, priority);`

	// add correlation_group column and index on (correlation_group, correlation_id)
	mysqlUpdate002 = `ALTER TABLE jobqueue_jobs ADD correlation_group varchar(255), ADD INDEX ix_jobs_correlation_group_and_id (correlation_group, correlation_id);`

	// add index on state and correlation_group and id
	mysqlUpdate003 = `ALTER TABLE jobqueue_jobs ADD INDEX ix_jobs_state_correlation_group_and_id (state, correlation_group, id);`

	// change args from text to mediumtext
	mysqlUpdate004 = `ALTER TABLE jobqueue_jobs CHANGE COLUMN args args MEDIUMTEXT;`
)

// Store represents a persistent MySQL storage implementation.
// It implements the jobqueue.Store interface.
type Store struct {
	db    *sql.DB
	debug bool

	stmtOnce           sync.Once
	createStmt         *sql.Stmt
	updateStmt         *sql.Stmt
	deleteStmt         *sql.Stmt
	nextStmt           *sql.Stmt
	lookupStmt         *sql.Stmt
	lookupByCorrIDStmt *sql.Stmt
}

// StoreOption is an options provider for Store.
type StoreOption func(*Store)

// NewStore initializes a new MySQL-based storage.
func NewStore(url string, options ...StoreOption) (*Store, error) {
	st := &Store{}
	for _, opt := range options {
		opt(st)
	}
	cfg, err := mysql.ParseDSN(url)
	if err != nil {
		return nil, err
	}
	dbname := cfg.DBName
	if dbname == "" {
		return nil, errors.New("no database specified")
	}
	// First connect without DB name
	cfg.DBName = ""
	initdb, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	defer initdb.Close()
	// Create database
	_, err = initdb.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbname))
	if err != nil {
		return nil, err
	}

	// Now connect again, this time with the db name
	st.db, err = sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}

	// Create schema
	_, err = st.db.Exec(mysqlSchema)
	if err != nil {
		return nil, err
	}

	// Apply update 001
	var count int64
	err = st.db.QueryRow(`
	SELECT COUNT(*) AS cnt
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME = 'jobqueue_jobs'
		AND COLUMN_NAME = 'rank'
	`, dbname).Scan(&count)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		// Apply migration
		_, err = st.db.Exec(mysqlUpdate001)
		if err != nil {
			return nil, err
		}
	}

	// Apply update 002
	err = st.db.QueryRow(`
		SELECT COUNT(*) AS cnt
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = 'jobqueue_jobs'
			AND COLUMN_NAME = 'correlation_group'
		`, dbname).Scan(&count)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		// Apply migration
		_, err = st.db.Exec(mysqlUpdate002)
		if err != nil {
			return nil, err
		}
	}

	// Apply update 003
	err = st.db.QueryRow(`
		SELECT COUNT(*) AS cnt
			FROM information_schema.STATISTICS
			WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = 'jobqueue_jobs'
			AND INDEX_NAME = 'ix_jobs_state_correlation_group_and_id'
		`, dbname).Scan(&count)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		// Apply migration
		_, err = st.db.Exec(mysqlUpdate003)
		if err != nil {
			return nil, err
		}
	}

	// Apply update 004
	err = st.db.QueryRow(`
		SELECT COUNT(*) AS cnt
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = 'jobqueue_jobs'
			AND COLUMN_NAME = 'args'
			AND DATA_TYPE = 'text'
		`, dbname).Scan(&count)
	if err != nil {
		return nil, err
	}
	if count == 1 {
		// Apply migration
		_, err = st.db.Exec(mysqlUpdate004)
		if err != nil {
			return nil, err
		}
	}

	return st, nil
}

// SetDebug indicates whether to enable or disable debugging (which will
// output SQL to the console).
func SetDebug(enabled bool) StoreOption {
	return func(s *Store) {
		s.debug = enabled
	}
}

func (s *Store) initStmt() {
	var err error

	// Create statement
	s.createStmt, err = s.db.Prepare("INSERT INTO jobqueue_jobs (id,topic,state,args,rank,priority,retry,max_retry,correlation_group,correlation_id,created,started,completed,last_mod) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		panic(err)
	}

	// Update statement
	s.updateStmt, err = s.db.Prepare("UPDATE jobqueue_jobs SET topic=?,state=?,args=?,rank=?,priority=?,retry=?,max_retry=?,correlation_group=?,correlation_id=?,created=?,started=?,completed=?,last_mod=? WHERE id=?")
	if err != nil {
		panic(err)
	}

	// Delete statement
	s.deleteStmt, err = s.db.Prepare("DELETE FROM jobqueue_jobs WHERE id=?")
	if err != nil {
		panic(err)
	}

	// Next statement
	s.nextStmt, err = s.db.Prepare("SELECT id,topic,state,args,rank,priority,retry,max_retry,correlation_group,correlation_id,created,started,completed,last_mod FROM jobqueue_jobs WHERE state=? ORDER BY rank DESC, priority DESC LIMIT 1")
	if err != nil {
		panic(err)
	}

	// Lookup (by id) statement
	s.lookupStmt, err = s.db.Prepare("SELECT id,topic,state,args,rank,priority,retry,max_retry,correlation_group,correlation_id,created,started,completed,last_mod FROM jobqueue_jobs WHERE id=? LIMIT 1")
	if err != nil {
		panic(err)
	}

	// Lookup by correlation id
	s.lookupByCorrIDStmt, err = s.db.Prepare("SELECT id,topic,state,args,rank,priority,retry,max_retry,correlation_group,correlation_id,created,started,completed,last_mod FROM jobqueue_jobs WHERE correlation_id=? LIMIT 1")
	if err != nil {
		panic(err)
	}
}

func (s *Store) wrapError(err error) error {
	if internal.IsNotFound(err) {
		// Map specific errors to jobqueue-specific "not found" error
		return jobqueue.ErrNotFound
	}
	return err
}

// Start is called when the manager starts up.
// We ensure that stale jobs are marked as failed so that we have place
// for new jobs.
func (s *Store) Start() error {
	s.stmtOnce.Do(s.initStmt)

	ctx := context.Background()
	err := internal.RunInTxWithRetry(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(
			ctx,
			`UPDATE jobqueue_jobs SET state = ?, completed = ? WHERE state = ?`,
			jobqueue.Failed,
			time.Now().UnixNano(),
			jobqueue.Working,
		)
		if err != nil {
			return err
		}
		return nil
	}, func(err error) bool {
		return internal.IsDeadlock(err)
	})
	if err != nil {
		return s.wrapError(err)
	}
	return nil
}

// Create adds a new job to the store.
func (s *Store) Create(ctx context.Context, job *jobqueue.Job) error {
	s.stmtOnce.Do(s.initStmt)

	j, err := newJob(job)
	if err != nil {
		return err
	}
	j.LastMod = j.Created

	err = internal.RunInTxWithRetry(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		res, err := tx.Stmt(s.createStmt).ExecContext(
			ctx,
			j.ID,
			j.Topic,
			j.State,
			j.Args,
			j.Rank,
			j.Priority,
			j.Retry,
			j.MaxRetry,
			j.CorrelationGroup,
			j.CorrelationID,
			j.Created,
			j.Started,
			j.Completed,
			j.LastMod,
		)
		if err != nil {
			return err
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected != 1 {
			return err
		}
		return nil
	}, func(err error) bool {
		return internal.IsDeadlock(err)
	})
	return s.wrapError(err)
}

// Update updates the job in the store.
func (s *Store) Update(ctx context.Context, job *jobqueue.Job) error {
	s.stmtOnce.Do(s.initStmt)

	j, err := newJob(job)
	if err != nil {
		return err
	}

	err = internal.RunInTxWithRetry(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		var id string
		err := tx.QueryRowContext(
			ctx,
			`SELECT id FROM jobqueue_jobs WHERE id = ? AND last_mod = ? FOR UPDATE`,
			job.ID,
			job.Updated,
		).Scan(&id)
		if err != nil {
			return err
		}
		j.LastMod = time.Now().UnixNano()
		res, err := tx.Stmt(s.updateStmt).ExecContext(
			ctx,
			j.Topic,
			j.State,
			j.Args,
			j.Rank,
			j.Priority,
			j.Retry,
			j.MaxRetry,
			j.CorrelationGroup,
			j.CorrelationID,
			j.Created,
			j.Started,
			j.Completed,
			j.LastMod,
			j.ID,
		)
		if err != nil {
			return err
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected != 1 {
			return err
		}
		job.Updated = j.LastMod
		return nil
	}, func(err error) bool {
		return internal.IsDeadlock(err)
	})
	return s.wrapError(err)
}

// Next picks the next job to execute, or nil if no executable job is available.
func (s *Store) Next() (*jobqueue.Job, error) {
	s.stmtOnce.Do(s.initStmt)

	var j Job
	ctx := context.Background()
	err := internal.RunWithRetry(ctx, s.db, func(ctx context.Context) error {
		err := s.nextStmt.QueryRowContext(ctx, jobqueue.Waiting).Scan(
			&j.ID,
			&j.Topic,
			&j.State,
			&j.Args,
			&j.Rank,
			&j.Priority,
			&j.Retry,
			&j.MaxRetry,
			&j.CorrelationGroup,
			&j.CorrelationID,
			&j.Created,
			&j.Started,
			&j.Completed,
			&j.LastMod,
		)
		if err != nil {
			return err
		}
		return nil
	}, func(err error) bool {
		return internal.IsDeadlock(err)
	})
	if internal.IsNotFound(err) {
		return nil, jobqueue.ErrNotFound
	}
	if err != nil {
		return nil, s.wrapError(err)
	}
	return j.ToJob()
}

// Delete removes a job from the store.
func (s *Store) Delete(ctx context.Context, job *jobqueue.Job) error {
	s.stmtOnce.Do(s.initStmt)

	err := internal.RunInTxWithRetry(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.Stmt(s.deleteStmt).ExecContext(ctx, job.ID)
		switch {
		case err == sql.ErrNoRows:
			return nil
		default:
			return err
		}
	}, func(err error) bool {
		return internal.IsDeadlock(err)
	})
	return s.wrapError(err)
}

// Lookup retrieves a single job in the store by its identifier.
func (s *Store) Lookup(ctx context.Context, id string) (*jobqueue.Job, error) {
	s.stmtOnce.Do(s.initStmt)

	var j Job
	err := internal.RunWithRetry(ctx, s.db, func(ctx context.Context) error {
		err := s.lookupStmt.QueryRowContext(ctx, id).Scan(
			&j.ID,
			&j.Topic,
			&j.State,
			&j.Args,
			&j.Rank,
			&j.Priority,
			&j.Retry,
			&j.MaxRetry,
			&j.CorrelationGroup,
			&j.CorrelationID,
			&j.Created,
			&j.Started,
			&j.Completed,
			&j.LastMod,
		)
		if err != nil {
			return err
		}
		return nil

	}, func(err error) bool {
		return internal.IsDeadlock(err)
	})
	if err != nil {
		return nil, s.wrapError(err)
	}
	return j.ToJob()
}

// LookupByCorrelationID returns the details of jobs by their correlation identifier.
// If no such job could be found, an empty array is returned.
func (s *Store) LookupByCorrelationID(ctx context.Context, correlationID string) ([]*jobqueue.Job, error) {
	s.stmtOnce.Do(s.initStmt)

	var jobs []Job
	err := internal.RunWithRetry(ctx, s.db, func(ctx context.Context) error {
		rows, err := s.lookupByCorrIDStmt.QueryContext(ctx, correlationID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var j Job
			err := rows.Scan(
				&j.ID,
				&j.Topic,
				&j.State,
				&j.Args,
				&j.Rank,
				&j.Priority,
				&j.Retry,
				&j.MaxRetry,
				&j.CorrelationGroup,
				&j.CorrelationID,
				&j.Created,
				&j.Started,
				&j.Completed,
				&j.LastMod,
			)
			if err != nil {
				return err
			}
			jobs = append(jobs, j)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return nil

	}, func(err error) bool {
		return internal.IsDeadlock(err)
	})
	if err != nil {
		return nil, s.wrapError(err)
	}
	result := make([]*jobqueue.Job, len(jobs))
	for i, j := range jobs {
		job, err := j.ToJob()
		if err != nil {
			return nil, s.wrapError(err)
		}
		result[i] = job
	}
	return result, nil
}

// List returns a list of all jobs stored in the data store.
func (s *Store) List(ctx context.Context, request *jobqueue.ListRequest) (*jobqueue.ListResponse, error) {
	s.stmtOnce.Do(s.initStmt)

	resp := &jobqueue.ListResponse{}

	columns := "id,topic,state,args,rank,priority,retry,max_retry,correlation_group,correlation_id,created,started,completed,last_mod"
	where := make(map[string]interface{})
	countBuilder := sq.Select("COUNT(*)").From("jobqueue_jobs")
	queryBuilder := sq.Select(columns).From("jobqueue_jobs")

	// Filters
	if v := request.Topic; v != "" {
		where["topic"] = v
	}
	if v := request.State; v != "" {
		where["state"] = v
	}
	if v := request.CorrelationGroup; v != "" {
		where["correlation_group"] = v
	}
	if v := request.CorrelationID; v != "" {
		where["correlation_id"] = v
	}

	// Count
	countBuilder = sq.Select("COUNT(*)").From("jobqueue_jobs").Where(where)
	{
		sql, args, err := countBuilder.ToSql()
		if err != nil {
			return nil, s.wrapError(err)
		}
		err = s.db.QueryRowContext(ctx, sql, args...).Scan(
			&resp.Total,
		)
		if err != nil {
			return nil, s.wrapError(err)
		}
	}

	// Iterate
	queryBuilder = sq.Select(columns).
		From("jobqueue_jobs").
		Where(where).
		OrderBy("last_mod DESC").
		Offset(uint64(request.Offset)).Limit(uint64(request.Limit))
	{
		sql, args, err := queryBuilder.ToSql()
		if err != nil {
			return nil, s.wrapError(err)
		}
		rows, err := s.db.QueryContext(ctx, sql, args...)
		if err != nil {
			return nil, s.wrapError(err)
		}
		defer rows.Close()
		for rows.Next() {
			var j Job
			err := rows.Scan(
				&j.ID,
				&j.Topic,
				&j.State,
				&j.Args,
				&j.Rank,
				&j.Priority,
				&j.Retry,
				&j.MaxRetry,
				&j.CorrelationGroup,
				&j.CorrelationID,
				&j.Created,
				&j.Started,
				&j.Completed,
				&j.LastMod,
			)
			if err != nil {
				return nil, s.wrapError(err)
			}
			job, err := j.ToJob()
			if err != nil {
				return nil, s.wrapError(err)
			}
			resp.Jobs = append(resp.Jobs, job)
		}
		if err := rows.Err(); err != nil {
			return nil, s.wrapError(err)
		}
	}

	return resp, nil
}

// Stats returns statistics about the jobs in the store.
func (s *Store) Stats(ctx context.Context, req *jobqueue.StatsRequest) (*jobqueue.Stats, error) {
	s.stmtOnce.Do(s.initStmt)

	stats := new(jobqueue.Stats)
	g, ctx := errgroup.WithContext(ctx)

	// Waiting
	g.Go(func() error {
		where := map[string]interface{}{
			"state": jobqueue.Waiting,
		}
		if v := req.Topic; v != "" {
			where["topic"] = v
		}
		if v := req.CorrelationGroup; v != "" {
			where["correlation_group"] = v
		}
		sql, args, err := sq.Select("COUNT(*)").From("jobqueue_jobs").Where(where).ToSql()
		if err != nil {
			return err
		}
		return s.db.QueryRowContext(ctx, sql, args...).Scan(&stats.Waiting)
	})

	// Working
	g.Go(func() error {
		where := map[string]interface{}{
			"state": jobqueue.Working,
		}
		if v := req.Topic; v != "" {
			where["topic"] = v
		}
		if v := req.CorrelationGroup; v != "" {
			where["correlation_group"] = v
		}
		sql, args, err := sq.Select("COUNT(*)").From("jobqueue_jobs").Where(where).ToSql()
		if err != nil {
			return err
		}
		return s.db.QueryRowContext(ctx, sql, args...).Scan(&stats.Working)
	})

	// Succeeded
	g.Go(func() error {
		where := map[string]interface{}{
			"state": jobqueue.Succeeded,
		}
		if v := req.Topic; v != "" {
			where["topic"] = v
		}
		if v := req.CorrelationGroup; v != "" {
			where["correlation_group"] = v
		}
		sql, args, err := sq.Select("COUNT(*)").From("jobqueue_jobs").Where(where).ToSql()
		if err != nil {
			return err
		}
		return s.db.QueryRowContext(ctx, sql, args...).Scan(&stats.Succeeded)
	})

	// Failed
	g.Go(func() error {
		where := map[string]interface{}{
			"state": jobqueue.Failed,
		}
		if v := req.Topic; v != "" {
			where["topic"] = v
		}
		if v := req.CorrelationGroup; v != "" {
			where["correlation_group"] = v
		}
		sql, args, err := sq.Select("COUNT(*)").From("jobqueue_jobs").Where(where).ToSql()
		if err != nil {
			return err
		}
		return s.db.QueryRowContext(ctx, sql, args...).Scan(&stats.Failed)
	})

	if err := g.Wait(); err != nil {
		return nil, s.wrapError(err)
	}
	return stats, nil
}

// -- MySQL-internal representation of a task --

type Job struct {
	ID               string
	Topic            string
	State            string
	Args             sql.NullString
	Rank             int
	Priority         int64
	Retry            int
	MaxRetry         int
	CorrelationGroup sql.NullString
	CorrelationID    sql.NullString
	Created          int64
	Started          int64
	Completed        int64
	LastMod          int64
}

func newJob(job *jobqueue.Job) (*Job, error) {
	var args string
	if job.Args != nil {
		v, err := json.Marshal(job.Args)
		if err != nil {
			return nil, err
		}
		args = string(v)
	}
	return &Job{
		ID:               job.ID,
		Topic:            job.Topic,
		State:            job.State,
		Args:             sql.NullString{String: args, Valid: args != ""},
		Rank:             job.Rank,
		Priority:         job.Priority,
		Retry:            job.Retry,
		MaxRetry:         job.MaxRetry,
		CorrelationGroup: sql.NullString{String: job.CorrelationGroup, Valid: job.CorrelationGroup != ""},
		CorrelationID:    sql.NullString{String: job.CorrelationID, Valid: job.CorrelationID != ""},
		Created:          job.Created,
		LastMod:          job.Updated,
		Started:          job.Started,
		Completed:        job.Completed,
	}, nil
}

func (j *Job) ToJob() (*jobqueue.Job, error) {
	var args []interface{}
	if j.Args.Valid && j.Args.String != "" {
		if err := json.Unmarshal([]byte(j.Args.String), &args); err != nil {
			return nil, err
		}
	}
	job := &jobqueue.Job{
		ID:               j.ID,
		Topic:            j.Topic,
		State:            j.State,
		Args:             args,
		Rank:             j.Rank,
		Priority:         j.Priority,
		Retry:            j.Retry,
		MaxRetry:         j.MaxRetry,
		CorrelationGroup: j.CorrelationGroup.String,
		CorrelationID:    j.CorrelationID.String,
		Created:          j.Created,
		Started:          j.Started,
		Updated:          j.LastMod,
		Completed:        j.Completed,
	}
	return job, nil
}
