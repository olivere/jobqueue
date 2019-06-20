package mysql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"

	"github.com/olivere/jobqueue"
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
)

// Store represents a persistent MySQL storage implementation.
// It implements the jobqueue.Store interface.
type Store struct {
	db    *gorm.DB
	debug bool
}

// StoreOption is an options provider for Store.
type StoreOption func(*Store)

// NewStore initializes a new MySQL-based storage.
func NewStore(url string, options ...StoreOption) (*Store, error) {
	st := &Store{}
	for _, opt := range options {
		opt(st)
	}
	cfg, err := mysqldriver.ParseDSN(url)
	if err != nil {
		return nil, err
	}
	dbname := cfg.DBName
	if dbname == "" {
		return nil, errors.New("no database specified")
	}
	// First connect without DB name
	cfg.DBName = ""
	setupdb, err := gorm.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	defer setupdb.Close()
	// Create database
	_, err = setupdb.DB().Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbname))
	if err != nil {
		return nil, err
	}

	// Now connect again, this time with the db name
	st.db, err = gorm.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	if st.debug {
		st.db = st.db.Debug()
	}

	// Create schema
	_, err = st.db.DB().Exec(mysqlSchema)
	if err != nil {
		return nil, err
	}

	// Apply update 001
	var count int64
	err = st.db.DB().QueryRow(`
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
		_, err = st.db.DB().Exec(mysqlUpdate001)
		if err != nil {
			return nil, err
		}
	}

	// Apply update 002
	err = st.db.DB().QueryRow(`
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
		_, err = st.db.DB().Exec(mysqlUpdate002)
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

/*
func SetCleaner(interval, expiry time.Duration) StoreOption {
	return func(s *Store) {
		s.interval = interval
		s.expiry = expiry
	}
}
*/

func (s *Store) wrapError(err error) error {
	if err == gorm.ErrRecordNotFound {
		// Map gorm.ErrRecordNotFound to jobqueue-specific "not found" error
		return jobqueue.ErrNotFound
	}
	return err
}

func (s *Store) runWithRetry(fn func() error) error {
	b := backoff.NewExponentialBackOff()
	b.MaxInterval = 5 * time.Second
	b.MaxElapsedTime = 15 * time.Second
	return backoff.Retry(fn, b)
}

// Start is called when the manager starts up.
// We ensure that stale jobs are marked as failed so that we have place
// for new jobs.
func (s *Store) Start() error {
	// TODO This will fail if we have two or more job queues working on the same database!
	err := s.db.Model(&Job{}).
		Where("state = ?", jobqueue.Working).
		Updates(map[string]interface{}{
			"state":     jobqueue.Failed,
			"completed": time.Now().UnixNano(),
		}).
		Error
	return s.wrapError(err)
}

// Create adds a new job to the store.
func (s *Store) Create(job *jobqueue.Job) error {
	j, err := newJob(job)
	if err != nil {
		return err
	}
	j.LastMod = j.Created
	err = s.runWithRetry(func() error {
		return s.db.Create(j).Error
	})
	return s.wrapError(err)
}

// Update updates the job in the store.
func (s *Store) Update(job *jobqueue.Job) error {
	j, err := newJob(job)
	if err != nil {
		return err
	}
	return s.runWithRetry(func() error {
		tx := s.db.Begin()
		var ids []string
		err = tx.Raw("SELECT id FROM jobqueue_jobs WHERE id = ? AND last_mod = ? FOR UPDATE", job.ID, job.Updated).
			Scan(&ids).
			Error
		if err != nil {
			tx.Rollback()
			return s.wrapError(err)
		}
		j.LastMod = time.Now().UnixNano()
		if err := tx.Save(&j).Error; err != nil {
			tx.Rollback()
			return s.wrapError(err)
		}
		if err := tx.Commit().Error; err != nil {
			return s.wrapError(err)
		}
		job.Updated = j.LastMod
		return nil
	})
}

// Next picks the next job to execute, or nil if no executable job is available.
func (s *Store) Next() (*jobqueue.Job, error) {
	var j Job
	err := s.db.Where("state = ?", jobqueue.Waiting).
		Order("rank desc, priority desc").
		First(&j).
		Error
	if err == gorm.ErrRecordNotFound {
		return nil, jobqueue.ErrNotFound
	}
	if err != nil {
		return nil, s.wrapError(err)
	}
	return j.ToJob()
}

// Delete removes a job from the store.
func (s *Store) Delete(job *jobqueue.Job) error {
	return s.runWithRetry(func() error {
		err := s.db.Where("id = ?", job.ID).Delete(&Job{}).Error
		return s.wrapError(err)
	})
}

// Lookup retrieves a single job in the store by its identifier.
func (s *Store) Lookup(id string) (*jobqueue.Job, error) {
	var j Job
	err := s.db.Where("id = ?", id).First(&j).Error
	if err != nil {
		return nil, s.wrapError(err)
	}
	job, err := j.ToJob()
	if err != nil {
		return nil, s.wrapError(err)
	}
	return job, nil
}

// LookupByCorrelationID returns the details of jobs by their correlation identifier.
// If no such job could be found, an empty array is returned.
func (s *Store) LookupByCorrelationID(correlationID string) ([]*jobqueue.Job, error) {
	var jobs []Job
	err := s.db.Where("correlation_id = ?", correlationID).Find(&jobs).Error
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
func (s *Store) List(request *jobqueue.ListRequest) (*jobqueue.ListResponse, error) {
	rsp := &jobqueue.ListResponse{}

	// Count
	qry := s.db.Model(&Job{})
	if request.Topic != "" {
		qry = qry.Where("topic = ?", request.Topic)
	}
	if request.State != "" {
		qry = qry.Where("state = ?", request.State)
	}
	if request.CorrelationGroup != "" {
		qry = qry.Where("correlation_group = ?", request.CorrelationGroup)
	}
	if request.CorrelationID != "" {
		qry = qry.Where("correlation_id = ?", request.CorrelationID)
	}
	err := qry.Count(&rsp.Total).Error
	if err != nil {
		return nil, s.wrapError(err)
	}

	// Find
	qry = s.db.Order("last_mod desc").
		Offset(request.Offset).
		Limit(request.Limit)
	if request.Topic != "" {
		qry = qry.Where("topic = ?", request.Topic)
	}
	if request.State != "" {
		qry = qry.Where("state = ?", request.State)
	}
	if request.CorrelationGroup != "" {
		qry = qry.Where("correlation_group = ?", request.CorrelationGroup)
	}
	if request.CorrelationID != "" {
		qry = qry.Where("correlation_id = ?", request.CorrelationID)
	}
	var list []*Job
	err = qry.Find(&list).Error
	if err != nil {
		return nil, s.wrapError(err)
	}
	for _, j := range list {
		job, err := j.ToJob()
		if err != nil {
			return nil, s.wrapError(err)
		}
		rsp.Jobs = append(rsp.Jobs, job)
	}
	return rsp, nil
}

// Stats returns statistics about the jobs in the store.
func (s *Store) Stats(req *jobqueue.StatsRequest) (*jobqueue.Stats, error) {
	stats := new(jobqueue.Stats)
	buildFilter := func(state string) *gorm.DB {
		f := s.db.Model(&Job{}).Where("state = ?", state)
		if req.Topic != "" {
			f = f.Where("topic = ?", req.Topic)
		}
		if req.CorrelationGroup != "" {
			f = f.Where("correlation_group = ?", req.CorrelationGroup)
		}
		return f
	}
	err := buildFilter(jobqueue.Waiting).Count(&stats.Waiting).Error
	if err != nil {
		return nil, s.wrapError(err)
	}
	err = buildFilter(jobqueue.Working).Count(&stats.Working).Error
	if err != nil {
		return nil, s.wrapError(err)
	}
	err = buildFilter(jobqueue.Succeeded).Count(&stats.Succeeded).Error
	if err != nil {
		return nil, s.wrapError(err)
	}
	err = buildFilter(jobqueue.Failed).Count(&stats.Failed).Error
	if err != nil {
		return nil, s.wrapError(err)
	}
	return stats, nil
}

// -- MySQL-internal representation of a task --

type Job struct {
	ID               string `gorm:"primary_key"`
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

func (Job) TableName() string {
	return "jobqueue_jobs"
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
