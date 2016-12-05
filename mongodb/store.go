package mongodb

import (
	"encoding/json"
	"errors"
	"net/url"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/olivere/jobqueue"
)

const (
	// socketTimeout should be long enough that even a slow mongo server
	// will respond in that length of time. Since mongo servers ping themselves
	// every 10 seconds, we use a value just over 2 ping periods to allow
	// for delayed pings due to issues such as CPU starvation etc.
	socketTimeout = 21 * time.Second

	// dialTimeout should be representative of the upper bound of the
	// time taken to dial a mongo server from within the same cloud/private
	// network.
	dialTimeout = 30 * time.Second

	// defaultCollectionName is the name of the collection in MongoDB.
	// It can be overridden by SetCollectionName.
	defaultCollectionName = "jobqueue_jobs"
)

// Store represents a MongoDB-based storage backend.
type Store struct {
	session        *mgo.Session
	db             *mgo.Database
	coll           *mgo.Collection
	collectionName string
}

// StoreOption is an options provider for Store.
type StoreOption func(*Store)

// NewStore creates a new MongoDB-based storage backend.
func NewStore(mongodbURL string, options ...StoreOption) (*Store, error) {
	st := &Store{
		collectionName: defaultCollectionName,
	}
	for _, opt := range options {
		opt(st)
	}

	uri, err := url.Parse(mongodbURL)
	if err != nil {
		return nil, err
	}
	if uri.Path == "" || uri.Path == "/" {
		return nil, errors.New("mongodb: database missing in URL")
	}
	dbname := uri.Path[1:]

	st.session, err = mgo.DialWithTimeout(mongodbURL, dialTimeout)
	if err != nil {
		return nil, err
	}

	st.session.SetMode(mgo.Monotonic, true)
	st.session.SetSocketTimeout(socketTimeout)

	// Create collection if it does not exist
	st.db = st.session.DB(dbname)
	st.coll = st.db.C(st.collectionName)

	// Create indices
	err = st.coll.EnsureIndexKey("state")
	if err != nil {
		return nil, err
	}
	err = st.coll.EnsureIndexKey("-rank", "-priority")
	if err != nil {
		return nil, err
	}
	err = st.coll.EnsureIndexKey("-last_mod")
	if err != nil {
		return nil, err
	}
	err = st.coll.EnsureIndexKey("correlation_id")
	if err != nil {
		return nil, err
	}
	err = st.coll.EnsureIndexKey("correlation_group", "correlation_id")
	if err != nil {
		return nil, err
	}

	return st, nil
}

// Close the MongoDB store.
func (s *Store) Close() error {
	s.session.Close()
	return nil
}

// SetCollectionName overrides the default collection name.
func SetCollectionName(collectionName string) StoreOption {
	return func(s *Store) {
		s.collectionName = collectionName
	}
}

func (s *Store) wrapError(err error) error {
	if err == mgo.ErrNotFound {
		// Map gorm.ErrRecordNotFound to jobqueue-specific "not found" error
		return jobqueue.ErrNotFound
	}
	return err
}

// Start is called when the manager starts up.
// We ensure that stale jobs are marked as failed so that we have place
// for new jobs.
func (s *Store) Start() error {
	// TODO This will fail if we have two or more job queues working on the same database!
	change := bson.M{"$set": bson.M{"state": jobqueue.Failed, "completed": time.Now().UnixNano()}}
	_, err := s.coll.UpdateAll(
		bson.M{"state": jobqueue.Working},
		change,
	)
	return s.wrapError(err)
}

// Create adds a new job to the store.
func (s *Store) Create(job *jobqueue.Job) error {
	j, err := newJob(job)
	if err != nil {
		return err
	}
	j.LastMod = j.Created
	return s.wrapError(s.coll.Insert(j))
}

// Update updates the job in the store.
func (s *Store) Update(job *jobqueue.Job) error {
	j, err := newJob(job)
	if err != nil {
		return err
	}
	j.LastMod = time.Now().UnixNano()
	return s.wrapError(s.coll.UpdateId(j.ID, j))
}

// Next picks the next job to execute, or nil if no executable job is available.
func (s *Store) Next() (*jobqueue.Job, error) {
	var j Job
	err := s.coll.Find(bson.M{"state": jobqueue.Waiting}).Sort("-rank", "-priority").One(&j)
	if err != nil {
		return nil, s.wrapError(err)
	}
	return j.ToJob()
}

// Delete removes a job from the store.
func (s *Store) Delete(job *jobqueue.Job) error {
	return s.wrapError(s.coll.RemoveId(job.ID))
}

// Lookup retrieves a single job in the store by its identifier.
func (s *Store) Lookup(id string) (*jobqueue.Job, error) {
	var j Job
	err := s.coll.FindId(id).One(&j)
	if err != nil {
		return nil, s.wrapError(err)
	}
	job, err := j.ToJob()
	if err != nil {
		return nil, s.wrapError(err)
	}
	return job, nil
}

// LookupByCorrelationID retrieves a single job in the store by its correlation identifier.
func (s *Store) LookupByCorrelationID(correlationID string) (*jobqueue.Job, error) {
	var j Job
	err := s.coll.Find(bson.M{"correlation_id": correlationID}).One(&j)
	if err != nil {
		return nil, s.wrapError(err)
	}
	job, err := j.ToJob()
	if err != nil {
		return nil, s.wrapError(err)
	}
	return job, nil
}

// List returns a list of all jobs stored in the data store.
func (s *Store) List(request *jobqueue.ListRequest) (*jobqueue.ListResponse, error) {
	rsp := &jobqueue.ListResponse{}

	// Common filters for both Count and Find
	query := bson.M{}
	if request.State != "" {
		query["state"] = request.State
	}
	if request.CorrelationGroup != "" {
		query["correlation_group"] = request.CorrelationGroup
	}

	// Count
	count, err := s.coll.Find(query).Count()
	if err != nil {
		return nil, s.wrapError(err)
	}
	rsp.Total = count

	// Find
	var list []*Job
	err = s.coll.Find(query).Sort("-last_mod").Skip(request.Offset).Limit(request.Limit).All(&list)
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
func (s *Store) Stats() (*jobqueue.Stats, error) {
	waiting, err := s.coll.Find(bson.M{"state": jobqueue.Waiting}).Count()
	if err != nil {
		return nil, s.wrapError(err)
	}
	working, err := s.coll.Find(bson.M{"state": jobqueue.Working}).Count()
	if err != nil {
		return nil, s.wrapError(err)
	}
	succeeded, err := s.coll.Find(bson.M{"state": jobqueue.Succeeded}).Count()
	if err != nil {
		return nil, s.wrapError(err)
	}
	failed, err := s.coll.Find(bson.M{"state": jobqueue.Failed}).Count()
	if err != nil {
		return nil, s.wrapError(err)
	}
	return &jobqueue.Stats{
		Waiting:   waiting,
		Working:   working,
		Succeeded: succeeded,
		Failed:    failed,
	}, nil
}

// -- MongoDB-internal representation of a task --

type Job struct {
	ID               string `bson:"_id"`
	Topic            string
	State            string
	Args             *string
	Rank             int
	Priority         int64
	Retry            int
	MaxRetry         int    `bson:"max_retry"`
	CorrelationGroup string `bson:"correlation_group"`
	CorrelationID    string `bson:"correlation_id"`
	Created          int64
	Started          int64
	Completed        int64
	LastMod          int64 `bson:"last_mod"`
}

func newJob(job *jobqueue.Job) (*Job, error) {
	var args *string
	if job.Args != nil {
		v, err := json.Marshal(job.Args)
		if err != nil {
			return nil, err
		}
		s := string(v)
		args = &s
	}
	return &Job{
		ID:               job.ID,
		Topic:            job.Topic,
		State:            job.State,
		Args:             args,
		Rank:             job.Rank,
		Priority:         job.Priority,
		Retry:            job.Retry,
		MaxRetry:         job.MaxRetry,
		CorrelationGroup: job.CorrelationGroup,
		CorrelationID:    job.CorrelationID,
		Created:          job.Created,
		Started:          job.Started,
		Completed:        job.Completed,
	}, nil
}

func (j *Job) ToJob() (*jobqueue.Job, error) {
	var args []interface{}
	if j.Args != nil && *j.Args != "" {
		if err := json.Unmarshal([]byte(*j.Args), &args); err != nil {
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
		CorrelationGroup: j.CorrelationGroup,
		CorrelationID:    j.CorrelationID,
		Created:          j.Created,
		Started:          j.Started,
		Completed:        j.Completed,
	}
	return job, nil
}
