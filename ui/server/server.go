// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package server

import (
	"net/http"
	"time"

	"github.com/olivere/jobqueue"
)

// Server is a simple web server with a WebSocket backend.
type Server struct {
	m *jobqueue.Manager
}

// New initializes a new Server.
func New(m *jobqueue.Manager) *Server {
	return &Server{
		m: m,
	}
}

// Serve initializes the mux and starts the web server at the given address.
func (srv *Server) Serve(addr string) error {
	r := http.DefaultServeMux
	r.Handle("/ws", wsserver{m: srv.m})
	r.Handle("/", http.FileServer(http.Dir("public")))
	StateUpdates = make(chan *State)
	defer close(StateUpdates)
	go watcher(srv.m)
	go h.run() // run websocket hub
	return http.ListenAndServe(addr, r)
}

// State is the current state of the job queue.
type State struct {
	Type      string          `json:"type"`
	Stats     *jobqueue.Stats `json:"stats,omitempty"`
	Waiting   []*jobqueue.Job `json:"waiting,omitempty"`
	Working   []*jobqueue.Job `json:"working,omitempty"`
	Succeeded []*jobqueue.Job `json:"succeeded,omitempty"`
	Failed    []*jobqueue.Job `json:"failed,omitempty"`
}

var StateUpdates chan *State

func watcher(m *jobqueue.Manager) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			newState := &State{Type: "SET_STATE"}
			stats, err := m.Stats()
			if err != nil {
				panic(err)
			}
			newState.Stats = stats
			rsp, err := m.List(&jobqueue.ListRequest{State: jobqueue.Waiting})
			if err != nil {
				panic(err)
			}
			newState.Waiting = rsp.Jobs
			rsp, err = m.List(&jobqueue.ListRequest{State: jobqueue.Working})
			if err != nil {
				panic(err)
			}
			newState.Working = rsp.Jobs
			rsp, err = m.List(&jobqueue.ListRequest{State: jobqueue.Succeeded, Limit: 10})
			if err != nil {
				panic(err)
			}
			newState.Succeeded = rsp.Jobs
			rsp, err = m.List(&jobqueue.ListRequest{State: jobqueue.Failed, Limit: 10})
			if err != nil {
				panic(err)
			}
			newState.Failed = rsp.Jobs
			StateUpdates <- newState
		}
	}
}
