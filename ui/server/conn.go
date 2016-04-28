// Portions of this code are:
// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package server

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/olivere/jobqueue"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// connection is an middleman between the websocket connection and the hub.
type connection struct {
	// The websocket connection.
	ws *websocket.Conn
	// Buffered channel of outbound messages.
	send chan []byte
	m    *jobqueue.Manager
}

// readPump pumps messages from the websocket connection to the hub.
func (c *connection) readPump() {
	defer func() {
		h.unregister <- c
		c.ws.Close()
	}()
	c.ws.SetReadLimit(maxMessageSize)
	c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error { c.ws.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		var msg struct {
			Type string `json:"type"`
			ID   string `json:"id"`
		}
		err := c.ws.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
				log.Printf("%v", err)
			}
			break
		}
		switch msg.Type {
		case "JOB_LOOKUP":
			var rsp struct {
				Type    string        `json:"type"`
				Message string        `json:"message,omitempty"`
				Job     *jobqueue.Job `json:"job,omitempty"`
			}
			rsp.Type = "JOB_LOOKUP"
			t, err := c.m.Lookup(msg.ID)
			if err != nil {
				if err == jobqueue.ErrNotFound {
					rsp.Message = "Job already removed"
				} else {
					rsp.Message = "Job cannot be found"
				}
			} else {
				rsp.Job = t
			}
			payload, _ := json.Marshal(rsp)
			h.broadcast <- payload
		}
	}
}

// write writes a message with the given message type and payload.
func (c *connection) write(mt int, payload []byte) error {
	c.ws.SetWriteDeadline(time.Now().Add(writeWait))
	return c.ws.WriteMessage(mt, payload)
}

// writePump pumps messages from the hub to the websocket connection.
func (c *connection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.ws.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				c.write(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.write(websocket.TextMessage, message); err != nil {
				return
			}
		case e, ok := <-StateUpdates:
			if !ok {
				c.write(websocket.CloseMessage, []byte{})
				return
			}
			v, err := json.Marshal(e)
			if err != nil {
				log.Printf("%v", err)
				return
			}
			if err := c.write(websocket.TextMessage, v); err != nil {
				return
			}
		}
	}
}

type wsserver struct {
	m *jobqueue.Manager
}

// ServeHTTP handles websocket requests from the peer.
func (srv wsserver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("%v", err)
		return
	}
	c := &connection{send: make(chan []byte, 256), ws: ws, m: srv.m}
	h.register <- c
	go c.writePump()
	c.readPump()
}
