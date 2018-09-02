// Copyright 2018 Collin Kreklow
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
// BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
// ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package relay

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"strings"
	"sync"
)

var logger *log.Logger

func init() {
	logger = log.New(ioutil.Discard, "relay", log.LstdFlags)
}

var errArg = errors.New("invalid argument")
var errChan = errors.New("message channel closed")
var errClosed = errors.New("invalid operation")
var errSend = errors.New("send to client failed")
var errUnknown = errors.New("unknown error")

// SetLogOutput sets the output destination for log messages. By default
// logs are discarded.
func SetLogOutput(w io.Writer) {
	logger.SetOutput(w)
}

// Statistics is a reporting object for a relay server.
type Statistics struct {
	Port           string
	ClientsActive  int
	ClientsFailed  int
	ClientsTotal   int
	MessagesSent   int
	MessagesQueued int
	Closed         bool
	Err            error
}

// Server is a TCP message relay server.
type Server struct {
	port     string
	clients  *clientlist
	exitWg   *sync.WaitGroup
	closeWg  *sync.WaitGroup
	listener net.Listener
	msgChan  chan []byte
	msgs     int
	closed   bool
	err      error
}

// NewServer returns a server configured to listen on the given port.
func NewServer(port string) *Server {
	var cl = clientlist{
		lock:    new(sync.RWMutex),
		wg:      new(sync.WaitGroup),
		clients: make(map[int]*client),
	}
	var s = Server{
		port:    port,
		clients: &cl,
		exitWg:  new(sync.WaitGroup),
		closeWg: new(sync.WaitGroup),
	}
	return &s
}

// Start opens the output port, relaying the messages passed via the
// supplied channel.
func (s *Server) Start(msgChan chan []byte) error {
	var err error

	if s.closed {
		return errClosed
	}

	if msgChan == nil {
		return errArg
	}

	s.listener, err = net.Listen("tcp", net.JoinHostPort("", s.port))
	if err != nil {
		return err
	}

	s.msgChan = msgChan

	s.exitWg.Add(2)
	s.closeWg.Add(2)
	go s.listen()
	go s.send()

	return nil
}

// Close stops all senders and closes all connections
func (s *Server) Close() {
	s.exitWg.Add(1)
	defer s.exitWg.Done()
	s.closed = true

	if s.listener != nil {
		s.listener.Close()
	}
	if s.clients != nil {
		s.clients.CloseAll()
	}
	s.closeWg.Wait()
}

// Wait waits for the server and all clients to complete before
// returning.
func (s *Server) Wait() {
	s.exitWg.Wait()
	s.clients.Wait()
}

// Stats returns a Statistics object.
func (s *Server) Stats() Statistics {
	return Statistics{
		Port:           s.port,
		ClientsActive:  s.clients.Active(),
		ClientsFailed:  s.clients.Failed(),
		ClientsTotal:   s.clients.Total(),
		MessagesSent:   s.msgs,
		MessagesQueued: len(s.msgChan),
		Closed:         s.closed,
		Err:            s.err,
	}
}

// Err returns any server errors.
func (s *Server) Err() error {
	return s.err
}

// listen accepts client connections, creating client objects and adding
// them to the client list
func (s *Server) listen() {
	var err error

	defer func() {
		s.closeWg.Done()
		s.Close()
		s.exitWg.Done()
	}()

	for {
		if s.closed {
			return
		}

		var c = new(client)
		c.conn, err = s.listener.Accept()
		if err != nil {
			if s.err == nil && !strings.Contains(err.Error(),
				"use of closed network connection") {
				s.err = err
			}
			return
		}
		if tc, ok := c.conn.(*net.TCPConn); ok {
			err = tc.SetLinger(0)
			if err != nil {
				if s.err == nil {
					s.err = err
				}
				return
			}
		}
		c.channel = make(chan []byte, 2)
		var wg = s.clients.Add(c)
		wg.Add(1)
		go c.start(wg)
	}
}

// send receives messages from the incoming channel and pushes them to
// the client list for broadcast.
func (s *Server) send() {
	defer func() {
		s.closeWg.Done()
		s.Close()
		s.exitWg.Done()
	}()

	for {
		if s.closed {
			return
		}
		// blocking receive, expects channel will close on exit or error
		msg, ok := <-s.msgChan
		if !ok {
			if s.err == nil {
				s.err = errChan
			}
			return
		}
		s.clients.Send(msg)
		s.msgs++
	}
}

// clientlist manages client connections.
type clientlist struct {
	lock    *sync.RWMutex
	wg      *sync.WaitGroup
	clients map[int]*client
	ctr     int
	failed  int
}

// Add locks the list and adds the client, preventing a client from
// being added during an iteration.
func (cl *clientlist) Add(c *client) *sync.WaitGroup {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	cl.ctr++
	c.id = cl.ctr
	cl.clients[c.id] = c
	log.Printf("new client %d: %s\n", c.id, c.conn.RemoteAddr().String())
	return cl.wg
}

// Remove locks the list and removes the client, preventing a client
// from being removed during an iteration.
func (cl *clientlist) Remove(id int) {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	c, ok := cl.clients[id]
	if !ok {
		return
	}
	delete(cl.clients, id)
	close(c.channel)
	log.Printf("removed client %d: %s\n", c.id, c.err)
}

// Send takes a read lock on the list and iterates over it, sending the
// message passed to all clients. A client will be removed if they have
// marked themselves failed, or have failed 10 send requests in a row.
func (cl *clientlist) Send(msg []byte) {
	cl.lock.RLock()
	defer cl.lock.RUnlock()
	for _, c := range cl.clients {
		if c.err != nil {
			// since Remove requires a write lock, we must spawn this as
			// a goroutine or we'll deadlock
			go cl.Remove(c.id)
			continue
		}
		// drop messages to slow or dead receivers instead of blocking
		select {
		case c.channel <- msg:
			if c.failcount > 0 {
				c.totalfails++
				c.failcount = 0
			}
		default:
			c.failcount++
		}
		if (c.failcount >= 10 || c.totalfails >= 10) && c.err == nil {
			cl.failed++
			c.err = errSend
		}
	}
}

// CloseAll locks the list and removes all the clients.
func (cl *clientlist) CloseAll() {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	for _, c := range cl.clients {
		delete(cl.clients, c.id)
		close(c.channel)
	}
}

// Wait blocks until all clients have been removed from the list.
func (cl *clientlist) Wait() {
	cl.wg.Wait()
}

// Active returns the count of active clients.
func (cl *clientlist) Active() int {
	cl.lock.RLock()
	defer cl.lock.RUnlock()
	return len(cl.clients)
}

// Failed returns the count of failed clients.
func (cl *clientlist) Failed() int {
	return cl.failed
}

// Total returns the count of total clients seen.
func (cl *clientlist) Total() int {
	return cl.ctr
}

// client manages a client connection.
type client struct {
	id         int
	conn       net.Conn
	channel    chan []byte
	failcount  int
	totalfails int
	err        error
}

// start listens for incoming messages and writes them to the network
// connection.
func (c *client) start(wg *sync.WaitGroup) {
	defer func() {
		c.conn.Close()
		if c.err == nil {
			c.err = errUnknown
		}
		wg.Done()
	}()
	for {
		msg, ok := <-c.channel
		if !ok {
			if c.err == nil {
				c.err = errChan
			}
			return
		}
		_, err := c.conn.Write(msg)
		if err != nil {
			ne, ok := err.(net.Error)
			if ok && ne.Temporary() {
				continue
			}
			if c.err == nil {
				c.err = err
			}
			return
		}
	}
}
