package relay

import (
	"errors"
	"net"
	"sync"
)

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
	// ClientErrors is a channel to receive client errors for logging
	ClientErrors <-chan error

	port     string
	clients  *clientlist
	exitWg   *sync.WaitGroup
	listener net.Listener
	inchan   chan []byte
	msgs     int
	closed   bool
	err      error
}

// NewServer returns a server configured to listen on the given port.
func NewServer(port string) *Server {
	var errs = make(chan error, 10)
	var cl = clientlist{
		lock:    new(sync.RWMutex),
		wg:      new(sync.WaitGroup),
		clients: make(map[int]*client),
		errchan: errs,
	}
	var s = Server{
		port:         port,
		clients:      &cl,
		exitWg:       new(sync.WaitGroup),
		ClientErrors: errs,
	}
	return &s
}

// Start opens the output port, relaying the messages passed via the
// supplied channel.
func (s *Server) Start(msg chan []byte) error {
	var err error

	if s.closed {
		return errors.New("cannot start closed server")
	}

	if msg == nil {
		return errors.New("invalid message channel")
	}

	s.listener, err = net.Listen("tcp", net.JoinHostPort("", s.port))
	if err != nil {
		return err
	}

	s.inchan = msg

	s.exitWg.Add(2)
	go s.listen()
	go s.send()

	return nil
}

// Close stops all senders and closes all connections
func (s *Server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
	if s.clients != nil {
		s.clients.CloseAll()
	}
	s.closed = true
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
		MessagesQueued: len(s.inchan),
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
			if s.err == nil {
				s.err = err
			}
			return
		}
		c.channel = make(chan []byte, 2)
		s.clients.Add(c)
		go c.start()
	}
}

// send receives messages from the incoming channel and pushes them to
// the client list for broadcast.
func (s *Server) send() {
	defer func() {
		s.Close()
		s.exitWg.Done()
	}()

	for {
		if s.closed {
			return
		}
		// blocking receive, expects channel will close on exit or error
		msg, ok := <-s.inchan
		if !ok {
			if s.err == nil {
				s.err = errors.New("inbound channel closed")
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
	errchan chan<- error
}

// Add locks the list and adds the client, preventing a client from
// being added during an iteration.
func (cl *clientlist) Add(c *client) {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	cl.ctr++
	c.id = cl.ctr
	cl.clients[c.id] = c
	cl.wg.Add(1)
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
	c.conn.Close()
	close(c.channel)
	if cl.errchan != nil && c.err != nil {
		select {
		case cl.errchan <- c.err:
		default:
		}
	}
	cl.wg.Done()
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
			cl.failed++
			continue
		}
		// drop messages to slow or dead receivers instead of blocking
		select {
		case c.channel <- msg:
			c.failcount = 0
		default:
			c.failcount++
			if c.failcount > 10 && c.err == nil {
				c.err = &ClientError{
					errors.New("client exceeded max send failures"),
					c.id,
					c.addr,
				}
			}
		}
	}
}

// CloseAll locks the list and removes all the clients.
func (cl *clientlist) CloseAll() {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	for _, c := range cl.clients {
		delete(cl.clients, c.id)
		c.conn.Close()
		close(c.channel)
		cl.wg.Done()
	}
}

// Wait blocks until all clients have been removed from the list.
func (cl *clientlist) Wait() {
	cl.wg.Wait()
}

// Active returns the count of active clients.
func (cl *clientlist) Active() int {
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
	id        int
	addr      string
	conn      net.Conn
	channel   chan []byte
	failcount int
	err       *ClientError
}

// start listens for incoming messages and writes them to the network
// connection.
func (c *client) start() {
	defer func() {
		if c.err == nil {
			c.err = &ClientError{
				errors.New("unknown client error"),
				c.id,
				c.addr,
			}
		}
	}()
	for {
		select {
		case msg, ok := <-c.channel:
			if !ok {
				if c.err == nil {
					c.err = &ClientError{
						errors.New("client channel closed"),
						c.id,
						c.addr,
					}
				}
				return
			}
			_, err := c.conn.Write(msg)
			if err != nil {
				if e, ok := err.(*net.OpError); ok {
					if e.Temporary() {
						continue
					}
				}
				if c.err == nil {
					c.err = &ClientError{
						err,
						c.id,
						c.addr,
					}
				}
				return
			}
		}
	}
}

// ClientError is an error returned by a client connection
type ClientError struct {
	error

	// ClientID is the internal client identifier
	ClientID int

	// ClientAddr is the remote client address
	ClientAddr string
}
