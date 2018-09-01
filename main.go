package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/cjkreklow/tcp-relay-pub-vrs/relay"
	"github.com/cjkreklow/tcp-relay-pub-vrs/stream"
	"github.com/dustin/go-humanize"
)

var config = new(struct {
	hostName   string
	portNum    string
	outportNum string
	reconMax   int
	reconDelay time.Duration
})

var exitChannel = make(chan bool)

// initialize command line and exit handling
func init() {
	var rMax = flag.Int("r", 0, "number of reconnect `attempts`")
	var rDelay = flag.Int("d", 10, "reconnect delay (in `seconds`)")
	flag.Parse()

	var args = flag.Args()
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: tcp-relay-pub-vrs <hostname> <hostport> <relayport>")
		os.Exit(1)
	}

	config.hostName = args[0]
	config.portNum = args[1]
	config.outportNum = args[2]
	config.reconMax = *rMax
	config.reconDelay = time.Duration(*rDelay) * time.Second

	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go signalExitWatcher(ch)
}

// main application logic
func main() {
	var err error

	// get relay server
	var server = relay.NewServer(config.outportNum)

	// start stream client
	var client = new(stream.StreamClient)
	client.ReconnectMax = config.reconMax
	client.ReconnectDelay = config.reconDelay
	err = client.Connect(config.hostName, config.portNum)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting upstream client: %s\n", err)
		os.Exit(1)
	}
	fmt.Println("upstream client connected")
	go func() {
		<-exitChannel
		client.Close()
		exit(-1)
	}()

	// start relay server and client receiver, discard messages if queue
	// is full
	err = server.Start(client.Scan(5, true))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting relay server: %s\n", err)
		os.Exit(1)
	}
	fmt.Println("relay server started")
	go func() {
		<-exitChannel
		server.Close()
		exit(-1)
	}()

	// start stat printer
	go runtimeStats(server)

	// wait for relay server to exit for some reason
	server.Wait()
	err = server.Err()
	if err != nil {
		fmt.Fprintf(os.Stderr, "server completed with error: %v\n", err)
	}

	// exit here
	exit(-1)
	os.Exit(exitVal)
}

// background goroutine to watch for OS exit signals and shutdown
func signalExitWatcher(c chan os.Signal) {
	// start graceful shutdown
	select {
	case <-c:
	case <-exitChannel:
	}

	exit(0)

	// second signal forces exit
	<-c
	os.Exit(2)
}

// stat printer
func runtimeStats(server *relay.Server) {
	var m runtime.MemStats
	var stats relay.Statistics
	var statTick = time.NewTicker(5 * time.Second)
	var a, ta uint64
	for {
		select {
		case e := <-server.ClientErrors:
			ce, ok := e.(*relay.ClientError)
			if ok {
				fmt.Fprintf(os.Stderr, "client error: %d %s - %v\n",
					ce.ClientID,
					ce.ClientAddr,
					ce)
			} else {
				fmt.Fprintf(os.Stderr, "client error: %v\n", e)
			}
		case <-statTick.C:
			stats = server.Stats()
			fmt.Println("    =========  ==========  ==========")
			fmt.Printf("    %s  |  Goroutines: %d  |  Client Connections: %d\n",
				config.outportNum, runtime.NumGoroutine(), stats.ClientsActive)
			runtime.ReadMemStats(&m)
			a = m.Mallocs - ta
			ta = m.Mallocs
			fmt.Printf("    Memory Acquired: %s\n", humanize.Bytes(m.Sys))
			fmt.Printf("    Recent Allocs: %d\n", a)
			fmt.Printf("    Last GC: %d\n", m.LastGC)
			fmt.Printf("    Next GC: %s  |  Heap Alloc: %s\n",
				humanize.Bytes(m.NextGC), humanize.Bytes(m.HeapAlloc))
			fmt.Println("    =========  ==========  ==========")
			fmt.Printf("%+v\n", stats)
		}
	}
}

// exitOnce and exitVal should only be used inside the exit function
// below.  This ensures that we're only closing the exit channel once,
// and only the first call to exit sets the exit value.
var exitOnce = new(sync.Once)
var exitVal int

// exit should be called on all exit paths to guarantee that the exit
// channel is closed and all goroutines have a chance to exit cleanly.
// The return value on the first call to exit will be used in the final
// call to os.Exit().
func exit(val int) {
	exitOnce.Do(func() {
		exitVal = val
		close(exitChannel)
		go func() {
			time.Sleep(30 * time.Second)
			fmt.Println("exit timeout")
			os.Exit(-2)
		}()
	})
}
